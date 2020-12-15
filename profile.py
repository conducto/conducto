import os
import sys
import time
import json
import socket
import subprocess
import urllib.error
import conducto as co
import jose
from conducto.shared import constants, log, agent_utils, container_utils
from . import api


def _enrich_profile(profile, data):
    # this writes the profile
    auth_api = api.Auth()
    auth_api.url = data["url"]

    dir_api = api.Dir()
    dir_api.url = data["url"]

    try:
        token = auth_api.get_refreshed_token(data["token"])
        org = dir_api.org(data["org_id"], token=token)
    except (jose.exceptions.JWTError, KeyError):
        data["org_name"] = log.format("Missing/invalid token", color="red", bold=False)
    except api.UnauthorizedResponse:
        data["org_name"] = log.format(
            "Unauthorized token, cannot fetch org details", color="red", bold=False
        )
    except urllib.error.URLError:
        data["org_name"] = log.format(
            "Invalid URL, cannot fetch org details", color="red", bold=False
        )
    except (api.InvalidResponse, api.UnauthorizedResponse, PermissionError):
        data["org_name"] = log.format(
            "Invalid token, cannot fetch org details", color="red", bold=False
        )
    else:
        data["org_name"] = org["name"]


def _print_profile(profile, data):
    _enrich_profile(profile, data)

    defstr = ""
    if data["default"]:
        default = log.format("default", color=log.Color.GREEN)
        defstr = " " + default
    elif data.get("dir-default", False):
        default = log.format("directory default", color=log.Color.GREEN)
        defstr = " " + default

    print(f"Profile {profile}{defstr}")
    print(f"\tURL:  {data['url']}")
    print(f"\tOrganization:  {data['org_name']}")
    print(f"\te-mail:  {data['email']}")
    print()


def profile_list():
    """
    Show conducto log-in profiles recognized on this computer.
    """
    conf = api.Config()

    for profile in conf.profile_sections():
        data = conf._profile_general(profile)

        try:
            _print_profile(profile, data)
        except KeyError:
            print(
                log.format(
                    f"Invalid or incomplete profile '{profile}'",
                    color="red",
                    bold=False,
                )
            )


def profile_set_default(id):
    """
    Update the default log-in profile used for running a pipeline.
    """
    config = api.Config()
    profiles = list(config.profile_sections())

    if id not in profiles:
        print(f"The profile {id} was not found.", file=sys.stderr)
        sys.exit(1)
    else:
        config.set("general", "default", id)


def _profile_add(url, default):
    # set global url for config object and clear profile to avoid clobbering
    # the default
    if not url.startswith("https://"):
        raise api.UserInputValidation(
            f"The URL {url} is not recognized as a valid Conducto server environment"
        )

    os.environ["CONDUCTO_URL"] = url.rstrip("/")
    os.environ["CONDUCTO_PROFILE"] = "__none__"

    # I want to explicitly drive the profile process here
    token = api.Config().get_token_from_shell(force_new=True, skip_profile=True)

    config = api.Config()
    config.default_profile = None
    # This may overwrite an existing profile; this depends on the credentials
    # entered.
    profile = config.write_profile(config.get_url(), token, default="first")

    if default:
        profile_set_default(profile)
    else:
        print("To set this profile as the default for command line usage:")
        print(f"conducto-profile set-default {profile}")
    return profile


def profile_add(url, default=False, token=None):
    """
    Add a new log-in profile for a specified Conducto URL.
    """
    if token is not None:
        os.environ["CONDUCTO_TOKEN"] = token

    profile = _profile_add(url, default=default)

    conf = api.Config()
    data = conf._profile_general(profile)
    _print_profile(profile, data)


def profile_delete(id=None, url=None, email=None, force=False):
    """
    Delete a log-in profile on this computer selected by id, url or email.
    """
    if id is None and url is None and email is None:
        print(
            "You must include at least one criterion (id, url, email)", file=sys.stderr
        )
        sys.exit(1)

    criteria = {"profile": id, "url": url, "email": email}
    criteria = {k: v for k, v in criteria.items() if v is not None}

    matches = []

    conf = api.Config()
    for profile in conf.profile_sections():
        data = conf._profile_general(profile)
        data["profile"] = profile

        match = all(data[k] == v for k, v in criteria.items())

        if match:
            matches.append(data)

    if len(matches) == 0:
        print("No profile matches the criteria.")
    elif len(matches) > 1:
        print("Multiple matches, specify the id.")
        for prof in matches:
            _print_profile(prof["profile"], prof)
    else:
        # Exactly one, delete it
        state = matches[0]
        os.environ["CONDUCTO_PROFILE"] = state["profile"]

        def containers_by_profile(profile):
            docker_ps = [
                "docker",
                "ps",
                "--all",
                "--no-trunc",
                "--filter",
                f"label=com.conducto.profile={profile}",
                "--format",
                "{{ json . }}",
            ]

            proc = subprocess.run(docker_ps, check=True, stdout=subprocess.PIPE)
            output = proc.stdout.decode("utf8")

            managers = []
            # should be 0 or 1
            agents = []

            for json_line in output.split("\n"):
                if json_line.strip() == "":
                    continue
                container = json.loads(json_line)

                if container["Names"].startswith("conducto_manager_"):
                    managers.append(container)
                elif container["Names"].startswith("conducto_agent_"):
                    agents.append(container)

            return managers, agents

        managers, agents = containers_by_profile(state["profile"])

        count_running = len(managers)

        profdir = constants.ConductoPaths.get_profile_base_dir(profile=state["profile"])
        pipedir = os.path.join(profdir, "pipelines")

        if os.path.exists(pipedir):
            local_pipelines = os.listdir(pipedir)
        else:
            local_pipelines = []

        if (len(local_pipelines) == 0 and count_running == 0) or force:
            response = "y"
        else:
            _enrich_profile(profile, state)

            msgs = []
            if count_running > 0:
                plural = "s" if count_running > 1 else ""
                msgs.append(f"{count_running} running manager{plural}")

            if len(local_pipelines) > 0:
                plural = "s" if len(local_pipelines) > 1 else ""
                msgs.append(f"{len(local_pipelines)} local pipeline{plural}")

            print(
                f"There are {' and '.join(msgs)} in the profile for {state['org_name']}. "
                "Deleting removes the serialized pipeline structure."
            )
            response = input("Are you sure you want to delete them? [yn] ")

        if response.lower() in ("y", "yes"):
            # sleep managers
            for manager in managers:
                docker_stop = ["docker", "stop", manager["ID"]]
                subprocess.run(docker_stop, check=True, stdout=subprocess.PIPE)

            # iterate until stopped
            while True:
                managers, _ = containers_by_profile(state["profile"])
                if len(managers) > 0:
                    time.sleep(1)
                else:
                    break

            # stop agent (should be at most one, but the loop is safe and
            # easy)
            for agent in agents:
                docker_stop = ["docker", "stop", agent["ID"]]
                subprocess.run(docker_stop, check=True, stdout=subprocess.PIPE)

            # iterate until stopped
            for _ in range(20):
                _, agents = containers_by_profile(state["profile"])
                if len(agents) > 0:
                    time.sleep(1)
                else:
                    break

            # delete pipelines - this step is imperfect, but it is written here
            # to avoid deleting pipelines on the server which may be in my
            # local dir which really shouldn't be. Of course this process is
            # imperfect and cannot be perfect since there is no obligation to
            # run conducto-profile delete before removing your entire home dir
            # (for instance).

            # allow default of empty list
            try:
                pipe_api = api.Pipeline()
                org_pipelines = pipe_api.list()
            except urllib.error.URLError:
                print(
                    "Warning: could not connect to Conducto servers to delete programs; they will be deleted after their retention period expires.",
                    file=sys.stderr,
                )
            except (api.InvalidResponse, api.UnauthorizedResponse, PermissionError):
                print(
                    "Warning: unauthorized connecting to Conducto servers to delete programs; they will be deleted after their retention period expires.",
                    file=sys.stderr,
                )
            except Exception as e:
                print(
                    f"Warning: unknown error connecting to Conducto servers to delete programs; {str(e)}",
                    file=sys.stderr,
                )
            else:
                hostname = socket.gethostname()
                pl = constants.PipelineLifecycle
                for pipedata in org_pipelines:
                    meta_host = pipedata.get("meta", {}).get("hostname", None)
                    is_my_local = (
                        pipedata["status"] in pl.local
                        and pipedata["pipeline_id"] in local_pipelines
                        and meta_host == hostname
                    )
                    if is_my_local:
                        log.debug(f"archiving {pipedata['pipeline_id']}")
                        pipe_api.archive(pipedata["pipeline_id"])

            # delete data
            conf.delete_profile(state["profile"])


def profile_start_agent(id=None):
    """
    Start the local agent for the default or specified profile.
    """
    if id is not None:
        os.environ["CONDUCTO_PROFILE"] = id

    token = api.Config().get_token_from_shell()

    start_status = agent_utils.launch_agent(token=token)

    if start_status.startswith("running"):
        print(f"Agent for profile {api.Config().default_profile} is already running")
    else:
        print(f"Agent launched for profile {api.Config().default_profile}")

    config = api.Config()
    if config.default_profile != config.get("general", "default"):
        print("To set this profile as the default for command line usage:")
        print(f"conducto-profile set-default {config.default_profile}")


def profile_stop_agent(id=None):
    """
    Stop the local agent for the default or specified profile.
    """
    if id is not None:
        os.environ["CONDUCTO_PROFILE"] = id

    container_name = agent_utils.name()

    running = container_utils.get_running_containers()
    if f"{container_name}-old" in running:
        cmd = ["docker", "stop", f"{container_name}-old"]
        subprocess.run(cmd, stdout=subprocess.PIPE)
    if container_name in running:
        cmd = ["docker", "stop", container_name]
        subprocess.run(cmd, stdout=subprocess.PIPE)
    else:
        config = api.Config()
        print(f"No agent running for profile {config.default_profile}")


def main():
    variables = {
        "list": profile_list,
        "set-default": profile_set_default,
        "add": profile_add,
        "delete": profile_delete,
        "start-agent": profile_start_agent,
        "stop-agent": profile_stop_agent,
    }
    co.main(variables=variables)
