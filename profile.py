import os
import sys
import time
import json
import socket
import subprocess
import urllib.error
import conducto as co
import conducto.api.config as config
from conducto.shared import constants, log, local_daemon_utils, container_utils
from . import api


def _enrich_profile(profile, data):
    # this writes the profile
    auth_api = api.Auth()
    auth_api.url = data["url"]

    token = data["token"]

    try:
        token = auth_api.get_refreshed_token(token)
    except api.UnauthorizedResponse:
        data["org_name"] = log.format(
            "Unauthorized token, cannot fetch org details", color="red", bold=False
        )
    except urllib.error.URLError:
        data["org_name"] = log.format(
            "Invalid URL, cannot fetch org details", color="red", bold=False
        )
    except api.InvalidResponse:
        data["org_name"] = log.format(
            "Invalid token, cannot fetch org details", color="red", bold=False
        )
    else:
        dir_api = api.Dir()
        dir_api.url = data["url"]

        org = dir_api.org(token, data["org_id"])
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

    mydir = os.getcwd()
    dirsettings = config.dirconfig_detect(mydir)
    dirprofile = dirsettings["profile-id"] if dirsettings else None

    for profile in conf.profile_sections():
        data = conf._profile_general(profile)

        data["dir-default"] = profile == dirprofile

        _print_profile(profile, data)


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
    os.environ["CONDUCTO_URL"] = url

    # this writes the profile
    token = api.Auth().get_token_from_shell(force=True)

    config = api.Config()
    for profile in config.profile_sections():
        if config.get_profile_general(profile, "token") == token:
            break
    else:
        raise Exception("Somehow there's no matching profile though we just made one.")

    if default:
        profile_set_default(profile)
    return profile


def profile_add(url, default=False):
    """
    Add a new log-in profile for a specified Conducto URL.
    """
    profile = _profile_add(url, default=default)

    conf = api.Config()
    data = conf._profile_general(profile)
    _print_profile(profile, data)


def profile_delete(id=None, url=None, email=None, force=False):
    """
    Delete a log-in profile on this computer selected by id, url or email.
    """
    # TODO:  validate that no manager is running when you delete the profile
    # because that would just leave a rubbish partial profile on the user's
    # computer.

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
            daemons = []

            for json_line in output.split("\n"):
                if json_line.strip() == "":
                    continue
                container = json.loads(json_line)

                if container["Names"].startswith("conducto_manager_"):
                    managers.append(container)
                elif container["Names"].startswith("conducto_daemon_"):
                    daemons.append(container)

            return managers, daemons

        managers, daemons = containers_by_profile(state["profile"])

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

            # stop daemon (should be at most one, but the loop is safe and
            # easy)
            for daemon in daemons:
                docker_stop = ["docker", "stop", daemon["ID"]]
                subprocess.run(docker_stop, check=True, stdout=subprocess.PIPE)

            # iterate until stopped
            for _ in range(20):
                _, daemons = containers_by_profile(state["profile"])
                if len(daemons) > 0:
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
            org_pipelines = []
            try:
                token = api.Config().get_token()
                token = api.Auth().get_refreshed_token(token)
                pipe_api = api.Pipeline()
                org_pipelines = pipe_api.list(token)
            except urllib.error.URLError:
                print(
                    "Warning: could not connect to Conducto servers to delete programs; they will be deleted after their retention period expires.",
                    file=sys.stderr,
                )
            except api.InvalidResponse:
                print(
                    "Warning: unauthorized connecting to Conducto servers to delete programs; they will be deleted after their retention period expires.",
                    file=sys.stderr,
                )

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
                    pipe_api.archive(token, pipedata["pipeline_id"])

            # delete data
            conf.delete_profile(state["profile"])


def dir_init(dir: str = ".", url: str = None, name: str = None):
    from . import api

    if url is None:
        url = "https://conducto.com"
    else:
        if not api.is_conducto_url(url):
            print(f"The url {url} is not recognized.", file=sys.stderr)
            sys.exit(1)

    dir = os.path.abspath(dir)

    if not os.path.isdir(dir):
        print(f"'{dir}' is not a directory or does not exist", file=sys.stderr)
        sys.exit(1)

    config = api.Config()
    for profile in config.profile_sections():
        if config.get_profile_general(profile, "url") == url:
            break
    else:
        profile = None

    create_new = True
    if profile is not None:
        # we already have a profile for this url, let's see what the intent is.

        profile_email = config.get_profile_general(profile, "email")
        email = os.environ.get("CONDUCTO_EMAIL")

        if email == profile_email:
            print(f"There is already a profile for {url} and e-mail {email}.")
            question = "Do you wish to connect this directory to this profile? [yn] "
            choice = input(question)

            if choice.lower()[0] == "y":
                # connect dir to this profile
                create_new = False

    if create_new:
        profile = _profile_add(url, default=False)

    profdata = config._profile_general(profile)
    api.dirconfig_write(dir, profdata["url"], profdata["org_id"], name=name)
    if name is not None:
        config.register_named_mount(profile, name, dir)


def profile_start_daemon(id=None):
    """
    Start the local daemon for the default or specified profile.
    """
    if id is not None:
        os.environ["CONDUCTO_PROFILE"] = id

    config = api.Config()

    local_daemon_utils.launch_local_daemon(config.get_token())


def profile_stop_daemon(id=None):
    """
    Stop the local daemon for the default or specified profile.
    """
    if id is not None:
        os.environ["CONDUCTO_PROFILE"] = id

    container_name = local_daemon_utils.name()

    running = container_utils.get_running_containers()
    if f"{container_name}-old" in running:
        cmd = ["docker", "stop", f"{container_name}-old"]
        subprocess.run(cmd, stdout=subprocess.PIPE)
    if container_name in running:
        cmd = ["docker", "stop", container_name]
        subprocess.run(cmd, stdout=subprocess.PIPE)
    else:
        config = api.Config()
        print(f"No daemon running for profile {config.default_profile}")


def main():
    variables = {
        "list": profile_list,
        "set-default": profile_set_default,
        "add": profile_add,
        "delete": profile_delete,
        "start-daemon": profile_start_daemon,
        "stop-daemon": profile_stop_daemon,
    }
    co.main(variables=variables)
