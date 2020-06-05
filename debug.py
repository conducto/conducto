import asyncio
import socket
import functools
import os
import json
import sys
import re
import subprocess
import time
import tarfile
import io
from conducto.shared.log import format
from conducto.shared import constants
import conducto.internal.host_detection as hostdet
import conducto.image as image_mod

NULL = subprocess.DEVNULL
PIPE = subprocess.PIPE


# Several debug modes:
# - "copy": shows command + environment variables, as it appears
#   inside the container
# - "login": running this container locally with all the
#   env set up.
# - "run": running this container, with all env, and running the
#   node's command as well
# - "liverun": only available if using co.Image(root="..."), similar to run
#   except that it bind mounts your root directory inside the image so that
#   you get your latest code.
# - "debug": similar to run except that instead of starting the docker shell
#   and running the command, it actually just prints the command and runs
#   bash so that you can modify it yourself prior to running.
#   TODO: put the command in the history using `history -s`, but not sure how to do that
# - "livedebug": livedebug is to debug as liverun is to live.


async def get_exec_node_queue_stats(token, id, node, timestamp=None):
    async def _internal():
        assert type(node) == str
        from . import api
        import json

        conn = await api.connect_to_pipeline(token, id)
        try:
            await conn.send(
                json.dumps(
                    {
                        "type": "RENDER_NODE_DETAILS",
                        "payload": {
                            "node": node,
                            "includeHiddenEnv": True,
                            **({"base": timestamp} if timestamp is not None else {}),
                        },
                    }
                )
            )
            async for msg_txt in conn:
                msg = json.loads(msg_txt)
                if msg["type"] == "RENDER_NODE_DETAILS":
                    payload = msg["payload"]
                    break
            else:
                raise Exception("Connection closed before receiving node info")
        finally:
            await conn.close()
        if "queueStatsCurrent" not in payload:
            raise Exception(f"Node {node} is not exec")
        payload["queueStats"].append(["commandSummary", payload["commandSummary"]])
        return payload["queueStats"]

    try:
        return await asyncio.wait_for(_internal(), 15)
    except asyncio.TimeoutError:
        raise Exception(f"Timed out getting node details. Are {id} and {node} correct?")


def get_param(payload, param, default=None):
    for name, val in payload:
        if name == param:
            return val
    return default


def start_container(payload, live):
    import random

    image = get_param(payload, "image", default={})
    image_name = image["name_debug"]

    if live and not image.get("path_map"):
        raise ValueError(
            f"Cannot do livedebug for image {image['name']} because it does not have a `copy_dir` or `path_map`"
        )

    container_name = "conducto_debug_" + str(random.randrange(1 << 64))
    print("Launching docker container...")
    if live:
        print("Context will be mounted read-write")
        print(
            "Make modifications on your local  machine, "
            "and they will be reflected in the container."
        )

    options = []
    if get_param(payload, "requires_docker"):
        options.append("-v /var/run/docker.sock:/var/run/docker.sock")

    options.append(f'--cpus {get_param(payload, "cpu")}')
    options.append(f'--memory {get_param(payload, "mem") * 1024**3}')

    from . import api

    # TODO: Should actually pass these variables from manager, iff local
    local_basedir = constants.ConductoPaths.get_profile_base_dir()
    if hostdet.is_wsl():
        local_basedir = os.path.realpath(local_basedir)
        local_basedir = hostdet.wsl_host_docker_path(local_basedir)
    elif hostdet.is_windows():
        local_basedir = hostdet.windows_docker_path(local_basedir)

    profile = api.Config().default_profile
    remote_basedir = f"{get_home_dir_for_image(image_name)}/.conducto/{profile}"
    options.append(f"-v {local_basedir}:{remote_basedir}")

    if live:
        for external, internal in image["path_map"].items():
            external = image_mod.resolve_registered_path(external)

            if not os.path.isabs(internal):
                internal = get_work_dir_for_image(image_name) + "/" + internal
            options.append(f"-v {external}:{internal}")

    command = f"docker run {' '.join(options)} --name={container_name} {image_name} tail -f /dev/null "

    subprocess.Popen(command, shell=True)
    time.sleep(1)
    return container_name


def dump_command(container_name, command, live):
    # Create in-memory tar file since docker will take that on stdin with no
    # tempfile needed.
    tario = io.BytesIO()
    with tarfile.TarFile(fileobj=tario, mode="w") as cmdtar:
        content = command
        if not content.endswith("\n"):
            content += "\n"
        tfile = tarfile.TarInfo("cmd.conducto")
        tfile.size = len(content)
        cmdtar.addfile(tfile, io.BytesIO(content.encode("utf-8")))

    args = ["docker", "cp", "-", f"{container_name}:/"]
    proc = subprocess.run(args, input=tario.getvalue(), stdout=PIPE, stderr=PIPE)
    if proc.returncode != 0:
        stderr = proc.stderr.decode("utf-8").strip()
        raise RuntimeError(f"error placing cmd.conducto: ({stderr})")

    execute_in(container_name, f"chmod u+x /cmd.conducto")
    print(f"Execute command by running {format('sh /cmd.conducto', color='cyan')}")


@functools.lru_cache()
def get_image_inspection(image_name):
    proc = subprocess.run(
        ["docker", "image", "inspect", image_name], stderr=PIPE, stdout=PIPE,
    )
    return proc.stdout.decode().strip()


@functools.lru_cache()
def get_work_dir_for_image(image_name):
    output = subprocess.check_output(
        ["docker", "image", "inspect", "--format", "{{.Config.WorkingDir}}", image_name]
    )
    return output.decode().strip()


@functools.lru_cache()
def get_work_dir_for_container(container_name):
    output = subprocess.check_output(
        [
            "docker",
            "container",
            "inspect",
            "--format",
            "{{.Config.WorkingDir}}",
            container_name,
        ]
    )
    return output.decode().strip()


def get_home_dir_for_image(image_name):
    output = subprocess.check_output(
        ["docker", "run", "--rm", "-it", image_name, "sh", "-c", "cd ~; pwd"]
    )
    return output.decode().strip()


def get_linux_flavor(container_name):
    cmd = 'sh -c "cat /etc/*-release | grep ^ID="'
    output = execute_in(container_name, cmd).decode("utf8").strip()

    flavor = re.search(r"^ID=(.*)", output, re.M)
    flavor = flavor.groups()[0].strip().strip('"')

    if "ubuntu" in flavor or "debian" in flavor:
        return "debian"
    elif "alpine" in flavor:
        return "alpine"
    else:
        return f"unknown - {flavor}"


def execute_in(container_name, command):
    return subprocess.check_output(
        f"docker exec {container_name} {command}", shell=True
    )


def print_editor_commands(container_name):
    flavor = get_linux_flavor(container_name)
    if flavor == "ubuntu":
        uid_str = execute_in(container_name, "id -u")
        base = "apt-get update"
        each = "apt-get install"
        if int(uid_str) != 0:
            base = f"sudo {base}"
            each = f"sudo {each}"
    elif flavor == "alpine":
        base = "apk update"
        each = "apk add"
    else:
        return

    editors = {"vim": "blue", "emacs": "cyan", "nano": "green"}
    print("To install an editor, run:", end="")
    sep = ""
    for editor, color in editors.items():
        print(sep, format(f"{base} && {each} {editor}", color=color), end="")
        sep = " or"
    print()


def start_shell(container_name, env_list):
    cmd = ["docker", "exec", "-it", *env_list, container_name]
    subp = subprocess.Popen(
        ["docker", "exec", container_name, "/bin/bash"], stdout=NULL, stderr=NULL
    )
    subp.wait()
    has_bash = subp.returncode == 0
    if has_bash:
        subprocess.call(cmd + ["/bin/bash"])
    else:
        subprocess.call(cmd + ["/bin/sh"])
    subprocess.call(["docker", "kill", container_name])
    subprocess.call(["docker", "rm", container_name], stdout=NULL, stderr=NULL)


async def _debug(id, node, live, timestamp):
    from . import api

    pl = constants.PipelineLifecycle

    pipeline_id = id
    token = api.Auth().get_token_from_shell(force=True)
    try:
        pipeline = api.Pipeline().get(token, pipeline_id)
    except api.InvalidResponse as e:
        if "not found" in str(e):
            print(str(e), file=sys.stderr)
            sys.exit(1)
        else:
            raise

    status = pipeline["status"]

    if status not in pl.active | pl.standby:
        print(
            "The pipeline {pipeline_id} is sleeping.  Wake with conducto show and retry this command to debug.",
            file=sys.stderr,
        )
        sys.exit(1)

    if status in pl.standby:
        api.Manager().launch(token, pipeline_id)
        pipeline = api.Pipeline().get(token, pipeline_id)

    payload = await get_exec_node_queue_stats(token, pipeline_id, node, timestamp)

    if status in pl.local:
        image = get_param(payload, "image", default={})
        image_name = image["name_debug"]
        inspect_json = get_image_inspection(image_name)
        inspect = json.loads(inspect_json)
        if len(inspect) == 0:
            m = f"The container for pipeline {pipeline_id} is not in this host's docker registry."
            host = pipeline["meta"].get("hostname", None)
            if host == socket.gethostname():
                m += f" The image may still be building in the pipeline.  Check the images drawer in the app at {api.Config().get_connect_url(pipeline_id)}."
            elif host != None:
                m += f" Try debugging it from '{host}' with conducto debug."

            print(m, file=sys.stderr)
            sys.exit(1)

    env_dict = get_param(payload, "Env", default={})
    secret_dict = get_param(payload, "Secrets", default={})
    autogen_dict = get_param(payload, "AutogenEnv", default={})

    autogen_dict["CONDUCTO_DATA_TOKEN"] = token
    autogen_dict["CONDUCTO_PROFILE"] = api.Config().default_profile

    ee = constants.ExecutionEnv
    eedebug = ee.DEBUG_CLOUD if status in pl.cloud else ee.DEBUG_LOCAL
    autogen_dict["CONDUCTO_EXECUTION_ENV"] = eedebug

    if not (hostdet.is_windows() or hostdet.is_wsl()):
        outer_xid = f"{os.getuid()}:{os.getgid()}"
    else:
        outer_xid = ""
    autogen_dict["CONDUCTO_OUTER_OWNER"] = outer_xid

    env_list = []
    for key, value in {**env_dict, **secret_dict, **autogen_dict}.items():
        env_list += ["-e", f"{key}={value}"]

    container_name = start_container(payload, live)
    if not live:
        print_editor_commands(container_name)
    dump_command(container_name, get_param(payload, "commandSummary"), live)

    start_shell(container_name, env_list)


async def debug(id, node, timestamp=None):
    await _debug(id, node, False, timestamp)


async def livedebug(id, node, timestamp=None):
    await _debug(id, node, True, timestamp)
