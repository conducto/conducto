import asyncio
import subprocess
import tempfile
import time
from conducto.shared.log import format
from conducto.shared import constants

NULL = subprocess.DEVNULL
PIPE = subprocess.PIPE


# Several debug modes:
# - "copy": shows command + environment variables, as it appears
#   inside the container
# - "login": running this container locally with all the
#   env set up.
# - "run": running this container, with all env, and running the
#   node's command as well
# - "liverun": only available if using do.Image(root="..."), similar to run
#   except that it bind mounts your root directory inside the image so that
#   you get your latest code.
# - "debug": similar to run except that instead of starting the docker shell
#   and running the command, it actually just prints the command and runs
#   bash so that you can modify it yourself prior to running.
#   TODO: put the command in the history using `history -s`, but not sure how to do that
# - "livedebug": livedebug is to debug as liverun is to live.


async def get_exec_node_queue_stats(id, node, timestamp=None):
    async def _internal():
        assert type(node) == str
        from . import api
        import json

        token = api.Auth().get_token_from_shell()
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
        if "queueStatsDiff" not in payload:
            raise Exception(f"Node {node} is not exec")
        payload["queueStats"].append(["commandSummary", payload["commandSummary"]])
        return payload["queueStats"]

    try:
        return await asyncio.wait_for(_internal(), 5)
    except asyncio.TimeoutError:
        raise Exception(f"Timed out getting node details. Are {id} and {node} correct?")


def get_param(payload, param, default=None):
    for name, val in payload:
        if name == param:
            return val
    return default


def start_container(payload):
    image_name = get_param(payload, "image", default={})["name_built"]
    import random

    container_name = "conducto_debug_" + str(random.randrange(1 << 64))
    print(format("Launching docker container...", color="red"))

    options = []
    if get_param(payload, "requires_docker"):
        options.append("-v /var/run/docker.sock:/var/run/docker.sock")
    options.append(f'--cpus {get_param(payload, "cpu")}')
    options.append(f'--memory {get_param(payload, "mem") * 1024**3}')

    # TODO: Should actually pass these variables from manager, iff local
    local_basedir = constants.ConductoPaths.get_local_base_dir()
    remote_basedir = "/usr/conducto/.conducto"
    options.append(f"-v {local_basedir}:{remote_basedir}")

    command = f"docker run {' '.join(options)} --name={container_name} {image_name} tail -f /dev/null "
    subprocess.Popen(command, shell=True)
    time.sleep(1)
    return container_name


def dump_command(container_name, command):
    with tempfile.NamedTemporaryFile() as tmp:
        with open(tmp.name, "w") as f:
            f.write(command)

        out = subprocess.check_output(
            ["docker", "exec", container_name, "pwd"], stderr=PIPE
        )
        command_location = out.decode("utf-8").strip() + "/conducto.cmd"

        subprocess.check_call(
            f"docker cp {tmp.name} {container_name}:{command_location}", shell=True
        )
    execute_in(container_name, f"chmod u+x {command_location}")
    print(format(f"Node command located at {command_location}", color="red"))
    print(format(f"Execute with ./conducto.cmd", color="red"))


def execute_in(container_name, command):
    subprocess.check_call(f"docker exec {container_name} {command}", shell=True)


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


async def debug(id, node, timestamp=None):
    from . import api

    payload = await get_exec_node_queue_stats(id, node, timestamp)
    env_dict = get_param(payload, "Env", default={})
    secret_dict = get_param(payload, "Secret", default={})
    autogen_dict = get_param(payload, "AutogenEnv", default={})

    autogen_dict["CONDUCTO_DATA_TOKEN"] = api.Auth().get_token_from_shell()

    env_list = []
    for key, value in {**env_dict, **secret_dict, **autogen_dict}.items():
        env_list += ["-e", f"{key}={value}"]

    container_name = start_container(payload)
    dump_command(container_name, get_param(payload, "commandSummary"))

    start_shell(container_name, env_list)
