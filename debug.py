import asyncio
import subprocess
import os
import time
from conducto.shared.log import format

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
            await conn.send(json.dumps({
                "type":"RENDER_NODE_DETAILS",
                "payload": {
                    "node": node,
                    **({"base":timestamp} if timestamp is not None else {})
                }
            }))
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
        payload['queueStats'].append(['commandSummary', payload['commandSummary']])
        return payload['queueStats']
    try:
        return await asyncio.wait_for(_internal(), 5)
    except asyncio.TimeoutError:
        raise Exception(f"Timed out getting node details. Are {id} and {node} correct?")


def get_param(payload, param):
    for name, val in payload:
        if name == param:
            return val
    if param == 'Env':
        return {}


def start_container(payload):
    image_name = get_param(payload, 'image')['name_built']
    import random
    container_name = "conducto_debug_" + str(random.randrange(1 << 64))
    print(format("Launching docker container...", color='red'))

    options = []
    if get_param(payload, 'requires_docker'):
        res = subprocess.Popen(["which", "docker"], stdout=PIPE, stderr=NULL)
        out, _ = res.communicate()
        out = out.decode('utf-8').strip()
        options.append(f"-v {out}:/usr/bin/docker")
        options.append('-v /var/run/docker.sock:/var/run/docker.sock')
    options.append(f'--cpus {get_param(payload, "cpu")}')
    options.append(f'--memory {get_param(payload, "mem") * 1023**3}')

    command = f"docker run {' '.join(options)} --name={container_name} {image_name} tail -f /dev/null "
    subprocess.Popen(command, shell=True)
    time.sleep(1)
    return container_name


def dump_command(container_name, command):
    TEMPFILE = '__conducto_tempfile_command'
    # if os.path.exists(TEMPFILE):
    #     raise Exception("Could not create command tempfile")
    with open(TEMPFILE, 'w+') as f:
        f.write(command)

    subp = subprocess.Popen(["docker", "exec", container_name, "pwd"], stdout=PIPE, stderr=PIPE)
    out, _ = subp.communicate()
    command_location = out.decode('utf-8').strip() + '/conducto.cmd'

    subp = subprocess.Popen(f"docker cp {TEMPFILE} {container_name}:{command_location}", shell=True)
    subp.wait()
    os.remove(TEMPFILE)
    execute_in(container_name, f'chmod u+x {command_location}')
    print(format(f"Node command located at {command_location}", color='red'))
    print(format(f"Execute with ./conducto.cmd", color='red'))


def execute_in(container_name, command):
    subprocess.Popen(f"docker exec {container_name} {command}", shell=True).wait()


def start_shell(container_name, env):
    cmd = f"docker exec -it {env} {container_name} {{}}"
    subp = subprocess.Popen(f"docker exec {container_name} /bin/bash", shell=True, stdout=NULL, stderr=NULL)
    subp.wait()
    has_bash = subp.returncode == 0
    if has_bash:
        subprocess.check_call(cmd.format("/bin/bash"), shell=True)
    else:
        subprocess.check_call(cmd.format("/bin/sh"), shell=True)
    subprocess.Popen(f"docker kill {container_name}", shell=True).wait()
    subprocess.Popen(f"docker rm {container_name}", shell=True, stdout=NULL, stderr=NULL).wait()


async def debug(id, node, timestamp=None):
    payload = await get_exec_node_queue_stats(id, node, timestamp)
    env = ' '.join(f'-e "{key}={value}"' for key, value in get_param(payload, 'Env').items())
    container_name = start_container(payload)
    dump_command(container_name, get_param(payload, 'commandSummary'))
    start_shell(container_name, env)
