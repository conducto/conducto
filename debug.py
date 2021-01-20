import asyncio
import socket
import os
import json
import sys
import re
import subprocess
import tarfile
import io
from conducto.shared.log import format
from conducto.shared import (
    constants,
    container_utils,
    imagepath,
    async_utils,
    client_utils,
)
import conducto.internal.host_detection as hostdet

NULL = subprocess.DEVNULL
PIPE = subprocess.PIPE

if sys.version_info < (3, 7):
    # create_task is stdlib in 3.7, but we can declare it as a synonym for the
    # 3.6 ensure_future
    asyncio.create_task = asyncio.ensure_future


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

        conn = await api.connect_to_pipeline(id, token=token)
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


async def start_container(pipeline, payload, live, token, logger):
    import random
    from . import api

    get_user_id_task = asyncio.create_task(api.AsyncAuth().get_user_id(token))

    image = get_param(payload, "image", default={})
    image_name = image["name_debug"]

    if live and not image.get("path_map"):
        raise ValueError(
            f"Cannot do livedebug for image {image['name']} because it does not have a `copy_dir` or `path_map`"
        )

    container_name = "conducto_debug_" + str(random.randrange(1 << 64))

    await async_utils.eval_in_thread(
        container_utils.refresh_image, image_name, verbose=True
    )

    logger("Launching docker container...")

    if live:
        logger("Context will be mounted read-write")
        logger(
            "Make modifications on your local machine, "
            "and they will be reflected in the container."
        )

    options = []
    if get_param(payload, "requires_docker"):
        options.append("-v /var/run/docker.sock:/var/run/docker.sock")

    options.append(f'--cpus {get_param(payload, "cpu")}')
    options.append(f'--memory {get_param(payload, "mem") * 1024**3}')

    if get_param(payload, "docker_run_args"):
        options += get_param(payload, "docker_run_args")

    in_manager = constants.ExecutionEnv.value() in constants.ExecutionEnv.manager_all
    in_cloud = constants.ExecutionEnv.value() in constants.ExecutionEnv.cloud

    if in_manager and not in_cloud:
        out, err = await async_utils.run_and_check(
            "docker",
            "inspect",
            "-f",
            "'{{json .Mounts}}'",
            f"conducto_manager_{pipeline['pipeline_id']}",
        )
        out_s = out.decode("utf-8").strip()

        # for some reason its enclosed in quotes
        if out_s.startswith("'"):
            out_s = out_s[1:-1]
        mounts = json.loads(out_s)
        for mount in mounts:
            if mount["Destination"].startswith("/root/.conducto"):
                local_base = mount["Source"]
                break
        else:
            raise Exception(
                "Inspected manager container and no ~/.conducto mount found."
            )
    else:
        # TODO: Should actually pass these variables from manager, iff local
        local_base = constants.ConductoPaths.get_profile_base_dir()
        local_base = imagepath.Path.from_localhost(local_base).to_docker_mount()

    profile = api.Config().default_profile
    image_home_dir, image_work_dir = await asyncio.gather(
        get_home_dir_for_image(image_name), get_work_dir_for_image(image_name)
    )
    if not in_cloud:
        remote_base = f"{image_home_dir}/.conducto/{profile}"
        pid = pipeline["pipeline_id"]

        options.append(f"-v {local_base}:{remote_base}")
        options.append(f"-v {local_base}/data:/conducto/data/user")
        options.append(f"-v {local_base}/pipelines/{pid}/data:/conducto/data/pipeline")

    if live:
        for external, internal in image["path_map"].items():
            external = imagepath.Path.from_dockerhost_encoded(external)

            # Corrected named shares to corresponding local paths
            external = external.resolve_named_share()

            if not os.path.isabs(internal):
                internal = f"{image_work_dir}/internal"

            mount = os.path.normpath(external.to_docker_mount())

            if live and in_manager:
                assert mount.startswith("/mnt/external")
                mount = mount[13:]

            logger(
                f"Mounting {format(mount, color='blue')} "
                f"at {format(internal, color='blue')}"
            )
            options.append(f"-v {mount}:{internal}")

    if pipeline["user_id"] != await get_user_id_task:
        logger(
            "Debugging another user's command, so their secrets are not available. "
            "This may cause unexpected behavior."
        )

    command = f"docker run -d --rm {' '.join(options)} --name={container_name} {image_name} tail -f /dev/null "

    await async_utils.run_and_check(command, shell=True)
    if not live:
        path_map = image["path_map"] or {}
        for internal in path_map.values():
            if internal == constants.ConductoPaths.COPY_LOCATION:
                await execute_in(
                    container_name,
                    f"sed -i 's/{{__conducto_token__}}/{token}/' {internal}/.git/config || true",
                )
                break
    return container_name


async def dump_command(container_name, command, shell, logger):
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
    try:
        out, err = await async_utils.run_and_check(*args, input=tario.getvalue())
    except client_utils.CalledProcessError as e:
        raise RuntimeError(f"Error placing /cmd.conducto: ({e})")

    await execute_in(container_name, f"chmod u+x /cmd.conducto")

    shell_alias = {"/bin/sh": "sh", "/bin/bash": "bash"}.get(shell, shell)
    logger(
        f"Execute command by running {format(f'{shell_alias} /cmd.conducto', color='cyan')}"
    )


async def get_image_inspection(image_name):
    out, err = await async_utils.run_and_check(
        "docker", "image", "inspect", image_name, stop_on_error=False
    )
    return out.decode().strip()


@async_utils.async_cache
async def get_work_dir_for_image(image_name):
    out, err = await async_utils.run_and_check(
        "docker", "image", "inspect", "--format", "{{.Config.WorkingDir}}", image_name
    )
    return out.decode().strip()


@async_utils.async_cache
async def get_work_dir_for_container(container_name):
    out, err = await async_utils.run_and_check(
        "docker",
        "container",
        "inspect",
        "--format",
        "{{.Config.WorkingDir}}",
        container_name,
    )
    return out.decode().strip()


async def get_home_dir_for_image(image_name):
    out, err = await async_utils.run_and_check(
        "docker", "run", "--rm", image_name, "sh", "-c", "cd ~; pwd"
    )
    return out.decode().strip()


async def get_linux_flavor(container_name):
    cmd = 'sh -c "cat /etc/*-release | grep ^ID="'
    output = (await execute_in(container_name, cmd)).decode("utf8").strip()

    flavor = re.search(r"^ID=(.*)", output, re.M)
    flavor = flavor.groups()[0].strip().strip('"')

    if "ubuntu" in flavor or "debian" in flavor:
        return "debian"
    elif "alpine" in flavor:
        return "alpine"
    else:
        return f"unknown - {flavor}"


async def execute_in(container_name, command):
    # using this instead of shell is slightly faster
    out, err = await async_utils.run_and_check(
        "docker",
        "exec",
        container_name,
        "sh",
        "-c",
        command,
    )
    return out


async def print_editor_commands(container_name, logger):
    flavor = await get_linux_flavor(container_name)
    if flavor == "ubuntu":
        uid_str = await execute_in(container_name, "id -u")
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
    logger("To install an editor, run:", end="")
    sep = ""
    for editor, color in editors.items():
        logger(sep, format(f"{base} && {each} {editor}", color=color), end="")
        sep = " or"
    logger()


def start_shell(container_name, env_list, shell):
    subprocess.call(["docker", "exec", "-it", *env_list, container_name, shell])
    subprocess.call(["docker", "kill", container_name])


def start_remote_shell(remote_callback, container_name, env_list, shell):
    """
    Executes into a remote debug container
    Returns:
        send() to send input into the container
        recv() to receive output from the container
        finish_task

    """
    import shlex
    import pexpect

    buf = client_utils.ByteBuffer(cb=remote_callback)

    child = pexpect.spawn(
        " ".join(
            shlex.quote(i)
            for i in ["docker", "exec", "-it", *env_list, container_name, shell]
        ),
        timeout=None,
    )
    child.logfile_read = buf

    close = child.close

    async def finish():
        try:
            while True:
                try:
                    await child.expect(pexpect.EOF, async_=True, timeout=5)
                except pexpect.TIMEOUT:
                    pass
                else:
                    break
        except:
            pass
        finally:
            close()
            await async_utils.run_and_check("docker", "kill", container_name)

    # wait on for the child to exit in the background, once it does exit terminate the container
    finish_task = asyncio.create_task(finish())

    return child.send, buf, close, finish_task, child.setwinsize


async def _debug(id, node, live, timestamp, remote_callback=None, logger=print):
    from . import api

    pl = constants.PipelineLifecycle

    pipeline_id = id
    token = api.Config().get_token_from_shell(force=True)

    get_queue_stats_task = asyncio.create_task(
        get_exec_node_queue_stats(token, pipeline_id, node, timestamp)
    )

    try:
        pipeline = await api.AsyncPipeline().get(pipeline_id, token=token)
    except api.InvalidResponse as e:
        if "not found" in str(e):
            logger(str(e), file=sys.stderr)
            sys.exit(1)
        else:
            raise

    status = pipeline["status"]

    if status not in pl.active | pl.standby:
        logger(
            f"The pipeline {pipeline_id} is sleeping.  Wake with `conducto show` and retry this command to debug.",
            file=sys.stderr,
        )
        sys.exit(1)

    if status in pl.standby:
        api.Manager().launch(pipeline_id, token=token)
        pipeline = api.Pipeline().get(pipeline_id, token=token)

    payload = await get_queue_stats_task

    if status in pl.local:
        image = get_param(payload, "image", default={})
        image_name = image["name_debug"]
        inspect_json = await get_image_inspection(image_name)
        inspect = json.loads(inspect_json)
        if len(inspect) == 0:
            m = f"The container for pipeline {pipeline_id} is not in this host's docker registry."
            host = pipeline["meta"].get("hostname", None)
            if host == socket.gethostname():
                m += f" The image may still be building in the pipeline.  Check the images drawer in the app at {api.Config().get_connect_url(pipeline_id)}."
            elif host != None:
                m += f" Try debugging it from '{host}' with conducto debug."

            logger(m, file=sys.stderr)
            sys.exit(1)

    env_dict = get_param(payload, "Env", default={})
    secret_dict = get_param(payload, "Secrets", default={})
    autogen_dict = get_param(payload, "AutogenEnv", default={})

    autogen_dict["CONDUCTO_TOKEN"] = token
    autogen_dict["CONDUCTO_PROFILE"] = api.Config().default_profile

    ee = constants.ExecutionEnv
    eedebug = ee.DEBUG_CLOUD if status in pl.cloud else ee.DEBUG_LOCAL
    autogen_dict["CONDUCTO_EXECUTION_ENV"] = eedebug

    if not (hostdet.is_windows() or hostdet.is_wsl1()):
        outer_xid = f"{os.getuid()}:{os.getgid()}"
    else:
        outer_xid = ""
    autogen_dict["CONDUCTO_OUTER_OWNER"] = outer_xid

    env_list = []
    for key, value in {**env_dict, **secret_dict, **autogen_dict}.items():
        env_list += ["-e", f"{key}={value}"]

    if status in pl.cloud:
        docker_domain = api.Config().get_docker_domain()

        cmd = [
            "docker",
            "login",
            "-u",
            "conducto",
            "--password-stdin",
            f"https://{docker_domain}",
        ]

        try:
            _, stderr = await async_utils.run_and_check(
                *cmd, input=token.encode("utf-8")
            )
        except client_utils.CalledProcessError as e:
            logger(e)
            logger(
                f"error logging into https://{docker_domain}; unable to debug\n{e}",
                file=sys.stderr,
            )
            sys.exit(1)

    container_name = await start_container(pipeline, payload, live, token, logger)

    shell = get_param(payload, "image", default={}).get("shell", "/bin/sh")
    dump_command_task = asyncio.create_task(
        dump_command(
            container_name, get_param(payload, "commandSummary"), shell, logger
        )
    )

    if not live:
        await print_editor_commands(container_name, logger)

    await dump_command_task
    if remote_callback:
        return start_remote_shell(remote_callback, container_name, env_list, shell)
    else:
        start_shell(container_name, env_list, shell)


async def remote_debug(send_to_ws, remote_callback, id, node, live, timestamp=None):
    # TODO (apeng) this will also cause all the logging in the manager to get messed up, find another solution
    # os.environ["TERM"] = ":)"  # hack to make format() work
    return await _debug(id, node, live, timestamp, remote_callback, send_to_ws)


async def debug(id, node, timestamp=None):
    await _debug(id, node, False, timestamp, logger=print)


async def livedebug(id, node, timestamp=None):
    await _debug(id, node, True, timestamp, logger=print)
