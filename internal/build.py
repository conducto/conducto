import os
import time
import subprocess
import functools
import pipes
import shutil
import socket
import sys

from conducto import api
from conducto.shared import client_utils, constants, log, types as t
import conducto.internal.host_detection as hostdet


@functools.lru_cache(None)
def _split_windocker(path):
    chunks = path.split("//")
    mangled = hostdet.wsl_host_docker_path(chunks[0])
    if len(chunks) > 1:
        newctx = f"{mangled}//{chunks[1]}"
    else:
        newctx = mangled
    return newctx


def _validate_wsl_locations(node):
    # collect contexts to convert to windows paths to mount in the manager
    # docker container.

    drives = set()

    image_ids = []
    imagelist = []
    for child in node.stream():
        if id(child.image) not in image_ids:
            image_ids.append(id(child.image))
            imagelist.append(child.image)

    for img in imagelist:
        path = img.context
        if path:
            newpath = _split_windocker(path)
            img.context = newpath
            drives.add(newpath[1])
        path = img.dockerfile
        if path:
            newpath = _split_windocker(path)
            img.dockerfile = newpath
            drives.add(newpath[1])
    return drives


def build(
    node,
    build_mode=constants.BuildMode.DEPLOY_TO_CLOUD,
    tags=None,
    title=None,
    use_shell=True,
    retention=7,
):
    assert node.parent is None
    assert node.name == "/"

    if hostdet.is_wsl():
        # Check if all node contexts can be mounted in docker.
        _validate_wsl_locations(node)

    from .. import api, shell_ui

    node.token = token = api.Auth().get_token_from_shell()

    serialization = node.serialize()

    command = " ".join(pipes.quote(a) for a in sys.argv)

    # Register pipeline, get <pipeline_id>
    cloud = build_mode == constants.BuildMode.DEPLOY_TO_CLOUD
    pipeline_id = api.Pipeline().create(
        token, command, cloud=cloud, retention=retention, tags=tags or [], title=title
    )

    url = shell_ui.connect_url(pipeline_id)
    print(f"Starting!  View at {url}")

    def cloud_deploy():
        # Get a token, serialize, and then deploy to AWS. Once that
        # returns, connect to it using the shell_ui.
        api.Pipeline().save_serialization(token, pipeline_id, serialization)
        api.Manager().launch(token, pipeline_id)
        log.debug(f"Connecting to pipeline_id={pipeline_id}")

    def local_deploy():
        # TODO (apeng) Leaving this out for now
        # when we run conducto in a container for our tests
        # this has a tendency to blow up all the logs, including
        # the logs of the running superpipe, which breaks it completely
        # clean_log_dirs(token)

        # Save to ~/.conducto/ -- write serialization.
        local_progdir = constants.ConductoPaths.get_local_path(pipeline_id)
        os.makedirs(local_progdir, exist_ok=True)
        serialization_path = os.path.join(
            local_progdir, constants.ConductoPaths.SERIALIZATION
        )
        with open(serialization_path, "w") as f:
            f.write(serialization)

        run_in_local_container(token, pipeline_id)

    if build_mode == constants.BuildMode.DEPLOY_TO_CLOUD:
        func = cloud_deploy
    else:
        func = local_deploy

    if use_shell and not _manager_debug():
        shell_ui.connect(token, pipeline_id, func, "Deploying")
    else:
        func()


def run_in_local_container(token, pipeline_id):
    # Remote base dir will be verified by container.
    local_basedir = constants.ConductoPaths.get_local_base_dir()

    if hostdet.is_wsl():
        local_basedir = os.path.realpath(local_basedir)
        local_basedir = hostdet.wsl_host_docker_path(local_basedir)
    elif hostdet.is_windows():
        local_basedir = hostdet.windows_docker_path(local_basedir)
    else:

        subp = subprocess.Popen(
            "head -1 /proc/self/cgroup|cut -d/ -f3", shell=True, stdout=subprocess.PIPE
        )
        container_id, err = subp.communicate()
        container_id = container_id.decode("utf-8").strip()

        if container_id:
            # Mount to the ~/.conducto of the host machine and not of the container
            import json

            subp = subprocess.Popen(
                f"docker inspect -f '{{{{ json .Mounts }}}}' {container_id}",
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
            )
            mount_data, err = subp.communicate()
            if subp.returncode == 0:
                mounts = json.loads(mount_data)
                for mount in mounts:
                    if mount["Destination"] == local_basedir:
                        local_basedir = mount["Source"]
                        break

    remote_basedir = "/usr/conducto/.conducto"

    tag = api.Config().get_image_tag()
    manager_image = constants.ImageUtil.get_manager_image(tag)
    serialization = (
        f"{remote_basedir}/logs/{pipeline_id}/{constants.ConductoPaths.SERIALIZATION}"
    )

    container_name = f"conducto_manager_{pipeline_id}"

    flags = [
        # Detached mode.
        "-d",
        # Remove container when done.
        "--rm",
        # --name is the name of the container, as in when you do `docker ps`
        # --hostname is the name of the host inside the container.
        # Set them equal so that the manager can use socket.gethostname() to
        # spin up workers that connect to its network.
        "--name",
        container_name,
        "--hostname",
        container_name,
        "--label",
        "conducto",
        # Mount local conducto basedir on container. Allow TaskServer
        # to access config and serialization and write logs.
        "-v",
        f"{local_basedir}:{remote_basedir}",
        # Mount docker sock so we can spin out task workers.
        "-v",
        "/var/run/docker.sock:/var/run/docker.sock",
        # Specify expected base dir for container to verify.
        "-e",
        f"CONDUCTO_BASE_DIR_VERIFY={remote_basedir}",
        "-e",
        f"CONDUCTO_LOCAL_BASE_DIR={local_basedir}",
        "-e",
        f"CONDUCTO_LOCAL_HOSTNAME={socket.gethostname()}",
    ]

    for env_var in "CONDUCTO_URL", "CONDUCTO_CONFIG", "IMAGE_TAG":
        if os.environ.get(env_var):
            flags.extend(["-e", f"{env_var}={os.environ[env_var]}"])

    if hostdet.is_wsl():
        lsdrives = "docker run --rm -v /:/mnt/external alpine ls /mnt/external/host_mnt"
        proc = subprocess.run(lsdrives, shell=True, stdout=subprocess.PIPE)

        results = proc.stdout.decode("utf-8").split("\n")
        drives = [s.strip().upper() for s in results if s.strip() != ""]

        for d in drives:
            # Mount whole system read-only to enable rebuilding images as needed
            mount = f"type=bind,source={d}:/,target={constants.ConductoPaths.MOUNT_LOCATION}/{d},readonly"
            flags += ["--mount", mount]
    else:
        # Mount whole system read-only to enable rebuilding images as needed
        mount = f"type=bind,source=/,target={constants.ConductoPaths.MOUNT_LOCATION},readonly"
        flags += ["--mount", mount]

    if _manager_debug():
        flags[0] = "-it"
        flags += ["-e", "CONDUCTO_LOG_LEVEL=0"]
        capture_output = False
    else:
        capture_output = True

    mcpu = _manager_cpu()
    if mcpu > 0:
        flags += ["--cpus", str(mcpu)]

    # WSL doesn't persist this into containers natively
    # Have to have this configured so that we can use host docker creds to pull containers
    docker_basedir = constants.ConductoPaths.get_local_docker_config_dir()
    if docker_basedir:
        flags += ["-v", f"{docker_basedir}:/root/.docker"]

    cmd_parts = [
        "python",
        "-m",
        "manager.src",
        "-p",
        pipeline_id,
        "-i",
        serialization,
        "--local",
    ]
    if hostdet.is_wsl():
        cmd_parts.append("--from_wsl")

    conducto_in_docker = os.environ.get("CONDUCTO_RUNNING_IN_DOCKER")
    if conducto_in_docker:
        # If we are launching a pipeline from with-in a docker container, the
        # .conducto folder may only reside in the (outer) docker container.
        # However the -v bind switch assumes the outer bound directory is on
        # the machine with the docker daemon.  This is an imperfect
        # work-around to recreate the way that the serialization is passed to
        # the new manager container in the .conducto folder.
        # See https://circleci.com/docs/2.0/building-docker-images/#mounting-folders

        # This rips out the -v .conducto:.conducto parameter pair
        for index, pair in enumerate(zip(flags[:-1], flags[1:])):
            x, y = pair
            if x == "-v" and ".conducto" in y:
                # remove x & y
                skinny_flags = flags[:index] + flags[index + 2 :]
                break
        else:
            raise ValueError(f"Cannot find '-v .conducto:.conducto' args in {flags}")
        skinny_flags += ["-e", f"CONDUCTO_RUNNING_IN_DOCKER={conducto_in_docker}"]

        volname = f"dotconducto_{pipeline_id}"

        docker_parts = [
            "docker",
            "create",
            "-v",
            remote_basedir,
            "--name",
            volname,
            "alpine:3.11",
            "/bin/true",
        ]
        log.debug(" ".join(pipes.quote(s) for s in docker_parts))
        client_utils.subprocess_run(
            docker_parts,
            capture_output=capture_output,
            msg="error build dummy volume container",
        )
        docker_parts = ["docker", "cp"] + [
            local_basedir + "/.",
            f"{volname}:{remote_basedir}",
        ]
        log.debug(" ".join(pipes.quote(s) for s in docker_parts))
        client_utils.subprocess_run(
            docker_parts,
            capture_output=capture_output,
            msg="error copying .conducto pipeline",
        )
        docker_parts = (
            ["docker", "run"]
            + skinny_flags
            + ["--volumes-from", volname]
            + [manager_image]
            + cmd_parts
        )
        log.debug(" ".join(pipes.quote(s) for s in docker_parts))
        client_utils.subprocess_run(
            docker_parts,
            capture_output=capture_output,
            msg="Error starting manager container",
        )
    else:
        # Pull manager image if from dockerhub.
        if manager_image.startswith("conducto/"):
            docker_parts = ["docker", "pull", manager_image]
            log.debug(" ".join(pipes.quote(s) for s in docker_parts))
            client_utils.subprocess_run(
                docker_parts,
                capture_output=capture_output,
                msg="Error pulling manager container",
            )
        # Run manager container.
        docker_parts = ["docker", "run"] + flags + [manager_image] + cmd_parts
        log.debug(" ".join(pipes.quote(s) for s in docker_parts))
        client_utils.subprocess_run(
            docker_parts,
            msg="Error starting manager container",
            capture_output=capture_output,
        )

    # When in debug mode the manager is run attached and it makes no sense to
    # follow that up with waiting for the manager to start.
    if not _manager_debug():
        log.debug(f"Verifying manager docker startup pipeline_id={pipeline_id}")

        def _get_docker_output():
            p = subprocess.run(["docker", "ps"], stdout=subprocess.PIPE)
            return p.stdout.decode("utf-8")

        pl = constants.PipelineLifecycle
        target = pl.active - pl.standby
        # wait 45 seconds, but this should be quick
        for _ in range(
            int(
                constants.ManagerAppParams.WAIT_TIME_SECS
                / constants.ManagerAppParams.POLL_INTERVAL_SECS
            )
        ):
            time.sleep(constants.ManagerAppParams.POLL_INTERVAL_SECS)
            log.debug(f"awaiting program {pipeline_id} active")
            data = api.Pipeline().get(token, pipeline_id)
            if data["status"] in target and data["pgw"] not in ["", None]:
                break

            dps = _get_docker_output()
            if container_name not in dps:
                attached = [param for param in docker_parts if param != "-d"]
                dockerrun = " ".join(pipes.quote(s) for s in attached)
                msg = f"There was an error starting the docker container.  Try running the command below for more diagnostics or contact us on Slack at ConductoHQ.\n{dockerrun}"
                raise RuntimeError(msg)
        else:
            # timeout, return error
            raise RuntimeError(
                f"no manager connection to pgw for {pipeline_id} after {constants.ManagerAppParams.WAIT_TIME_SECS} seconds"
            )

        log.debug(f"Manager docker connected to pgw pipeline_id={pipeline_id}")


def clean_log_dirs(token):
    from .. import api

    pipelines = api.Pipeline().list(token)
    pipeline_ids = set(p["pipeline_id"] for p in pipelines)

    # Remove all outdated logs directories.
    local_basedir = os.path.join(constants.ConductoPaths.get_local_base_dir(), "logs")
    if os.path.isdir(local_basedir):
        for subdir in os.listdir(local_basedir):
            if subdir not in pipeline_ids:
                shutil.rmtree(os.path.join(local_basedir, subdir), ignore_errors=True)


def _manager_debug():
    return t.Bool(os.getenv("CONDUCTO_MANAGER_DEBUG"))


def _manager_cpu():
    return float(os.getenv("CONDUCTO_MANAGER_CPU", "1"))
