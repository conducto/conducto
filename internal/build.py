import os
import time
import pipes
import shutil
import socket
import sys

from conducto import api, image as image_mod
from conducto.shared import (
    client_utils,
    constants,
    container_utils,
    local_daemon_utils,
    log,
    types as t,
)
import conducto.internal.host_detection as hostdet


def build(
    node,
    build_mode=constants.BuildMode.DEPLOY_TO_CLOUD,
    use_shell=False,
    use_app=True,
    retention=7,
    is_public=False,
    prebuild_images=False,
):
    assert node.parent is None
    assert node.name == "/"

    from .. import api

    # refresh the token for every pipeline launch
    # Force in case of cognito change
    node.token = token = api.Auth().get_token_from_shell(force=True)

    command = " ".join(pipes.quote(a) for a in sys.argv)

    # Register pipeline, get <pipeline_id>
    cloud = build_mode == constants.BuildMode.DEPLOY_TO_CLOUD
    pipeline_id = api.Pipeline().create(
        token,
        command,
        cloud=cloud,
        retention=retention,
        tags=node.tags or [],
        title=node.title,
        is_public=is_public,
    )

    # note: image_mod.make_all will set the .pre_built attribute in the images
    # it is important to take the serialization after this step otherwise
    # these changes are not recorded
    if prebuild_images:
        image_mod.make_all(
            node, pipeline_id, push_to_cloud=build_mode != constants.BuildMode.LOCAL
        )
    serialization = node.serialize()

    launch_from_serialization(
        serialization, pipeline_id, build_mode, use_shell, use_app, token
    )


def launch_from_serialization(
    serialization,
    pipeline_id,
    build_mode=constants.BuildMode.DEPLOY_TO_CLOUD,
    use_shell=False,
    use_app=True,
    token=None,
    inject_env=None,
    is_migration=False,
):
    if not token:
        token = api.Auth().get_token_from_shell(force=True)

    def cloud_deploy():
        # Get a token, serialize, and then deploy to AWS. Once that
        # returns, connect to it using the shell_ui.
        api.Pipeline().save_serialization(token, pipeline_id, serialization)
        api.Manager().launch(
            token, pipeline_id, env=inject_env, is_migration=is_migration
        )
        log.debug(f"Connecting to pipeline_id={pipeline_id}")

    def local_deploy():
        clean_log_dirs(token)

        # Write serialization to ~/.conducto/
        local_progdir = constants.ConductoPaths.get_local_path(pipeline_id)
        os.makedirs(local_progdir, exist_ok=True)
        serialization_path = os.path.join(
            local_progdir, constants.ConductoPaths.SERIALIZATION
        )

        with open(serialization_path, "w") as f:
            f.write(serialization)

        api.Pipeline().update(token, pipeline_id, {"program_path": serialization_path})

        run_in_local_container(
            token, pipeline_id, inject_env=inject_env, is_migration=is_migration
        )

    if build_mode == constants.BuildMode.DEPLOY_TO_CLOUD:
        func = cloud_deploy
        starting = False
    else:
        func = local_deploy
        starting = True

    # Make sure that a local daemon is running before we launch. A local manager will
    # start it if none are running, but cloud has no way to do that.
    local_daemon_utils.launch_local_daemon(token, inside_container=False)

    run(token, pipeline_id, func, use_app, use_shell, "Starting", starting)

    return pipeline_id


def run(token, pipeline_id, func, use_app, use_shell, msg, starting):
    from .. import api, shell_ui

    config = api.Config()
    url = config.get_connect_url(pipeline_id)
    u_url = log.format(url, underline=True)

    if starting:
        tag = config.get_image_tag()
        is_test = os.environ.get("CONDUCTO_USE_TEST_IMAGES")
        manager_image = constants.ImageUtil.get_manager_image(tag, is_test)
        try:
            client_utils.subprocess_run(["docker", "image", "inspect", manager_image])
        except client_utils.CalledProcessError:
            docker_parts = ["docker", "pull", manager_image]
            print("Downloading the Conducto docker image that runs your pipeline.")
            log.debug(" ".join(pipes.quote(s) for s in docker_parts))
            client_utils.subprocess_run(
                docker_parts, msg="Error pulling manager container",
            )

    print(f"{msg} pipeline {pipeline_id}.")

    func()

    if _manager_debug():
        return

    if use_app:
        print(
            f"Viewing at {u_url}. To disable, specify '--no-app' on the command line."
        )
        hostdet.system_open(url)
    else:
        print(f"View at {u_url}")

    data = api.Pipeline().get(token, pipeline_id)
    if data.get("is_public"):
        unauth_password = data["unauth_password"]
        url = config.get_url()
        public_url = f"{url}/app/s/{pipeline_id}/{unauth_password}"
        u_public_url = log.format(public_url, underline=True)
        print(f"\nPublic view at:\n{u_public_url}")

    if use_shell:
        shell_ui.connect(token, pipeline_id, "Deploying")


def run_in_local_container(
    token, pipeline_id, update_token=False, inject_env=None, is_migration=False
):
    if inject_env is None:
        inject_env = {}

    # The homedir inside the manager is /root. Mapping will be verified by manager,
    # internal to the container.
    local_profdir = container_utils.get_external_conducto_dir(is_migration)
    config = api.Config()
    profile = config.default_profile
    remote_basedir = "/root/.conducto"
    # unix format path for docker
    remote_profdir = "/".join([remote_basedir, profile])

    ccp = constants.ConductoPaths
    pipelinebase = ccp.get_local_path(pipeline_id, expand=False, base=remote_basedir)
    # Note: This path is in the docker which is always unix
    pipelinebase = pipelinebase.replace(os.path.sep, "/")
    serialization = f"{pipelinebase}/{ccp.SERIALIZATION}"

    container_name = f"conducto_manager_{pipeline_id}"

    labels = [
        "--label",
        f"com.conducto.profile={profile}",
        "--label",
        f"com.conducto.pipeline={pipeline_id}",
    ]

    network_name = os.getenv("CONDUCTO_NETWORK", f"conducto_network_{pipeline_id}")
    if not is_migration:
        try:
            client_utils.subprocess_run(
                ["docker", "network", "create", network_name] + labels
            )
        except client_utils.CalledProcessError as e:
            if f"network with name {network_name} already exists" in e.stderr.decode():
                pass
            else:
                raise

    if not (hostdet.is_windows() or hostdet.is_wsl()):
        outer_xid = f"{os.getuid()}:{os.getgid()}"
    else:
        outer_xid = ""

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
        "--network",
        network_name,
        "--hostname",
        container_name,
        *labels,
        # Mount local conducto profdir on container. Allow TaskServer
        # to access config and serialization and write logs.
        "-v",
        f"{local_profdir}:{remote_profdir}",
        # Mount docker sock so we can spin out task workers.
        "-v",
        "/var/run/docker.sock:/var/run/docker.sock",
        # Specify expected base dir for container to verify.
        "-e",
        f"CONDUCTO_BASE_DIR_VERIFY={remote_profdir}",
        "-e",
        f"CONDUCTO_PROFILE_DIR={local_profdir}",
        "-e",
        f"CONDUCTO_LOCAL_HOSTNAME={socket.gethostname()}",
        "-e",
        f"CONDUCTO_NETWORK={network_name}",
        "-e",
        f"CONDUCTO_OUTER_OWNER={outer_xid}",
        "-e",
        f"CONDUCTO_OS={hostdet.os_name()}",
    ]

    if config.get_image_tag():
        # this is dev/test only so we do not always set it
        tag = config.get_image_tag()
        flags.extend(["-e", f"CONDUCTO_IMAGE_TAG={tag}"])

    for env_var in (
        "CONDUCTO_URL",
        "CONDUCTO_DEV_REGISTRY",
        "CONDUCTO_USE_TEST_IMAGES",
    ):
        if os.environ.get(env_var):
            flags.extend(["-e", f"{env_var}={os.environ[env_var]}"])
    for k, v in inject_env.items():
        flags.extend(["-e", f"{k}={v}"])

    flags += container_utils.get_whole_host_mounting_flags()

    if _manager_debug():
        flags[0] = "-it"
        flags += ["-e", "CONDUCTO_LOG_LEVEL=0"]
        capture_output = False
    else:
        capture_output = True

    mcpu = _manager_cpu()
    if mcpu > 0:
        flags += ["--cpus", str(mcpu)]

    flags += container_utils.get_docker_dir_mount_flags()

    cmd_parts = [
        "python",
        "-m",
        "manager.src",
        "-p",
        pipeline_id,
        "-i",
        serialization,
        "--profile",
        config.default_profile,
        "--local",
    ]

    if update_token:
        cmd_parts += ["--update_token", "--token", token]

    tag = config.get_image_tag()
    is_test = os.environ.get("CONDUCTO_USE_TEST_IMAGES")
    manager_image = constants.ImageUtil.get_manager_image(tag, is_test)
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
            if data["status"] in target:
                break

            if container_name not in container_utils.get_running_containers():
                attached = [param for param in docker_parts if param != "-d"]
                dockerrun = " ".join(pipes.quote(s) for s in attached)
                msg = f"There was an error starting the docker container.  Try running the command below for more diagnostics or contact us on Slack at ConductoHQ.\n{dockerrun}"
                raise RuntimeError(msg)
        else:
            # timeout, return error
            raise RuntimeError(
                f"no manager connection to gw for {pipeline_id} after {constants.ManagerAppParams.WAIT_TIME_SECS} seconds"
            )

        log.debug(f"Manager docker connected to gw pipeline_id={pipeline_id}")


def clean_log_dirs(token):
    from .. import api

    pipelines = api.Pipeline().list(token)
    pipeline_ids = set(p["pipeline_id"] for p in pipelines)

    # Remove all outdated logs directories.
    local_basedir = constants.ConductoPaths.get_profile_base_dir()
    local_basedir = os.path.join(local_basedir, "pipelines")
    if os.path.isdir(local_basedir):
        for subdir in os.listdir(local_basedir):
            if subdir not in pipeline_ids:
                shutil.rmtree(os.path.join(local_basedir, subdir), ignore_errors=True)


def _manager_debug():
    return t.Bool(os.getenv("CONDUCTO_MANAGER_DEBUG"))


def _manager_cpu():
    return float(os.getenv("CONDUCTO_MANAGER_CPU", "1"))
