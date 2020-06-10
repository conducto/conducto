import os
import pipes
import socket
import conducto as co
from . import client_utils, constants, container_utils, log
from ..internal import host_detection as hostdet


def name():
    profile_id = co.api.Config().default_profile
    return f"conducto_daemon_{profile_id}"


def launch_local_daemon(token, inside_container=False):
    container_name = name()

    running = container_utils.get_running_containers()
    if container_name in running or f"{container_name}-old" in running:
        return

    # The homedir inside the manager is /root. Mapping will be verified by manager,
    # internal to the container.
    external_profile_dir = container_utils.get_external_conducto_dir(inside_container)
    internal_base_dir = "/root/.conducto"
    config = co.api.Config()
    profile = config.default_profile
    internal_profile_dir = "/".join([internal_base_dir, profile])

    external_profile_dir = external_profile_dir.replace(profile, "")
    internal_profile_dir = internal_profile_dir.replace(profile, "")

    # Name of the external host. Use environment variable gets passed at each new layer
    # so that the innermost layers can always know the outermost name.
    hostname = os.environ.get("CONDUCTO_LOCAL_HOSTNAME", socket.gethostname())

    eedaemon = constants.ExecutionEnv.DAEMON_LOCAL

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
        "--label",
        f"com.conducto.profile={profile}",
        # Mount local conducto basedir on container. Allow TaskServer
        # to access config and serialization and write logs.
        "-v",
        f"{external_profile_dir}:{internal_profile_dir}",
        # Mount docker sock so we can spin out task workers.
        "-v",
        "/var/run/docker.sock:/var/run/docker.sock",
        # Specify expected base dir for container to verify.
        "-e",
        f"CONDUCTO_EXECUTION_ENV={eedaemon}",
        "-e",
        f"CONDUCTO_BASE_DIR_VERIFY={internal_base_dir}",
        "-e",
        f"CONDUCTO_LOCAL_BASE_DIR={external_profile_dir}",
        "-e",
        f"CONDUCTO_LOCAL_HOSTNAME={hostname}",
        "-e",
        f"CONDUCTO_OS={hostdet.os_name()}",
    ]

    for env_var in "CONDUCTO_URL", "CONDUCTO_IMAGE_TAG":
        if os.environ.get(env_var):
            flags.extend(["-e", f"{env_var}={os.environ[env_var]}"])

    flags += container_utils.get_whole_host_mounting_flags()

    if co.env_bool("CONDUCTO_DAEMON_DEBUG"):
        flags[0] = "-it"
        flags += ["-e", "CONDUCTO_LOG_LEVEL=0"]
        capture_output = False
    else:
        capture_output = True

    flags += container_utils.get_docker_dir_mount_flags()

    cmd_parts = [
        "python",
        "-m",
        "local_daemon.src",
        "--token",
        token,
        "--profile",
        config.default_profile,
    ]

    tag = config.get_image_tag()
    is_test = os.environ.get("CONDUCTO_USE_TEST_IMAGES")
    daemon_image = constants.ImageUtil.get_local_daemon_image(tag, is_test)
    if daemon_image.startswith("conducto/"):
        docker_parts = ["docker", "pull", daemon_image]
        log.debug(" ".join(pipes.quote(s) for s in docker_parts))
        client_utils.subprocess_run(
            docker_parts,
            capture_output=capture_output,
            msg="Error pulling daemon container",
        )
    # Run local_daemon container.
    docker_parts = ["docker", "run"] + flags + [daemon_image] + cmd_parts
    log.debug(" ".join(pipes.quote(s) for s in docker_parts))
    client_utils.subprocess_run(
        docker_parts,
        msg="Error starting Conducto daemon container",
        capture_output=capture_output,
    )
