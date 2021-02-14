import os
import pipes
import shutil
import socket
import sys
import time
import datetime
from http import HTTPStatus as hs

from conducto import api
from conducto.shared import (
    client_utils,
    constants,
    container_utils,
    agent_utils,
    path_utils,
    log,
    resource_validation,
    types as t,
)
import conducto.internal.host_detection as hostdet

_UNSET = object()


def _get_common(parent_dict, child_dicts):
    common_in_children = None
    set_in_parent = {k: v for k, v in parent_dict.items() if v is not None}

    for d in child_dicts:
        # Find all key/value pairs that are identical among all children and are unset
        # in the parent
        if common_in_children is None:
            common_in_children = {
                k: v
                for k, v in d.items()
                if v is not None and parent_dict.get(k) is None
            }
        else:
            for common_k, common_v in list(common_in_children.items()):
                if d.get(common_k, _UNSET) != common_v:
                    del common_in_children[common_k]

        # Find all key/value pairs where the child's value either is unset or is
        # identical to that of the parent
        for key, value in list(set_in_parent.items()):
            child_value = d.get(key)
            if child_value is not None and child_value != value:
                del set_in_parent[key]

    # There should be no overlap between these two dicts: keys in set_in_parent must
    # have had non-None value in the parent, whereas keys in common_in_children must
    # have been None in the parent. Anything in either can be pulled up to the parent.
    return {**set_in_parent, **common_in_children}


def simplify_attributes(root):
    for node in root.stream(reverse=True):
        if node.children:
            # Propagate user_set attributes, setting them to None in the children
            common_attrs = _get_common(
                node.user_set, [c.user_set for c in node.children.values()]
            )
            for k, v in common_attrs.items():
                node.user_set[k] = v
                for child in node.children.values():
                    child.user_set[k] = None

            # Propagate environment variables, removing them from the children
            common_env = _get_common(node.env, [c.env for c in node.children.values()])
            for k, v in common_env.items():
                node.env[k] = v
                for child in node.children.values():
                    child.env.pop(k, None)


def validate_tree(node, cloud, check_images=True, set_default=True):
    def validate_env(desc):
        for key, value in desc.env.items():
            if not isinstance(key, str):
                raise TypeError(
                    f"{desc} has {type(key).__name__} in env key when str is required"
                )
            if not isinstance(value, str):
                raise TypeError(
                    f"{desc} has {type(value).__name__} in env value for {key} when str is required"
                )

    if set_default and node.image is None:
        from conducto.image import Image

        node.image = Image(name="conducto-default")

    for desc in node.stream():
        validate_env(desc)
        if hasattr(desc, "command"):
            desc.resolve_intermediate_paths()

    node.repo.finalize()
    if check_images:
        node.check_images()

    if cloud:
        _check_nodes_for_cloud(node)

    simplify_attributes(node)


def build(
    node,
    build_mode=constants.BuildMode.DEPLOY_TO_CLOUD,
    use_shell=False,
    use_app=True,
    retention=7,
    is_public=False,
):
    assert node.parent is None
    assert node.name == "/"

    const_ee = constants.ExecutionEnv
    if const_ee.value() not in const_ee.agent | {const_ee.EXTERNAL}:
        raise RuntimeError(
            "It is not supported to launch pipelines from with-in other pipelines. "
            "Consider using `co.Lazy` to compose pipelines dynamically."
        )

    from .. import api

    # refresh the token for every pipeline launch
    # Force in case of cognito change
    try:
        token = api.Config().get_token(refresh=True)
    except PermissionError:
        token = None
    if token is None and sys.stdin.isatty():
        token = api.Auth().get_token_from_shell(force=True, force_new=True)
    node.token = token
    command = " ".join(pipes.quote(a) for a in sys.argv)

    # Register pipeline, get <pipeline_id>
    cloud = build_mode != constants.BuildMode.LOCAL

    validate_tree(node, cloud)

    if constants.ExecutionEnv.images_only():
        pipeline_id = "zzz-zzz"
    else:
        pipeline_id = api.Pipeline().create(
            command,
            token=token,
            cloud=cloud,
            retention=retention,
            tags=node.tags or [],
            title=node.title,
            is_public=is_public,
        )

    serialization = node.serialize()

    return launch_from_serialization(
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
    _auto_set_headless()

    if not token:
        token = api.Auth().get_token_from_shell(force=True)

    if inject_env is None:
        inject_env = {}

    def cloud_deploy():
        if t.Bool(os.getenv("CONDUCTO_INHERIT_GIT_VARS")):
            for k, v in os.environ.items():
                if k.startswith("CONDUCTO_GIT_"):
                    inject_env[k] = v

        # Get a token, serialize, and then deploy to AWS.
        pipeline_api = api.Pipeline()
        pipeline_api.save_serialization(pipeline_id, serialization, token=token)
        try:
            api.Manager().launch(pipeline_id, token=token, env=inject_env)
        except api.api_utils.InvalidResponse as e:
            # Archive pipeline so it does not pollute app list view.
            pipeline_api.archive(pipeline_id, token=token)
            if e.status_code == hs.TOO_MANY_REQUESTS:
                msg = str(e.message)
            else:
                msg = f"Failed to launch cloud manager for pipeline {pipeline_id}. Please try again and/or contact us on Slack at ConductoHQ."
            raise api.api_utils.NoTracebackError(msg)

    def k8s_deploy():
        if t.Bool(os.getenv("CONDUCTO_INHERIT_GIT_VARS")):
            for k, v in os.environ.items():
                if k.startswith("CONDUCTO_GIT_"):
                    inject_env[k] = v

        # Get a token, serialize, and then deploy to k8s.
        # TODO: This saves to our S3, make it optionally save to user-specified S3.
        pipeline_api = api.Pipeline()
        pipeline_api.save_serialization(pipeline_id, serialization, token=token)

        # Generate job.yaml
        job_yaml = get_k8s_job_yaml(pipeline_id, token, inject_env)

        opts = os.environ.get("CONDUCTO_KUBECTL_OPTIONS", "")
        kubectl_cmd = f"kubectl {opts} apply -f -"

        # Submit manager job to k8s cluster.
        try:
            client_utils.subprocess_streaming(
                *kubectl_cmd.split(),
                buf=client_utils.ByteBuffer(),
                input=job_yaml.encode(),
            )
        except client_utils.CalledProcessError as e:
            raise Exception(e.output)

    def local_deploy():
        if not constants.ExecutionEnv.images_only():
            clean_log_dirs(token)

        # Write serialization to ~/.conducto/
        local_progdir = constants.ConductoPaths.get_local_path(pipeline_id)
        path_utils.makedirs(local_progdir, exist_ok=True)
        serialization_path = os.path.join(
            local_progdir, constants.ConductoPaths.SERIALIZATION
        )

        with open(serialization_path, "w") as f:
            f.write(serialization)
        path_utils.outer_chown(serialization_path)

        if not constants.ExecutionEnv.images_only():
            api.Pipeline().update(
                pipeline_id, {"program_path": serialization_path}, token=token
            )

        run_in_local_container(
            token, pipeline_id, inject_env=inject_env, is_migration=is_migration
        )

    if build_mode == constants.BuildMode.DEPLOY_TO_CLOUD:
        func = cloud_deploy
        starting = False
    elif build_mode == constants.BuildMode.DEPLOY_TO_K8S:
        func = k8s_deploy
        starting = False
    else:
        func = local_deploy
        starting = True

    if not constants.ExecutionEnv.headless():
        # Make sure that an agent is running before we launch. A local manager
        # will start it if none are running, but cloud has no way to do that.
        agent_utils.launch_agent(token=token)

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
        container_utils.refresh_image(manager_image, verbose=False)

    timestamp = datetime.datetime.now().strftime("%c")
    print(f"{msg} pipeline {pipeline_id} [{timestamp}]")

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

    data = api.Pipeline().get(pipeline_id, token=token)
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
    if t.Bool(os.getenv("CONDUCTO_INHERIT_GIT_VARS")):
        for k, v in os.environ.items():
            if k.startswith("CONDUCTO_GIT_"):
                inject_env[k] = v

    # The homedir inside the manager is /root. Mapping will be verified by manager,
    # internal to the container.
    inside_container = container_utils.get_current_container_id()
    local_profdir = container_utils.get_external_conducto_dir(
        is_migration or inside_container
    )
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
        "--label",
        "conducto",
    ]

    network_name = os.getenv("CONDUCTO_NETWORK", f"conducto_network_{pipeline_id}")
    if not is_migration:
        try:
            subp = client_utils.subprocess_run(
                ["docker", "network", "create", network_name] + labels
            )
            stderr = subp.stderr.decode()
            # this is the first docker command that we run, in the stderr we would probably catch a good number
            # of issues where the user's docker is misconfigured, i.e.:
            # Warning on attempt to create network:
            # WARNING: unable to read config file: open /Users/alwinpeng/.docker/config.json: permission denied
            # WARNING: Error loading config file: /Users/alwinpeng/.docker/config.json:
            # open /Users/alwinpeng/.docker/config.json: permission denied

            if "warning" in stderr.lower() or "error" in stderr.lower():
                print("Warning or error on attempt to create network: ")
                print(stderr)
        except client_utils.CalledProcessError as e:
            if f"network with name {network_name} already exists" in e.stderr.decode():
                pass
            else:
                raise

    if os.getenv("CONDUCTO_OUTER_OWNER"):
        outer_xid = os.getenv("CONDUCTO_OUTER_OWNER")
    elif not (hostdet.is_windows() or hostdet.is_wsl1()):
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
        "-e",
        f"CONDUCTO_EXECUTION_ENV={constants.ExecutionEnv.MANAGER_LOCAL}",
    ]

    if config.get_image_tag():
        # this is dev/test only so we do not always set it
        tag = config.get_image_tag()
        flags.extend(["-e", f"CONDUCTO_IMAGE_TAG={tag}"])

    if constants.ExecutionEnv.headless():
        flags.extend(["-e", "CONDUCTO_HEADLESS=1"])

    for env_var in (
        "CONDUCTO_URL",
        "CONDUCTO_DEV_REGISTRY",
        "CONDUCTO_USE_TEST_IMAGES",
        "CONDUCTO_USE_ID_TOKEN",
        "CONDUCTO_LOCAL_STANDBY",
        "CONDUCTO_HOST_ID",
        "CONDUCTO_IMAGES_ONLY",
    ):
        if os.environ.get(env_var):
            flags.extend(["-e", f"{env_var}={os.environ[env_var]}"])
    for k, v in inject_env.items():
        flags.extend(["-e", f"{k}={v}"])

    flags += container_utils.get_whole_host_mounting_flags(is_migration)

    if _manager_debug():
        if sys.stdin.isatty():
            flags[0] = "-it"
        else:
            flags[0] = "-i"
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
            data = api.Pipeline().get(pipeline_id, token=token)
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


def get_k8s_job_yaml(pipeline_id, token, inject_env):
    # Set additional CONDUCTO_ environment variables.
    if inject_env is None:
        inject_env = {}
    if t.Bool(os.getenv("CONDUCTO_INHERIT_GIT_VARS")):
        for k, v in os.environ.items():
            if k.startswith("CONDUCTO_GIT_"):
                inject_env[k] = v
    for env_var in (
        "CONDUCTO_URL",
        "CONDUCTO_DEV_REGISTRY",
        "CONDUCTO_USE_TEST_IMAGES",
        "CONDUCTO_USE_ID_TOKEN",
        "CONDUCTO_LOCAL_STANDBY",
        "CONDUCTO_HOST_ID",
        "CONDUCTO_IMAGES_ONLY",
        "CONDUCTO_IMAGE_TAG",
        "CONDUCTO_KUBECTL_OPTIONS",
        "CONDUCTO_K8S_SERVICE_ACCOUNT_NAME",
    ):
        if os.environ.get(env_var):
            inject_env[env_var] = os.environ[env_var]
    inject_env["CONDUCTO_EXECUTION_ENV"] = constants.ExecutionEnv.MANAGER_K8S
    inject_env["DOCKER_HOST"] = "tcp://localhost:2375"

    # Create yaml with environment variables.
    e = """
            - name: {}
              value: \"{}\" """
    env = ""
    for k, v in inject_env.items():
        env += e.format(k, v)

    # Create yaml elements for docker config.
    if os.environ.get("CONDUCTO_DEV_REGISTRY"):
        dev_docker_config_mount = """
          volumeMounts:
            - name: docker-config
              mountPath: /root/.conducto/.docker/
              readOnly: true
"""
        dev_docker_volume = f"""
        - name: docker-config
          secret:
            secretName: {constants.DEV_DOCKER_K8S_SECRET_NAME}
            items:
              - key: .dockerconfigjson
                path: config.json
"""
        dev_docker_secret = f"""
      imagePullSecrets:
        - name: {constants.DEV_DOCKER_K8S_SECRET_NAME}
"""
    else:
        dev_docker_config_mount = ""
        dev_docker_volume = ""
        dev_docker_secret = ""

    # Get manager image.
    config = api.Config()
    tag = config.get_image_tag()
    is_test = os.environ.get("CONDUCTO_USE_TEST_IMAGES")
    manager_image = constants.ImageUtil.get_manager_image(tag, is_test)

    # Set port for manager to serve on.
    manager_port = 55555

    service_account_name = os.environ.get(
        "CONDUCTO_K8S_SERVICE_ACCOUNT_NAME", "default"
    )

    # Generate yaml for manager job.
    return f"""
apiVersion: batch/v1
kind: Job
metadata:
  name: conducto-manager-{pipeline_id}
spec:
  template:
    spec:
      containers:
        - name: conducto-manager
          image: {manager_image}
          command: [
            "python",
            "-m",
            "manager.src",
            "-p",
            "{pipeline_id}",
            "--token",
            "{token}",
            "--k8s"
          ]
          #resources:
          #  limits:
          #    cpu: 0.5
          #    memory: 250M
          env:{env}
            - name: CONDUCTO_MANAGER_IP
              valueFrom:
                fieldRef:
                  fieldPath: status.podIP
            - name: CONDUCTO_MANAGER_PORT
              value: "{manager_port}"
          ports:
            - containerPort: {manager_port}
          {dev_docker_config_mount}
          imagePullPolicy: Always
        - name: dind-daemon
          image: docker:19.03-dind
          #resources:
          #  limits:
          #    cpu: 0.5
          #    memory: 250M
          env:
            - name: DOCKER_TLS_CERTDIR
              value: ""
          securityContext:
            privileged: true
          volumeMounts:
            - name: docker-graph-storage
              mountPath: /var/lib/docker
      volumes:
        - name: docker-graph-storage
          emptyDir: {{}}
        {dev_docker_volume}
      {dev_docker_secret}
      restartPolicy: Never
      serviceAccountName: {service_account_name}
  backoffLimit: 0
"""


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


# TODO: Give warning for max time cloud mode override
# https://app.clickup.com/t/98dt85
def _check_nodes_for_cloud(root):
    from rich.console import Console
    from rich.table import Table

    console = Console()

    adjusted_nodes = []
    for n in root.stream():
        if n.docker_run_args is not None:
            raise resource_validation.InvalidCloudParams(
                f"Node {n} uses docker_run_args, which is not supported for cloud mode.\n"
                f"  docker_run_args: {n.docker_run_args}"
            )
        if n.cpu is not None or n.mem is not None:
            req_cpu = n.get_inherited_attribute("cpu")
            req_mem = n.get_inherited_attribute("mem")
            try:
                new_cpu, new_mem = resource_validation.round_resources_for_cloud(
                    req_cpu, req_mem
                )
            except resource_validation.InvalidCloudParams:
                raise resource_validation.InvalidCloudParams(
                    f"Node {n} will not fit in the cloud. Limits:\n"
                    f"  cpu<=4 (requested: {req_cpu})\n"
                    f"  mem<=32 (requested: {req_mem})"
                )
            if new_cpu != req_cpu or new_mem != req_mem:
                adjusted_nodes.append((req_cpu, req_mem, new_cpu, new_mem, str(n)))

    if adjusted_nodes:
        table = Table("cpu", "mem", "node", title="Resources adjusted for cloud mode")
        adjusted_nodes.sort()
        if len(adjusted_nodes) > 10:
            idxs = [0, 1, 2, 3, 4, None, -5, -4, -3, -2, -1]
        else:
            idxs = range(len(adjusted_nodes))
        for idx in idxs:
            if idx is None:
                table.add_row("...", "...", "...")
            else:
                req_cpu, req_mem, new_cpu, new_mem, n = adjusted_nodes[idx]
                if req_cpu == new_cpu:
                    cpu_str = f"[dim]{new_cpu}[/dim]"
                else:
                    cpu_str = f"[red]{req_cpu}[/red]->[green]{new_cpu}[/green]"
                if req_mem == new_mem:
                    mem_str = f"[dim]{new_mem}[/dim]"
                else:
                    mem_str = f"[red]{req_mem}[/red]->[green]{new_mem}[/green]"
                table.add_row(cpu_str, mem_str, n)
        console.print(table)


def _manager_debug():
    return t.Bool(os.getenv("CONDUCTO_MANAGER_DEBUG"))


def _manager_cpu():
    return float(os.getenv("CONDUCTO_MANAGER_CPU", "1"))


def _auto_set_headless():
    if os.environ.get("CONDUCTO_HEADLESS") is None:
        try:
            client_utils.subprocess_run(["docker", "info"])
        except (client_utils.CalledProcessError, FileNotFoundError):
            log.debug("Docker not found, auto setting CONDUCTO_HEADLESS=1")
            os.environ["CONDUCTO_HEADLESS"] = "1"
