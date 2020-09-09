import os
import re
import string
import urllib.parse
from . import types as t


def skip(base_state):
    if base_state in State.skipped:
        return base_state
    return base_state + "_skipped"


def unskip(skipped_state):
    if skipped_state in State.unskipped:
        return skipped_state
    return skipped_state[:-8]


class State:
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    DONE = "done"
    WARNING = "warning"
    ERROR = "error"
    WORKER_ERROR = "worker_error"

    stopped = {PENDING, DONE, WARNING, ERROR, WORKER_ERROR}
    in_progress = {QUEUED, RUNNING}
    finished = {DONE, WARNING, ERROR, WORKER_ERROR}
    failed = {ERROR, WORKER_ERROR}

    stopped |= {s + "_skipped" for s in stopped}
    in_progress |= {s + "_skipped" for s in in_progress}
    finished |= {s + "_skipped" for s in finished}

    all = stopped | in_progress

    skipped = {s for s in all if s.endswith("_skipped")}
    unskipped = {s for s in all if not s.endswith("_skipped")}

    skip = skip
    unskip = unskip


class Perms:
    LAUNCH = "launch"
    CHANGE_STATE = "change_state"
    MODIFY = "modify"
    MODIFY_CMD = "modify_cmd"
    READ = "read"
    READ_ORG_SECRETS = "read_org_secrets"
    READ_USER_SECRETS = "read_user_secrets"
    all = [
        LAUNCH,
        CHANGE_STATE,
        MODIFY,
        MODIFY_CMD,
        READ,
        READ_ORG_SECRETS,
        READ_USER_SECRETS,
    ]


QSUB_KWARGS = ["cpu", "mem", "gpu", "image", "requires_docker"]

WORKER_VERSION = "v3.1.3"


class ImageUtil:
    MANAGER_VERSION = "0.1"
    AGENT_VERSION = "0.1"

    @staticmethod
    def get_manager_image(tag, is_test):
        return ImageUtil._get_image("manager", ImageUtil.MANAGER_VERSION, tag, is_test)

    @staticmethod
    def get_agent_image(tag, is_test):
        return ImageUtil._get_image("agent", ImageUtil.AGENT_VERSION, tag, is_test)

    @staticmethod
    def _get_image(base, version, tag, is_test):
        if tag is None:
            image = f"conducto/{base}:{version}"
            if is_test:
                image += "-test"
            return image
        else:
            image = f"{base}-dev:{version}-{tag}"
            registry = os.environ.get("CONDUCTO_DEV_REGISTRY")
            if registry:
                return f"{registry}/{image}"
            else:
                return image


class BuildMode:
    LOCAL = "local"  # Run task workers on local machine.
    DEPLOY_TO_CLOUD = (
        "deploy_to_cloud"  # From a local machine, send to AWS for Cloud run
    )
    ALREADY_IN_CLOUD = (
        "already_in_cloud"  # If it's already in AWS and now needs to be run
    )
    SHELL = "shell"  # Skip all the fancy interfaces and just run in shell. Debug usage only.
    TEST = "test"  # Write no logs to disk, don't register, just run in shell then return when done.


class ManagerAppParams:
    WAIT_TIME_SECS = 45
    POLL_INTERVAL_SECS = 0.25


class GwParams:
    # 6 MB
    WEBSOCKET_FRAME_BYTES = 6 * 1024 ** 2
    # 5 MB
    MAX_DATA_BYTES = 5 * 1024 ** 2


class RdParams:
    WAIT_IN_USE_TIME = 180


class ConductoPaths:
    MOUNT_LOCATION = "/mnt/external"
    GIT_LOCATION = "/mnt/git"
    COPY_LOCATION = "/mnt/conducto"
    SERIALIZATION = "serialization"

    @staticmethod
    def get_local_base_dir(expand=True):
        defaultBaseDir = os.path.join("~", ".conducto")
        base_dir = os.environ.get("CONDUCTO_BASE_DIR", defaultBaseDir)
        if expand:
            base_dir = os.path.expanduser(base_dir)
        return base_dir

    @staticmethod
    def get_local_docker_config_dir(expand=True):
        base_dir = os.environ.get("DOCKER_CONFIG_BASE_DIR", None)
        if expand and base_dir:
            base_dir = os.path.expanduser(base_dir)
        return base_dir

    @staticmethod
    def get_profile_base_dir(expand=True, profile=None):
        import conducto.api as api

        conducto_root = ConductoPaths.get_local_base_dir(expand=expand)
        if ExecutionEnv.value() == ExecutionEnv.WORKER_CLOUD:
            return conducto_root
        else:
            if not profile:
                # only load api.Config() if necessary
                profile = api.Config().default_profile
            if profile is None:
                # TODO:  This is almost surely an error that deserves an
                # exception, but that causes too much trouble for tonight.
                return conducto_root
            return os.path.join(conducto_root, profile)

    @staticmethod
    def get_local_path(pipeline_id, expand=True, base=None):
        import conducto.api as api

        if base:
            base_dir = base
        else:
            base_dir = ConductoPaths.get_local_base_dir(expand=False)
        profile = api.Config().default_profile
        if profile is None:
            profile = "logs"
        log_dir = os.path.join(base_dir, profile, "pipelines")
        if expand:
            return os.path.expanduser(os.path.join(log_dir, pipeline_id))
        else:
            return os.path.join(log_dir, pipeline_id)

    @staticmethod
    def git_clone_dest(pipeline_id, copy_repo, copy_url, copy_branch):
        if copy_repo is not None:
            # TODO: this should depend on owner once we know it, not just repo
            base = copy_repo
        elif copy_url is not None:
            # Clean up URL: remove username/password, and sanitize the rest
            res = urllib.parse.urlparse(copy_url)
            url = res._replace(netloc=res.hostname).geturl()
            base = re.sub("[^\w_:@?=\-]+", "_", url)
        else:
            raise ValueError("Cannot clone if copy_repo and copy_url are both None")

        if not isinstance(copy_branch, str):
            raise TypeError(f"Expected str. Got copy_branch={repr(copy_branch)}")

        return f"{ConductoPaths.GIT_LOCATION}/{pipeline_id}:{base}:{copy_branch}"


# 8 characters, matches a host_id
HOST_ID_NO_AGENT = "*nohost*"


class ExecutionEnv:
    # environment -- CONDUCTO_EXECUTION_ENV

    # Local states
    MANAGER_LOCAL = "manager_local"
    WORKER_LOCAL = "worker_local"
    DEBUG_LOCAL = "debug_local"
    AGENT_LOCAL = "agent_local"

    # Cloud states
    MANAGER_CLOUD = "manager_cloud"
    WORKER_CLOUD = "worker_cloud"
    DEBUG_CLOUD = "debug_cloud"

    # Interactive state
    EXTERNAL = "external"

    local = {MANAGER_LOCAL, WORKER_LOCAL, DEBUG_LOCAL, AGENT_LOCAL}
    cloud = {MANAGER_CLOUD, WORKER_CLOUD, DEBUG_CLOUD}

    worker_all = {DEBUG_LOCAL, DEBUG_CLOUD, WORKER_LOCAL, WORKER_CLOUD}
    worker = {WORKER_LOCAL, WORKER_CLOUD}
    debug = {DEBUG_LOCAL, DEBUG_CLOUD}
    manager_all = {MANAGER_LOCAL, MANAGER_CLOUD, AGENT_LOCAL}
    manager = {MANAGER_LOCAL, MANAGER_CLOUD}
    agent = {AGENT_LOCAL}

    @staticmethod
    def value():
        return os.getenv("CONDUCTO_EXECUTION_ENV", ExecutionEnv.EXTERNAL)

    @staticmethod
    def headless():
        """
        Headless/Agent-less modes, primarily for manager spawned from a Git webhook.
        * The webhook is the same as External but it is not interactive, has no profile
          directory, and should launch no Agent.
        * It creates a Cloud manager that is mostly normal but cannot use an Agent.
        * For debugging we may create a Local manager that does not use an Agent.
        * Workers run from Headless pipelines should not create Images that cannot be
          built without an Agent.
        """
        return t.Bool(os.getenv("CONDUCTO_HEADLESS"))


class PipelineLifecycle:
    # Local states
    DEPLOYING_LOCAL = "deploying_local"
    ACTIVE_LOCAL = "active_local"
    SLEEPING_LOCAL = "sleeping_local"
    STANDBY_LOCAL = "standby_local"

    # Cloud states
    DEPLOYING_CLOUD = "deploying_cloud"
    ACTIVE_CLOUD = "active_cloud"
    SLEEPING_CLOUD = "sleeping_cloud"
    STANDBY_CLOUD = "standby_cloud"

    local = {DEPLOYING_LOCAL, ACTIVE_LOCAL, SLEEPING_LOCAL, STANDBY_LOCAL}
    cloud = {DEPLOYING_CLOUD, ACTIVE_CLOUD, SLEEPING_CLOUD, STANDBY_CLOUD}

    deploying = {DEPLOYING_LOCAL, DEPLOYING_CLOUD}
    active = {ACTIVE_LOCAL, ACTIVE_CLOUD}
    standby = {STANDBY_LOCAL, STANDBY_CLOUD}
    sleeping = {SLEEPING_LOCAL, SLEEPING_CLOUD}


class Hashing:
    PRIME_BASE = 1000000007
    PRIME_MOD = (1 << 127) - 1
    b62alphabet = string.ascii_uppercase + string.ascii_lowercase + string.digits

    @staticmethod
    def hash(hash_this, base=1):
        for i in hash_this:
            base *= Hashing.PRIME_BASE
            base += ord(i)
            base %= Hashing.PRIME_MOD
        return base

    @staticmethod
    def encode(integer_hash):
        res = []
        while integer_hash:
            res.append(Hashing.b62alphabet[integer_hash % 62])
            integer_hash //= 62
        return "".join(res)


class ContainerReuseContext:
    """

    Consider the pipeline as a directed graph where all the parents point to their children
    Let's define a "valid" path as a path from a node to one of its descendants if all nodes on
    that path have container_reuse_context=None (the original node isn't counted as part of the path).

    Let's define a node v is reachable from a node u if there exists a valid path from u to v

    u with container_reuse_context=ContainerReuseContext.NEW forces all reachable v from u to run in the same container
    u with container_reuse_context=ContainerReuseContext.GLOBAL forces all reachable v from u to run with default behavior

    This translates into:
    ContainerReuseContext.NEW -> container_id is set to the node id
    ContainerReuseContext.GLOBAL -> container_id is force set to -1
    None -> container_id is deleted and regular inheritance applies
    """

    NEW = "new"
    GLOBAL = "global"


# deprecated
class SameContainer:
    """
    Deprecated.  See ContainerReuseContext.
    """

    NEW = ContainerReuseContext.NEW
    ESCAPE = ContainerReuseContext.GLOBAL
    INHERIT = None


# TODO: https://app.clickup.com/t/8jcxwd
class ErrorCodes:
    NODE_MAX_TIME_EXCEEDED = "Node max time exceeded"


class Block:
    SIZE = 3600
    # how many seconds into block n + 1 do we start generating a snapshot for block n
    DELAY = 900
