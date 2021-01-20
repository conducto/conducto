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

    base = {PENDING, QUEUED, RUNNING, DONE, WARNING, ERROR, WORKER_ERROR}

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


class SleepReason:
    SLEEP_WHEN_DONE = "sleep_when_done"
    USER_SLEPT = "user_slept"
    DISCONNECTION = "disconnection"
    BULK_SLEPT = "bulk_slept"
    LAUNCH_ERROR = "launch_error"
    RUNTIME_ERROR = "runtime_error"
    RECONNECT_FAILURE = "reconnect_failure"


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
    START_VERSION = "0.1"

    @staticmethod
    def get_manager_image(tag, is_test):
        return ImageUtil._get_image("manager", ImageUtil.MANAGER_VERSION, tag, is_test)

    @staticmethod
    def get_agent_image(tag, is_test):
        return ImageUtil._get_image("agent", ImageUtil.AGENT_VERSION, tag, is_test)

    @staticmethod
    def get_start_image(tag, is_test):
        return ImageUtil._get_image("start", ImageUtil.START_VERSION, tag, is_test)

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
    DEPLOY_TO_K8S = "deploy_to_k8s"  # From a local machine, send to kubernetes
    ALREADY_IN_K8S = (
        "already_in_k8s"  # If it's already in kubernetes and now needs to run
    )


class ManagerAppParams:
    WAIT_TIME_SECS = 45
    POLL_INTERVAL_SECS = 0.25

    # limit per minute (per org)
    LAUNCH_THROTTLE = 10
    # total deploy/active/standby cloud programs (per org)
    ACTIVE_LIMIT = 100
    FREE_ACTIVE_LIMIT = 5

    WORKER_MAX_CONCURRENT = 200
    FREE_WORKER_MAX_CONCURRENT = 1


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
    S3_LOCATION = "/mnt/s3"
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
    def git_clone_dest(pipeline_id, url, branch) -> str:
        # Clean up URL: remove username/password, and sanitize the rest
        res = urllib.parse.urlparse(url)
        cleaner_url = res._replace(netloc=res.hostname).geturl()
        cleaned_url = re.sub(r"[^\w_:@?=\-]+", "_", cleaner_url)

        if not isinstance(branch, str):
            raise TypeError(f"Expected str. Got copy_branch={repr(branch)}")

        return f"{ConductoPaths.GIT_LOCATION}/{pipeline_id}:{cleaned_url}:{branch}"

    @staticmethod
    def s3_copy_dest(pipeline_id, url):
        cleaned_url = re.sub(r"[^\w_:@?=\-]+", "_", url)
        return f"{ConductoPaths.S3_LOCATION}/{pipeline_id}:{cleaned_url}"


# 8 characters, matches a host_id
HOST_ID_NO_AGENT = "*nohost*"
HOST_ID_CLOUDDEV = "clouddev"


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

    # Kubernetes states
    MANAGER_K8S = "manager_k8s"
    WORKER_K8S = "worker_k8s"
    DEBUG_K8S = "debug_k8s"

    # Interactive state
    EXTERNAL = "external"

    local = {MANAGER_LOCAL, WORKER_LOCAL, DEBUG_LOCAL, AGENT_LOCAL}
    cloud = {MANAGER_CLOUD, WORKER_CLOUD, DEBUG_CLOUD}
    k8s = {MANAGER_K8S, WORKER_K8S, DEBUG_K8S}

    worker_all = {
        DEBUG_LOCAL,
        DEBUG_CLOUD,
        DEBUG_K8S,
        WORKER_LOCAL,
        WORKER_CLOUD,
        WORKER_K8S,
    }
    worker = {WORKER_LOCAL, WORKER_CLOUD, WORKER_K8S}
    debug = {DEBUG_LOCAL, DEBUG_CLOUD, DEBUG_K8S}
    manager_all = {MANAGER_LOCAL, MANAGER_CLOUD, MANAGER_K8S, AGENT_LOCAL}
    manager = {MANAGER_LOCAL, MANAGER_CLOUD, MANAGER_K8S}
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

    @staticmethod
    def images_only():
        return t.Bool(os.getenv("CONDUCTO_IMAGES_ONLY"))


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

    inactive = standby | sleeping


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


class CONFIG_EVENTS:
    PR = "pr"
    PUSH = "push"
    CREATE_BRANCH = "create-branch"
    DELETE_BRANCH = "delete-branch"
    CREATE_TAG = "create-tag"
    DELETE_TAG = "delete-tag"
    CRON = "cron"

    events = {PR, PUSH, CREATE_BRANCH, DELETE_BRANCH, CREATE_TAG, DELETE_TAG, CRON}


# ec2provision instance statuses
class InstanceStatus:
    AVAILABLE = 0
    RESERVED = 1
    IN_USE = 2
    PREALLOCATED = 3
    PREALLOCATE_RESERVED = 4


# max times for remote docker ops
# ops taking longer than these limits indicate a faulty instance
class InstanceTransitionLimits:
    MAX_START = 120
    MAX_STOP = 600


class IntegrationStatus:
    # stored in sql db as MessageID (Integer)
    # printed to user as Message (String)
    class MessageID:
        # this may just be premature optimization
        # but I'm worried how we can't perfectly
        # clean up the database.
        NONE = "0"
        PENDING = "1"
        SUCCESS = "2"
        DEFERRED_CLOUD = "3"
        DEFERRED_LOCAL = "4"
        SUPPRESSED = "5"
        MISSING_CFG = "6"
        LAUNCH_ERROR = "7"
        INVALID_RESPONSE = "8"

    class Message:
        NONE = "<NO MESSAGE SET>"
        PENDING = ""
        SUCCESS = ""
        DEFERRED_CLOUD = "Deferred (.conducto.cfg setting)"
        DEFERRED_LOCAL = "Deferred (.conducto.cfg setting)"
        SUPPRESSED = "Suppressed (.conducto.cfg setting)"
        MISSING_CFG = "Missing .conducto.cfg"
        LAUNCH_ERROR = "Launch error"
        INVALID_RESPONSE = "Invalid response"

    messageID_to_UI = {
        MessageID.NONE: Message.NONE,
        MessageID.PENDING: Message.PENDING,
        MessageID.SUCCESS: Message.SUCCESS,
        MessageID.DEFERRED_CLOUD: Message.DEFERRED_CLOUD,
        MessageID.DEFERRED_LOCAL: Message.DEFERRED_LOCAL,
        MessageID.SUPPRESSED: Message.SUPPRESSED,
        MessageID.MISSING_CFG: Message.MISSING_CFG,
        MessageID.LAUNCH_ERROR: Message.LAUNCH_ERROR,
        MessageID.INVALID_RESPONSE: Message.INVALID_RESPONSE,
    }


DEV_DOCKER_K8S_SECRET_NAME = "conducto-dev-docker-config"
