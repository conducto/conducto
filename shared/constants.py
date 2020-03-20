import os
import string


def skip(base_state):
    return base_state + "_skipped"


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

    stopped |= {skip(s) for s in stopped}
    in_progress |= {skip(s) for s in in_progress}
    finished |= {skip(s) for s in finished}

    all = stopped | in_progress

    skipped = {s for s in all if s.endswith("_skipped")}
    unskipped = {s for s in all if not s.endswith("_skipped")}

    skip = skip


QSUB_KWARGS = ["cpu", "mem", "gpu", "image", "requires_docker"]


WORKER_VERSION = "v3.1.3"


class ImageUtil:
    MANAGER_VERSION = "0.1"

    @staticmethod
    def get_manager_image(tag):
        if tag is None:
            return f"conducto/manager:{ImageUtil.MANAGER_VERSION}"
        else:
            return f"manager-dev:{ImageUtil.MANAGER_VERSION}-{tag}"


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


class RdParams:
    WAIT_IN_USE_TIME = 180


class ConductoPaths:
    MOUNT_LOCATION = "/mnt/external"

    @staticmethod
    def s3_bucket():
        # Environment takes precedence
        if "CONDUCTO_S3_BUCKET" in os.environ:
            return os.environ["CONDUCTO_S3_BUCKET"]

        # Next try reading from the config file
        from .. import api
        import urllib.parse

        res = urllib.parse.urlparse(api.Config().get_url())
        if res.netloc.endswith(".conducto.io"):
            prefix = res.netloc.rsplit(".", 2)[0].lower()
            if "." not in prefix:
                return f"conducto-programs-{prefix}"

        # Otherwise just return a reasonable default
        return "conducto-programs-dev1"

    SERIALIZATION = "serialization"

    @staticmethod
    def get_pipeline_dir(user_id, pipeline_id):
        return f"{user_id}/{pipeline_id}"

    @staticmethod
    def get_local_base_dir():
        defaultBaseDir = os.path.join("~", ".conducto")
        baseDir = os.environ.get("CONDUCTO_BASE_DIR", defaultBaseDir)
        return os.path.expanduser(baseDir)

    @staticmethod
    def get_local_docker_config_dir():
        defaultBaseDir = os.path.join("~", ".docker")
        baseDir = os.environ.get("DOCKER_CONFIG_BASE_DIR", None)
        return os.path.expanduser(baseDir) if baseDir else None

    @staticmethod
    def get_local_path(pipeline_id):
        baseDir = ConductoPaths.get_local_base_dir()
        defaultLogDir = os.path.join(baseDir, "logs")
        logDir = os.environ.get("CONDUCTO_LOG_DIR", defaultLogDir)
        return os.path.expanduser(os.path.join(logDir, pipeline_id))


class PipelineLifecycle:
    # Local states
    DEPLOYING_LOCAL = "deploying_local"
    ACTIVE_LOCAL = "active_local"
    SLEEPING_LOCAL = "sleeping_local"
    # no STANDBY_LOCAL for now.

    # Cloud states
    DEPLOYING_CLOUD = "deploying_cloud"
    ACTIVE_CLOUD = "active_cloud"
    SLEEPING_CLOUD = "sleeping_cloud"
    STANDBY_CLOUD = "standby_cloud"

    local = {DEPLOYING_LOCAL, ACTIVE_LOCAL, SLEEPING_LOCAL}
    cloud = {DEPLOYING_CLOUD, ACTIVE_CLOUD, SLEEPING_CLOUD, STANDBY_CLOUD}

    deploying = {DEPLOYING_LOCAL, DEPLOYING_CLOUD}
    active = {ACTIVE_LOCAL, ACTIVE_CLOUD}
    standby = {STANDBY_CLOUD}
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


class SameContainer:
    """
    Consider the pipeline as a directed graph where all the parents point to their children
    Let's define a "valid" path as a path from a node to one of its descendants if all nodes on
    that path have same_container=SameContainer.INHERIT (the original node isn't counted as part of the path).

    Let's define a node v is reachable from a node u if there exists a valid path from u to v

    u with same_container=SameContainer.NEW forces all reachable v from u to run in the same container
    u with same_container=SameContainer.ESCAPE forces all reachable v from u to run with default behavior

    This translates into:
    SameContainer.NEW -> container_id is set to the node id
    SameContainer.ESCAPE -> container_id is force set to -1
    SameContainer.INHERIT -> container_id is deleted and regular inheritance applies
    """

    NEW = "new"
    ESCAPE = "escape"
    INHERIT = "inherit"
