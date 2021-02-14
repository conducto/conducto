import functools
import shlex

from conducto.shared import parse_utils


# cpu: [mem...]
ALLOWED_UNITS = {0.25: [0.5, 1], 0.5: [1, 2], 1: [2, 4], 2: [4, 8, 16], 4: [8, 16, 32]}

CPU_KEYS = sorted(ALLOWED_UNITS)


EXCLUDED_DOCKER_RUN_ARGS = {"--cpus", "--memory", "--network"}


class InvalidCloudParams(ValueError):
    pass


@functools.lru_cache(1000)
def round_resources_for_cloud(cpu, mem):
    for c in CPU_KEYS:
        if c >= cpu:
            for m in ALLOWED_UNITS[c]:
                if m >= mem:
                    return c, m
    raise InvalidCloudParams(f"Cannot fit cpu={cpu} and mem={mem} into Conducto Cloud")


def cpu(arg):
    """CPU can be 0.25, 0.5, or a whole number. TODO: AWS does have a maximum but we don't enforce that yet"""
    if arg is None:
        return None
    if not isinstance(arg, (float, int)):
        raise ValueError(f"Invalid value for cpu: {repr(arg)}")
    if arg > 0 and (arg == 0.25 or arg == 0.5 or int(arg) == arg):
        return arg
    raise ValueError(f"Invalid value for cpu: {repr(arg)}")


def gpu(arg):
    """GPU can be integers 0-4 right now."""
    if arg is None:
        return None
    if not isinstance(arg, int):
        raise ValueError(f"Invalid value for gpu: {repr(arg)}")
    if 0 <= arg <= 4:
        return arg
    raise ValueError(f"Invalid value for gpu: {repr(arg)}")


def mem(arg):
    if arg is None:
        return None
    if not isinstance(arg, (float, int)):
        raise ValueError(f"Invalid value for mem: {repr(arg)}")
    if arg > 0 and (arg == 0.5 or int(arg) == arg):
        return arg
    raise ValueError(f"Invalid value for mem: {repr(arg)}")


def image(arg):
    # check against container registry
    # TODO: https://app.clickup.com/t/8wgzqd
    return arg


def requires_docker(arg):
    if not isinstance(arg, (bool)):
        raise ValueError(f"Invalid value for requires_docker: {repr(arg)}")
    return arg


def seconds_from_max_time(max_time):
    if max_time is None:
        return None
    if isinstance(max_time, (float, int)):
        return float(max_time)
    return parse_utils.duration_string(str(max_time))


def max_time(arg):
    try:
        seconds = seconds_from_max_time(arg)
    except ValueError as err:
        raise ValueError(f"Invalid value for max_time: {repr(arg)}: {err}")
    if seconds > 0:
        return arg
    raise ValueError(f"Invalid value for max_time: {repr(arg)}")


def exectime(arg):
    if arg is None:
        return None
    import datetime

    if isinstance(arg, (datetime.time, datetime.datetime)):
        return arg
    raise ValueError(f"Invalid exectime: {repr(arg)}")


def docker_run_args(arg):
    if arg is None:
        return None
    if isinstance(arg, str):
        output = shlex.split(arg)
    else:
        try:
            output = list(arg)
        except TypeError:
            raise ValueError(
                f"Invalid value for docker_run_args, str or List[str] required: {repr(arg)}"
            )
        if not all(isinstance(a, str) for a in output):
            raise ValueError(
                f"Invalid value for docker_run_args, str or List[str] required: {repr(arg)}"
            )
    for a in output:
        prefix = a.split("=", 1)[0]
        if prefix in EXCLUDED_DOCKER_RUN_ARGS:
            raise ValueError(
                f"Invalid value for docker_run_args, cannot specify any of {EXCLUDED_DOCKER_RUN_ARGS}: {repr(arg)}"
            )
    return output
