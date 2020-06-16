import bisect
import functools


# Fargate supports different configurations of cpu/mem.
# See https://aws.amazon.com/fargate/pricing/
#
#     Mem                  1 1 1 1 1 1 1 1 1 1 2 2 2 2 2 2 2 2 2 2 3
# CPU|.5 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0
# .25| X X X
#  .5|   X X X X
#   1|     X X X X X X X
#   2|         X X X X X X X X X X X X X
#   4|                 X X X X X X X X X X X X X X X X X X X X X X X
#
# Make a list of them and binary search through it
class FG:
    def __init__(self, cpu, mem):
        self.cpu = cpu
        self.mem = mem

    def __lt__(self, other):
        return self.cpu < other.cpu or self.mem < other.mem


FG.ALLOWED = [
    *[FG(0.25, x) for x in [0.5, 1, 2]],
    *[FG(0.5, x) for x in range(1, 5)],
    *[FG(1, x) for x in range(2, 9)],
    *[FG(2, x) for x in range(4, 17)],
    *[FG(4, x) for x in range(8, 31)],
]


class InvalidCloudParams(ValueError):
    pass


@functools.lru_cache(1000)
def round_resources_for_fargate(cpu, mem):
    i = bisect.bisect_left(FG.ALLOWED, FG(cpu, mem))
    try:
        res = FG.ALLOWED[i]
    except IndexError:
        raise InvalidCloudParams(f"Cannot fit cpu={cpu} and mem={mem} into Fargate")
    return res.cpu, res.mem


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
    # TODO: check against container registry
    return arg


def requires_docker(arg):
    if not isinstance(arg, (bool)):
        raise ValueError(f"Invalid value for requires_docker: {repr(arg)}")
    return arg


def exectime(arg):
    if arg is None:
        return None
    import datetime

    if isinstance(arg, (datetime.time, datetime.datetime)):
        return arg
    raise ValueError(f"Invalid exectime: {repr(arg)}")
