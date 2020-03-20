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
