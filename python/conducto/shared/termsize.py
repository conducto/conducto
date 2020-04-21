import functools, os, time

_THROTTLE_PER_S = (
    10  # Cache the terminal size to throttle computations to 10 per second.
)

# TODO (kzhang): We may not _need_ to throttle this. From what I read, getting
# the terminal size should take on the order of a few microseconds.
@functools.lru_cache(1)
def _getTerminalSize(_cur_time: int):
    """
    Return (NCOLS, NROWS) of the current terminal.

    @param: upon every `_cur_time` increment, `lru_cache` will compute a new value.
            `_cur_time` does not necessarily reflect the current time, just that an
            expiry period has passed.

    """
    env = os.environ

    # TODO (kzhang): Can we use `os.get_terminal_size()` and something similar for windows?
    def ioctl_GWINSZ(fd):
        try:
            import fcntl, termios, struct

            cr = struct.unpack("hh", fcntl.ioctl(fd, termios.TIOCGWINSZ, "1234"))
            if not isinstance(cr, tuple) or len(cr) != 2 or cr[0] <= 0 or cr[1] <= 0:
                return
        except:
            return
        return cr

    cr = ioctl_GWINSZ(0) or ioctl_GWINSZ(1) or ioctl_GWINSZ(2)
    if not cr:
        try:
            fd = os.open(os.ctermid(), os.O_RDONLY)
            cr = ioctl_GWINSZ(fd)
            os.close(fd)
        except:
            pass
    if not cr:
        cr = (env.get("LINES", 25), env.get("COLUMNS", 80))
    if env.get("FORCE_WIDTH", None):
        cr = (cr[0], int(env.get("FORCE_WIDTH")))
    return int(cr[1]), int(cr[0])


def getTerminalSize():
    """
    Return (NCOLS, NROWS) of the current terminal.

    """
    return _getTerminalSize(int(time.time() * _THROTTLE_PER_S))


def getTerminalWidth():
    return getTerminalSize()[0]


def getTerminalHeight():
    return getTerminalSize()[1]
