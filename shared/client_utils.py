import sys
import shlex
import subprocess
from . import log
import asyncio

if sys.version_info < (3, 7):
    # create_task is stdlib in 3.7, but we can declare it as a synonym for the
    # 3.6 ensure_future
    asyncio.create_task = asyncio.ensure_future


def schedule_instance_async_non_concurrent(async_fxn):
    """
    Make it such that two calls of obj().async_fxn can never run at the same time
    if obj().async_fxn is called while it is already running, wait for the existing function
    to finish, and then schedule a new one

    Note: for a given object, two calls of its method may not run concurrently, but two different objects
    can have their async_fxn run at the same time

    Additionally changes an async function into a synchronous one that
    schedules the async function to run in the background
    """
    # raise NotImplementedError(
    #     "jmarcus believes this doesn't catch errors properly and we should instead use "
    #     "asyncio.Lock."
    # )

    def inner(self, *args, **kwargs):
        if not hasattr(self, "last_task"):
            res = asyncio.Future()
            res.set_result(None)
            self.last_task = res

        to_await = self.last_task

        async def _coro():
            await to_await
            await async_fxn(self, *args, **kwargs)

        self.last_task = asyncio.create_task(_coro())

    return inner


def isiterable(o):
    """
    Return True if the given object supports having iter() called on it BUT is not an
    instance of string.  Otherwise return False.

    """
    if isinstance(o, (str, bytes)):
        return False
    try:
        iter(o)
    except TypeError:
        return False
    else:
        return True


def makeIterable(o):
    """
    Return an iterable version of the given object.

    If the object already isiterable(), return it verbatim.  Otherwise return a list
    containing the object as its only element.

    """
    if isiterable(o):
        return o
    return [o]


class ExpectedOnlyOneError(AssertionError):
    pass


class GotZeroInsteadOfOneError(ExpectedOnlyOneError):
    pass


class GotManyInsteadOfOneError(ExpectedOnlyOneError):
    pass


def getOnly(values, msg=None):
    """
    Return the only element present in the given iterable argument.

    Parameters
    ----------
    values : iterable
        The iterable which is expected to only have one item.
    msg : str
        A message to pass into the exception, if necessary. The message
        will be formatted passing in the values as a named parameter. If a
        callable, then the values will be passed to the callable, and it should
        return the string to pass to the exception that will raise. If None,
        then a default message is provided.

    Raises
    ------
    TypeError
        If ``values`` is not iterable.
    GotZeroInsteadOfOneError
        When there are no values. The ``values`` attribute holds the values.
    GotManyInsteadOfOneError
        When there is more than one value. The ``values`` attribute holds the
        values.

    """
    if msg is None:
        msg = f"Expected exactly one value, but got {repr(values)}."
        msg = msg.format(values=repr(values))
    elif callable(msg):
        msg = msg(values)
    else:
        msg = msg.format(values=values)

    # Would be nice to use subclass of KwargException here, but too much
    # legacy code is expecting AssertionError already.
    if len(values) == 0:
        e = GotZeroInsteadOfOneError(msg)
        e.values = values
        raise e
    elif len(values) > 1:
        e = GotManyInsteadOfOneError(msg)
        e.values = values
        raise e
    else:
        return next(iter(values))


class CalledProcessError(subprocess.CalledProcessError):
    def __init__(self, returncode, cmd, stdout, stderr, msg, stdin=None):
        super().__init__(returncode, cmd, stdout, stderr)
        self.msg = msg
        self.stdin = stdin

    def __str__(self):
        parts = [self.msg, f"command: {self.cmd}", f"returncode: {self.returncode}"]
        if self.stdin is not None:
            parts.append(f"stdin: {self.stdin.decode()}")
        if self.stdout is not None:
            parts.append(f"stdout: {self.stdout.decode()}")
        if self.stderr is not None:
            parts.append(f"stderr: {self.stderr.decode()}")
        sep = "\n" + "-" * 80 + "\n"
        return sep.join(parts)


def subprocess_run(cmd, *args, shell=False, msg="", capture_output=True, **kwargs):
    if capture_output:
        # NOTE:  Python 3.6 does not support the capture_output parameter of
        # `subprocess.run`.  This translation supports 3.6 and later.
        kwargs["stdout"] = subprocess.PIPE
        kwargs["stderr"] = subprocess.PIPE
    try:
        result = subprocess.run(cmd, *args, shell=shell, **kwargs, check=True)
    except subprocess.CalledProcessError as e:
        if not shell:
            import pipes

            cmd = " ".join(pipes.quote(a) for a in cmd)
        raise CalledProcessError(e.returncode, cmd, e.stdout, e.stderr, msg) from None
    else:
        return result


class ByteBuffer(bytearray):
    def __init__(self, cb=None):
        super(ByteBuffer, self).__init__()
        self.cb = cb
        self.flush_idx = 0
        self.write = self.extend

    @schedule_instance_async_non_concurrent
    async def flush_util(self):
        to_send = bytes(self[self.flush_idx :])
        self.flush_idx += len(to_send)
        if to_send:
            await self.cb(to_send)

    def flush(self):
        if self.cb:
            self.flush_util()


def subprocess_streaming(
    *args, buf: ByteBuffer, input: bytes = None, timeout=None, **kwargs
):
    import pexpect

    cmd = " ".join(shlex.quote(a) for a in args)
    if input is not None:
        log.info(f"stdin: {input}")
        cmd = f"echo {shlex.quote(input.decode('utf-8'))} | {cmd}"

    cmd = f"sh -c {shlex.quote(cmd)}"

    log.info(f"Running: {cmd}")

    # `buf` may already be populated. Record its current length so that if we have an
    # error message we can include only the portion from this command.
    start_idx = len(buf)

    output, exitstatus = pexpect.run(
        cmd,
        logfile=buf,
        timeout=timeout,
        withexitstatus=1,
        **kwargs,
    )

    if exitstatus != 0:
        output = bytes(buf[start_idx:])
        raise CalledProcessError(
            exitstatus, cmd, output.decode("utf-8"), stderr=None, msg="", stdin=input
        ) from None
