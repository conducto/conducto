import pipes
import asyncio, functools, logging, time
import concurrent.futures
from . import client_utils, log
import inspect

logger = logging.getLogger("conducto_async_utils")
logger.setLevel(logging.INFO)

"""Asyncio cheat sheet

Called *BEFORE* running loop:
 - asyncio.run*() # Begin *new* loop & run coroutine (any futures created before this call cannot be applied)
 - loop.run*() # Begin loop until condition

Called *DURING* loop:
 - asyncio.create_task() # Schedules a coro to *currently running* loop

Called *EITHER* during running/before loop:
 - loop.create_future() # Schedules awaitable to default loop
 - loop.call*() # schedules a callback
 - asyncio.get_*_loop() # Gets default loop, running or not
 - asyncio.ensure_future() # Schedules awaitable to be executed when loop is running
"""


def as_future(d):
    """Can be flexibly used with Deferreds or static values"""
    if asyncio.isfuture(d):
        return d
    elif asyncio.iscoroutine(d):
        return asyncio.ensure_future(d)
    else:
        fut = create_future()
        fut.set_result(d)
        return fut


def create_future():
    return asyncio.get_event_loop().create_future()


def cancel(*futs, allow_none=False):
    """Cancels and awaits all futures. Will not raise, regardless of terminating condition."""
    if allow_none:
        futs = [f for f in futs if f]
    [f.cancel() for f in futs]
    return asyncio.gather(*futs, return_exceptions=True)


# TODO: too lazy?
async def eval_in_thread(cb, *args, **kwargs):
    with concurrent.futures.ThreadPoolExecutor(1) as pool:
        # NOTE: get_running_loop would be nice here, but this is python 3.6 compatible
        loop = asyncio.get_event_loop()
        assert loop.is_running()
        return await loop.run_in_executor(pool, functools.partial(cb, *args, **kwargs))


# Usage:
# @to_async(threadpool): executes the function in the threadpool
# @to_async: each instance of the function is evaluated in a separate thread
def to_async(pool_or_fxn):

    if not isinstance(pool_or_fxn, concurrent.futures.ThreadPoolExecutor):
        assert not inspect.iscoroutinefunction(pool_or_fxn) and callable(pool_or_fxn)

        @functools.wraps(pool_or_fxn)
        async def _inner(*args, **kwargs):
            return await eval_in_thread(pool_or_fxn, *args, **kwargs)

        return _inner
    else:

        def decorator(fxn):
            @functools.wraps(fxn)
            async def _inner(*args, **kwargs):
                # NOTE: get_running_loop would be nice here, but this is python 3.6 compatible
                loop = asyncio.get_event_loop()
                assert loop.is_running()
                return await loop.run_in_executor(
                    pool_or_fxn, functools.partial(fxn, *args, **kwargs)
                )

            return _inner

        return decorator


def return_control_to_loop(iterable, max_time=0.25):
    """Return an asynchronous iterator that returns execution to the event loop every max_time seconds"""

    async def iterator():
        t = time.time()
        for i in iterable:
            yield i
            if time.time() - t > max_time:
                await asyncio.sleep(0)
                t = time.time()

    return iterator()


def done_future():
    res = asyncio.Future()
    res.set_result(None)
    return res


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
            self.last_task = done_future()

        to_await = self.last_task

        async def _coro():
            await to_await
            await async_fxn(self, *args, **kwargs)

        self.last_task = asyncio.create_task(_coro())

    return inner


def loop(func, interval, ignore_errors=True):
    """Call the given `func` in a perpetual loop, waiting for `interval` in between."""

    async def _loop():
        while True:
            try:

                check = func.func if isinstance(func, functools.partial) else func

                if asyncio.iscoroutinefunction(check):
                    # cancelling the loop should not interrupt the final call of func()
                    # these tasks are eventually finished in _wait_tasks
                    task = asyncio.create_task(func())
                    await asyncio.shield(task)
                else:
                    func()
            # Errors produced by `func` will be logged and will terminate the
            # loop, unless ignore_errors is True. If the (async) `func`
            # coroutine is cancelled, propagate cancellation w/o logging.
            except asyncio.CancelledError:
                raise
            except Exception as e:
                import traceback

                traceback.print_exc()
                logger.exception(e)
                if not ignore_errors:
                    raise
            await asyncio.sleep(interval)

    return asyncio.get_event_loop().create_task(_loop())


def async_cache(fxn):
    memo = {}

    async def wrapper(*args):
        key = tuple(args)
        if key not in memo:
            memo[key] = asyncio.ensure_future(fxn(*args))
        return await memo[key]

    return wrapper


async def run_and_check(*args, input=None, stop_on_error=True, shell=False, **kwargs):
    log.info("Running:", " ".join(pipes.quote(a) for a in args))
    if input is not None:
        log.info("stdin:", input)
    PIPE = asyncio.subprocess.PIPE
    if shell:
        proc = await asyncio.subprocess.create_subprocess_shell(
            *args, stdin=PIPE, stdout=PIPE, stderr=PIPE, **kwargs
        )
    else:
        try:
            proc = await asyncio.subprocess.create_subprocess_exec(
                *args, stdin=PIPE, stdout=PIPE, stderr=PIPE, **kwargs
            )
        except FileNotFoundError as e:
            cmd_str = " ".join(pipes.quote(a) for a in args)
            raise client_utils.CalledProcessError(
                -1, cmd_str, stdout="", stderr=str(e), stdin=input, msg=""
            )
    stdout, stderr = await proc.communicate(input=input)

    if stop_on_error and proc.returncode != 0:
        cmd_str = " ".join(pipes.quote(a) for a in args)
        raise client_utils.CalledProcessError(
            proc.returncode, cmd_str, stdout, stderr, msg="", stdin=input
        )
    else:
        return stdout, stderr
