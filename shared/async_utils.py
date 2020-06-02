import pipes
import asyncio, functools, logging, time
import concurrent.futures
from . import client_utils, log

logger = logging.getLogger("conducto_async_utils")
logger.setLevel(logging.INFO)

"""Asynio cheat sheet

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


def async_eager(fn):
    """Async functions with `@async_eager` will be executed inline as far as possible

    Such functions will mimic `@defer.inlineCallbacks` in the sense that the
    coroutine will run as much as possible (including nested coroutines) until
    a blocking condition is reached. The function will always return a future
    which:
    - Will be immediately "DONE" if the coroutine ran to completion
    - Will be "PENDING" if the coroutine reached a blocking condition

    `@async_eager` functions can be called pre-loop (without `await`) and during-loop
    (with or without `await`).
    """
    assert asyncio.iscoroutinefunction(fn), "Must use @async_eager on async function"

    @functools.wraps(fn)
    def _wrap(*args, **kwargs):
        coro = fn(*args, **kwargs)
        # Regardless of whether this coro can run to completion without
        # blocking, or becomes blocked and later requires an event loop to
        # complete, we return the result as a future so that callers can
        # await this function call in any situation.
        result = asyncio.get_event_loop().create_future()
        try:
            # Allow the coro to run as far as possible (including inner coros)
            # until blocked on some future. If the coro ran to completion,
            # `StopIteration` will be hit instead.
            blocked_on: asyncio.Future = coro.send(None)
            assert asyncio.isfuture(blocked_on), f"Expected Future, got {blocked_on}"
        except StopIteration as e:
            # ======== Ran to completion on first go ========
            # Set the result value immediately
            result.set_result(e.value)
            return result
        except Exception as e:
            # If we throw during an eager run, propagate the Exception to the
            # caller if we are not yet within a loop. Otherwise, set the
            # Exception on the Future.
            # This is to ensure similar behavior with `defer.inlineCallbacks`
            if asyncio.get_event_loop().is_running():
                result.set_exception(e)
                return result
            else:
                raise e
        else:
            # ======== Blocked on first go - Cannot complete yet ========
            # We need to schedule the coro to resume once the future is done.
            # Because the future has been yielded once already from the
            # underlying generator, the coro cannot be scheduled until *after*
            # the future is done. Otherwise, we will get
            # `RuntimeError("await wasn't used with future")` from
            # futures.py:262.
            def _on_done(fut: asyncio.Future):
                if fut._exception:
                    result.set_exception(fut.exception())
                else:
                    result.set_result(fut.result())

            def _resume_coro(fut: asyncio.Future):
                asyncio.create_task(coro).add_done_callback(_on_done)

            blocked_on.add_done_callback(_resume_coro)
            return result

    return _wrap


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


async def AsyncMakeInterruptible(fut, interrupt, tryToCancel=False):
    """
    Pairs the given future with another one that can interrupt it. If the
    future fires first, it returns normally. If the interrupt happens first,
    an InterruptedError is raised.
    """
    fut, interrupt = [asyncio.ensure_future(f) for f in [fut, interrupt]]
    done, _ = await asyncio.wait([fut, interrupt], return_when=asyncio.FIRST_COMPLETED)
    # Touch exceptions to avoid warnings
    [d.exception() for d in done if d._exception]

    if fut in done:  # fut is done, interrupt is _possibly_ done, but ignore it
        if fut._exception:
            raise fut._exception
        return fut.result()
    else:  # fut is _not_ done, but interrupt is done
        if tryToCancel:
            # TODO: (kzhang) If the original `fut` was not a `Future` and was
            # wrapped with `create_future()`, this operation isn't very helpful
            fut.cancel()
        if interrupt._exception:
            raise interrupt._exception
        raise InterruptedError(interrupt.result())


# TODO: too lazy?
async def eval_in_thread(cb, *args, **kwargs):
    with concurrent.futures.ThreadPoolExecutor(1) as pool:
        return await asyncio.get_running_loop().run_in_executor(
            pool, functools.partial(cb, *args, **kwargs)
        )


# Usage: @to_async(threadpool)
def to_async(pool):
    def decorator(fxn):
        async def _inner(*args, **kwargs):
            return await asyncio.get_running_loop().run_in_executor(
                pool, functools.partial(fxn, *args, **kwargs)
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


def loop(func, interval, ignore_errors=True):
    """Call the given `func` in a perpetual loop, waiting for `interval` in between."""

    async def _loop():
        while True:
            try:
                if asyncio.iscoroutinefunction(func):
                    await func()
                else:
                    func()
            # Errors produced by `func` will be logged and will terminate the
            # loop, unless ignore_errors is True. If the (async) `func`
            # coroutine is cancelled, propagate cancellation w/o logging.
            except asyncio.CancelledError:
                raise
            except Exception as e:
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


async def run_and_check(*args, input=None, stop_on_error=True, shell=False):
    log.info("Running:", " ".join(pipes.quote(a) for a in args))
    if input is not None:
        log.info("stdin:", input)
    PIPE = asyncio.subprocess.PIPE
    if shell:
        proc = await asyncio.subprocess.create_subprocess_shell(
            *args, stdin=PIPE, stdout=PIPE, stderr=PIPE
        )
    else:
        proc = await asyncio.subprocess.create_subprocess_exec(
            *args, stdin=PIPE, stdout=PIPE, stderr=PIPE
        )
    stdout, stderr = await proc.communicate(input=input)

    if stop_on_error and proc.returncode != 0:
        cmd_str = " ".join(pipes.quote(a) for a in args)
        raise client_utils.CalledProcessError(
            proc.returncode, cmd_str, stdout, stderr, "", stdin=input
        )
    else:
        return stdout, stderr
