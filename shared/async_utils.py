import asyncio
import pipes
import subprocess
import traceback

from . import log


async def run_and_check(*args, input=None, stop_on_error=True):
    log.info("Running:", " ".join(pipes.quote(a) for a in args))
    if input is not None:
        log.info("stdin:", input)
    PIPE = asyncio.subprocess.PIPE
    proc = await asyncio.subprocess.create_subprocess_exec(
        *args, stdin=PIPE, stdout=PIPE, stderr=PIPE
    )
    stdout, stderr = await proc.communicate(input=input)
    if stop_on_error and proc.returncode != 0:
        cmd_str = " ".join(pipes.quote(a) for a in args)
        try:
            raise subprocess.CalledProcessError(
                proc.returncode, cmd_str, stdout, stderr
            )
        except:
            traceback.print_exc()
            raise
    else:
        return stdout, stderr
