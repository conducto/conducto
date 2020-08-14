import os
import sys
import signal
import asyncio
import json
import time
import traceback
import typing
import socket
import re
import select

import websockets

if sys.platform != "win32":
    import termios
    import tty
else:
    import msvcrt
    import win32api
from .. import api
from ..shared import constants, log, types as t
from ..shared.constants import State
import conducto.internal.host_detection as hostdet


if sys.version_info < (3, 7):
    # create_task is stdlib in 3.7, but we can declare it as a synonym for the
    # 3.6 ensure_future
    asyncio.create_task = asyncio.ensure_future


STATE_TO_COLOR = {
    State.PENDING: log.Color.TRUEWHITE,
    State.QUEUED: log.Color.GRAY,
    State.RUNNING: log.Color.BLUE,
    State.DONE: log.Color.GREEN,
    State.ERROR: log.Color.RED,
    State.WORKER_ERROR: log.Color.PURPLE,
}


class Listener(object):
    def update_node(self, name, data):
        pass

    async def background_task(self, title):
        pass

    async def key_press(self, char):
        # Listeners are passed the quit_func so that they can decide when to exit
        pass

    def render(self):
        pass

    def shutdown(self):
        pass


def connect(token: t.Token, pipeline_id: t.PipelineId, starthelp: str):
    pipeline = api.Pipeline().get(pipeline_id, token=token)

    ui = ShellUI(token, pipeline, starthelp)
    if sys.platform == "win32":
        win32api.SetConsoleCtrlHandler(ui.ctrl_c, True)
    try:
        asyncio.get_event_loop().run_until_complete(ui.run())
    except Exception:
        ui.reset_stdin()
        traceback.print_exc()


class ShellUI(object):
    def __init__(self, token, pipeline: dict, starthelp: str):
        self.pipeline = pipeline
        self.quitting = False
        self.loop = asyncio.get_event_loop()
        self.gw_socket = None
        self.start_func_complete = None
        self.starthelp = starthelp

        from . import one_line, full_screen

        self.listeners: typing.List[Listener] = [one_line.OneLineDisplay(self)]

    @property
    def allow_sleep(self):
        # TODO:  This is an ugly time-out to avoid shutting down the shell UI
        # because the NS cache still believes the pipeline is sleeping.
        return self.start_func_complete and time.time() > self.start_func_complete + 3.0

    async def view_loop(self):
        """
        Every 0.25 seconds render the pipeline
        """
        log.info("[view] starting")
        while True:
            await asyncio.sleep(0.25)
            for listener in self.listeners:
                listener.render()

    def set_gw(self, gw_socket):
        self.gw_socket = gw_socket

    async def wait_gw(self):
        while self.gw_socket is None:
            await asyncio.sleep(0.1)

    async def start_pipeline(self):
        if self.gw_socket is None:
            pipeline_id = self.pipeline["pipeline_id"]
            api.Manager().launch(pipeline_id)
            await self.wait_gw()

        payload = {"type": "SET_AUTORUN", "payload": {"value": True}}
        await self.gw_socket.send(json.dumps(payload))

    async def sleep_pipeline(self):
        if self.gw_socket is None:
            pipeline_id = self.pipeline["pipeline_id"]
            api.Pipeline().sleep_standby(pipeline_id)
        else:
            payload = {"type": "CLOSE_PROGRAM", "payload": None}
            await self.gw_socket.send(json.dumps(payload))

    async def reset(self):
        if self.gw_socket is None:
            pipeline_id = self.pipeline["pipeline_id"]
            api.Manager().launch(pipeline_id)
            await self.wait_gw()

        payload = {"type": "RESET", "payload": ["/"]}
        await self.gw_socket.send(json.dumps(payload))

    async def gw_socket_loop(self):
        """
        Loop and listen for socket messages
        """
        start_tasks = await self.run_start_func()

        pl = constants.PipelineLifecycle
        while True:
            if (
                self.pipeline is None
                or self.pipeline.get("status", None) not in pl.active
            ):
                await asyncio.sleep(0.5)
                continue

            if start_tasks is not None:
                tasks = start_tasks
                # we clear the start_tasks now since later reconnects should
                # show reconnecting.
                start_tasks = None
            else:
                msg = "Connection lost. Reconnecting"
                pretasks = [xx.background_task(msg) for xx in self.listeners]
                tasks = [asyncio.create_task(task) for task in pretasks]

            try:
                websocket = await api.connect_to_pipeline(self.pipeline["pipeline_id"])
            except PermissionError:
                print()
                print("You are not permitted to connect to this pipeline.")
                self.quit()
                break
            except ConnectionError:
                self.quit()
                break

            for task in tasks:
                task.cancel()

            for listener in self.listeners:
                listener.install_normal_key_mode()

            self.set_gw(websocket)

            was_slept = False

            try:
                await websocket.send(
                    json.dumps({"type": "RENDER_NODE", "payload": "/"})
                )

                log.info("[gw_socket_loop] starting")
                async for msg_text in websocket:
                    msg = json.loads(msg_text)
                    if msg["type"] in ("NODES_STATE_UPDATE", "RENDER_NODE"):
                        log.debug(f"incoming gw message {msg['type']}")
                        for name, data in msg["payload"].items():
                            for listener in self.listeners:
                                listener.update_node(name, data)
                    elif msg["type"] == "SLEEP":
                        was_slept = True
                        # we are done here, do not try to reconnect.
                        break
            except websockets.ConnectionClosedError as e:
                log.debug(f"ConnectionClosedError {e.code} {e.reason}")

            self.set_gw(None)
            if was_slept:
                break

    def get_ns_url(self):
        url = api.Config().get_url()
        url = re.sub("^http", "ws", url) + "/ns/"
        return url

    async def reconnect_ns(self):
        ns_url = self.get_ns_url()
        log.debug("[run] Connecting to", ns_url)
        header = {"Authorization": f"bearer {api.Config().get_token(refresh=False)}"}

        # we retry connection for roughly 2 minutes
        for i in range(45):
            try:
                websocket = await websockets.connect(ns_url, extra_headers=header)
                break
            except (
                websockets.ConnectionClosedError,
                websockets.InvalidStatusCode,
                socket.gaierror,
            ):
                log.debug(f"cannot connect to ns ... waiting {i}")
                await asyncio.sleep(min(3.0, (2 ** i) / 8))
        else:
            self.quit()
            return None

        log.debug("[run] ns Connected")
        return websocket

    async def ns_socket_loop(self):
        """
        Loop and listen for socket messages
        """
        while True:
            msg = "Connection lost. Reconnecting"
            if self.start_func_complete is not None:
                pretasks = [xx.background_task(msg) for xx in self.listeners]
            else:
                pretasks = []
            tasks = [asyncio.create_task(task) for task in pretasks]

            websocket = await self.reconnect_ns()
            for task in tasks:
                task.cancel()
            if websocket is None:
                if self.start_func_complete is not None:
                    for listener in self.listeners:
                        listener.install_disconnect_mode()
                self.quit()
                break

            if self.start_func_complete is not None:
                for listener in self.listeners:
                    listener.install_normal_key_mode()

            subscribe = {
                "type": "SUBSCRIBE",
                "payload": {"pipeline_id": self.pipeline["pipeline_id"]},
            }
            await websocket.send(json.dumps(subscribe))

            try:
                log.info("[ns_socket_loop] starting")
                async for msg_text in websocket:
                    msg = json.loads(msg_text)
                    if msg["type"] in ("FULL_INFO_UPDATE",):
                        log.debug(f"incoming ns message {msg['type']}")
                        progs = msg["payload"]["programIdToInfo"]

                        try:
                            self.pipeline = progs[self.pipeline["pipeline_id"]]
                        except KeyError:
                            # TODO:  the NS cache may not yet have the pipeline,
                            # this is to allow for that.
                            if self.allow_sleep:
                                raise
                            else:
                                continue

                        if "state" not in self.pipeline["meta"]:
                            self.pipeline["meta"] = {
                                "state": "pending",
                                "stateCounts": {x: 0 for x in STATE_TO_COLOR.keys()},
                            }

                        pl = constants.PipelineLifecycle
                        if self.pipeline["status"] in pl.sleeping and self.allow_sleep:
                            self.quit(display_reconnect=True)
                        elif self.pipeline["status"] not in pl.active:
                            for listener in self.listeners:
                                listener.update_node("/", self.pipeline["meta"])
            except websockets.ConnectionClosedError:
                pass

    def ctrl_c(self, a, b=None):
        # This is the windows control C handler
        self.quit(display_reconnect=True)
        return True

    async def key_loop(self):
        """
        Loop and listen for key inputs
        """
        log.info("[key_loop] starting")
        if sys.platform != "win32":
            self.old_settings = termios.tcgetattr(sys.stdin.fileno())
            tty.setraw(sys.stdin.fileno())
        async for char in stream_as_char_generator(self.loop, sys.stdin):
            if ord(char) in (3, 4):
                # Ctrl+c (sigint) & Ctrl+d (eof) get captured as a non-printing
                # characters with ASCII code 3 & 4 respectively. Quit
                # gracefully.
                self.quit(display_reconnect=True)
            elif ord(char) == 26:
                # Ctrl+z gets captured as a non-printing character with ASCII
                # code 26. Send SIGSTOP and reset the terminal.
                self.reset_stdin()
                os.kill(os.getpid(), signal.SIGSTOP)
                if sys.platform != "win32":
                    self.old_settings = termios.tcgetattr(sys.stdin.fileno())
                    tty.setraw(sys.stdin.fileno())

            for listener in self.listeners:
                await listener.key_press(char)
        self.reset_stdin()

    def reset_stdin(self):
        if hasattr(self, "old_settings"):
            termios.tcsetattr(sys.stdin.fileno(), termios.TCSADRAIN, self.old_settings)

    async def run_start_func(self):
        pretasks = [
            xx.background_task(self.starthelp, immediate=True) for xx in self.listeners
        ]
        tasks = [asyncio.create_task(task) for task in pretasks]

        self.start_func_complete = time.time()
        return tasks

    async def run(self):
        # Start all the loops. The view and socket loops are nonblocking The
        # key_loop needs to be run separately because it blocks on user input
        tasks = [
            self.loop.create_task(self.view_loop()),
            self.loop.create_task(self.gw_socket_loop()),
            self.loop.create_task(self.ns_socket_loop()),
            self.loop.create_task(self.key_loop()),
        ]

        # Wait on all of them. The `gather` variable can be cancelled in
        # `key_task()` if the user Ctrl+c's, which will cause the other loops
        # to be cancelled gracefully.
        self.gather_handle = asyncio.gather(*tasks)

        try:
            await self.gather_handle
        except asyncio.CancelledError:
            return
        except websockets.ConnectionClosedError:
            self.reset_stdin()
            return
        else:
            log.error("gather_handle returned but it shouldn't have!")
            raise Exception("gather_handle returned but it shouldn't have!")
        finally:
            for listener in self.listeners:
                listener.shutdown()

    def disconnect(self):
        self.quit(display_reconnect=True)

    def quit(self, display_reconnect=False):
        """
        Make all event loops quit
        """
        self.reset_stdin()
        if display_reconnect:
            # Observe the end of line handling:
            # 1) bare \n prints differently if in stdin in raw mode so that
            #    \r\n is really what we want here.
            # 2) elect end="" here because that leaves the cursor at a spot
            #    consistent with the one_line renderer (i.e. in the middle of
            #    no-where)
            print(
                log.format(
                    f"\r\nTo reconnect, run:"
                    f"\r\n{hostdet.host_exec()} -m conducto show --id={self.pipeline['pipeline_id']}",
                    color="cyan",
                ),
                end="",
            )
        self.quitting = True
        self.gather_handle.cancel()


def stdin_data():
    return select.select([sys.stdin], [], [], 0) == ([sys.stdin], [], [])


async def stream_as_char_generator(loop, stream):
    if sys.platform != "win32":
        has_key = stdin_data
        read_key = lambda: stream.read(1)
    else:
        has_key = msvcrt.kbhit
        read_key = lambda: msvcrt.getch().decode("ascii")

    while True:
        await asyncio.sleep(0.05)
        if has_key():
            char = read_key()
            if not char:  # EOF.
                break
            yield char
