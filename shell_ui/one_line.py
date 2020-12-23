import asyncio
import re
import types
import collections
from ansiwrap import ansilen

from . import Listener, STATE_TO_COLOR
from conducto.shared import log, termsize
from conducto.shared.constants import State


class OneLineDisplay(Listener):
    def __init__(self, outer):
        self.state_counts = None
        self.state = None
        self.outer = outer
        self.key_mode = None
        self.key_mode_install_func = None
        self.install_normal_key_mode()

    async def background_task(self, title, immediate=False):
        # wait one second to avoid needless blinking
        if not immediate:
            await asyncio.sleep(1)

        def marquee():
            dots = "   ...  "
            while True:
                for i in range(6):
                    yield dots[5 - i : 8 - i]

        def install_marquee(txt):
            output = KeyMode(txt)
            self.key_mode = output
            self.key_mode_install_func = lambda x=txt: install_marquee(x)

        for scroll in marquee():
            install_marquee(title + scroll)
            await asyncio.sleep(0.5)

    # Key modes
    def install_normal_key_mode(self):
        output = KeyMode(None)
        if self.state == State.PENDING:
            output.add_handler("&run", self.send_run_command)
        output.add_handler("&disconnect", self.install_confirm_quit)
        output.add_handler("&sleep", self.install_confirm_sleep)
        if self.state in State.finished:
            output.add_handler("&reset", self.send_reset_command)
        self.key_mode = output
        self.key_mode_install_func = self.install_normal_key_mode

    def install_confirm_quit(self):
        output = KeyMode("Do you want to disconnect monitor?")
        output.add_handler("&disconnect", self.disconnect)
        output.add_handler("&cancel", self.install_normal_key_mode)
        self.key_mode = output
        self.key_mode_install_func = self.install_confirm_quit

    def install_confirm_sleep(self):
        if self.state in State.in_progress:
            output = KeyMode("Put pipeline to sleep? This will KILL RUNNING NODES.")
        else:
            output = KeyMode("Put pipeline to sleep?")
        output.add_handler("&sleep", self.send_sleep_command)
        output.add_handler("&cancel", self.install_normal_key_mode)
        self.key_mode = output
        self.key_mode_install_func = self.install_confirm_sleep

    def install_slept(self):
        self.key_mode = KeyMode("Sleep signal sent.")
        self.key_mode_install_func = self.install_slept

    def install_disconnect_mode(self):
        self.key_mode = KeyMode("There is no internet connection.")
        self.key_mode_install_func = self.install_disconnect_mode

    def install_disconnected(self):
        self.key_mode = KeyMode("Disconnected.")
        self.key_mode_install_func = self.install_disconnected

    # Event handlers
    def update_node(self, name, data):
        if name == "/":
            old_state, self.state = self.state, data["state"]
            self.state_counts = collections.Counter(data["stateCounts"])
            if self.state != old_state:
                self.key_mode_install_func()

    async def key_press(self, char):
        if char.lower() in self.key_mode.keymap:
            func = self.key_mode.keymap[char.lower()]
            result = func()
            if asyncio.isfuture(result):
                await result
            elif isinstance(result, types.CoroutineType):
                await asyncio.ensure_future(result)

    # User-specified actions
    async def send_run_command(self):
        await self.outer.start_pipeline()

    async def send_sleep_command(self):
        self.install_slept()
        await asyncio.sleep(0.2)
        await self.outer.sleep_pipeline()

    async def send_reset_command(self):
        await self.outer.reset()

    async def disconnect(self):
        self.install_disconnected()
        await asyncio.sleep(0.2)
        self.outer.disconnect()

    # Rendering
    def state_count_str(self, state):
        count = int(self.state_counts[state] + self.state_counts[State.skip(state)])
        return log.format(count, bold=True, color=STATE_TO_COLOR[state])

    def render(self):
        if self.state_counts:
            state_str = self.state.replace(State.PENDING, "ready").upper()
            line = "{help}  {state}  P:{P}  Q:{Q}  R:{R}  D:{D}  E:{E}  K:{K}".format(
                help=self.key_mode.help(),
                state=log.format(
                    state_str, bold=False, color=STATE_TO_COLOR[self.state]
                ),
                P=self.state_count_str(State.PENDING),
                Q=self.state_count_str(State.QUEUED),
                R=self.state_count_str(State.RUNNING),
                D=self.state_count_str(State.DONE),
                E=self.state_count_str(State.ERROR),
                K=self.state_count_str(State.WORKER_ERROR),
            )
        else:
            line = self.key_mode.help()
        screen_size = termsize.getTerminalWidth()

        # NOTE: would use StringIO but we need to examine the string using
        # `ansilen` at each step
        output_line = ""
        for c in line:
            if ansilen(output_line) >= screen_size:
                break
            output_line += c

        print(f"\r{log.Control.ERASE_LINE}{output_line}", end="")

    def shutdown(self):
        print()


class KeyMode(object):
    def __init__(self, msg):
        self.msg = msg
        self.keymap = {}
        self.help_clauses = []

    def add_handler(self, text, func):
        help_str = re.sub(
            "&(.)",
            lambda m: log.format(m.group(1).upper(), bold=True, underline=True),
            text,
        )
        self.help_clauses.append(help_str)

        key = re.search("&(.)", text).group(1)
        self.keymap[key] = func

    def help(self):
        if self.msg and self.help_clauses:
            return f"[{self.msg} {'; '.join(self.help_clauses)}]"
        elif self.msg:
            return f"[{self.msg}]"
        else:
            return f"[{'; '.join(self.help_clauses)}]"
