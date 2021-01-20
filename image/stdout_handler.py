import time
from conducto.shared import log
import itertools


def shrink_stdout_screen(stdout):
    import tempfile
    import pexpect

    with tempfile.NamedTemporaryFile(mode="w") as raw:

        raw.write(stdout)
        raw.flush()

        with tempfile.NamedTemporaryFile(mode="r") as cooked:
            log.log(f"Created temporary files")
            term1 = pexpect.spawn(
                "sh",
                env={"TERM": "xterm-256color"},
            )
            term1.sendline("ulimit -n 32")
            term1.sendline("screen")
            term1.sendline(f"cat {raw.name}")
            term1.sendline(f"screen -X hardcopy -h {cooked.name}")

            # you can also term1.expect("screen is terminating")
            # however the regex for expecting text ends up being a giant slowdown,
            # so instead we expect EOF with a timeout
            while True:
                try:
                    term1.expect(pexpect.EOF, timeout=0.5)
                    break
                except pexpect.TIMEOUT:
                    term1.sendline("exit")

            out = cooked.read().splitlines()
            for i in range(len(out)):
                if out[i].startswith("# cat"):
                    begin_idx = i + 1
                    break
            else:
                raise Exception

            for i in range(len(out) - 1, -1, -1):
                if out[i]:
                    #  sometimes there will be an additional new input line in the shell
                    #  once we see output, if we see blank shell line (starts with #), then we don't want to include
                    #  it, otherwise its still part of output from cat {raw.name}
                    end_idx = i + int(not out[i].startswith("#"))
                    break
            else:
                raise Exception
            if begin_idx >= end_idx:
                log.log(f"Parsing issue {out}")
            return "\r\n".join(out[begin_idx:end_idx])


def shrink_stdout_pyte(stdout):
    import pyte

    # this should be some default standardized between the backend and the frontend
    columns = 80

    # couldn't figure out how to get the terminal to auto resize when we exceeded the number of lines
    # hack solution: exponential search for the correct number of lines in O(N * log(line_count))
    # since we terminate early, if cursor jumps to new lines are distributed uniformly, the complexity
    # becomes O(N)
    def check_lines(n_lines):

        screen = pyte.Screen(lines=n_lines, columns=columns)
        stream = pyte.Stream(screen)

        for char in stdout:
            # Note: we cannot check the cursor position and call screen.resize above a certain
            # threshold, since  you can move up and down an arbitrary of lines,
            # and resize also resets the cursor position
            stream.feed(char)
            y = screen.cursor.y
            if y == n_lines - 1:
                return False
        return screen

    def display(screen):
        # shrink_stdout_screen gets raw text only, so i guess ignore special formatting here
        # to be consistent
        return screen.display

        def render(line):
            if not line:
                return
            # TODO (apeng) also preserve italics, background color, strikethrough
            # wrap format() around more than one line to not repeat escape sequences
            for (color, bold, underline), group in itertools.groupby(
                [line[i] for i in range(max(line) + 1)],
                key=lambda x: (x.fg, x.bold, x.underscore),
            ):
                yield log.format(
                    "".join(i.data for i in group),
                    color=color if color in log.Color.MAP else None,
                    bold=bold,
                    underline=underline,
                    force=True,
                )

        return ["".join(render(screen.buffer[y])) for y in range(screen.lines)]

    lines = 20
    while True:
        out = check_lines(lines)
        if out:
            back_line = out.default_char.data * columns
            d = display(out)
            while d and d[-1] == back_line:
                d.pop()
            return "\r\n".join(i.rstrip() for i in d)
        lines *= 2


def shrink_stdout_piecewise(stdout):
    fn = shrink_stdout_pyte if len(stdout) < 100000 else shrink_stdout_screen
    t = time.time()
    out = fn(stdout)
    log.log(
        f"Shrank {len(stdout)} -> {len(out)} in {time.time() - t} with {fn.__name__}"
    )
    return out
