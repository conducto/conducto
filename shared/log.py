import contextlib, hashlib, io, multiprocessing, os, re, sys, time, traceback
from . import types as t

try:
    import conducto.internal.host_detection as hostdet

    is_windows = hostdet.is_windows
except ImportError:
    is_windows = lambda: False


class Control:
    ERASE_LINE = "\033[K"


class Color:
    GRAY = 30
    RED = 31
    GREEN = 32
    YELLOW = 33
    BLUE = 34
    PURPLE = 35
    CYAN = 36
    TRUEWHITE = 37

    # The "WHITE" name below is for legacy reasons: it has been used in much of our
    # code to mean "reset to default foreground color" rather than "give me white".
    #
    # Note that although control code 38 does have a color-resetting function IN
    # CERTAIN LIMITED CIRCUMSTANCES, it is not actually defined in the VT100
    # standard.  39 is the standard code for returning to the default color.
    WHITE = DEFAULT = 39

    MAP = {
        # Short versions
        "a": GRAY,
        "r": RED,
        "g": GREEN,
        "y": YELLOW,
        "b": BLUE,
        "p": PURPLE,
        "c": CYAN,
        "t": TRUEWHITE,
        # full names
        "gray": GRAY,
        "red": RED,
        "green": GREEN,
        "yellow": YELLOW,
        "blue": BLUE,
        "purple": PURPLE,
        "cyan": CYAN,
        "white": TRUEWHITE,
    }

    # Use RANDOM to select a random color that is deterministic based on the input string
    RANDOM = "RANDOM"

    @staticmethod
    def Get(color):
        try:
            return Color.MAP[color.lower()]
        except KeyError:
            msg = "Unrecognized color code: {}. Known colors are: {}".format(
                color, Color.MAP
            )
            raise Exception(msg)


def format(s, color=None, bold=True, underline=False, dim=False):
    """
    Return a string representation of the given object, augmented with VT100 control
    codes to effect the requested appearance modifications.

    If specified, color must be (a) a legal VT100 foreground color integer, (b) one
    of the pre-defined VT100 color variables in class Color, or (c) True (which
    equates to specifying cyan).

    """
    if "TERM" not in os.environ and not is_windows():
        # Some terminals cannot handle colors, so don't add any codes.
        return s

    if t.Bool(os.environ.get("NO_COLOR")):
        color = None
        bold = None

    if color is True:
        color = Color.CYAN

    elif color == Color.RANDOM or str(color).lower() == "random":
        # Gray looks disabled and shouldn't be included in the allows random colors.
        randomColors = sorted({v for v in Color.MAP.values() if v != Color.GRAY})
        i = int(hashlib.md5(str(s).encode("utf-8")).hexdigest(), 16)
        color = randomColors[i % len(randomColors)]

    elif isinstance(color, str):
        color = Color.Get(color)

    codes = []
    if bold:
        codes.append("1")
    if dim:
        codes.append("2")
    if underline:
        codes.append("4")
    if color:
        codes.append(str(color))
    if not codes:
        return s
    codeStr = ";".join(codes)
    return f"\033[{codeStr}m{s}\033[0m"


def strip_format(string):
    regex = re.compile(
        r"""
        \033\[             # Start with '\033['
        (\d);              # '0;' is regular, '1;' is bold
        (\d+)m             # The number + 'm' is the end of the color sequence
        (.*?)              # This is the text that is colorized
        \033\[(?:0|0;0)m   # End with returning it to normal
    """,
        re.DOTALL | re.VERBOSE,
    )
    return re.sub(regex, "\\3", string)


class _utils(object):
    REPR_INT = "REPR_INT"
    REPR_NAME = "REPR_NAME"
    REPR_STR = "REPR_STR"

    DEBUG = 0
    INFO = 1
    LOG = 2
    WARN = 3
    ERROR = 4
    CRIT = 5

    ENV_VAR_NAME = "CONDUCTO_LOG_LEVEL"
    DEFAULT_LEVEL = 2

    LEVELS = [
        DEBUG,
        INFO,
        LOG,
        WARN,
        ERROR,
        CRIT,
    ]
    NAMES = {
        DEBUG: "DEBUG",
        INFO: "INFO",
        LOG: "LOG",
        WARN: "WARN",
        ERROR: "ERROR",
        CRIT: "CRIT",
    }
    REVERSE_NAMES = {v: k for k, v in NAMES.items()}

    @staticmethod
    def getReprType(lvl):
        """
        For the given log level argument, return its type: REPR_INT, REPR_NAME,
        or REPR_STR.

        For example:
            2      -> REPR_INT
            "INFO" -> REPR_NAME
            "2"    -> REPR_STR

        If `lvl` is not a valid log level, return None.

        """
        try:
            if isinstance(lvl, int) and min(_utils.LEVELS) <= lvl <= max(_utils.LEVELS):
                return _utils.REPR_INT
            if isinstance(lvl, str):
                if lvl in _utils.REVERSE_NAMES:
                    return _utils.REPR_NAME
                elif min(_utils.LEVELS) <= int(lvl) <= max(_utils.LEVELS):
                    return _utils.REPR_STR
            return None
        except Exception:
            return None

    @staticmethod
    def normalize(lvl):
        """
        Convert the given log level argument to its REPR_INT representation.
        Assume that `lvl` is a valid log level argument.

        For example:
            "INFO" -> 1
            "1"    -> 1
            2      -> 2

        """
        if lvl in base_logger.REVERSE_NAMES:
            return base_logger.REVERSE_NAMES[lvl]
        return int(lvl)

    @staticmethod
    def normalizedWrap(position, key=None, retain_repr=False):
        """
        Utility decorator factory that passes normalized (REPR_INT) version of log level argument regardless
        of what was actually passed and potentially converts results of wrapped function to original REPR.

        @param position Which argument representes log level and should be normalized
        @param key Which keyword argument represents log level and should be normalized
        @param retain_repr Whether to convert value returned by wrapped function to initial REPR
        """

        @decorator
        def decorate(func):
            def wrapped(*args, **kwargs):
                orig_val = None

                # Extract log level value from args/kwargs and normalize it
                if len(args) > position:
                    orig_val = args[position]
                    args = (
                        args[:position]
                        + (_utils.normalize(orig_val),)
                        + args[position + 1 :]
                    )
                if key in kwargs:
                    orig_val = kwargs[key]
                    kwargs[key] = _utils.normalize(orig_val)

                # Call function with normalized version of log arg/kwarg
                result = func(*args, **kwargs)

                # If user asked for it, convert returned value to original value
                if orig_val is not None and result is not None and retain_repr:
                    orig_type = _utils.getReprType(orig_val)
                    if orig_type == _utils.REPR_STR:
                        result = str(result)
                    elif orig_type == _utils.REPR_NAME:
                        return _utils.NAMES[result]

                return result

            return wrapped

        return decorate


########################################
class base_logger(object):
    CURRENT_LEVEL = _utils.DEFAULT_LEVEL
    _logLevels = []

    DEBUG = _utils.DEBUG
    INFO = _utils.INFO
    LOG = _utils.LOG
    WARN = _utils.WARN
    ERROR = _utils.ERROR
    CRIT = _utils.CRIT

    LEVELS = _utils.LEVELS
    NAMES = _utils.NAMES
    REVERSE_NAMES = _utils.REVERSE_NAMES

    _LOCK = multiprocessing.Lock()
    _IN_PROGRESS = False

    def __init__(self, level=None):
        self.level = level
        self.call_defaults = (
            dict()
        )  # defaults for certain keyword arguments to __call__
        self.format_defaults = (
            dict()
        )  # defaults for certain keyword arguments to _format
        # self.memoized__call__ = memoized(self.__call__, strict=False)

    @staticmethod
    def SetLogLevel(lvl):
        """
        Sets log level to provided value.
        """
        base_logger.CURRENT_LEVEL = base_logger.CapLevel(lvl)

    @staticmethod
    def NormalizeLevel(lvl):
        return _utils.normalize(lvl)

    @staticmethod
    # @_utils.normalizedWrap(0, retain_repr=True)
    def CapLevel(lvl):
        maxLvl = max(base_logger.LEVELS)
        minLvl = min(base_logger.LEVELS)
        return min(maxLvl, max(minLvl, lvl))

    @staticmethod
    # @_utils.normalizedWrap(0, retain_repr=True)
    def GetIncreasedLevel(lvl, delta=1):
        return base_logger.CapLevel(lvl + delta)

    @staticmethod
    @contextlib.contextmanager
    # @_utils.normalizedWrap(0)
    def LogLevelContext(lvl):
        useLvl = base_logger.CapLevel(lvl)
        styles = base_logger.GetLogLevels()
        yield styles.append(useLvl)
        assert styles[-1] == useLvl
        del styles[-1]

    @staticmethod
    def GetLogLevels():
        return base_logger._logLevels

    @staticmethod
    def GetCurrentLevel():
        return base_logger.CURRENT_LEVEL

    @staticmethod
    def GetLogLevel():
        levels = base_logger.GetLogLevels()
        if not levels:
            return base_logger.GetCurrentLevel()
        else:
            return levels[-1]

    @staticmethod
    def GetVerbosity():
        return max(base_logger.LEVELS) - base_logger.GetLogLevel()

    ####################################
    @staticmethod
    def _format(*args, **kwargs):
        """
        Return the string result of formatting *args according to **kwargs.

        Return the string ‹scaffold›‹message›, where

          ‹scaffold› ≔ `noscaff` ⁇ ∅ ∷ ‹sindent›‹ts›‹vmem›‹cframe›‹loglvl›
            ‹sindent›  ≔ ␣ * `sindent`
            ‹ts›       ≔ `notime`  ⁇ ∅ ∷ `timecolor`%m/%d/%Y %H:%M:%S.%f␣
            ‹vmem›     ≔ `nomem`   ⁇ ∅ ∷ `memcolor`‹current vmem usage›/‹max vmem usage›␣
            ‹cframe›   ≔ `noframe` ⁇ ∅ ∷ `framecolor`[‹source file of caller›‹cframel›]␣
              ‹cframel›  ≔ `noline`  ⁇ ∅ ∷ : ‹line number of caller in caller's source file›
            ‹loglvl›   ≔ `nolevel` ⁇ ∅ ∷ [`level`]

          ‹message›  ≔ `nomsg` ⁇ ∅ ∷ ‹mindent›`arg`⌈`delim``arg`…⌋
            ‹mindent›  ≔ ␣ * `mindent`

        Other keyword arguments and defaults
        ------------------------------------
        Each `no*` keyword argument defaults to False, except as indicated
        below.  Each `*color` argument may be passed as None to omit coloration.

        back ..... - in ‹cframe›, display info about the stack frame this many
                     levels farther (>0) or nearer (<0) in the call stack than
                     the call to this function (default: 0).  See CAUTION below.

        sindent .. - default: 0
        mindent .. - default: 2
        level   .. - default: None

        plain .... - if True (default: False), do not apply color to *any* part
                     of the returned string, regardless of *any* other kwarg
        timecolor  - default: bold Color.GRAY
        memcolor . - default: bold Color.GRAY
        framecolor - default: non-bold Color.TRUEWHITE
        color .... - Color to apply to each *arg (default: None)
        bold ..... - if True (default), and `color` is in effect, use bold ANSIColors.  No effect otherwise.
        underline  - if True and `color` is in effect, use underlines ANSIColors. No effect otherwise.

        nofw ..... - if True, do NOT enforce a fixed width on the memory and stack frame presentation

        CAUTION: This method is not intended for use by clients other than
        base_logger.__call__.  Other clients should take special care when
        specifying the `back` kwarg, for example.

        """
        noscaff = kwargs.get("noscaff", False)
        nomsg = kwargs.get("nomsg", False)

        back = kwargs.get("back", 0)
        notime = kwargs.get("notime", False)
        nomem = kwargs.get("nomem", False)
        noline = kwargs.get("noline", False)
        noframe = kwargs.get("noframe", False)
        nomethod = kwargs.get(
            "nomethod", False
        )  # TODO: Should default `False` for back-compat behavior?
        nolevel = kwargs.get("nolevel", False)

        plain = kwargs.get("plain", False)

        sindent = kwargs.get("sindent", 0)
        timecolor = kwargs.get("timecolor", Color.GRAY)
        memcolor = kwargs.get("memcolor", Color.GRAY)
        framecolor = kwargs.get("framecolor", Color.TRUEWHITE)
        nofw = kwargs.get("nofw", False)

        # Message
        #########
        level = kwargs.get("level", None)
        mindent = kwargs.get("mindent", 2)
        indentNewLines = kwargs.get("indentNewLines", None)
        bold = kwargs.get("bold", True)
        underline = kwargs.get("underline", False)
        color = kwargs.get("color", None)
        delim = kwargs.get("delim", " ")

        scaffoldPieces = []
        scaffold = io.StringIO()
        if not noscaff:
            # Construct scaffold
            if sindent > 0:
                # Add scaffold indentation
                scaffold.write(" " * sindent)

            # Add timestamp
            t = time.time()
            millis = ("%.3f" % (t % 1))[2:]
            ts = time.strftime("%m/%d/%Y %H:%M:%S", time.localtime(t)) + "." + millis
            if not plain and timecolor is not None:
                tsStr = format(ts, timecolor)
            else:
                tsStr = ts
            if not notime:
                scaffoldPieces.append(tsStr)

            # Add memory
            rss, vsize, cgroup_usage, cgroup_max_usage = memUsedDetail()
            if nofw:
                # In non-fixed-width mode we can choose to always show MB.
                if cgroup_max_usage > 0:
                    memMarkUp = "%d/%d|%d/%dM" % (
                        rss / 2 ** 20,
                        vsize / 2 ** 20,
                        cgroup_usage / 2 ** 20,
                        cgroup_max_usage / 2 ** 20,
                    )
                else:
                    memMarkUp = "%d/%dM" % (rss / 2 ** 20, vsize / 2 ** 20)
            else:
                # In fixed-width mode we use whatever SI suffix allows us to keep the
                # number short.
                maxMem = max(vsize, cgroup_max_usage)
                mult, unit = 1024 * 1024.0, "M"
                if maxMem > 9 * 1024 * 1024 * 1024:
                    mult, unit = 1024 * 1024 * 1024.0, "G"
                if cgroup_max_usage > 0:
                    memMarkUp = "%4d/%4d|%4d/%4d%s" % (
                        round(rss / mult),
                        round(vsize / mult),
                        round(cgroup_usage / mult),
                        round(cgroup_max_usage / mult),
                        unit,
                    )
                else:
                    memMarkUp = "%4d/%4d%s" % (
                        round(rss / mult),
                        round(vsize / mult),
                        unit,
                    )

            if not plain and memcolor is not None:
                memStr = format(memMarkUp, memcolor)
            else:
                memStr = memMarkUp
            if not nomem:
                scaffoldPieces.append(memStr)

            if not noframe:
                # Add one to getFileAndLine's `back` kwarg because we're a wrapper around
                # it.
                f, line, method = getFileAndLine(back=back + 1)

                # Finalize display of file name.
                f = re.sub(r"\.pyc?", "", f)
                if nofw:
                    # In non-fixed-width mode, display the entire file name, no padding.
                    fileDisplay = f
                else:
                    # In fixed-width mode, truncate the file name or add padding to fit
                    # the assigned space.
                    fileDisplay = "{:<12s}".format(f[:12])

                if nofw:
                    # In non-fixed-width mode, display the entire method name, no padding
                    methodDisplay = method
                else:
                    # In fixed-width mode, truncate the method name or add padding to fit
                    # the assigned space. Right align to be aesthetic.
                    methodDisplay = "{:>12s}".format(method[:12])

                # Finalize display of line number.
                if nofw:
                    # In non-fixed-width mode, display the line number, no padding.
                    lineDisplay = "{:d}".format(line)
                else:
                    # In fixed-width mode, pad all line numbers to 4 places and hope that
                    # we never see a line >= 10000.
                    lineDisplay = "{:4d}".format(line)

                frameMarkUp = "[{}".format(fileDisplay)
                if not noline:
                    frameMarkUp += ":{}".format(lineDisplay)
                if not nomethod:
                    frameMarkUp += ":{}".format(methodDisplay)
                frameMarkUp += "]"

                if not plain and framecolor is not None:
                    frameStr = format(
                        frameMarkUp, framecolor, bold=False
                    )  # do not bold the stack frame display
                else:
                    frameStr = frameMarkUp
                scaffoldPieces.append(frameStr)

            if level is not None and not nolevel:
                scaffoldPieces.append("[{:<5s}]".format(base_logger.NAMES[level]))

            # Join the scaffold pieces
            scaffold.write(" ".join(scaffoldPieces))
        scaffoldStr = scaffold.getvalue()

        message = io.StringIO()
        if not nomsg:
            # Construct message
            if mindent > 0:
                # Add message indentation
                message.write(" " * mindent)

            if len(args) > 0:
                # Write args
                if plain or color is None:
                    message.write(delim.join(str(arg) for arg in args))
                else:
                    message.write(
                        delim.join(
                            format(arg, color=color, bold=bold, underline=underline)
                            for arg in args
                        )
                    )
        messageStr = message.getvalue()
        if indentNewLines and 0:
            newKwargs = dict(kwargs)
            newKwargs.pop("indentNewLines")
            numSpaces = len(strip_format(base_logger._format(**newKwargs)))
            messageStr = " " * mindent + indent(messageStr, spaces=numSpaces).lstrip()

        return scaffoldStr + messageStr

    ####################################
    def applies(self):
        """
        Return True if the call self(x) would result in the printing of x.
        Otherwise return False.

        For example, if self.level is DEBUG but GetCurrentLevel() is INFO,
        return False.

        """
        return base_logger.GetLogLevel() <= self.level

    ####################################
    def __call__(self, *args, **kwargs):
        """
        Format the passed content as requested, write it to an output stream,
        and return it.

        However, if the current global base_logger LogLevel is greater than
        self.level, write nothing and return the empty string.  (Note that that
        return value is irregular; it is the same as the value returned when a
        completely empty string is printed.)

        Process the following keyword arguments to __call__; pass all others
        verbatim to _format except as described further below:

        - stderr : if True (default: call_defaults value, then False), write to
                   sys.stderr; otherwise write to sys.stdout.  Do not pass to
                   _format.

        - nonewln: if True (default: call_defaults value, then False), do not
                   write a terminal newline character to the stream after
                   writing the formatted content; otherwise do so.  In either
                   case, do not alter the returned string.  Do not pass to
                   _format.

        - level .: if specified and different from self.level, raise an
                   Exception.  If not specified (default), populate it with the
                   value of self.level before passing to _format.

        - back ..: if not specified, default to 0.  In either case, add 1 before
                   passing to _format.  (See _format for further discussion.)

        - once ..: if True (default: False), invoke a memoized version of this
                   function with all the same arguments, except:

                   - this argument, and
                   - `back`, which must first be adjusted to Do The Right Thing

                   and return the result.

        As mentioned above, consult `call_defaults` for values for `stderr` and
        `nonewln` if they were not passed.

        Similarly, prior to calling _format with the remaining kwargs, add to
        kwargs each entry in instance variable `format_defaults` that is not
        already present in kwargs.

        ------------
        For convenience, here is a review of the static base_logger methods that
        affect global LogLevel settings.  Refer to each directly for further
        documentation.

        SetLogLevel(lvl)     - Set the class-wide log level to `lvl`
        LogLevelContext(lvl) - Return a context manager within which the class-wide log level is `lvl`
        GetLogLevels()       - Return the (mutable) stack of class-wide log levels
        GetLogLevel()        - Return the current class-wide log level

        """
        if kwargs.pop("once", False):
            return self.memoized__call__(
                *args, back=kwargs.pop("back", 0) + 2, **kwargs
            )

        ############
        # Check instance for level (and kwargs for disagreement).  Pass level to
        # _format even if it was not passed to us.
        if self.level is None:
            raise Exception(
                "Attempted to write to a base_logger instance with no `level` attribute defined."
            )

        if "level" in kwargs:
            if self.level != kwargs["level"]:
                raise ValueError(
                    "Attempted to log at level {} using a base_logger at level {}".format(
                        kwargs["level"], self.level
                    )
                )
        else:
            # Assign the correct level for the below check and passing to
            # _format.
            kwargs["level"] = self.level

        # Short-circuit based on level.
        if base_logger.GetLogLevel() > kwargs["level"]:
            return ""

        handle = (
            sys.stderr
            if kwargs.pop("stderr", self.call_defaults.get("stderr", False))
            else sys.stdout
        )
        nonewln = kwargs.pop("nonewln", self.call_defaults.get("nonewln", False))

        # Add one to `back` before passing to _format because we are a wrapper
        # around _format.
        kwargs["back"] = kwargs.get("back", 0) + 1

        # Check instance for _format kwarg defaults.
        for key, val in self.format_defaults.items():
            if key not in kwargs:
                kwargs[key] = val

        msg = self._format(*args, **kwargs)

        # Write the output and return it.
        with self._LOCK:
            if self._IN_PROGRESS:
                handle.write("\n")
                self._IN_PROGRESS = False
            handle.write(msg)
            if not nonewln:
                handle.write("\n")
            handle.flush()
            return msg

    ################
    def setCallDefaults(self, **kwargs):
        """
        For each given k/v pair, register v as the default value for k in the
        event k is not provided as a kwarg to __call__.

        A key that is meaningless to __call__ will be stored, but __call__ will
        never access it.

        """
        for k, v in kwargs.items():
            self.call_defaults[k] = v

    ################
    def setFormatDefaults(self, **kwargs):
        """
        For each given k/v pair, register v as the default value for k in the
        event k is not provided as a kwarg to __call__ for pass-through to
        _format.

        A key that is meaningless to _format will be stored and passed to
        _format, but _format will ignore it.

        """
        for k, v in kwargs.items():
            self.format_defaults[k] = v


debug = base_logger(level=base_logger.DEBUG)
info = base_logger(level=base_logger.INFO)
log = base_logger(level=base_logger.LOG)
warn = base_logger(level=base_logger.WARN)
error = base_logger(level=base_logger.ERROR)
crit = base_logger(level=base_logger.CRIT)

########################################
# @_utils.normalizedWrap(0, 'default')
def getLogLevel(default=_utils.DEFAULT_LEVEL):
    return os.environ.get(_utils.ENV_VAR_NAME, default)


########################################
def addVerbosity(parser):
    import optparse, argparse

    if isinstance(parser, optparse.OptionParser):

        def setr(option, opt_str, value, parser):
            if value.upper() in base_logger.REVERSE_NAMES:
                value = base_logger.REVERSE_NAMES[value.upper()]
            else:
                value = int(value)
            parser.values._vincr = 0
            parser.values._vdecr = 0
            parser.values.loglevel = value

        def incr(option, opt_str, value, parser):
            if isinstance(parser.values.loglevel, str):
                parser.values.loglevel = int(parser.values.loglevel)

            parser.values.loglevel += 1
            if not hasattr(parser.values, "_vincr"):
                parser.values._vincr = 0
            parser.values._vincr += 1

        def decr(option, opt_str, value, parser):
            if isinstance(parser.values.loglevel, str):
                parser.values.loglevel = int(parser.values.loglevel)

            parser.values.loglevel -= 1
            if not hasattr(parser.values, "_vdecr"):
                parser.values._vdecr = 0
            parser.values._vdecr += 1

        default = getLogLevel()

        grp = parser.add_option_group(
            "Verbosity control. Log levels: {}".format(base_logger.NAMES)
        )
        grp.add_option(
            "--loglevel",
            action="callback",
            callback=setr,
            help="Set logging verbosity by number or keyword. default: {}.".format(
                default
            ),
            dest="loglevel",
            type=str,
            default=default,
        )
        grp.add_option(
            "--verbose",
            "-v",
            action="callback",
            callback=decr,
            help="Increase logging verbosity",
        )
        grp.add_option(
            "--quiet",
            "-q",
            action="callback",
            callback=incr,
            help="Decrease logging verbosity",
        )
    elif isinstance(parser, argparse.ArgumentParser):

        class Setr(argparse.Action):
            def __call__(self, parser, namespace, values, option_string):
                if values.upper() in base_logger.REVERSE_NAMES:
                    values = base_logger.REVERSE_NAMES[values.upper()]
                else:
                    values = int(values)
                namespace._vincr = 0
                namespace._vdecr = 0
                namespace.loglevel = values

        class Incr(argparse.Action):
            def __init__(self, *args, **kwargs):
                kwargs["nargs"] = 0
                super(Incr, self).__init__(*args, **kwargs)

            def __call__(self, parser, namespace, values, option_string):
                if isinstance(namespace.loglevel, str):
                    namespace.loglevel = int(namespace.loglevel)
                namespace.loglevel += 1
                if not hasattr(namespace, "_vincr"):
                    namespace._vincr = 0
                namespace._vincr += 1

        class Decr(argparse.Action):
            def __init__(self, *args, **kwargs):
                kwargs["nargs"] = 0
                super(Decr, self).__init__(*args, **kwargs)

            def __call__(self, parser, namespace, values, option_string):
                if isinstance(namespace.loglevel, str):
                    namespace.loglevel = int(namespace.loglevel)
                namespace.loglevel -= 1
                if not hasattr(namespace, "_vdecr"):
                    namespace._vdecr = 0
                namespace._vdecr += 1

        default = getLogLevel()

        grp = parser.add_argument_group(
            "Verbosity control. Log levels: {}".format(base_logger.NAMES)
        )
        grp.add_argument(
            "--loglevel",
            action=Setr,
            help="Set logging verbosity by number or keyword. default: {}.".format(
                default
            ),
            dest="loglevel",
            default=default,
        )
        grp.add_argument(
            "--verbose", "-v", action=Decr, help="Increase logging verbosity"
        )
        grp.add_argument(
            "--quiet", "-q", action=Incr, help="Decrease logging verbosity"
        )
    else:
        raise Exception(
            "Cannot add verbosity options to parser of type: {}".format(type(parser))
        )


########################################
def readVerbosity(opts):
    loglevel = _utils.normalize(opts.loglevel)
    incrs = getattr(opts, "_vincr", 0)
    decrs = getattr(opts, "_vdecr", 0)
    base_logger.SetLogLevel(loglevel)


def memUsedDetail():
    try:
        import psutil
    except ImportError:
        return 0, 0, 0, 0

    info = psutil.Process().memory_info()

    cgroup = "/sys/fs/cgroup/memory"
    if os.path.exists(cgroup):
        try:
            cgroup_usage = int(open(cgroup + "/memory.memsw.usage_in_bytes").read())
            cgroup_max_usage = int(
                open(cgroup + "/memory.memsw.max_usage_in_bytes").read()
            )
        except FileNotFoundError:
            cgroup_usage = int(open(cgroup + "/memory.usage_in_bytes").read())
            cgroup_max_usage = int(open(cgroup + "/memory.max_usage_in_bytes").read())
    else:
        cgroup_usage = 0
        cgroup_max_usage = 0

    return info.rss, info.vms, cgroup_usage, cgroup_max_usage


def getFileAndLine(back=0):
    """
    Return a 3-ple consisting of

    (a) the basename of the file (or other source) responsible for the stack frame
        that called this function, and
    (b) the line number in said file at which said call occurred.
    (c) the method from which the call was made

    Arguments

    back - report the frame an integer number of frames either farther (back>0) or
           less far (back<0) into the call stack than the one that called this
           function.  (default: 0)

           This is useful for implementing a wrapper around this function -- instead
           of reporting the (probably useless) file/line of the call to this
           function, back=1 means report the file/line of the call to the wrapper.

    """
    callStack = traceback.extract_stack()

    # Frame -1 is this frame.  Frame -2 is the calling frame.  Frame -3 would be 1
    # farther than the calling frame.
    framesFromEnd = 2 + back
    frameOfInterest = callStack[-framesFromEnd]

    basename = os.path.basename(frameOfInterest[0])
    if basename == "__init__.py":
        basename = os.path.basename(os.path.dirname(frameOfInterest[0])) + "/"

    return (basename, frameOfInterest[1], frameOfInterest[2])  # (file, line, method)


def unindent(string):
    """
    Return an unindented copy of the passed `string`.

    Replace each occurrence in `string` of a newline followed by spaces with a
    newline followed by the number of spaces by which said occurrence exceeds the
    minimum number of spaces in any such occurrence.  Further strip all whitespace
    from both ends of the resulting string before returning it.

    """
    # Expand tabs to be 4 spaces long
    string = string.replace("\t", "    ")

    # Find minimum indentation distance
    indents = [len(match) for match in re.findall("\n( +)\\S", string)]
    if indents:
        minIndent = min(indents)

        # Unindent by that much
        string = string.replace("\n" + " " * minIndent, "\n")
        string = string.strip()
        return string
    else:
        return string


def indent(string, spaces=4, tabs=0, notAtStart=False, notAtEnd=False):
    """
    Return a str copy of the input where the sequence

    - `tabs` (default: 0) horizontal tab characters, followed by
    - `spaces` (default: 4) space characters

    has been inserted

    - before the first character of the input, and
    - after every newline in the input

    If `notAtStart` is True (default: False), then do not insert the indentation
    sequence before the start of the input.

    If `notAtEnd` is True (default: False), then do not insert the indentation
    sequence at the end of the input even if the final character is a newline.

    NOTE: Encode each tab character as 'backslash t' rather than as ASCII HT.

    """

    indentationSequence = "\t" * tabs + " " * spaces
    newlineIndentationSequence = "\n" + indentationSequence

    result = io.StringIO()

    if not notAtStart:
        result.write(indentationSequence)

    if not notAtEnd or not string.endswith("\n"):
        result.write(string.replace("\n", newlineIndentationSequence))
    else:
        result.write(
            string.replace("\n", newlineIndentationSequence, string.count("\n") - 1)
        )

    return result.getvalue()


# Set the default log level from the environment, if it is specified
if _utils.ENV_VAR_NAME in os.environ:
    base_logger.SetLogLevel(_utils.normalize(getLogLevel()))
