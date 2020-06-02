import argparse
import collections
import copy
import datetime
import inspect
import pipes
import typing

from ..shared import client_utils, log, types as t, kwarg_exception, termsize


class GlueArgumentError(kwarg_exception.KwargException):
    defaultMessage = "Error constructing or parsing glue argument"


# TODO (kzhang): Do we need `NO_DEFAULT` or can we use `inspect._empty`?
NO_DEFAULT = "__no_default__"
CALLEE_ARG = "__callee_arg__"
MANUAL_ARG = "__manual_arg__"
DEFAULT_FROM_FUNC = "__default_from_func__"

UNSET = object()


def is_collection_type(typ):
    typeclasses = (typing.List[str].__class__, typing.List[int].__class__)
    return isinstance(typ, typeclasses)


class _Args(object):
    """
    Container for many arguments. Exists for:
     - Organizing arguments of many functions in such a way that help can be easier
     - Providing a translation layer between command line formats and formats
       for calling the functions that use them
    """

    __slots__ = ["args", "helps", "helpStr", "ignore"]

    def __init__(self, args=None, helpStr=None):
        # A uniquely key-ed list of arguments. No duplicates, because that makes no sense for a command line parser.
        self.helpStr = helpStr
        self.args = collections.OrderedDict()
        self.helps = collections.defaultdict(list)

        self.ignore = set()

        if args:
            if isinstance(args, (dict, _Args)):
                values = args.values()
            elif isinstance(args, list):
                values = args
            else:
                raise ValueError()
            for arg in values:
                self.addArgument(arg)

    def addArgument(self, arg, checkForDuplicates=False):
        self.helps[arg.name].append((arg.source, arg.help))
        if arg.name not in self.args:
            self.args[arg.name] = arg
        elif checkForDuplicates:
            existingArg = self.args[arg.name]
            if CALLEE_ARG in (arg.default, existingArg.default):
                # Can't expect matching types if the other argument is positional
                return

            log.debug("Not adding:", arg)
            if type(arg) != type(existingArg):
                log.warn(
                    "Found duplicated, and inconsistent, definition of {}.\n  {}\n  {}".format(
                        arg.name, arg, existingArg
                    )
                )
            if arg.type != existingArg.type:
                log.warn(
                    "Found duplicated, and inconsistent, definition of {}.\n  {}\n  {}".format(
                        arg.name, arg, existingArg
                    )
                )
        else:
            pass

    def ignoreArgs(self, args):
        self.ignore |= set(args)

    def addArgs(
        self,
        other,
        handleArgsWithNoDefaults=False,
        allowPositional=False,
        ignoreArgs=True,
        checkForDuplicates=False,
    ):
        if ignoreArgs:
            self.ignore |= other.ignore

        for arg in other.args.values():
            # If my caller knows it wants some args ignored, do that here.
            if arg.name in self.ignore:
                continue
            if not allowPositional:
                if arg.positional:
                    raise Exception(
                        "Tried to add positional argument {}. Positional arguments can only be defined on for CLI use, should never be called from within another function.".format(
                            arg.name
                        )
                    )

            if handleArgsWithNoDefaults:
                # If the Args are from a callee or for reading from the command
                # line, we need to change handling of args with no defaults.
                # The caller may be responsible for setting these arguments in
                # the call, so they shouldn't be considered "required" when
                # calling the outer function.
                arg = arg.copy()
                if arg.default == NO_DEFAULT:
                    arg.default = CALLEE_ARG
                log.debug("Adding:", arg)
            self.addArgument(arg, checkForDuplicates=checkForDuplicates)
        return self

    def getCommandlineParser(self, **kwargs):
        width = termsize.getTerminalWidth()
        parser = argparse.ArgumentParser(
            formatter_class=lambda prog: argparse.RawTextHelpFormatter(
                prog, max_help_position=width
            ),
            description=(self.helpStr or "").strip(),
            fromfile_prefix_chars="@",
            **kwargs,
        )

        # Group arguments by destination
        byDest, noDest = {}, []
        for arg in self.args.values():
            for clArg in arg.generateCmdlineArgs():
                if clArg.dest is not None:
                    byDest.setdefault(clArg.dest, []).append(clArg)
                else:
                    noDest.append(clArg)

        defaultGroup = parser.add_argument_group("Auto-detected arguments")

        for arg in sorted(noDest, key=lambda x: x.name):
            helpMsg = self.helpMsg(arg)
            if helpMsg is False:
                continue  # helpMsg returns False iff all the help messages are False, meaning we don't want to display/parse this

            if not arg.positional:
                # allow hyphenated argument names in place of underscores where applicable
                varargs = [
                    f"--{name}" for name in {arg.name, arg.name.replace("_", "-")}
                ]
                if arg.short is not None:
                    varargs.append("-" + arg.short)
                keywords = {"metavar": arg.metavar, "help": helpMsg, "dest": arg.dest}
            else:
                assert not arg.short
                varargs = [arg.name]
                keywords = {"metavar": arg.metavar, "help": helpMsg}

            keywords.update(arg._argparseKwargs)
            defaultGroup.add_argument(*varargs, **keywords)

        # Arguments with specified dest are assigned as mutually exclusive
        # argments that assign the argument name onto the dest.
        destHelp = {
            "__method__": "glue.cmdline methods",
            "__node__": "glue.node methods",
        }
        for dest, arguments in sorted(byDest.items()):
            log.debug("Adding mutually exclusive group:", dest)
            grp = parser.add_argument_group(
                destHelp.get(dest, "Options for specifying {}".format(dest))
            )
            exgrp = grp.add_mutually_exclusive_group()

            for arg in sorted(arguments, key=lambda x: x.name):
                # allow hyphenated argument names in place of underscores where applicable
                varargs = [
                    f"--{name}" for name in {arg.name, arg.name.replace("_", "-")}
                ]
                if arg.short is not None:
                    varargs.append("-" + arg.short)

                keywords = {
                    "metavar": arg.metavar,
                    "help": self.helpMsg(arg),
                    "dest": arg.dest,
                    "action": "store_const",
                    "const": arg.name,
                }
                keywords.update(arg._argparseKwargs)
                exgrp.add_argument(*varargs, **keywords)

        return parser

    def readCommandLine(
        self, argv: typing.Iterable[str], ignoreUnknown=False, **kwargs
    ):
        if argv is None or not client_utils.isiterable(argv):
            raise TypeError(
                f"Expecting `argv` to be `list[str]`, got: {argv} {type(argv)}"
            )
        not_str = [arg for arg in argv if not isinstance(arg, str)]
        if not_str:
            raise TypeError(f"Found non-str items in `argv`: {not_str}")

        output = collections.OrderedDict()
        parser = self.getCommandlineParser(**kwargs)

        if ignoreUnknown:
            ns, _ = parser.parse_known_args(argv)
        else:
            ns = parser.parse_args(argv)

        for n, arg in self.args.items():
            optName = arg.dest or arg.name
            shortName = arg.short
            if optName in ns:
                if getattr(ns, optName) is not None:
                    output[optName] = arg.parseCL(getattr(ns, optName))
            elif shortName and arg.short in ns:
                if getattr(ns, shortName) is not None:
                    output[optName] = arg.parseCL(getattr(ns, shortName))
        return output

    def getArgs(self, forCall=False):
        """
        forCall=True implies that we care about all required (i.e. unspecified) arguments.
        """
        include = [NO_DEFAULT]
        if forCall:
            include += [CALLEE_ARG, MANUAL_ARG]

        # return [ arg.name for arg in self.args.values() if arg.default == NO_DEFAULT ]
        return [arg.name for arg in self.args.values() if arg.default in include]

    def getKwargs(self):
        return [
            arg.name
            for arg in self.args.values()
            if arg.default not in (NO_DEFAULT, CALLEE_ARG, MANUAL_ARG)
        ]

    def __len__(self):
        return len(self.args)

    def __contains__(self, key):
        return key in self.args

    def __iter__(self):
        return self.args.values()

    def __getitem__(self, key):
        return self.args[key]

    def keys(self):
        return self.args.keys()

    def values(self):
        return self.args.values()

    def items(self):
        return self.args.items()

    def helpMsg(self, arg):
        msgParts = set()
        for src, msg in self.helps[arg.name]:
            if arg.source is None:
                continue
            if (
                msg is False
            ):  # Setting help=False for an argument hides it from command line calls
                continue

            if log.info.applies():
                # Only provide more info if more verbose help is requested
                if arg.help:
                    msgParts.add("{}({})".format(_prettyFuncName(src), msg))
                else:
                    msgParts.add(_prettyFuncName(src))

        msgParts = sorted(msgParts)

        if arg.default not in (None, NO_DEFAULT, CALLEE_ARG, MANUAL_ARG):
            msgParts = ["default: {}".format(arg.default)] + msgParts

        if not msgParts:
            return arg.help

        return "\n".join(msgParts)

    def formatArg(self, **kwargs):
        # argvalues = inspect.getcallargs(self.function, *args, **kwargs)
        parts = []
        for name, arg in sorted(self.args.items()):
            if name in kwargs:
                # How to do this serialization?
                parts += [arg.formatArg(kwargs[name])]
        return " ".join([p for p in parts if p])


def _prettyFuncName(func):
    if func.__module__ == "__main__":
        name = func.__name__
    else:
        name = "{}.{}".format(func.__module__, func.__name__)
    return log.format(name, color=log.Color.GRAY)


class _FuncArgs(_Args):
    """
    Represents a function's arguments. Basically works like inspect.ArgSpec,
    but as implemented here so I can control it more.
    """

    def __init__(self, function):
        self.function = function
        super(_FuncArgs, self).__init__()

    def __repr__(self):
        return "<_FuncArgs: {}({})>".format(
            self.function.__name__, ", ".join(self.args)
        )

    @staticmethod
    def FromFunction(function):
        """
        Given a function, inspect it for arguments.
        """
        output = _FuncArgs(function)

        for name, param in inspect.signature(function).parameters.items():
            output.addArgument(
                Base(name, default=param.default, defaultType=param.annotation)
            )
        return output


# Certain python types do not have nice string-to-instance conversion,
# so we use our own
TYPE_WRAPPERS = {
    bool: t.Bool,
    datetime.date: t.Datetime_Date,
    datetime.time: t.Datetime_Time,
    datetime.datetime: t.Datetime_Datetime,
    list: t.List,
}


def _wrap_type(typ):
    if typ in TYPE_WRAPPERS:
        return TYPE_WRAPPERS[typ]
    elif isinstance(
        typ, typing.TypeVar
    ):  # un-specific type parameter, always parse as string
        return str
    elif is_collection_type(typ):
        if typ.__origin__ not in (list, typing.List):  # TODO (kzhang): add more support
            raise TypeError(
                "When using types from `typing`, only typing.List[...] is supported right now."
            )
        item_type = typ.__args__[0]  # type arg inside typing.List[T]
        # Check for no inner iterables (excluding `str` of course)
        item_class = t.runtime_type(item_type)
        if item_class != str and issubclass(item_class, collections.abc.Iterable):
            raise TypeError(
                "Parameterized type, {}, with underlying type {}, for {} cannot be iterable".format(
                    item_type, item_class, typ
                )
            )
        # Recursively `_wrap_type` for special handling of parameterized types such as `bool` and `datetime.date`
        return t.List[_wrap_type(item_type)]
    elif t.is_NewType(typ):
        return _wrap_type(typ.__supertype__)
    return typ


class Base(object):
    """
    Emphasize arguments as function arguments.

    """

    __slots__ = [
        "name",
        "default",
        "defaultFunc",
        "source",
        "short",
        "positional",
        "type",
        "metavar",
        "dest",
        "realkey",
        "help",
    ]
    _argparseKwargs = {}

    def __init__(
        self,
        name,
        default=NO_DEFAULT,
        defaultFunc=None,
        help=None,
        defaultType=None,
        dest=None,
        positional=False,
        source=None,
        realkey=None,
        short=None,
        metavar=None,
    ):
        self.name = name
        if default == inspect._empty:
            default = NO_DEFAULT
        if defaultType == inspect._empty:
            defaultType = None
        if defaultFunc is not None:
            assert (
                default == NO_DEFAULT
            ), "You can't provide both a defaultFunc and a default."
            self.default = DEFAULT_FROM_FUNC
        else:
            self.default = default
        self.defaultFunc = defaultFunc

        self.short = short
        self.positional = positional

        self.help = help
        self.source = source

        self.type = self.inferDefaultType(default, defaultType)

        self.metavar = name
        if self.type is not None:
            if inspect.isclass(self.type):
                self.metavar = self.type.__name__.upper()
            # TODO (kzhang): This is the best check I can come up with for now
            if is_collection_type(self.type):
                # TODO (kzhang): This may be too verbose, ok for now.
                self.metavar = str(self.type)
        if metavar is not None:
            self.metavar = metavar

        self.dest = dest

        self.realkey = realkey

    # TODO (kzhang): Does not correctly infer types of default values such as
    #   [1,2,3]. It returns <class 'list'> instead of
    #   typing.List[int|Any|object]. Maybe this won't matter since we should
    #   always supply explicit type annotations anyways?
    def inferDefaultType(self, default, defaultType):
        # In case of method(arg=None), defaultType is None but we don't really
        # know anything.
        if defaultType is None and default != NO_DEFAULT and default is not None:
            # Infer the type from the default if possible (not None), unless it has been set manually
            defaultType = type(default)

        return _wrap_type(defaultType)

    def generateCmdlineArgs(self):
        return [self]

    def copy(self):
        return copy.deepcopy(self)

    def getKey(self):
        if self.realkey is not None:
            return self.realkey
        else:
            return "--{}".format(self.name)

    def __repr__(self):
        return "<{}: {}>".format(self.__class__.__name__, self.pretty())

    def pretty(self):
        prettyParts = [self.name]
        if self.default != NO_DEFAULT:
            prettyParts.append("default={}".format(self.default))
        if self.help is not None:
            prettyParts.append("help={}".format(self.help))
        if self.source is not None:
            prettyParts.append("source={}".format(self.source))
        if self.dest is not None:
            prettyParts.append("dest={}".format(self.dest))
        if self.type is not None:
            prettyParts.append("type={}".format(self.type))
        return ", ".join(prettyParts)

    def formatArg(self, value, printDefaults=False, forceUserSpecified=False):
        """
        Return the command line string that would be parsed to produce myself!

        If both value and values are set, the returned string will generate ONLY
        the value, the command line cannot set both value and values.

        """

        base = self.getKey()

        if (value != self.default) or printDefaults:
            return "%s=%s" % (base, pipes.quote(self.formatAndValidateCL(value)))
        else:
            return ""

    def formatAndValidateCL(self, value):
        retVal = self._formatCL(value)
        parsed = self._parseCL(retVal)
        if parsed != value:
            log.warn(
                self.name,
                "was given value:",
                value,
                "({})".format(type(value)),
                ", is serialized to",
                retVal,
                " but it deserialized to",
                parsed,
                "({})".format(repr(parsed)),
            )
        return retVal

    def _formatCL(self, value):
        return str(value)

    def parseCL(self, rawArg):
        return self._parseCL(rawArg)

    def _parseCL(self, value):
        try:
            return t.deserialize(self.type or str, value)
        except ValueError as e:
            e.args += ("parameter: '{}'".format(self.name),)
            raise e

    def helpMsg(self):
        return self.help


class Toggle(Base):
    __slots__ = Base.__slots__ + ["onValue", "_argparseKwargs"]

    def __init__(self, *args, **kwargs):
        self.onValue = kwargs.pop("onValue", True)
        self.default = False
        self._argparseKwargs = {"action": "store_const", "const": self.onValue}
        super(Toggle, self).__init__(*args, **kwargs)

    def formatArg(self, value, printDefaults=False, forceUserSpecified=False):
        base = self.getKey()

        # There are only two values and it is on or off
        if value == self.onValue:  # on branch
            return base
        else:  # off branch
            if value != self.default:
                log.warn(
                    f"{self.name} was given value: {value} ({type(value)})"
                    f"but it deserialized to {self.default} ({type(self.default)})"
                )
            return ""

    def _parseCL(self, arg):
        # There is only one value and it is on or off
        if arg:
            return self.onValue  # on branch
        else:
            return self.default  # off branch
