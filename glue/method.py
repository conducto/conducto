import asyncio
import functools
import inspect
import os
import re
import pipes
import pprint
import sys
import types
import typing

from ..shared import client_utils, constants, log, types as t
from .._version import __version__, __sha1__

from .. import api, callback, image as image_mod, pipeline
from . import arg

_UNSET = object()
CONDUCTO_ARGS = []


def lazy_shell(command, node_type, env=None, **exec_args) -> pipeline.Node:
    output = Lazy(command, node_type=node_type, **exec_args)
    output.env = env
    return output


def lazy_py(func, *args, **kwargs) -> pipeline.Node:
    return Lazy(func, *args, **kwargs)


def Lazy(command_or_func, *args, node_type=_UNSET, **kwargs) -> pipeline.Node:
    """
    This node constructor returns a co.Serial containing a pair of nodes. The
    first, **`Generate`**, runs `func(*args, **kwargs)` and prints out the
    resulting pipeline. The second, **`Execute`**, imports that pipeline into the
    current one and runs it.

    :param command_or_func: A shell command to execute or a python callable

    If a Python callable is specified for the command the `args` and `kwargs`
    are serialized and a `conducto` command line is constructed to launch the
    function for that node in the pipeline.
    """

    if callable(command_or_func):
        # If a function is passed, then `node_type` might be a valid argument. If it is
        # passed to `Lazy` then pass it along to `command_or_func`.
        if node_type is not _UNSET:
            kwargs["node_type"] = node_type

        # Infer the node_type from the return type of `command_or_func`.
        hints = typing.get_type_hints(command_or_func)
        node_type = hints.get("return")

    if not issubclass(node_type, (pipeline.Parallel, pipeline.Serial)):
        raise ValueError(
            "Can only call co.Lazy() on a function that returns a Parallel or Serial "
            f"node, but got: {node_type}"
        )

    output = pipeline.Serial()
    output["Generate"] = pipeline.Exec(command_or_func, *args, **kwargs)
    output["Execute"] = node_type()
    output["Generate"].on_done(
        callback.base("deserialize_into_node", target=output["Execute"])
    )
    return output


def meta(
    func=None,
    *,
    mem=None,
    cpu=None,
    gpu=None,
    env=None,
    image=None,
    requires_docker=None,
):
    if func is None:
        return functools.partial(
            meta,
            mem=mem,
            cpu=cpu,
            gpu=gpu,
            image=image,
            env=env,
            requires_docker=requires_docker,
        )

    func._conducto_wrapper = Wrapper(
        func,
        mem=mem,
        cpu=cpu,
        gpu=gpu,
        env=env,
        image=image,
        requires_docker=requires_docker,
    )
    return func


class NodeMethodError(Exception):
    pass


class Wrapper(object):
    def __init__(
        self,
        func,
        mem=None,
        cpu=None,
        gpu=None,
        env=None,
        image=None,
        requires_docker=None,
    ):

        if isinstance(func, staticmethod):
            self.function = func.__func__
        else:
            self.function = func

        self.callFunc = func

        # Most docstrings are indented like this:
        # def func():
        #     """
        #     Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod
        #     tempor incididunt ut labore et dolore magna aliqua.
        #     """
        # This extra space will render the docstring as a code block in markdown, which
        # isn't usually what we want. log.unindent intelligently removes this indent.
        doc = log.unindent(func.__doc__) if func.__doc__ else None

        self.exec_params = {
            "mem": mem,
            "cpu": cpu,
            "gpu": gpu,
            "env": env,
            "image": image,
            "requires_docker": requires_docker,
            "doc": doc,
        }

    def get_exec_params(self, *args, **kwargs):
        output = {}
        for k, v in self.exec_params.items():
            if v is None:
                pass
            elif callable(v):
                output[k] = v(*args, **kwargs)
            elif k == "env":
                output[k] = dict(v)
            else:
                output[k] = v
        return output

    def getArguments(self):
        # These are the core arguments that define the args and kwargs that the
        # function is able to understand as inputs.
        args = arg._FuncArgs.FromFunction(self.function)
        args.helpStr = (
            log.unindent(self.function.__doc__) if self.function.__doc__ else None
        )
        return args

    def to_command(self, *args, **kwargs):
        abspath = os.path.abspath(inspect.getfile(self.callFunc))
        ctxpath = image_mod.Image.get_contextual_path(abspath)
        # see also parse_registered_path
        mm = re.match(r"^(\$\{([A-Z_][A-Z0-9_]*)(|=([^}]*))\})(.*)", ctxpath)
        unique_string = "__CBIH5EX57NYHGC6YO69U__"
        replacement = None
        if mm:
            # ctxpath became a named mount
            replacement = mm.group(1)
            ctxpath = ctxpath.replace(mm.group(1), unique_string)

        parts = [
            "conducto",
            f"__conducto_path:{ctxpath}:endpath__",
            self.callFunc.__name__,
        ]

        sig = inspect.signature(self.callFunc)
        bound = sig.bind(*args, **kwargs)
        for k, v in bound.arguments.items():
            if v is True:
                parts.append(f"--{k}")
                continue
            if v is False:
                parts.append(f"--no-{k}")
                continue
            if client_utils.isiterable(v):
                parts += ["--{}={}".format(k, t.List.join(map(t.serialize, v)))]
            else:
                parts += ["--{}={}".format(k, t.serialize(v))]
        command = " ".join(pipes.quote(part) for part in parts)
        if replacement:
            command = command.replace(unique_string, replacement)
        return command

    def pretty(self):
        myargs = self.getArguments()
        args = myargs.getArgs(forCall=True)
        kwargs = myargs.getKwargs()
        bridge = ""

        try:
            fullName = "".join([self.__module__, ".", self.function.__name__])
        except NodeMethodError:
            fullName = "".join([self.__module__, ".", repr(self.function)])

        prettyParts = [log.format(fullName, color=log.Color.CYAN, bold=True), "("]

        if args:
            prettyParts.append(", ".join(args))
            bridge = ", "
        if kwargs:
            prettyParts.append(bridge)
            prettyParts.append(
                ", ".join(
                    ["{}={}".format(arg, repr(myargs[arg].default)) for arg in kwargs]
                )
            )

        prettyParts.append(")")

        return "".join(prettyParts)

    def getCallArgs(self, **kwargs):
        """
        Combine a dictionary and **kwargs, such that kwargs takes precedence
        over the values in __vardict__.
        """

        func_args = self.getArguments()
        argVars = func_args.getArgs()
        kwargVars = func_args.getKwargs()

        # Two error cases I can catch here: not all parameters are set,
        # and not all parameters are present.
        unknownKeys = set(argVars) - kwargs.keys()
        if unknownKeys:
            msg = ["Missing argments for: {}".format(self.pretty())]
            for key in unknownKeys:
                msg.append("  Please specify: {}".format(log.format(key, color="r")))
            # msg.append("Current state:")
            # msg.append(state.pretty(printif=lambda sv: sv.name in args.keys()))
            raise NodeMethodError("\n".join(msg))

        unusedKeys = kwargs.keys() - func_args.keys()
        if unusedKeys:
            log.warn(
                "All manually passed keyword args must be used by the callee, "
                "but {} had no use for: {}".format(
                    self.pretty(), ", ".join(sorted(unusedKeys))
                )
            )
            raise NodeMethodError(
                "All manually passed keyword args must be used by the callee, "
                "but {} had no use for: {}".format(
                    self.pretty(), ", ".join(sorted(unusedKeys))
                )
            )

        # direct gets, since these had better be in the state
        callKwargs = {name: kwargs[name] for name in argVars}

        if False and func_args.keywords:
            callKwargs.update({n: kwargs[n] for n in kwargs if n not in argVars})
        else:
            for name in kwargVars:
                if name in kwargs:
                    callKwargs[name] = kwargs[name]
                # This arg supposed to be generated by its defaultFunc
                elif func_args.args[name].defaultFunc:
                    callKwargs[name] = func_args.args[name].defaultFunc(**kwargs)

        return callKwargs

    @staticmethod
    def get_or_create(func):
        if hasattr(func, "_conducto_wrapper"):
            return func._conducto_wrapper
        else:
            return Wrapper(func)


def _get_common(tuples):
    from collections import defaultdict

    d = defaultdict(set)
    for k, v in tuples:
        if v is not None:
            d[k].add(v)
    return {k: d[k].pop() for k, v in d.items() if len(v) == 1}


def simplify_attributes(root):
    attributes = ["mem", "cpu", "gpu", "image", "requires_docker"] + [
        i for i in root.user_set if i.startswith("__env__")
    ]

    for node in root.stream(reverse=True):
        nodes = list(node.children.values()) + [node]
        for attr in attributes:
            common = _get_common([(attr, c.user_set.get(attr)) for c in nodes])
            for k, v in common.items():
                node.user_set[k] = v
                for child in node.children.values():
                    child.user_set[k] = None


def beautify(function, name, space):
    from conducto.shared.log import format, Color

    sig = inspect.signature(function)

    def format_annotation(an):
        if an == inspect.Parameter.empty:
            return ""
        if isinstance(an, types.FunctionType) and (
            an.__code__.co_filename.endswith("typing.py")
            and an.__code__.co_name == "new_type"
        ):
            return an.__name__
        out = inspect.formatannotation(an)
        out = out.replace("conducto.pipeline.", "")
        return out

    def parse_parameter(s):
        res = []
        res.append(format(s.name))
        use = format_annotation(s.annotation)
        if use:
            res.append(format(":" + use, dim=True))
        if s.default != inspect.Parameter.empty:
            res.append(format("=" + repr(s.default), dim=True))
        return "".join(res)

    def parse_return_annotation():
        res = format_annotation(sig.return_annotation)
        if res:
            return " -> " + format(res, color="purple")
        else:
            return ""

    def parse_docstring():
        if function.__doc__ is None:
            return ""
        return "\n".join(" " * 4 + i for i in function.__doc__.split("\n"))

    add = space - len(name)
    name = format(name, color=Color.BLUE)
    name = "    " + name + " " * add
    params = (
        f'({", ".join(parse_parameter(p) for p in sig.parameters.values())})'
        + parse_return_annotation()
    )
    return name + params + parse_docstring()


def _get_calling_filename():
    """
    Iterate through the stack to find the filename that is actually being
    called, not this one.
    """
    for st in inspect.stack()[::-1]:
        if os.path.basename(st.filename) != "runpy.py":
            calling_file = st.filename
            break
    else:
        calling_file = "<file>"

    if os.path.basename(calling_file).startswith("__main__."):
        return os.path.basename(os.path.dirname(calling_file))
    else:
        return os.path.basename(calling_file) + " "


def _get_default_title(is_local, specifiedFuncName, default_method_name):
    # Construct a default title from the command line arguments.  Start
    # from a copy of sys.argv to not modify in-place!
    args = sys.argv[:]
    args = [a for a in args if a not in CONDUCTO_ARGS]

    executable = os.path.basename(args[0])
    if (
        executable.startswith("python")
        or executable == "conducto"
        or executable == "__main__.py"
    ):
        args = args[1:]

    if (
        default_method_name is not None
        and specifiedFuncName == default_method_name
        and specifiedFuncName not in args
    ):
        args.insert(1, specifiedFuncName)

    # here is the default title
    return " ".join(pipes.quote(a) for a in args)


def main(
    variables=None,
    default=None,
    argv=None,
    env=None,
    cpu=None,
    gpu=None,
    mem=None,
    requires_docker=False,
    image: typing.Union[None, str, image_mod.Image] = None,
    filename=None,
    printer=pprint.pprint,
):
    """
    Command-line helper that allows you from the shell to easily execute methods that return Conducto nodes.

    :param default:  Specify a method that is the default to run if the user doesn't specify one on the command line.
    :param image: Specify a default docker image for the pipeline. (See also :py:class:`conducto.Image`).
    :param env, cpu, mem, requires_docker: Computational attributes to set on any Node called through `co.main`. 
      
    See :ref:`Node Methods and Attributes` for more details.
    """

    if sys.platform.startswith("win"):
        try:
            import colorama

            # This is designed to be run once at the start of a program.  Python import
            # semantics is that __init__ is not re-run so this colorizes ansi colors on
            # Windows for any program that imports conducto.
            colorama.init()
        except ImportError:
            # we pass if the colorama module is not installed, we only install it
            # on windows.
            pass

    # in case we ever add functionality where argv is an empty list
    if argv is None:
        argv = list(sys.argv[1:])
    if variables is None:
        stack = inspect.stack()
        frame, path, _, source, _, _ = stack[1]
        log.debug("Reading locals from", source, "in", path)
        variables = dict(frame.f_locals)
        if not filename:
            filename = path

    methods = {
        name: obj
        for name, obj in variables.items()
        if not name.startswith("_") and not inspect.isclass(obj) and callable(obj)
    }

    if filename and constants.ExecutionEnv.value() in constants.ExecutionEnv.external:
        api.dirconfig_select(filename)

    if "__all__" in variables:
        methods = {
            func: methods[func] for func in variables["__all__"] if func in methods
        }

    returns_node = []
    doesnt_return_node = []

    for name, fxn in methods.items():
        try:
            hints = typing.get_type_hints(fxn)
        except (TypeError, ValueError):
            continue
        if (
            "return" in hints
            and isinstance(hints["return"], type)
            and issubclass(hints["return"], pipeline.Node)
        ):
            returns_node.append((fxn, name))
        else:
            doesnt_return_node.append((fxn, name))

    spacing = 2 + max(len(i) for i in methods) if methods else 0

    def beautify_method_list(lst):
        return "\n".join(beautify(*i, space=spacing) for i in lst)

    titles = (
        ["methods that return conducto pipelines", "other methods"]
        if returns_node
        else ["", "methods"]
    )
    returns_node = (
        f"{titles[0]}:\n" + beautify_method_list(returns_node) if returns_node else ""
    )
    doesnt_return_node = (
        f"{titles[1]}:\n" + beautify_method_list(doesnt_return_node)
        if doesnt_return_node
        else ""
    )

    valid_methods = returns_node + "\n" + doesnt_return_node

    # if main is executed from __main__, some functions will have
    # __module__ == "__main__". For these, we need to set their name properly.
    for name, obj in methods.items():
        if obj.__module__ == "__main__":
            obj.name = name

    config = api.Config()
    who = api.Config().get("dev", "who")
    accepts_cloud = who != None

    import argparse

    prog = _get_calling_filename()

    if returns_node:
        commands = "[--local] [--run] [--sleep-when-done]"
        if accepts_cloud:
            commands = "[--cloud] " + commands

        node_l1 = commands
        node_l2 = "[--app / --no-app] [--shell / --no-shell]"
        node_usage = "\n".join([" " * (len(prog) + 8) + l for l in [node_l1, node_l2]])
    else:
        node_usage = ""

    usage_message = (
        f"{prog}[-h] <method> [< --arg1 val1 --arg2 val2 ...>]\n"
        + f"{node_usage}\n"
        + valid_methods
    )

    default_method_name = default.__name__ if default != None else None

    parser = argparse.ArgumentParser(prog=prog, usage=usage_message)
    parser.add_argument(
        "--version",
        action="version",
        version=f"{__version__} (sha1={__sha1__})",
        help="show conducto package version",
    )
    if default is not None:
        parser.add_argument(
            "method", nargs="?", default=default.__name__, help=argparse.SUPPRESS
        )
    else:
        parser.add_argument("method", help=argparse.SUPPRESS)

    dispatchState, unknown = parser.parse_known_args(argv)
    dispatchState = vars(dispatchState)
    specifiedFuncName = dispatchState.get("method")
    callFunc = None
    if isinstance(specifiedFuncName, str):
        if specifiedFuncName not in methods:
            if specifiedFuncName != argv[0]:
                parser.error(f"{argv[0]} is not a valid method")
                exit(1)

            parser.error(f"'{specifiedFuncName}' is not a valid method")
            exit(1)
        callFunc = methods[specifiedFuncName]

    types = {}

    for param_name, sig in inspect.signature(callFunc).parameters.items():
        _required = sig.default == inspect.Parameter.empty
        args = ["--" + param_name]
        if "_" in param_name:
            args.append("--" + param_name.replace("_", "-"))
        if _required:
            default = inspect.Parameter.empty
        elif sig.default is None:
            default = None
        else:
            default = t.serialize(sig.default)

        if sig.annotation != inspect.Parameter.empty:
            types[param_name] = arg._wrap_type(sig.annotation)
        elif sig.default != inspect.Parameter.empty:
            types[param_name] = sig.default.__class__
        else:
            types[param_name] = str

        # Add arguments to the argparser for each of callFunc's parameters. Don't
        # include the defaults here because that would cause them to be parsed later
        # unnecessarily. Leave the defaults unset, and they will get
        if types[param_name] in (bool, t.Bool):
            group = parser.add_mutually_exclusive_group(required=_required)
            group.add_argument(*args, dest=param_name, action="store_true")
            negated = ["--no-" + a[2:] for a in args]
            group.add_argument(*negated, dest=param_name, action="store_false")
        else:
            parser.add_argument(*args, required=_required)

    wrapper = Wrapper.get_or_create(callFunc)

    return_type = typing.get_type_hints(callFunc).get("return")
    if isinstance(return_type, type) and issubclass(return_type, pipeline.Node):
        called_func_returns_node = True
    else:
        called_func_returns_node = False

    def bool_mutex_group(parser, base, default=None):
        group = parser.add_mutually_exclusive_group(required=False)
        group.add_argument(f"--{base}", dest=base, action="store_true")
        group.add_argument(f"--no-{base}", dest=base, action="store_false")
        if default != None:
            parser.set_defaults(**{base: default})

    if called_func_returns_node:
        default_shell = t.Bool(config.get("general", "show_shell", default=False))
        default_app = t.Bool(config.get("general", "show_app", default=True))

        if accepts_cloud:
            parser.add_argument("--cloud", action="store_true")
        parser.add_argument("--local", action="store_true")
        parser.add_argument("--run", action="store_true")
        bool_mutex_group(parser, "shell", default=default_shell)
        bool_mutex_group(parser, "app", default=default_app)
        parser.add_argument("--no-clean", action="store_true")
        parser.add_argument("--prebuild-images", action="store_true")
        parser.add_argument("--sleep-when-done", action="store_true")
        parser.add_argument("--public", action="store_true")
        global CONDUCTO_ARGS
        CONDUCTO_ARGS = [
            "cloud",
            "local",
            "run",
            "shell",
            "app",
            "no_clean",
            "prebuild_images",
            "sleep_when_done",
            "public",
        ]

    call_state = vars(parser.parse_args(argv))
    call_state.pop("method")

    conducto_state = {k: call_state.pop(k, None) for k in CONDUCTO_ARGS}

    call_state = {
        name: arg.Base(name, defaultType=types[name]).parseCL(value)
        for name, value in call_state.items()
        if name not in CONDUCTO_ARGS
        and value != inspect.Parameter.empty
        and value is not None
    }

    output = callFunc(**wrapper.getCallArgs(**call_state))

    # Support async methods. There's not necessarily a strong need to do so. but
    # it's so trivial that there's no real reason not to.
    if inspect.isawaitable(output):
        output = asyncio.get_event_loop().run_until_complete(output)

    # There are two possibilities with buildable methods (ones returning a Node):
    # - If user requested --build, then call build()
    # - Otherwise dumping the serialized Node to stdout, for user to view or for
    #   co.Lazy to deserialize and import.
    if called_func_returns_node:
        if not isinstance(output, pipeline.Node):
            raise NodeMethodError(
                f"Expected {callFunc.__name__} to return a Node, "
                f"but instead it returned {repr(output)}"
            )

        # Allow easier setting of attributes on any returned node. Most obviously
        # useful for env/image, but conceivable for the others to.
        #
        # Present in Node.__init__ but omitted here: stop_on_error, same_container,
        # and suppress_errors. They seem too specific to a single node to be
        # generally applicable.
        for key in "env", "cpu", "gpu", "mem", "image":
            value = locals()[key]
            if value is not None:
                existing_value = getattr(output, key, None)
                if existing_value is not None and existing_value != value:
                    raise Exception(
                        f"Trying to overwrite `{key}`={value} that is already set."
                    )
                setattr(output, key, value)

        # Set the doc on the Node
        if output.doc is None and callFunc.__doc__ is not None:
            output.doc = log.unindent(callFunc.__doc__)

        # Read command-line args
        is_cloud = conducto_state["cloud"]
        is_local = conducto_state["local"]
        use_app = conducto_state["app"]
        use_shell = conducto_state["shell"]
        no_clean = conducto_state["no_clean"]
        run = conducto_state["run"]
        sleep_when_done = conducto_state["sleep_when_done"]
        prebuild_images = conducto_state["prebuild_images"]
        is_public = conducto_state["public"]
        will_build = is_cloud or is_local

        if will_build:
            if output.title is None:
                output.title = _get_default_title(
                    is_local, specifiedFuncName, default_method_name
                )
            BM = constants.BuildMode
            output._build(
                use_shell=use_shell,
                use_app=use_app,
                prebuild_images=prebuild_images,
                build_mode=BM.LOCAL if is_local else BM.DEPLOY_TO_CLOUD,
                run=run,
                sleep_when_done=sleep_when_done,
                is_public=is_public,
            )
        else:
            if t.Bool(os.getenv("__RUN_BY_WORKER__")):
                # Variable is set in conducto_worker/__main__.py to avoid
                # printing ugly serialization when not needed.
                simplify_attributes(output)
                s = output.serialize()
                print(f"<__conducto_serialization>{s}</__conducto_serialization>\n")
            print(output.pretty(strict=False))
    elif output is not None:
        printer(output)

    return output
