import asyncio
import functools
import subprocess
import inspect
import os
import pipes
import pprint
import sys
import typing

import conducto.internal.host_detection as hostdet
from ..shared import client_utils, constants, log, types as t
from .._version import __version__

from .. import api, callback, image as image_mod, pipeline
from . import arg


# Validate arguments for the given function without calling it.
# This is useful for raising early errors on `co.lazy_py()`
def _validate_args(wrapper, *args, **kwargs):
    params = wrapper.getSignature().parameters

    # TODO (kzhang): can target function have a `*args` or `**kwargs` in the
    # signature? If so, handle it.
    invalid_params = [
        (name, str(param.kind))
        for name, param in params.items()
        if param.kind != inspect.Parameter.POSITIONAL_OR_KEYWORD
    ]
    if invalid_params:
        raise TypeError(
            f"Unsupported parameter types of "
            f"{wrapper.function.__name__}: {invalid_params} - "
            f"Only {str(inspect.Parameter.POSITIONAL_OR_KEYWORD)} is allowed."
        )

    # this will also validate against too-many or too-few arguments
    call_args = inspect.getcallargs(wrapper.function, *args, **kwargs)
    for name in call_args.keys():
        arg_value = call_args[name]
        param_type = params[name].annotation
        if not t.is_instance(arg_value, param_type):
            raise TypeError(
                f"Argument {name}={arg_value} {type(arg_value)} for "
                f"function {wrapper.function.__name__} is not compatible "
                f"with expected type: {param_type}"
            )


def lazy_shell(command, node_type, env=None, **exec_args) -> pipeline.Node:
    if env is None:
        env = {}
    output = pipeline.Serial(env=env)
    output["Generate"] = pipeline.Exec(command, **exec_args)
    output["Execute"] = node_type()
    output["Generate"].on_done(
        callback.base("deserialize_into_node", target=output["Execute"])
    )
    return output


def lazy_py(func, *args, **kwargs) -> pipeline.Node:
    wrapper = _Wrapper.get_or_create(func)
    return_type = wrapper.getSignature().return_annotation

    exec_params = wrapper.get_exec_params(*args, **kwargs)

    _validate_args(wrapper, *args, **kwargs)
    if issubclass(return_type, pipeline.Node):
        # Do a GenJobs/GridJobs if a Node is returned
        if issubclass(return_type, pipeline.Exec):
            raise ValueError(
                "Cannot call co.lazy_py() on a function that returns an Exec(). "
                "Don't defer that Exec(), just do it."
            )

        env = exec_params.pop("env", {})
        return lazy_shell(
            wrapper.to_command(*args, **kwargs), return_type, env, **exec_params
        )
    else:
        return pipeline.Exec(wrapper.to_command(*args, **kwargs), **exec_params)


def meta(
    func=None,
    *,
    mem=None,
    cpu=None,
    gpu=None,
    env=None,
    image=None,
    requires_docker=None,
    title=None,
    tags=None,
):
    if func is None:
        return functools.partial(
            meta,
            mem=mem,
            cpu=cpu,
            gpu=gpu,
            image=image,
            env=env,
            title=title,
            tags=tags,
            requires_docker=requires_docker,
        )

    func._conducto_wrapper = _Wrapper(
        func,
        mem=mem,
        cpu=cpu,
        gpu=gpu,
        env=env,
        title=title,
        tags=tags,
        image=image,
        requires_docker=requires_docker,
    )
    return func


class NodeMethodError(Exception):
    pass


class _Wrapper(object):
    def __init__(
        self,
        func,
        mem=None,
        cpu=None,
        gpu=None,
        env=None,
        title=None,
        image=None,
        tags=None,
        requires_docker=None,
    ):

        if isinstance(func, staticmethod):
            self.function = func.__func__
        else:
            self.function = func

        self.callFunc = func

        self.exec_params = {
            "mem": mem,
            "cpu": cpu,
            "gpu": gpu,
            "env": env,
            "image": image,
            "requires_docker": requires_docker,
            "doc": func.__doc__,
        }
        self.title = title
        self.tags = api.Pipeline.sanitize_tags(tags)

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
        args.helpStr = self.function.__doc__
        return args

    def getSignature(self):
        return inspect.signature(self.callFunc)

    def to_command(self, *args, **kwargs):
        abspath = os.path.abspath(inspect.getfile(self.callFunc))
        ctxpath = image_mod.Image.get_contextual_path(abspath)
        if hostdet.is_wsl():
            import conducto.internal.build as cib

            ctxpath = cib._split_windocker(ctxpath)
        elif hostdet.is_windows():
            ctxpath = hostdet.windows_docker_path(ctxpath)
        parts = [
            "conducto",
            f"__conducto_path:{ctxpath}:endpath__",
            self.callFunc.__name__,
        ]

        sig = self.getSignature()
        bound = sig.bind(*args, **kwargs)
        for k, v in bound.arguments.items():
            if not client_utils.isiterable(v):
                v = [v]
            parts += ["--{}={}".format(k, t.LIST_DELIM.join(map(t.serialize, v)))]
        return " ".join(pipes.quote(part) for part in parts)

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
            return _Wrapper(func)


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

    def parse_parameter(s):
        res = []
        res.append(format(s.name))
        if s.annotation != inspect.Parameter.empty:
            use = inspect.formatannotation(s.annotation)
            res.append(format(":" + use, dim=True))
        if s.default != inspect.Parameter.empty:
            res.append(format("=" + repr(s.default), dim=True))
        return "".join(res)

    def parse_return_annotation():
        if sig.return_annotation == inspect.Parameter.empty:
            return ""
        else:
            res = inspect.formatannotation(sig.return_annotation)
            res = res.replace("conducto.pipeline.", "")
            return " -> " + format(res, color="purple")

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
    try:
        # the cloud/local is shown in the icon, strip it here
        args.remove("--local" if is_local else "--cloud")
    except ValueError:
        pass

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
):
    # in case we ever add functionality where argv is an empty list
    if argv is None:
        argv = list(sys.argv[1:])
    if variables is None:
        stack = inspect.stack()
        frame, path, _, source, _, _ = stack[1]
        log.debug("Reading locals from", source, "in", path)
        variables = dict(frame.f_locals)
    methods = {
        name: obj
        for name, obj in variables.items()
        if not name.startswith("_") and not inspect.isclass(obj) and callable(obj)
    }

    if "__all__" in variables:
        methods = {
            func: methods[func] for func in variables["__all__"] if func in methods
        }

    returns_node = []
    doesnt_return_node = []

    for name, fxn in methods.items():
        try:
            # ignore builtin functions, i.e. heapq.heappush
            sig = inspect.signature(fxn)
        except ValueError:
            continue
        if issubclass(sig.return_annotation, pipeline.Node):
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

        node_usage = " " * (len(prog) + 8) + commands
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
        version=__version__,
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

        if types[param_name] in (bool, t.Bool):
            group = parser.add_mutually_exclusive_group(required=_required)
            group.add_argument(*args, dest=param_name, action="store_true")
            negated = ["--no-" + a[2:] for a in args]
            group.add_argument(*negated, dest=param_name, action="store_false")
            if default != None:
                parser.set_defaults(**{param_name: default})
        else:
            parser.add_argument(*args, required=_required, default=default)

    wrapper = _Wrapper.get_or_create(callFunc)

    return_type = wrapper.getSignature().return_annotation

    if issubclass(return_type, pipeline.Node):
        if accepts_cloud:
            parser.add_argument("--cloud", action="store_true")
        parser.add_argument("--local", action="store_true")
        parser.add_argument("--run", action="store_true")
        parser.add_argument("--no-shell", action="store_true")
        parser.add_argument("--no-clean", action="store_true")
        parser.add_argument("--prebuild-images", action="store_true")
        parser.add_argument("--sleep-when-done", action="store_true")
        conducto_args = [
            "cloud",
            "local",
            "run",
            "no_shell",
            "no_clean",
            "prebuild_images",
            "sleep_when_done",
        ]
    else:
        conducto_args = []

    call_state = vars(parser.parse_args(argv))
    call_state.pop("method")

    conducto_state = {k: call_state.pop(k, None) for k in conducto_args}

    call_state = {
        name: arg.Base(name, defaultType=types[name]).parseCL(value)
        for name, value in call_state.items()
        if name not in conducto_args and value != inspect.Parameter.empty
    }

    output = callFunc(**wrapper.getCallArgs(**call_state))

    # Support async methods. There's not necessarily a strong need to do so. but
    # it's so trivial that there's no real reason not to.
    if inspect.isawaitable(output):
        output = asyncio.get_event_loop().run_until_complete(output)

    # There are two possibilities with buildable methods (ones returning a Node):
    # - If user requested --build, then call build()
    # - Otherwise dumping the serialized Node to stdout, for user to view or for
    #   co.lazy_py to deserialize and import.
    if issubclass(return_type, pipeline.Node):
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

        # Read command-line args
        is_cloud = conducto_state["cloud"]
        is_local = conducto_state["local"]
        no_shell = conducto_state["no_shell"]
        no_clean = conducto_state["no_clean"]
        run = conducto_state["run"]
        sleep_when_done = conducto_state["sleep_when_done"]
        prebuild_images = conducto_state["prebuild_images"]
        will_build = is_cloud or is_local

        if will_build:
            if t.Bool(os.getenv("BUILD_ONLY")):
                log.log("BUILD_ONLY requested, and just finished building. Exiting.")
                return

            title = (
                wrapper.title
                if wrapper.title != None
                else _get_default_title(
                    is_local, specifiedFuncName, default_method_name
                )
            )
            BM = constants.BuildMode
            output._build(
                use_shell=not no_shell,
                title=title,
                tags=api.Pipeline.sanitize_tags(wrapper.tags),
                prebuild_images=prebuild_images,
                build_mode=BM.LOCAL if is_local else BM.DEPLOY_TO_CLOUD,
                run=run,
                sleep_when_done=sleep_when_done,
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
        pprint.pprint(output)

    return output
