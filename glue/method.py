import argparse
import asyncio
import configparser
import functools
import inspect
import json
import os
import pprint
import shlex
import sys
import types
import typing
import pathlib

from ..shared import client_utils, constants, log, types as t
from .._version import __version__, __sha1__

from .. import api, callback, image as image_mod, pipeline
from . import arg

_UNSET = object()
CONDUCTO_ARGS = [
    "cloud",
    "local",
    "run",
    "shell",
    "app",
    "prebuild_images",
    "sleep_when_done",
    "public",
]


def lazy_shell(command, env=None, **exec_args) -> pipeline.Node:
    output = Lazy(command, **exec_args)
    output.env = env
    return output


def lazy_py(func, *args, **kwargs) -> pipeline.Node:
    return Lazy(func, *args, **kwargs)


def Lazy(command_or_func, *args, **kwargs) -> pipeline.Node:
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

    output = pipeline.Serial()
    output["Generate"] = pipeline.Exec(command_or_func, *args, **kwargs)
    output["Execute"] = pipeline.Parallel()
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
    EXE = "conducto"

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
        abspath = os.path.realpath(inspect.getfile(self.callFunc))
        ctxpath = image_mod.Image.get_contextual_path(abspath)

        parts = [self.callFunc.__name__]

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
        quoted = [shlex.quote(part) for part in parts]

        serialized = f"__conducto_path:{ctxpath.linear()}:endpath__"
        command = [Wrapper.EXE, serialized] + quoted

        return " ".join(command)

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


def _get_common(parent_dict, child_dicts):
    common_in_children = None
    set_in_parent = {k: v for k, v in parent_dict.items() if v is not None}

    for d in child_dicts:
        # Find all key/value pairs that are identical among all children and are unset
        # in the parent
        if common_in_children is None:
            common_in_children = {
                k: v
                for k, v in d.items()
                if v is not None and parent_dict.get(k) is None
            }
        else:
            for common_k, common_v in list(common_in_children.items()):
                if d.get(common_k, _UNSET) != common_v:
                    del common_in_children[common_k]

        # Find all key/value pairs where the child's value either is unset or is
        # identical to that of the parent
        for key, value in list(set_in_parent.items()):
            child_value = d.get(key)
            if child_value is not None and child_value != value:
                del set_in_parent[key]

    # There should be no overlap between these two dicts: keys in set_in_parent must
    # have had non-None value in the parent, whereas keys in common_in_children must
    # have been None in the parent. Anything in either can be pulled up to the parent.
    return {**set_in_parent, **common_in_children}


def simplify_attributes(root):
    for node in root.stream(reverse=True):
        if node.children:
            # Propagate user_set attributes, setting them to None in the children
            common_attrs = _get_common(
                node.user_set, [c.user_set for c in node.children.values()]
            )
            for k, v in common_attrs.items():
                node.user_set[k] = v
                for child in node.children.values():
                    child.user_set[k] = None

            # Propagate environment variables, removing them from the children
            common_env = _get_common(node.env, [c.env for c in node.children.values()])
            for k, v in common_env.items():
                node.env[k] = v
                for child in node.children.values():
                    child.env.pop(k, None)


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


def _get_default_title(specifiedFuncName, default_was_used):
    """
    Construct a default title from the command line arguments.
    """

    # Ignore arguments that are parsed by Conducto, those aren't useful as a title
    args = []
    for a in sys.argv:
        if a.startswith("--"):
            rest = a[2:]
            if rest in CONDUCTO_ARGS or rest.replace("-", "_") in CONDUCTO_ARGS:
                continue
        args.append(a)

    # Strip off the executable if it's uninteresting.
    executable = os.path.basename(args[0])
    if (
        executable.startswith("python")
        or executable == "conducto"
        or executable == "__main__.py"
    ):
        args = args[1:]

    if default_was_used:
        args.insert(1, specifiedFuncName)

    # here is the default title
    return " ".join(shlex.quote(a) for a in args)


def _get_default_shell():
    return t.Bool(api.Config().get("general", "show_shell", default=False))


def _get_default_app():
    return t.Bool(api.Config().get("general", "show_app", default=True))


def _get_call_func(argv, default, methods):
    prog = _get_calling_filename()

    usage_message = _make_usage_message(methods)

    if argv and argv[0] == "--version":
        print(f"{__version__} (sha1={__sha1__})")
        sys.exit(0)
    if argv and argv[0] in ("-h", "--help"):
        print("usage:", usage_message, file=sys.stderr)
        sys.exit(0)
    if default is None:
        if not argv:
            print("usage:", usage_message, file=sys.stderr)
            print(f"{prog}: error: must specify method", file=sys.stderr)
            sys.exit(1)
        if argv[0] not in methods:
            print(f"{prog}: error: {argv[0]} is not a valid method", file=sys.stderr)
            sys.exit(1)
        callFunc = methods[argv[0]]
        remainder = argv[1:]
        default_was_used = False
    else:
        if argv and not argv[0].startswith("--"):
            if argv[0] not in methods:
                print(
                    f"{prog}: error: {argv[0]} is not a valid method", file=sys.stderr
                )
                sys.exit(1)
            callFunc = methods[argv[0]]
            remainder = argv[1:]
            default_was_used = False
        else:
            callFunc = default
            remainder = argv
            default_was_used = True

    return callFunc, remainder, default_was_used


def _make_usage_message(methods):
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
    methods_pretty = returns_node + "\n" + doesnt_return_node

    prog = _get_calling_filename()

    if returns_node:
        commands = "[--local | --cloud] [--run] [--sleep-when-done]"
        node_l1 = commands
        node_l2 = "[--app | --no-app] [--shell | --no-shell]"
        node_usage = "\n".join([" " * (len(prog) + 8) + l for l in [node_l1, node_l2]])
    else:
        node_usage = ""

    return (
        f"{prog} [-h] <method> [method arguments]\n"
        + f"{node_usage}\n"
        + methods_pretty
    )


def _bool_mutex_group(parser, base, default=None):
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument(f"--{base}", dest=base, action="store_true")
    group.add_argument(f"--no-{base}", dest=base, action="store_false")
    if default != None:
        parser.set_defaults(**{base: default})


def _get_state(callFunc, remainder):
    prog = _get_calling_filename()
    parser = argparse.ArgumentParser(prog=prog)

    types = {}
    empty = inspect.Parameter.empty
    signature = inspect.signature(callFunc)

    for param_name, sig in signature.parameters.items():
        args = ["--" + param_name]
        if "_" in param_name:
            args.append("--" + param_name.replace("_", "-"))

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
            group = parser.add_mutually_exclusive_group()
            group.add_argument(
                *args, default=empty, dest=param_name, action="store_true"
            )
            negated = ["--no-" + a[2:] for a in args]
            group.add_argument(
                *negated, default=empty, dest=param_name, action="store_false"
            )
        else:
            parser.add_argument(*args, default=empty)

    return_type = typing.get_type_hints(callFunc).get("return")
    if isinstance(return_type, pipeline.Node):
        raise TypeError(
            f"'{callFunc.__name__}' returns an instance of Node, but it should return a Node type"
        )
    elif isinstance(return_type, type) and issubclass(return_type, pipeline.Node):
        called_func_returns_node = True
    else:
        called_func_returns_node = False

    parser.add_argument("--profile")
    if called_func_returns_node:
        _add_argparse_options_for_node(parser)

    args, kwargs = _parse_args(parser, remainder)

    profile = kwargs.pop("profile")
    if profile:
        authed = list(api.Config().profile_sections())
        if profile in authed:
            os.environ["CONDUCTO_PROFILE"] = profile
        else:
            msg = (
                f"The profile '{profile}' is not recognized. "
                "Log-in with 'conducto-profile add --url=<url>'. "
                "List authenticated profiles with 'conducto-profile list'."
            )
            print(msg, file=sys.stderr)
            sys.exit(1)

    # Separate out conducto args and strip unset ones
    if called_func_returns_node:
        conducto_state = {k: kwargs.pop(k, None) for k in CONDUCTO_ARGS}
    else:
        conducto_state = {}
    kwargs = {k: v for k, v in kwargs.items() if v != empty}

    # Apply the variable and named args to callFunc
    try:
        bound = signature.bind(*args, **kwargs)
    except TypeError as e:
        print(f"{prog}: {e}", file=sys.stderr)
        sys.exit(1)

    # Parse the arguments according to their types
    call_state = {
        name: arg.Base(name, defaultType=types[name]).parseCL(value)
        for name, value in bound.arguments.items()
    }
    return call_state, conducto_state, called_func_returns_node


def _add_argparse_options_for_node(parser):
    parser.add_argument("--cloud", action="store_true")
    parser.add_argument("--local", action="store_true")
    parser.add_argument("--run", action="store_true")
    _bool_mutex_group(parser, "shell", default=_get_default_shell())
    _bool_mutex_group(parser, "app", default=_get_default_app())
    parser.add_argument("--prebuild-images", action="store_true")
    parser.add_argument("--sleep-when-done", action="store_true")
    parser.add_argument("--public", action="store_true")


def _parse_args(parser, argv):
    """
    Mimic argparse except we allow options to come after positional args. This feels
    more Pythonic - kwargs come after args. Only handle the subset of argparse that is
    relevant for _get_state().
    """
    i = 0
    args = []
    kwargs = {}

    for action in parser._option_string_actions.values():
        if action.dest != "help":
            kwargs[action.dest] = action.default

    positionals = parser._get_positional_actions()
    if len(positionals) == 0:
        wildcard = None
    elif len(positionals) == 1:
        action = positionals[0]
        if action.nargs != argparse.REMAINDER:
            raise Exception(
                f"Cannot parse position argument: {action} with nargs={action.nargs}"
            )
        wildcard = action.dest
        kwargs[wildcard] = []
    else:
        raise Exception(f"Cannot handle multiple positional arguments: {positionals}")

    while i < len(argv):
        arg = argv[i]
        if arg.startswith("--"):
            if "=" in arg:
                key, value = arg.split("=", 1)
            else:
                key = arg
                value = None
            try:
                action = parser._option_string_actions[key]
            except KeyError:
                if wildcard is None:
                    raise ValueError(f"Unknown argument: {arg}")
                kwargs[wildcard].append(arg)
                if i + 1 < len(argv) and not argv[i + 1].startswith("--"):
                    kwargs[wildcard].append(argv[i + 1])
                    i += 1
                i += 1
                continue

            if isinstance(action, argparse._StoreAction):
                if value is None:
                    value = argv[i + 1]
                    i += 1
            elif isinstance(action, argparse._StoreConstAction):
                if value is not None:
                    raise ValueError(
                        f"--{key} accepts no arguments. Got: {repr(value)}"
                    )
                value = action.const
            else:
                raise Exception(f"Cannot handle argparse action: {action}")
            kwargs[action.dest] = value
        else:
            if wildcard is not None:
                kwargs[wildcard].append(arg)
            else:
                args.append(arg)
        i += 1

    if wildcard is not None:
        kwargs[wildcard] = " ".join(shlex.quote(a) for a in kwargs[wildcard])

    return args, kwargs


def _parse_image_kwargs_from_config_section(section):
    get_json = lambda key: json.loads(section.get(key, "null"))

    if "copy_repo" in section:
        raise ValueError(
            "Not allowed to specify 'copy_repo' in config. It is set automatically."
        )
    if "copy_branch" in section:
        raise ValueError(
            "Not allowed to specify 'copy_branch' in config. It is set automatically."
        )
    if "copy_dir" in section:
        raise ValueError(
            "Not allowed to specify 'copy_dir' in config. It conflicts with 'copy_repo' which is set automatically."
        )
    if "copy_url" in section:
        raise ValueError(
            "Not allowed to specify 'copy_url' in config. It conflicts with 'copy_repo' which is set automatically."
        )

    return {
        "image": section.get("image"),
        "dockerfile": section.get("dockerfile"),
        "docker_build_args": get_json("docker_build_args"),
        "context": section.get("context"),
        "docker_auto_workdir": section.getboolean("docker_auto_workdir", True),
        "reqs_py": get_json("reqs_py"),
        "path_map": get_json("path_map"),
        "name": section.get("name"),
    }


def _parse_config_args_from_cmdline(argv):
    # Parse the command,
    conducto_kwargs = {
        "shell": _get_default_shell(),
        "app": _get_default_app(),
    }
    branch_cmd = None
    wildcard_args = []
    i = 0
    while i < len(argv):
        arg = argv[i]
        if arg.startswith("--"):
            if "=" in arg:
                key, value = arg.split("=", 1)
            else:
                key = arg
                value = None
            basekey = key[2:].replace("-", "_")
            if basekey in CONDUCTO_ARGS:
                if value is not None:
                    raise ValueError(
                        f"Invalid argument '{arg}'. --{key} takes no argument."
                    )
                conducto_kwargs[basekey] = True
                i += 1
                continue
            if basekey in ("no_shell", "no_app"):
                if value is not None:
                    raise ValueError(
                        f"Invalid argument '{arg}'. --{key} takes no argument."
                    )
                conducto_kwargs[basekey.replace("no_", "")] = False
                i += 1
                continue
            if basekey == "branch":
                if value is None:
                    value = argv[i + 1]
                    i += 1
                branch_cmd = value
                i += 1
                continue
        wildcard_args.append(arg)
        i += 1
    return conducto_kwargs, wildcard_args, branch_cmd


def run_cfg(
    file: typing.IO,
    argv,
    copy_repo=True,
    copy_branch=None,
    headless=False,
    token=None,
    tags=None,
):
    import conducto as co

    # Read the config file
    cp = configparser.ConfigParser()
    cp.read_file(file)

    if not argv:
        # TODO: Make better exception for invalid command
        raise NotImplementedError(
            "Make better exception for when no command is specified"
        )
    if argv[0] not in cp:
        if "*" in cp:
            section = cp["*"]
            method = "*"
            remainder = argv
        else:
            raise NotImplementedError("Make better exception for invalid command")
    else:
        method, *remainder = argv
        section = cp[method]

    # Parse the command template. Find which variables need to be passed.
    command_template = section["command"]
    image_kwargs = _parse_image_kwargs_from_config_section(section)
    conducto_kwargs, wildcard_args, branch_cmd = _parse_config_args_from_cmdline(
        remainder
    )

    # Build the output node
    wildcard_str = " ".join(shlex.quote(arg) for arg in wildcard_args)
    if "{*}" in command_template:
        command = command_template.replace("{*}", wildcard_str)
    else:
        command = command_template
        if wildcard_args:
            raise ValueError(
                f"Cannot specify additional arguments because command for [{method}] "
                f"does not contain '{{*}}'.\n"
                f"  Command: {command_template}\n"
                f"  User specified: {wildcard_str}"
            )

    output = co.Lazy(command)
    output.title = _get_default_title(None, default_was_used=False)
    output.tags = tags

    branch_env = os.environ.get("CONDUCTO_GIT_BRANCH")
    if branch_cmd is not None and branch_env is not None and branch_cmd != branch_env:
        raise ValueError(
            f"Conflicting definitions for branch: --branch={branch_cmd} and CONDUCTO_GIT_BRANCH={branch_env}"
        )
    elif branch_env is not None:
        output.title += f" --branch={os.environ['CONDUCTO_GIT_BRANCH']}"
    elif branch_cmd is not None:
        os.environ["CONDUCTO_GIT_BRANCH"] = branch_cmd

    # Normally `co.Image` looks through the stack to infer the repo, but the stack
    # doesn't go through the CFG file. Specify `_CONTEXT` as a workaround.
    co.Image._CONTEXT = file.name
    try:
        output.image = co.Image(
            copy_repo=copy_repo, copy_branch=copy_branch, **image_kwargs
        )
    finally:
        co.Image._CONTEXT = None

    # Run the node or print it. This code is duplicated from main() down below.
    is_cloud = conducto_kwargs.pop("cloud", False)
    is_local = conducto_kwargs.pop("local", False)
    will_build = is_cloud or is_local

    if will_build:
        BM = constants.BuildMode
        output._build(
            build_mode=BM.LOCAL if is_local else BM.DEPLOY_TO_CLOUD,
            headless=headless,
            token=token,
            **conducto_kwargs,
        )
    else:
        if t.Bool(os.getenv("__RUN_BY_WORKER__")):
            # Variable is set in conducto_worker/__main__.py to avoid
            # printing ugly serialization when not needed.
            s = output.serialize()
            print(f"<__conducto_serialization>{s}</__conducto_serialization>\n")
        print(output.pretty(strict=False))


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

    if constants.ExecutionEnv.value() in constants.ExecutionEnv.external:
        # default filename to current working directory
        # allows python -m conducto debug to respect local .conducto/profile
        if filename is None:
            filename = str(pathlib.Path().absolute()) + "/dummy"
        api.dirconfig_select(filename)

    if "__all__" in variables:
        methods = {
            func: methods[func] for func in variables["__all__"] if func in methods
        }

    # if main is executed from __main__, some functions will have
    # __module__ == "__main__". For these, we need to set their name properly.
    for name, obj in methods.items():
        if obj.__module__ == "__main__":
            obj.name = name

    # Parse argv to figure out which method is requested
    callFunc, remainder, default_was_used = _get_call_func(argv, default, methods)

    # Parse the remainder to get the request for the callFunc and for conducto
    call_state, conducto_state, called_func_returns_node = _get_state(
        callFunc, remainder
    )

    wrapper = Wrapper.get_or_create(callFunc)
    try:
        output = callFunc(**wrapper.getCallArgs(**call_state))

        # Support async methods. There's not necessarily a strong need to do so. but
        # it's so trivial that there's no real reason not to.
        if inspect.isawaitable(output):
            output = asyncio.get_event_loop().run_until_complete(output)
    except api.UserInputValidation as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)

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
        # Present in Node.__init__ but omitted here: stop_on_error, container_reuse_context,
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
        is_cloud = conducto_state.pop("cloud")
        is_local = conducto_state.pop("local")
        will_build = is_cloud or is_local

        simplify_attributes(output)

        if will_build:
            if output.title is None:
                output.title = _get_default_title(callFunc.__name__, default_was_used)
            BM = constants.BuildMode
            output._build(
                build_mode=BM.LOCAL if is_local else BM.DEPLOY_TO_CLOUD,
                **conducto_state,
            )
        else:
            if t.Bool(os.getenv("__RUN_BY_WORKER__")):
                # Variable is set in conducto_worker/__main__.py to avoid
                # printing ugly serialization when not needed.
                s = output.serialize()
                print(f"<__conducto_serialization>{s}</__conducto_serialization>\n")
            print(output.pretty(strict=False))
    elif output is not None:
        printer(output)

    return output
