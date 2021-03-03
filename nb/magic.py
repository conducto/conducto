from IPython.core.magic import Magics, magics_class, line_magic, cell_magic
import ast, astsearch
import re
import shlex
from textwrap import indent


class Param(object):
    def __init__(self, name, vtype, value=None, metadata=None):
        self.name = name
        self.type = vtype
        self.value = value
        self.metadata = metadata or {}

    def __repr__(self):
        params = [repr(self.name), self.type.__name__]
        if self.value is not None:
            params.append(f"value={self.value!r}")
        if self.metadata:
            params.append(f"metadata={self.metadata!r}")
        return f"Param({', '.join(params)})"

    def with_value(self, value):
        """Returns a copy with value set to a new value."""
        return type(self)(self.name, self.type, value, self.metadata or None)

    def __eq__(self, other):
        if isinstance(other, Param):
            return (
                self.name == other.name
                and self.type == other.type
                and self.value == other.value
            )


def any_supported_node(node, path):
    if isinstance(node, (ast.Num, ast.Str)):
        return
    elif isinstance(node, ast.NameConstant) and (node.value in (True, False)):
        return
    elif isinstance(node, ast.Dict):
        for n in node.keys:
            any_supported_node(n, path)
        for n in node.values:
            any_supported_node(n, path)
        return
    elif isinstance(node, ast.List):
        for n in node.elts:
            any_supported_node(n, path)
        return

    raise Exception(
        "Parsing an unsupported param in first cell. Only number, string, or Boolean supported: {path}, {node}"
    ) from None


pydefine_ptrn = ast.Assign(targets=[ast.Name()], value=any_supported_node)


def type_and_value(node):
    if isinstance(node, ast.Num):
        return type(node.n), node.n
    elif isinstance(node, ast.Str):
        return str, node.s
    elif isinstance(node, ast.NameConstant) and (node.value in (True, False)):
        return (bool, node.value)
    elif isinstance(node, ast.Dict):
        return (
            dict,
            {
                type_and_value(node.keys[i])[1]: type_and_value(node.values[i])[1]
                for i in range(len(node.keys))
            },
        )
    elif isinstance(node, ast.List):
        return (list, [type_and_value(n)[1] for n in node.elts])

    raise Exception(
        "Parsing an unsupported param in first cell. Only number, string, or Boolean supported: {node}"
    ) from None


def get_param_definitions(cellstr):
    definitions = list()
    cellast = ast.parse(cellstr)
    for assign in astsearch.ASTPatternFinder(pydefine_ptrn).scan_ast(cellast):
        definitions.append(Param(assign.targets[0].id, *type_and_value(assign.value)))
    return definitions


def get_conbparam_definitions(co_nb_params):
    definitions = list()
    ARG_ASSIGNMENT = re.compile(r"([\-a-zA-Z0-9]+)=(.*)")
    for arg in co_nb_params:
        kv = re.search(ARG_ASSIGNMENT, arg)
        if kv:
            name = kv.group(1).lstrip("-")
            value = kv.group(2)
            param = Param(name, str, value)
            definitions.append(param)
        else:
            name = arg.lstrip("-")
            value = not name.startswith("no-")
            if not value:
                name = name[3:]
            param = Param(name, bool, value)
            definitions.append(param)
    return definitions


RUN_IPYTHON_LOAD_PARAMS = """
import sys
sys.argv[1:] = co_nb_params
del co_nb_params
"""

from IPython.core.magics.namespace import NamespaceMagics
import IPython.core as core
import ipynbname


@magics_class
class ConductoMagics(Magics):
    @cell_magic
    def conducto(self, line, cell):
        # try load co_nb_params from IPython's DB
        self.shell.run_line_magic("store", "-r co_nb_params")
        has_co_nb_params = self.shell.ev("'co_nb_params' in locals()")
        if not has_co_nb_params:
            raise Exception(
                "Couldn't store or retrieve parameters in IPython"
            ) from None

        # extract co_nb_params from user namespace
        co_nb_params = self.shell.ev("co_nb_params")

        nbname = ipynbname.name()
        # set this notebook's `params` list
        # co_nb_params has the following structure (follows same as payload)
        # [('<notebook_name>', its sys.argv list),...]
        params = co_nb_params[nbname] if nbname in co_nb_params else list()

        # overwrite co_nb_params to have only this notebook's params
        self.shell.ex(f"co_nb_params = {str(params)}")

        # override sys.argv in user namespace with co_nb_params
        for line in RUN_IPYTHON_LOAD_PARAMS.split("\n"):
            self.shell.ex(line)

        # get Param objects for user's cell variables
        # and the cli-specified variables
        userparams = get_param_definitions(cell)
        cliparams = get_conbparam_definitions(params)

        # override user params with cli params
        overrides = list()
        for cliparam in cliparams:
            for param in userparams:
                if param.name == cliparam.name:
                    if param.type == str:
                        overrides.append(f"{param.name} = '{cliparam.value}'")
                    elif param.type in (list, dict, tuple):
                        raise Exception(
                            f"list type notebook arguments not supported. variable '{param.name}'"
                        ) from None
                    else:  # bool, int, float
                        overrides.append(f"{param.name} = {cliparam.value}")

        # first, run user's code to get defaults and anything we don't override in this cell
        self.shell.run_cell(cell, store_history=False)

        # now, override a subset of those vars withour custom values
        if len(overrides):
            overrides_statements = "\n".join(overrides)
            notification_of_overrides = (
                f'print("Applying arguments from the command line: (use `conducto-notebook clearargs` to clear)")\n'
                f'print("""{indent(overrides_statements, "  ")}""")'
            )
            overrides_cell = f"{overrides_statements}\n{notification_of_overrides}"
            self.shell.run_cell(overrides_cell, store_history=False)


def load_ipython_extension(ipython):
    """This function is called when the extension is
    loaded. It accepts an IPython InteractiveShell
    instance."""
    ipython.register_magics(ConductoMagics)
