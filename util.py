import importlib
import inspect
import os
import re
import traceback
from . import pipeline
from .shared import log, types as t


def env_bool(key):
    return t.Bool(os.getenv(key))


def mkdirs(baseNode, path, nodeType=None):
    if nodeType is None:
        nodeType = pipeline.Parallel
    curr = baseNode
    for part in path.split("/"):
        if part not in curr.children:
            curr[part] = nodeType()
        curr = curr[part]
    return curr


def makenestednodes(
    baseNode, values, func, makeLeafNode=False, sort=True, reverse=False, nodeType=None
):
    if sort:
        values = sorted(values, reverse=reverse)

    for idx, value in enumerate(values):
        path = func(idx, value, values)

        if not makeLeafNode:
            path = "/".join(path.split("/")[:-1])
        mkdirs(baseNode, path, nodeType=nodeType)

        yield baseNode[path], value


def makeyyyymmnodes(baseNode, yyyymms, makeLeafNode=False, reverse=True, nodeType=None):
    def pathFunc(idx, yyyymm, values):
        if len(values) < 10:
            return yyyymm
        else:
            return f"{str(yyyymm)[:4]}/{yyyymm}"

    return makenestednodes(
        baseNode,
        yyyymms,
        pathFunc,
        makeLeafNode=makeLeafNode,
        sort=True,
        reverse=reverse,
        nodeType=nodeType,
    )


def makedatenodes(baseNode, dates, makeLeafNode=False, reverse=True, nodeType=None):
    dates = list(dates)  # Exhaust iterators.
    yearMonths = set(str(date)[:7] for date in dates)

    def pathFunc(idx, date, values):
        if len(values) <= 10:
            return str(date)
        elif len(yearMonths) < 10:
            return f"{str(date)[:7]}/{str(date)}"
        else:
            return f"{str(date)[:4]}/{str(date)[:7]}/{str(date)}"

    return makenestednodes(
        baseNode,
        dates,
        pathFunc,
        makeLeafNode=makeLeafNode,
        sort=True,
        reverse=reverse,
        nodeType=nodeType,
    )


def magic_doc(*, func=None, doc_only=False):
    if func is None:
        st = traceback.extract_stack(limit=2)
        func_name = st[-2].name
        module_name = os.path.basename(st[-2].filename).split(".py")[0]
        module = importlib.import_module(module_name)
        func = getattr(module, func_name)
    else:
        module = inspect.getmodule(func)

    docstring = func.__doc__
    if docstring is not None:
        # Strip out the docstring from the function.
        escaped_code = inspect.getsource(func)
        unescaped_code = escaped_code.replace("\\\\", "\\")
        success = False
        for quote in '"""', "'''", '"', "'":
            for code in escaped_code, unescaped_code:
                if f"{quote}{docstring}{quote}" in code:
                    # This first step leaves a blank line with whitespace, so the second
                    # step removes it.
                    code = code.replace(f"{quote}{docstring}{quote}", "", 1)
                    code = re.sub("\n\\s+\n", "\n", code, 1)
                    success = True
                    break
            if success:
                break
    else:
        docstring = module.__doc__ or ""
        code = inspect.getsource(func)

    pretty_doc = log.unindent(docstring)

    if doc_only:
        return pretty_doc
    else:
        return f"{pretty_doc}\n\n```python\n{code}\n```"
