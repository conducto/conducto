import os
from . import pipeline
from .shared import types as t


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
            return "%s/%s" % (str(yyyymm)[:4], yyyymm)

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
            return "%s/%s" % (str(date)[:7], str(date))
        else:
            return "%s/%s/%s" % (str(date)[:4], str(date)[:7], str(date))

    return makenestednodes(
        baseNode,
        dates,
        pathFunc,
        makeLeafNode=makeLeafNode,
        sort=True,
        reverse=reverse,
        nodeType=nodeType,
    )
