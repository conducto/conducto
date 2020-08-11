import conducto as co
from typing import Union
import os
import fnmatch
from enum import Enum


class Libraries(Enum):
    PYTHON_UNITTEST = 0
    PYTHON_PYTEST = 1


def gen_command(path, file, lib=Libraries.PYTHON_UNITTEST, from_dir=""):

    cd_cmd = "" if from_dir.strip() == "" else f"cd {from_dir};"

    lib_str = "unittest" if lib == Libraries.PYTHON_UNITTEST else "pytest"

    return f"{cd_cmd} python -m {lib_str} {path}{file}"


def fix_dir(dir):
    if dir[-1] == "/":
        return dir
    return dir + "/"


def discover(
    discover_dir, lib=Libraries.PYTHON_UNITTEST, source_dir=None, pat="test_*.py"
) -> co.Parallel:
    """
    :param discover_dir: directory from which tests will be discovered
    :param lib: test library (must be one of {Libraries.PYTHON_UNITTEST, Libraries.PYTHON_PYTEST})
    :param source_dir: directory from which all tests will be executed
    :param pat: pattern for test file discovery (default test_*.py)
    :return: co.Parallel
    """

    # data validation
    if source_dir is None:
        source_dir = discover_dir

    discover_dir = fix_dir(discover_dir)
    source_dir = fix_dir(source_dir)

    if not isinstance(lib, Libraries):
        raise ValueError(
            f"lib must be one of {set(str(i) for i in Libraries)}, got {lib}"
        )

    if not os.path.exists(discover_dir):
        raise ValueError(f"discover_dir does not exist (got {discover_dir})")

    if not os.path.exists(source_dir):
        raise ValueError(f"source_dir does not exist (got {source_dir})")

    # we want source_dir > base_dir

    if not discover_dir.startswith(source_dir):
        raise ValueError(
            f"Improperly configured directories. {source_dir} must be a parent "
            f"of {discover_dir}."
        )

    # where tests should be executed in docker

    image = co.Image(copy_dir=source_dir)

    output = co.Parallel(image=image)

    # first, traverse to see which directories either contain files
    # that match the given pattern or have directories that do
    have_pat = set()

    def first_traversal(dr):
        p, d, f = next(os.walk(dr))

        # don't traverse into directories that don't have an __init__.py file
        # tests won't really work properly otherwise
        if "__init__.py" not in f:
            return

        for i in d:
            if (
                i != "__pycache__"
            ):  # don't traverse into __pycache__ folders since tests won't appear there
                first_traversal(f"{dr}{i}/")

        if any(fnmatch.fnmatch(i, pat) for i in f) or any(
            f"{dr}{i}/" in have_pat for i in d
        ):
            have_pat.add(p)

    first_traversal(discover_dir)

    # now that we know what directories we want to traverse, actually build the tree
    def second_traversal(dr, node):
        p, d, f = next(os.walk(dr))

        d.sort()
        f.sort()

        for i in d:
            full_path = f"{dr}{i}/"
            if full_path in have_pat:
                node[i] = co.Parallel()
                second_traversal(full_path, node[i])

        for i in f:
            if fnmatch.fnmatch(i, pat):
                file_name = i.split(".")[0]
                rel_path = dr.replace(discover_dir, "")
                node[file_name] = co.Exec(gen_command(rel_path, i, lib=lib))

    second_traversal(discover_dir, output)

    return output
