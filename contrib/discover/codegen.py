from .treegen import discover, Libraries
import conducto as co
from collections import defaultdict


def codegen(
    filename,
    discover_dir,
    lib=Libraries.PYTHON_UNITTEST,
    source_dir=None,
    pat="test_*.py",
):
    """
    :param filename: file to write generated code to
    :param discover_dir: directory from which tests will be discovered
    :param lib: test library (must be one of {Libraries.PYTHON_UNITTEST, Libraries.PYTHON_PYTEST})
    :param source_dir: directory from which all tests will be executed
    :param pat: pattern for test file discovery (default test_*.py)
    :return:
    """
    if source_dir is None:
        source_dir = discover_dir

    # generate a tree using the other discover
    # use this tree to build up a python file with code
    tree = discover(discover_dir, lib, source_dir, pat)

    # code for the discover function
    func_code = []
    need_indent = False

    # traversal algorithm that ensures proper indentation levels and functions names
    def traverse(node, level):

        # context for this node
        nonlocal need_indent
        if need_indent:
            func_code.append("")
            need_indent = False
        func_code.append(level * "    " + f'with co.Parallel(name="{node.name}"):')

        for i, j in node.children.items():
            if isinstance(j, co.Parallel):
                traverse(j, level + 1)
            if isinstance(j, co.Exec):
                # append to current code block

                func_code.append(
                    (level + 1) * "    "
                    + f'co.Exec(name="{j.name}", command="{j.command}")'
                )
                need_indent = True

                # print(j.command)

    traverse(tree, 1)

    if not func_code:
        raise ValueError(
            "Tree is empty. Please make sure your unittests are actually discoverable!"
        )

    # fix function code to use a normal assignment on the first level
    del func_code[0]

    func_code.append("")
    func_code.append(f"    return output")

    reqs_py = ["pytest"] if lib == lib.PYTHON_PYTEST else []

    # write to file
    f = open(filename, "w+")

    f.write(
        f"""# Code generated using "conducto discover"
# To run this pipeline, run "python {filename} main --local"
import conducto as co\n\n
def discover() -> co.Parallel:
    # this is the image used to run these tests
    # modify it as appropriate 
    # https://www.conducto.com/docs/basics/images 
    image = co.Image(copy_dir=\"{source_dir}\", reqs_py={reqs_py}) 

    with co.Parallel(image=image) as output:
"""
    )

    for i in func_code:
        f.write(i)
        f.write("\n")

    f.write(
        f"""\n
if __name__ == "__main__":
    co.main(default=discover)
"""
    )

    f.close()
