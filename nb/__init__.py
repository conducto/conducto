import ast
import contextlib
import importlib
import inspect
import os
import traceback


IN_MARKDOWN = False


@contextlib.contextmanager
def cell():
    st = traceback.extract_stack(limit=3)
    module_name = os.path.basename(st[-3].filename).split(".py")[0]
    module = importlib.import_module(module_name)

    lineno = st[-3].lineno
    src_lines = inspect.findsource(module)[0]
    src = "".join(src_lines)
    root = ast.parse(src)
    for node in ast.walk(root):
        if isinstance(node, ast.With) and node.lineno == lineno:
            break
    else:
        raise Exception("Can't find 'with' statement in code")

    start = node.lineno
    if hasattr(node, "end_lineno"):
        # In Python 3.8+, end_lineno will be the end of the 'with' block
        end = node.end_lineno
    else:
        # Otherwise, find the line number of the last child. This is incorrect for
        # multi-line statements, but hopefully there won't be many of those.
        end = node.body[-1].lineno
    block_lines = src_lines[start:end]
    block = inspect.cleandoc("".join(block_lines))

    # Find comments at the top and render them as markdown
    code_lines = block.splitlines(keepends=True)
    comment_lines = []
    for i, line in enumerate(code_lines):
        line = line.lstrip()
        if line.startswith("#"):
            comment_lines.append(line[1:])
        else:
            code_lines = code_lines[i:]
            break
    comment = "".join(comment_lines)
    if any(l != "pass" for l in code_lines):
        code = f"```python\n{''.join(code_lines)}\n```"
    else:
        code = ""

    print(f"\n<ConductoMarkdown>{comment}\n{code}")

    global IN_MARKDOWN
    IN_MARKDOWN = True
    try:
        yield
    finally:
        print("</ConductoMarkdown>")
        IN_MARKDOWN = False


def matplotlib_inline():
    import matplotlib

    matplotlib.use("module://conducto.nb.backend_matplotlib")


__all__ = ["cell", "matplotlib_inline"]
