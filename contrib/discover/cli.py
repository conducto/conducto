import os
import conducto as co
from .treegen import fix_dir, Libraries
from .treegen import discover as treegen
from .codegen import codegen


# implementation of cli function for discover
def discover_cli(
    discover_dir=".",
    lib="unittest",
    source_dir=None,
    file=None,
    run_pipeline=False,
    cloud=False,
    pat="test_*.py",
):
    """
    --discover_dir - directory from which tests will be discovered
    --lib - test library (must be one of {unittest, pytest})
    --source_dir - directory from which all tests will be executed
    --file - file to write generated code to
    --run_pipeline - specify to launch the generated pipeline instantly
    --cloud - specify to run the launched pipeline in the cloud
    --pat - pattern for test file discovery (default test_*.py)
    """
    # fix arguments
    cwd = fix_dir(os.getcwd())

    if discover_dir[0] != "/":
        discover_dir = fix_dir(cwd + discover_dir)

    if source_dir is not None and source_dir[0] != "/":
        source_dir = fix_dir(cwd + source_dir)

    if lib not in {"unittest", "pytest"}:
        raise ValueError('lib must be one of {"unittest", "pytest"}' + f", got {lib}")

    lib = Libraries.PYTHON_UNITTEST if lib == "unittest" else Libraries.PYTHON_PYTEST

    if file is None and run_pipeline is None:
        raise ValueError("Either file must be not None or run must be True")

    if file is not None:
        # write generated code to file
        codegen(file, discover_dir, lib=lib, source_dir=source_dir, pat=pat)

    # it's possible to both generate code to a file and run that pipeline
    if run_pipeline:

        reqs_py = ["pytest"] if lib == lib.PYTHON_PYTEST else []

        if source_dir is None:
            source_dir = discover_dir

        def root() -> co.Parallel:
            return treegen(discover_dir, lib=lib, source_dir=source_dir, pat=pat)

        co.main(default=root, argv=["--cloud" if cloud else "--local"])
