import conducto as do, typing
from . import utils

__package__ = "ci_cd"

# Use the standard python 3.8 image as a base and add all files from the current dir.
IMG = do.Image(image="python:3.8", context=".")


def ci_cd(projects=utils.get_projects()) -> do.Serial:
    "Build all projects, run tests if builds succeed, then deploy if tests pass"
    output = do.Serial(image=IMG)
    output["Build"] = build(projects)
    output["Test"] = test(projects)
    output["Deploy"] = do.Exec("echo aws cloudformation deploy")
    return output


def build(projects: typing.List[str]) -> do.Parallel:
    "Build projects in parallel, using simple shell command."
    output = do.Parallel(requires_docker=True)
    for project in projects:
        # Command needs docker; inherits flag from parent node
        output[project] = do.Exec(f"cd {project} && docker build .")
    return output


def test(projects: typing.List[str]) -> do.Parallel:
    "Group tests by project, all in parallel, with the do.lazy_py helper"
    output = do.Parallel()
    for project in projects:
        output[project] = do.Parallel()
        for name in utils.get_tests(project):
            # do.lazy_py makes a node that calls the given method and args,
            output[project][name] = do.lazy_py(utils.run_test, project, test=name)
    return output


if __name__ == "__main__":
    do.main(default=ci_cd)
