import conducto as co, typing
from . import utils

__package__ = "ci_cd"


def ci_cd(projects=utils.get_projects()) -> co.Serial:
    "Build all projects, run tests if builds succeed, then deploy if tests pass"

    # User the standard python 3.8 image as a base and add all files from
    # the current dir. We also need to install conducto in the image in
    # order to dynamically generate the tree with Lazy in test().
    img = co.Image(image="python:3.8", copy_dir=".", reqs_py=["conducto"])

    output = co.Serial(image=img)
    output["Build"] = build(projects)
    output["Test"] = test(projects)
    output["Deploy"] = co.Exec("echo aws cloudformation deploy")
    return output


def build(projects: typing.List[str]) -> co.Parallel:
    "Build projects in parallel, using simple shell command."

    # Override the parent image to use one with docker installed.
    img = co.Image(image="docker:19.03", copy_dir=".")

    output = co.Parallel(image=img, requires_docker=True)
    for project in projects:
        # Command needs docker; inherits flag from parent node
        output[project] = co.Exec(f"cd {project} && docker build .")
    return output


def test(projects: typing.List[str]) -> co.Parallel:
    "Group tests by project, all in parallel."
    output = co.Parallel()
    for project in projects:
        output[project] = co.Parallel()
        for name in utils.get_tests(project):
            # co.Exec often accepts a command string. In this case it takes (func, *args, **kwargs),
            output[project][name] = co.Exec(utils.run_test, project, test=name)
    return output


if __name__ == "__main__":
    co.main(default=ci_cd)
