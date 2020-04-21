"""
These methods are mocks that support tutorials/ci_cd/__main__.py. A real project
will have its own CI/CD code, with real projects and real tests. Here we return
hardcoded tests and sleep; you would fill these methods in with real code.
"""


def get_projects():
    return ["app", "backend", "monitoring"]


def get_tests(project):
    return {
        "app": ["unit_test", "code_quality", "coverage"],
        "backend": ["test_one", "failover", "scale"],
        "monitoring": ["connectivity", "endpoints"],
    }[project]


def run_test(project, test):
    print(f"Running {test} test in {project}")
    import time

    for i in range(50):
        time.sleep(0.1)
        print(".", end="", flush=True)
    print("\nDone!")
