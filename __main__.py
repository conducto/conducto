import sys
import os.path
import importlib.util
import colorama
import conducto as co
from conducto.shared import constants
from conducto.debug import debug, livedebug

colorama.init()


def show(id):
    """
    Attach to a an active pipeline.  If it is sleeping it will be awakened.
    """
    from . import api, shell_ui

    pl = constants.PipelineLifecycle

    pipeline_id = id
    token = api.Auth().get_token_from_shell(force=True)
    try:
        pipeline = api.Pipeline().get(token, pipeline_id)
    except api.InvalidResponse as e:
        if "not found" in str(e):
            print(str(e), file=sys.stderr)
            sys.exit(1)
        else:
            raise

    status = pipeline["status"]
    if status not in pl.active | pl.standby and status in pl.local:
        local_basedir = constants.ConductoPaths.get_local_base_dir()
        cpser = constants.ConductoPaths.SERIALIZATION
        serialization = f"{local_basedir}/logs/{pipeline_id}/{cpser}"

        if not os.path.exists(serialization):
            m = (
                f"The serialization for {pipeline_id} could not be found.  "
                "This is likely because it is local to another computer."
            )
            host = pipeline["meta"].get("hostname", None)
            if host != None:
                m += f"  Try waking it from '{host}' with conducto show."
            m += f"  For further assistance, contact us on Slack at ConductoHQ."

            print(m, file=sys.stderr)
            sys.exit(1)

    url = shell_ui.connect_url(pipeline_id)
    print(f"View at {url}")

    def cloud_wakeup():
        api.Manager().launch(token, pipeline_id)

    def local_wakeup():
        from conducto.internal import build

        build.run_in_local_container(token, pipeline_id, update_token=True)

    if status in pl.active | pl.standby:
        func = lambda: 0
        starthelp = "Connecting"
    elif status in pl.local:
        func = local_wakeup
        starthelp = "Waking"
    elif status in pl.cloud:
        func = cloud_wakeup
        starthelp = "Waking"
    else:
        raise RuntimeError(
            f"Pipeline status {pipeline['status']} for {pipeline_id} is not recognized."
        )

    shell_ui.connect(token, pipeline_id, func, starthelp)


def _load_file_module(filename):
    # put the directory of the file on sys.path for relative imports
    norm = os.path.realpath(filename)
    sys.path.append(os.path.dirname(norm))

    # get module name
    basename = os.path.basename(filename)
    modname = os.path.splitext(basename)[0]

    # import
    spec = importlib.util.spec_from_file_location(modname, filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main():
    # _thisfile, file_to_execute, *arguments = sys.argv
    args = sys.argv[1:]
    if not args or args[0] in (
        "-h",
        "--help",
        "--version",
        "show",
        "debug",
        "livedebug",
    ):
        variables = {"show": show, "debug": debug, "livedebug": livedebug}
        co.main(variables=variables)
    else:
        file_to_execute, *arguments = args

        if not os.path.exists(file_to_execute):
            print(f"No such file or directory: '{file_to_execute}'", file=sys.stderr)
            sys.exit(1)

        module = _load_file_module(file_to_execute)
        variables = {k: getattr(module, k) for k in dir(module)}
        co.main(variables=variables, argv=arguments)


if __name__ == "__main__":
    main()
