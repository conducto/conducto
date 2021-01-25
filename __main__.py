import sys
import json
import os.path
import importlib.util
import conducto as co
from conducto.shared import constants
from conducto import api
from conducto.contrib.discover.cli import discover_cli
from conducto.debug import debug, livedebug
from conducto.glue import method
import asyncio


def show(id, app=method._get_default_app(), shell=method._get_default_shell()):
    """
    Attach to a an active pipeline.  If it is sleeping it will be awakened.
    """
    from .internal import build

    pl = constants.PipelineLifecycle

    pipeline_id = id
    token = co.api.Config().get_token_from_shell(force=True)
    pipeline = method._get_pipeline_validated(token, pipeline_id)
    perms = co.api.Pipeline().perms(pipeline_id, token=token)

    status = pipeline["status"]
    if status not in pl.active | pl.standby and status in pl.local:
        local_basedir = constants.ConductoPaths.get_profile_base_dir()
        cpser = constants.ConductoPaths.SERIALIZATION
        serialization_path = f"{local_basedir}/pipelines/{pipeline_id}/{cpser}"

        if not os.path.exists(serialization_path):
            m = (
                f"The serialization for {pipeline_id} could not be found.  "
                "This is likely because it is local to another computer."
            )
            host = pipeline["meta"].get("hostname", None)
            if host is not None:
                m += f"  Try waking it from '{host}' with conducto show."
            m += "  For further assistance, contact us on Slack at ConductoHQ."

            print(m, file=sys.stderr)
            sys.exit(1)

    def cloud_wakeup():
        co.api.Manager().launch(pipeline_id, token=token)

    def local_wakeup():
        build.run_in_local_container(token, pipeline_id, update_token=True)

    if status in pl.active | pl.standby:
        if not app and not shell:
            print(f"Pipeline {pipeline_id} is already running.")
            return
        msg = "Connecting to"
        func = lambda: 0
        starting = True
    elif status in pl.local:
        if constants.Perms.LAUNCH not in perms:
            raise PermissionError(
                f"Pipeline {pipeline_id} is sleeping and you do not have permissions to wake it."
            )
        func = local_wakeup
        msg = "Waking"
        starting = True
    elif status in pl.cloud:
        if constants.Perms.LAUNCH not in perms:
            raise PermissionError(
                f"Pipeline {pipeline_id} is sleeping and you do not have permissions to wake it."
            )
        func = cloud_wakeup
        msg = "Waking"
        starting = False
    else:
        raise RuntimeError(
            f"Pipeline status {pipeline['status']} for {pipeline_id} is not recognized."
        )

    build.run(token, pipeline_id, func, app, shell, msg, starting)


async def sleep(id):
    pipeline_id = id
    token = co.api.Config().get_token_from_shell(force=True)
    pipeline = method._get_pipeline_validated(token, pipeline_id)

    status = pipeline["status"]
    pl = constants.PipelineLifecycle
    if status in pl.active:
        conn = await co.api.connect_to_pipeline(pipeline_id, token=token)
        try:
            await conn.send(json.dumps({"type": "CLOSE_PROGRAM"}))

            async def await_confirm(conn):
                was_slept = False
                async for msg_text in conn:
                    msg = json.loads(msg_text)
                    if msg["type"] == "SLEEP":
                        was_slept = True
                        # we are done here, acknowledged!
                        break
                return was_slept

            # 60 seconds is an extravagantly long expectation here, but it is
            # intended to cover our bases and only error on true errors.
            await asyncio.wait_for(await_confirm(conn), timeout=60.0)
        except asyncio.TimeoutError:
            print("The pipeline was not slept successfully.", file=sys.stderr)
            sys.exit(1)
        finally:
            await conn.close()
    else:
        co.api.Pipeline().sleep_standby(pipeline_id, token=token)


def dump_serialization(id, outfile=None):
    import gzip
    import base64

    string = gzip.decompress(base64.b64decode(method.return_serialization(id)))
    data = json.loads(string)
    if outfile is None:
        print(json.dumps(data, indent=4, sort_keys=True))
    else:
        with open(outfile, "w") as f2:
            json.dump(data, f2)


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


def share_directory(
    name,
    relative,
):
    from conducto.image import Image

    Image.share_directory(name, relative)


def validate_serialization():
    serialization = sys.stdin.read()
    node = co.Node.deserialize(serialization)
    if os.getenv("__RUN_BY_WORKER__"):
        # Variable is set in conducto_worker/__main__.py to avoid
        # printing ugly serialization when not needed.
        from conducto.internal.build import validate_tree

        validate_tree(node, cloud=False, check_images=False, set_default=False)

        s = node.serialize()
        print(f"\n<__conducto_serialization>{s}</__conducto_serialization>\n")
    print(node.pretty(strict=False))


def build(
    shell=False,
    app=True,
    local=False,
    cloud=False,
    retention=7,
    is_public=False,
    run=False,
):
    from conducto.internal import build

    assert local ^ cloud, "An invalid number of build modes were specified"

    serialization = sys.stdin.read()
    node = co.Node.deserialize(serialization)
    node._autorun = run
    build.build(
        node,
        use_shell=shell,
        use_app=app,
        build_mode=constants.BuildMode.LOCAL
        if local
        else constants.BuildMode.DEPLOY_TO_CLOUD,
        retention=retention,
        is_public=is_public,
    )


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
        "dump-serialization",
        "validate-serialization",
        "share-directory",
        "build",
        "sleep",
        "discover",
    ):
        variables = {
            "build": build,
            "show": show,
            "debug": debug,
            "livedebug": livedebug,
            "dump-serialization": dump_serialization,
            "validate-serialization": validate_serialization,
            "share-directory": share_directory,
            "sleep": sleep,
            "discover": discover_cli,
        }
        co.main(variables=variables)
    else:
        file_to_execute, *arguments = args

        if file_to_execute.endswith(".js"):
            fxn, props = arguments

            script = f'let out = require("./{file_to_execute}").{fxn}({props});'
            script += (
                'if(typeof(out) == "object") { '
                "let cls_name = out.constructor.name;  "
                'if(cls_name == "Exec" || cls_name == "Parallel" || cls_name == "Serial") out.output();}'
            )

            to_exec = f"node -r esm -e '{script}'"
            import subprocess

            subprocess.run(to_exec, shell=True, check=True)
            return

        if not os.path.exists(file_to_execute):
            print(f"No such file or directory: '{file_to_execute}'", file=sys.stderr)
            sys.exit(1)

        if file_to_execute.endswith(".cfg"):
            with open(file_to_execute) as f:
                co.glue.run_cfg(f, arguments)
        else:
            module = _load_file_module(file_to_execute)
            variables = {k: getattr(module, k) for k in dir(module)}
            co.main(variables=variables, argv=arguments, filename=file_to_execute)


if __name__ == "__main__":
    main()
