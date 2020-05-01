import sys
import os.path
import importlib.util
import conducto as co
from conducto.shared import constants
from conducto.debug import debug, livedebug
import asyncio


def show(id, app=True, shell=False):
    """
    Attach to a an active pipeline.  If it is sleeping it will be awakened.
    """
    from . import api, shell_ui
    from .internal import build

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
    perms = api.Pipeline().perms(token, pipeline_id)

    status = pipeline["status"]
    if status not in pl.active | pl.standby and status in pl.local:
        local_basedir = constants.ConductoPaths.get_local_base_dir()
        cpser = constants.ConductoPaths.SERIALIZATION
        profile = api.Config().default_profile
        serialization_path = f"{local_basedir}/{profile}/{pipeline_id}/{cpser}"

        if not os.path.exists(serialization_path) and status not in pl.active:
            # TODO:  remove in May 2020 -- perhaps this is a pre-profile
            # pipeline and it needs to be moved to the correct profile
            # directory.  Check and convert if so.
            oldser = f"{local_basedir}/logs/{pipeline_id}/{cpser}"
            if os.path.exists(oldser):
                import shutil

                olddir = f"{local_basedir}/logs/{pipeline_id}"
                newdir = f"{local_basedir}/{profile}/{pipeline_id}"
                shutil.move(olddir, newdir)

                api.Pipeline().update(
                    token, pipeline_id, {"program_path": serialization_path}
                )

        if not os.path.exists(serialization_path):
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

    def cloud_wakeup():
        api.Manager().launch(token, pipeline_id)

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


def init(dir: str = ".", url: str = None):
    from . import api

    if url is None:
        url = "https://conducto.com"
    else:
        if not api.is_conducto_url(url):
            print(f"The url {url} is not recognized.", file=sys.stderr)
            sys.exit(1)

    dir = os.path.abspath(dir)

    if not os.path.isdir(dir):
        print(f"'{dir}' is not a directory or does not exist", file=sys.stderr)
        sys.exit(1)

    config = api.Config()
    for profile in config.profile_sections():
        if config.get(profile, "url") == url:
            break
    else:
        profile = None

    create_new = False
    if profile is not None:
        # we already have a profile for this url, let's see what the intent is.

        email = config.get(profile, "email")
        print(f"There is already a profile for {url} and e-mail {email}.")

        question = "Do you wish to connect this directory to this profile? [yn] "
        choice = input(question)

        if choice.lower()[0] == "y":
            pass
            # connect dir to this profile
        else:
            create_new = True
    else:
        create_new = True

    if create_new:
        os.environ["CONDUCTO_URL"] = url

        token = api.Auth().get_token_from_shell(force=True)

        config = api.Config()
        for profile in config.profile_sections():
            if config.get(profile, "token") == token:
                break

    api.dirconfig_write(dir, config.get(profile, "url"), config.get(profile, "org_id"))


async def migrate(pipeline_id):
    from . import api
    import json

    token = api.Auth().get_token_from_shell(force=True)
    conn = await api.connect_to_pipeline(token, pipeline_id)
    try:
        await conn.send(json.dumps({"type": "MIGRATE"}))
        # sleep, if I don't do this sometimes the command doesn't go through ¯\_(ツ)_/¯
        await asyncio.sleep(0.1)
    finally:
        await conn.close()


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
        "init",
        "migrate",
    ):
        variables = {
            "show": show,
            "debug": debug,
            "livedebug": livedebug,
            "init": init,
            "migrate": migrate,
        }
        co.main(variables=variables)
    else:
        file_to_execute, *arguments = args

        if not os.path.exists(file_to_execute):
            print(f"No such file or directory: '{file_to_execute}'", file=sys.stderr)
            sys.exit(1)

        module = _load_file_module(file_to_execute)
        variables = {k: getattr(module, k) for k in dir(module)}
        co.main(variables=variables, argv=arguments, filename=file_to_execute)


if __name__ == "__main__":
    main()
