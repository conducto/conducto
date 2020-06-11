import asyncio
import collections
import contextlib
import concurrent.futures
import functools
import hashlib
import json
import os
import re
import subprocess
import sys
import time
import traceback
import typing
import uuid

import conducto.internal.host_detection as hostdet
from conducto.shared import async_utils, log, constants
import conducto
from .. import pipeline
from . import dockerfile as dockerfile_mod, names


def relpath(path):
    """
    Construct a path with decoration to enable translation inside a docker
    image for a node.  This may be used to construct path parameters to a
    command line tool.

    This is used internally by :py:class:`conducto.Exec` when used with a
    Python callable to construct the command line which executes that callable
    in the pipeline.
    """

    ctxpath = Image.get_contextual_path(path)
    return f"__conducto_path:{ctxpath}:endpath__"


def parse_registered_path(path):
    """
    This function takes a path which may include registered names for the
    active profile and parses out the registered name, path hints and tail.
    """

    regpath = collections.namedtuple("regpath", ["name", "hint", "tail"])

    mm = re.match(r"^\$\{([A-Z_][A-Z0-9_]*)(|=([^}]*))\}(.*)", path)
    if mm is not None:
        name = mm.group(1)
        hint = mm.group(3)
        tail = mm.group(4)
        if tail is None:
            tail = ""

        return regpath(name, hint, tail)
    return None


def resolve_registered_path(path):
    # This converts a path from a serialization to a host machine.  It may or
    # may not the same host machine as the one that made the serialization and
    # it may need to be interactive with the user.

    if constants.ExecutionEnv.value() not in constants.ExecutionEnv.external:
        raise RuntimeError(
            "this is an interactive function and has no manager (or worker) implementation"
        )

    regparse = parse_registered_path(path)
    if regparse is None:
        result = path
    else:
        conf = conducto.api.Config()
        mounts = conf.get_named_mount_paths(conf.default_profile, regparse.name)

        if len(mounts) == 1:
            result = "/".join([mounts[0], regparse.tail.lstrip("/")])
        elif len(mounts) == 0:
            # ask
            print(
                f"The named mount {regparse.name} is not known on this host. Enter a path for that mount (blank to abort)."
            )
            path = input("path:  ")
            if path == "":
                raise RuntimeError("error mounting directories")
            result = "/".join([path, regparse.tail.lstrip("/")])
            conf.register_named_mount(conf.default_profile, regparse.name, path)
        elif regparse.hint in mounts:
            result = "/".join([regparse.hint, regparse.tail.lstrip("/")])
        else:
            # disambiguate by asking which
            print(
                f"The named mount {regparse.name} is associated with multiple directories on this host. Select one or enter a new directory."
            )
            for index, mpath in enumerate(mounts):
                print(f"\t[{index+1}] {mpath}")
            print("\t[new] Enter a new path")

            def safe_int(s):
                try:
                    return int(s)
                except ValueError:
                    return None

            while True:
                sel = input(f"path [1-{len(mounts)}, new]:  ")
                if sel == "":
                    raise RuntimeError("error mounting directories")
                if sel == "new" or safe_int(sel) is not None:
                    break

            if sel == "new":
                path = input("path:  ")
                conf.register_named_mount(conf.default_profile, regparse.name, path)
            else:
                path = mounts[int(sel) - 1]
            result = "/".join([path, regparse.tail.lstrip("/")])

    if hostdet.is_windows() or hostdet.is_wsl():
        # The profile is kept on windows and the mount path is kept as windows format
        result = hostdet.windows_docker_path(result)

    return result


def serialization_path_interpretation(p):
    # This converts a path from a serialization to the manager or the host
    # machine which created this serialization.  It is will return a valid path
    # supposing that the host machine's directory structure matches what was at
    # the point of pipeline creation.  No information required from the user as
    # in resolve_registered_path.

    if constants.ExecutionEnv.value() in constants.ExecutionEnv.manager_all:
        regparse = parse_registered_path(p)
        baseseg = regparse.hint if regparse else p
        if os.getenv("WINDOWS_HOST") and baseseg[1] == ":":
            baseseg = hostdet.windows_docker_path(baseseg)
        tail = regparse.tail if regparse else ""

        # host `/` is mounted at `/mnt/external`
        return Image.PATH_PREFIX + baseseg + tail
    else:
        regparse = parse_registered_path(p)
        if regparse is not None:
            unix_path = f"{regparse.hint}/{regparse.tail}"
        else:
            unix_path = p
        # this results in a unix host path ...
        if hostdet.is_wsl() or hostdet.is_windows():
            # ... or a docker-friendly windows path
            unix_path = hostdet.windows_docker_path(unix_path)
        return unix_path


@functools.lru_cache(None)
def _split_windocker(path):
    # TODO:  is this used?
    chunks = path.split("//")
    mangled = hostdet.wsl_host_docker_path(chunks[0])
    if len(chunks) > 1:
        newctx = f"{mangled}//{chunks[1]}"
    else:
        newctx = mangled
    return newctx


@functools.lru_cache(None)
def _split_winpath(path):
    chunks = path.split("//")
    mangled = hostdet.windows_drive_path(chunks[0]).replace("\\", "/")
    if len(chunks) > 1:
        newctx = f"{mangled}//{chunks[1]}"
    else:
        newctx = mangled
    return newctx


class Status:
    PENDING = "pending"
    QUEUED = "queued"
    PULLING = "pulling"
    BUILDING = "building"
    COMPLETING = "completing"
    EXTENDING = "extending"
    PUSHING = "pushing"
    DONE = "done"
    ERROR = "error"
    CANCELLED = "cancelled"
    order = [
        PENDING,
        QUEUED,
        PULLING,
        BUILDING,
        COMPLETING,
        EXTENDING,
        PUSHING,
        DONE,
        ERROR,
        CANCELLED,
    ]


class Repository:
    """A collection of images with different names"""

    class DuplicateImageError(Exception):
        pass

    def __init__(self):
        self.images: typing.Dict[str, Image] = {}

    def __delitem__(self, key):
        if type(key) == str:
            del self.images[key]
        else:
            for name, img in list(self.images.items()):
                if img == key:
                    del self[name]
                    break
            else:
                raise KeyError

    def __getitem__(self, name):
        return self.images[name]

    def Image(self, *args, **kwargs):
        img = Image(*args, **kwargs)
        self.add(img)
        return img

    def add(self, image):
        if image.name in self.images and self.images[image.name] != image:
            raise self.DuplicateImageError(
                f"{image.name} already present with a different definition in this repository"
            )
        self.images[image.name] = image

    def merge(self, repo):
        # this makes merging all images into the root O(NlogN)
        if len(repo.images) > len(self.images):
            self.images, repo.images = repo.images, self.images
        for img in repo.images.values():
            self.add(img)


class Image:
    """
    :param image:  Specify the base image to start from. Code can be added with
        various context* variables, and packages with reqs_* variables.
    :param dockerfile:  Use instead of image and pass a path to a Dockerfile.
        Relative paths are evaluated starting from the file where this code is
        written. Unless otherwise specified, it uses the directory of the
        Dockerfile as the build context
    :param docker_build_args: Dict mapping names of arguments to `docker
        --build-args` to values
    :param docker_auto_workdir: `bool`, default `True`, set the work-dir to the
        destination of `copy_dir`
    :param context:  Use this to specify a custom docker build context when
        using `dockerfile`.
    :param copy_dir:  Path to a directory. All files in that directory (and its
        subdirectories) will be copied into the generated Docker image.
    :param copy_url:  URL to a Git repo. Conducto will clone it and copy its
        contents into the generated Docker image. Authenticate to private
        GitHub repos with a URL like `https://{user}:{token}@github.com/...`.
        See secrets for more info on how to store this securely. Must also
        specify copy_branch.
    :param copy_branch:  A specific branch name to clone. Required if using copy_url.
    :param path_map:  Dict that maps external_path to internal_path. Needed for
        live debug and :py:func:`conducto.Lazy`. It can be inferred from
        `copy_dir`; if not using that, you must specify `path_map`.
    :param reqs_py:  List of Python packages for Conducto to pip install into
        the generated Docker image.
    :param name: Name this `Image` so other Nodes can reference it by name. If
        no name is given, one will automatically be generated from a list of
        our favorite Pokemon. I choose you, angry-bulbasaur!
    """

    PATH_PREFIX = ""
    _PULLED_WORKER = False

    def __init__(
        self,
        image=None,
        *,
        dockerfile=None,
        docker_build_args=None,
        context=None,
        copy_dir=None,
        copy_url=None,
        copy_branch=None,
        docker_auto_workdir=True,
        reqs_py=None,
        path_map=None,
        name=None,
        pre_built=False,
    ):

        if name is None:
            name = names.NameGenerator.name()

        self.name = name

        if image is not None and dockerfile is not None:
            raise ValueError(
                f"Cannot specify both image ({image}) and dockerfile ({dockerfile})"
            )
        if image is None and dockerfile is None:
            # Default to user's current version of python, if none is specified
            image = f"python:{sys.version_info[0]}.{sys.version_info[1]}-slim"
            if not reqs_py:
                reqs_py = ["conducto"]
        if copy_dir is not None and copy_url is not None:
            raise ValueError(
                f"Cannot specify both copy_dir ({copy_dir}) "
                f"and copy_url ({copy_url})"
            )
        if copy_url is not None and copy_branch is None:
            raise ValueError(
                f"If specifying copy_url ({copy_url}) must "
                f"also specify copy_branch ({copy_branch})"
            )

        self.image = image
        self.dockerfile = dockerfile
        self.docker_build_args = docker_build_args
        self.context = context
        self.copy_dir = copy_dir
        self.copy_url = copy_url
        self.copy_branch = copy_branch
        self.docker_auto_workdir = docker_auto_workdir
        self.reqs_py = reqs_py
        self.path_map = path_map

        self.pre_built = pre_built

        if not self.pre_built:
            if dockerfile is not None:
                if context is None:
                    # NOTE:  get_contextual_path is not idempotent (as below)
                    context = os.path.dirname(self.dockerfile)
                self.dockerfile = self.get_contextual_path(dockerfile)
                self.context = self.get_contextual_path(context)

            if copy_dir is not None:
                self.copy_dir = self.get_contextual_path(copy_dir)
                # NOTE:  get_contextual_path is not idempotent (although
                # perhaps it should be).  This is the reason why I assign the
                # key as copy_dir here and the keys are transformed with
                # get_contextual_path below.
                self.path_map = {copy_dir: dockerfile_mod.COPY_DIR}

        if self.path_map:
            # Don't use a dict comprehension here - get_contextual_path looks back a
            # specific number of frames in the stack to determine context, but a dict
            # comprehension adds an extra layer that messes it up.
            tmp_path_map = {}
            for external, internal in self.path_map.items():
                external = self.get_contextual_path(external)
                tmp_path_map[external] = internal
            self.path_map = tmp_path_map

        self.history = [HistoryEntry(Status.PENDING)]

        if self.pre_built:
            self.history.append(HistoryEntry(Status.DONE, finish=True))

        self._make_fut: typing.Optional[asyncio.Future] = None

        self._remote_exec_fn: typing.Optional[typing.Callable] = None
        self._pipeline_id = os.getenv("CONDUCTO_PIPELINE_ID")

    def __eq__(self, other):
        return isinstance(other, Image) and self.to_dict() == other.to_dict()

    # hack to get this to serialize
    @property
    def _id(self):
        return self.to_dict()

    def to_dict(self):
        return {
            "name": self.name,
            "image": self.image,
            "dockerfile": self.dockerfile,
            "docker_build_args": self.docker_build_args,
            "docker_auto_workdir": self.docker_auto_workdir,
            "context": self.context,
            "copy_dir": self.copy_dir,
            "copy_url": self.copy_url,
            "copy_branch": self.copy_branch,
            "reqs_py": self.reqs_py,
            "path_map": self.path_map,
            "pre_built": self.pre_built,
        }

    def to_raw_image(self):
        return {
            "image": self.image,
            "dockerfile": self.dockerfile,
            "docker_build_args": self.docker_build_args,
            "docker_auto_workdir": self.docker_auto_workdir,
            "context": self.context,
            "copy_dir": self.copy_dir,
            "copy_url": self.copy_url,
            "copy_branch": self.copy_branch,
            "reqs_py": self.reqs_py,
            "path_map": self.path_map,
        }

    @staticmethod
    def get_contextual_path(p):
        if constants.ExecutionEnv.value() in constants.ExecutionEnv.manager_all:
            # Note:  This can run in the worker in Lazy nodes.
            return p

        op = os.path

        regparse = parse_registered_path(p)
        if regparse is not None:
            return p

        # Translate relative paths as starting from the file where they were defined.
        if not op.isabs(p):
            # Walk the stack. The first file that's not in the Conducto dir is the one
            # the user called this from. Evaluate `p` relative to that file.

            for frame, _lineno in traceback.walk_stack(None):
                filename = frame.f_code.co_filename
                if not filename.startswith(_conducto_dir):
                    from_dir = op.dirname(filename)
                    p = op.realpath(op.join(from_dir, p))
                    break

        # Apply context specified from outside this container. Needed for recursive
        # co.Lazy calls inside an Image with ".path_map".
        path_map_text = os.getenv("CONDUCTO_PATH_MAP")
        if path_map_text:
            path_map = json.loads(path_map_text)
            for external, internal in path_map.items():
                if p.startswith(internal):
                    p = p.replace(internal, external, 1)

        # If ".copy_dir" wasn't set, find the git root so that we could possibly use this
        # inside an Image with ".copy_url" set.
        elif op.exists(p):
            if op.isfile(p):
                dirname = op.dirname(p)
            else:
                dirname = p
            git_root = Image._get_git_root(dirname)
            if git_root:
                p = p.replace(git_root, git_root + "/", 1)

        if hostdet.is_wsl():
            # The point is largely that we state paths in terms of the docker
            # host and the recommended docker method in WSL1 is to use the
            # windows installation as a remote docker host.
            p_host = _split_winpath(p).replace("/", "\\")
        else:
            p_host = p

        auto_reg_path = None
        auto_reg_name = None
        dirsettings = conducto.api.dirconfig_detect(p)
        if dirsettings and "registered" in dirsettings:
            auto_reg_name = dirsettings["registered"]
            auto_reg_path = dirsettings["dir-path"]

        # iterate through named mounts looking for a match in this org
        if auto_reg_path is None:
            conf = conducto.api.Config()
            mounts = conf.get_named_mount_mapping(conf.default_profile)

            def enum():
                for name, paths in mounts.items():
                    for path in paths:
                        yield name, path

            for name, path in enum():
                if p_host.startswith(path):
                    # bingo, we have recognized this as a mount for your org
                    auto_reg_name = name
                    auto_reg_path = path
                    break

        if auto_reg_path:
            # convert to registered path and return
            unix_tail = p_host[len(auto_reg_path) :].replace("\\", "/")
            p = f"${{{auto_reg_name.upper()}={auto_reg_path}}}" + unix_tail
            return p
        else:
            # convert to windows now
            if hostdet.is_wsl():
                p = _split_winpath(p).replace("/", "\\")
            elif hostdet.is_windows():
                p = hostdet.windows_docker_path(p)

        return p

    @staticmethod
    @functools.lru_cache(maxsize=None)
    def _get_git_root(dirpath):
        result = None

        try:
            PIPE = subprocess.PIPE
            args = ["git", "-C", dirpath, "rev-parse", "--show-toplevel"]
            out, err = subprocess.Popen(args, stdout=PIPE, stderr=PIPE).communicate()

            nongit = "fatal: not a git repository"
            if not err.decode("utf-8").rstrip().startswith(nongit):
                result = out.decode("utf-8").rstrip()
        except FileNotFoundError:
            # log, but essentially pass
            log.debug("no git installation found, skipping directory indication")
        return result

    @staticmethod
    def register_directory(name, relative):
        op = os.path

        # Translate relative paths as starting from the file where they were defined.
        if not op.isabs(relative):
            # Walk the stack. The first file that's not in the Conducto dir is the one
            # the user called this from. Evaluate `p` relative to that file.

            for frame, _lineno in traceback.walk_stack(None):
                filename = frame.f_code.co_filename
                if not filename.startswith(_conducto_dir):
                    from_dir = op.dirname(filename)
                    relative = op.realpath(op.join(from_dir, relative))
                    break

        config = conducto.api.Config()
        config.register_named_mount(config.default_profile, name, relative)

    @property
    def name_built(self):
        if self.needs_building():
            key = json.dumps(self.to_raw_image()).encode()
            tag = hashlib.md5(key).hexdigest()
            return f"conducto_built:{self._pipeline_id}_{tag}"
        else:
            return self.image

    @property
    def name_complete(self):
        if self.needs_completing():
            key = json.dumps(self.to_raw_image()).encode()
            tag = hashlib.md5(key).hexdigest()
            return f"conducto_complete:{self._pipeline_id}_{tag}"
        else:
            return self.name_built

    @property
    def name_local_extended(self):
        tag = hashlib.md5(self.name_complete.encode()).hexdigest()
        return f"conducto_extended:{self._pipeline_id}_{tag}"

    @property
    def name_cloud_extended(self):
        from .. import api

        if self._pipeline_id is None:
            raise ValueError("Must specify pipeline_id before pushing to cloud")
        docker_domain = api.Config().get_docker_domain()
        tag = hashlib.md5(self.name_complete.encode()).hexdigest()
        return f"{docker_domain}/{self._pipeline_id}:{tag}"

    @property
    def status(self):
        return self.history[-1].status

    @property
    def build_error(self):
        if self.history[-1].status == Status.ERROR:
            h = self.history[-1]
            if h.stderr:
                return h.stderr
            else:
                return h.stdout
        return None

    def _set_remote_exec_fn(self, func):
        """
        Support remote building. `func` must behave like `async_utils.run_and_check`.
        """
        self._remote_exec_fn = func

    def _exec(self, *args, **kwargs):
        # Docker steps may run in the cloud if they don't rely on anything from a user's
        # machine. This is only 'dockerfile' and 'copy_dir'. Other inputs, like 'image',
        # 'copy_url', or 'reqs_*' can all be done from anywhere, so those images can be
        # built in the cloud.
        may_run_in_cloud = self.dockerfile is None and self.copy_dir is None

        # If it may not run in the cloud, and if a remote executor is installed, then
        # use that runner.
        if not may_run_in_cloud and self._remote_exec_fn is not None:
            return self._remote_exec_fn(*args, **kwargs)
        else:
            # Otherwise, just build it here using `run_and_check`.
            return async_utils.run_and_check(*args, **kwargs)

    async def make(self, push_to_cloud, callback=lambda: None):
        # Only call _make() once, and all other calls should just return the
        # same result.

        if self._make_fut is None:
            self._make_fut = asyncio.ensure_future(self._make(push_to_cloud, callback))
        is_already_done = self._make_fut.done()
        try:
            await self._make_fut
        finally:
            # If the _make() call had already finished, then there's no event
            # to run the callback on.
            if not is_already_done:
                callback()

    async def _make(self, push_to_cloud, callback):
        async for _ in self._make_generator(push_to_cloud, callback):
            pass

    async def _make_generator(self, push_to_cloud, callback):
        """
        Generator that pulls/builds/extends/pushes this Image.
        """
        if self.history and self.history[-1].end is None:
            self.history[-1].finish()

        # Pull the image if needed
        if self.image and "/" in self.image:
            with self._new_status(Status.PULLING) as st:
                callback()
                out, err = await self._exec("docker", "pull", self.image)
                st.finish(out, err)
        yield

        # Build the image if needed
        if self.needs_building():
            with self._new_status(Status.BUILDING) as st:
                callback()
                out, err = await self._build()
                st.finish(out, err)
        yield

        # If needed, copy files into the image and install packages
        if self.needs_completing():
            with self._new_status(Status.COMPLETING) as st:
                callback()
                out, err = await self._complete()
                st.finish(out, err)
        yield

        with self._new_status(Status.EXTENDING) as st:
            callback()
            out, err = await self._extend()
            st.finish(out, err)
        yield

        if push_to_cloud:
            with self._new_status(Status.PUSHING) as st:
                callback()
                out, err = await self._push()
                st.finish(out, err)

        self.history.append(HistoryEntry(Status.DONE, finish=True))

    @contextlib.contextmanager
    def _new_status(self, status):
        entry = HistoryEntry(status)
        self.history.append(entry)
        try:
            yield entry
        except subprocess.CalledProcessError as e:
            entry.finish(e.stdout, e.stderr)
            x = HistoryEntry(Status.ERROR, finish=True)
            x.finish(e.stdout, e.stderr)
            self.history.append(x)
            raise
        except (asyncio.CancelledError, concurrent.futures.CancelledError):
            entry.finish(None, None)
            self.history.append(HistoryEntry(Status.CANCELLED, finish=True))
            raise
        except Exception:
            entry.finish(None, traceback.format_exc())
            self.history.append(HistoryEntry(Status.ERROR, finish=True))
            raise
        else:
            if not entry.end:
                entry.finish()

    async def _build(self):
        """
        "Build" this image. If a Dockerfile is specified, build it first. If copy_* or
        reqs_py are specified, auto-generate a Dockerfile and add them.
        Otherwise, run 'docker build', possibly auto-generating a Dockerfile.
        """
        assert self.dockerfile is not None
        # Build the specified dockerfile
        build_args = []
        if self.docker_build_args is not None:
            for k, v in self.docker_build_args.items():
                build_args += ["--build-arg", "{}={}".format(k, v)]
        build_args += [
            "-f",
            serialization_path_interpretation(self.dockerfile),
            serialization_path_interpretation(self.context),
        ]

        out, err = await self._exec(
            "docker",
            "build",
            "-t",
            self.name_built,
            "--label",
            "com.conducto.build",
            *build_args,
        )
        return out, err

    async def _complete(self):
        # Create dockerfile from stdin. Replace "-f <dockerfile> <copy_dir>"
        # with "-"
        if self.copy_dir:
            build_args = [
                "-f",
                "-",
                serialization_path_interpretation(self.copy_dir),
            ]
        else:
            build_args = ["-"]
        build_args += ["--build-arg", f"CONDUCTO_CACHE_BUSTER={uuid.uuid4()}"]
        text = await dockerfile_mod.text_for_build_dockerfile(
            self.name_built,
            self.reqs_py,
            self.copy_dir,
            self.copy_url,
            self.copy_branch,
            self.docker_auto_workdir,
            self._exec,
        )

        out, err = await self._exec(
            "docker",
            "build",
            "-t",
            self.name_complete,
            "--label",
            "com.conducto.complete",
            *build_args,
            input=text.encode(),
        )
        return out, err

    async def _extend(self):
        # Writes Dockerfile that extends user-provided image.
        text, worker_image = await dockerfile_mod.text_for_extend_dockerfile(
            self.name_complete, self._exec
        )
        if "/" in worker_image:
            pull_worker = True
            if os.environ.get("CONDUCTO_DEV_REGISTRY"):
                # If image is not present locally, then try ecr login.
                try:
                    await self._exec("docker", "inspect", worker_image)
                    pull_worker = False
                except:
                    await self._exec(
                        "$(aws ecr get-login --no-include-email --region us-east-2)",
                        shell=True,
                    )
            if pull_worker and not Image._PULLED_WORKER:
                await self._exec("docker", "pull", worker_image)
                Image._PULLED_WORKER = True

        profile = conducto.api.Config().default_profile
        pipeline_id = os.getenv("CONDUCTO_PIPELINE_ID")

        out, err = await self._exec(
            "docker",
            "build",
            "-t",
            self.name_local_extended,
            "--label",
            "com.conducto.extend",
            "--label",
            f"com.conducto.profile={profile}",
            "--label",
            f"com.conducto.pipeline={pipeline_id}",
            "-",
            input=text.encode(),
        )
        return out, err

    async def _push(self):
        # If push_to_cloud, tag the local image and push it
        await self._exec(
            "docker", "tag", self.name_local_extended, self.name_cloud_extended
        )
        out, err = await self._exec("docker", "push", self.name_cloud_extended)
        return out, err

    def needs_building(self):
        return self.dockerfile is not None

    def needs_completing(self):
        return (
            self.copy_dir is not None
            or self.copy_url is not None
            or self.reqs_py is not None
        )


def make_all(node: "pipeline.Node", pipeline_id, push_to_cloud):
    images = {}
    for n in node.stream():
        if n.user_set["image_name"]:
            img = n.repo[n.user_set["image_name"]]
            img.pre_built = True
            img._pipeline_id = pipeline_id
            if img.name_complete not in images:
                images[img.name_complete] = img

    def _print_status():
        line = "Preparing images:"
        sep = ""
        for status in Status.order:
            count = sum(i.history[-1].status == status for i in images.values())
            if count > 0:
                line += f"{sep} {count} {status}"
                sep = ","
            print(f"\r{log.Control.ERASE_LINE}{line}", end=".", flush=True)

    # Run all the builds concurrently.
    # TODO: limit simultaneous builds using an asyncio.Semaphore
    futs = [img.make(push_to_cloud, callback=_print_status) for img in images.values()]

    asyncio.get_event_loop().run_until_complete(asyncio.gather(*futs))
    print(f"\r{log.Control.ERASE_LINE}", end="", flush=True)


class HistoryEntry:
    _UNSET = object()

    def __init__(self, status, start=_UNSET, finish=False):
        self.status = status
        self.start = time.time() if start is self._UNSET else start
        self.end = None
        self.stdout = None
        self.stderr = None
        if finish:
            self.finish()

    def finish(self, stdout=None, stderr=None):
        self.end = time.time()
        self.stdout = _to_str(stdout)
        self.stderr = _to_str(stderr)

    def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            if self.end is not None:
                raise Exception(
                    f"Error in <HistoryEntry status={self.status}> after it was finished"
                )
            if issubclass(exc_type, subprocess.CalledProcessError):
                self.finish(stdout=exc_val.stdout, stderr=exc_val.stderr)
            else:
                self.finish(stderr=traceback.format_exc())

    def to_dict(self):
        return {
            "status": self.status,
            "start": self.start,
            "end": self.end,
            "stdout": self.stdout,
            "stderr": self.stderr,
        }


def _to_str(s):
    if s is None:
        return None
    if isinstance(s, bytes):
        return s.decode()
    if isinstance(s, str):
        return s
    raise TypeError(f"Cannot convert {repr(s)} to str.")


_conducto_dir = os.path.dirname(os.path.dirname(__file__)) + os.path.sep
