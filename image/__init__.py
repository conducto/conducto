import asyncio
import contextlib
import concurrent.futures
import hashlib
import json
import os
import subprocess
import sys
import time
import traceback
import typing
import uuid

from conducto.shared import async_utils, log, constants, imagepath
import conducto
from .. import pipeline
from . import dockerfile as dockerfile_mod, names

if sys.version_info >= (3, 7):
    asynccontextmanager = contextlib.asynccontextmanager
else:
    from conducto.shared import async_backport

    asynccontextmanager = async_backport.asynccontextmanager


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
    return f"__conducto_path:{ctxpath.linear()}:endpath__"


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
    :param copy_repo:  Set to `True` to automatically copy the entire current Git repo
        into the Docker image. Use this so that a single Image definition can either use
        local code or can fetch from a remote repo.

        Normal use of this parameter uses local code, so it sets `copy_dir` to point to
        the repo root. Specify the `CONDUCTO_GIT_BRANCH` environment variable to use a remote
        repository. This is commonly done for CI/CD. When specified, parameters will be
        auto-populated based on environment variables:

        `copy_url` is set to `CONDUCTO_GIT_URL` if specified, otherwise the user's Org must have
        a Git integration installed which provides the URL.

        `copy_branch` is set to `CONDUCTO_GIT_BRANCH` which must be specified in a CI/CD context.

        `copy_repo` is set to `CONDUCTO_GIT_REPO` if specified, otherwise it is auto-detected.

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

    _PULLED_IMAGES = set()
    _CONTEXT = None

    def __init__(
        self,
        image=None,
        *,
        dockerfile=None,
        docker_build_args=None,
        context=None,
        copy_repo=None,
        copy_dir=None,
        copy_url=None,
        copy_branch=None,
        docker_auto_workdir=True,
        reqs_py=None,
        path_map=None,
        name=None,
        pre_built=False,
        git_sha=None,
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
        if (copy_dir is not None) + (copy_url is not None) > 1:
            raise ValueError(
                f"Must not specify more than 1 of copy_dir ({copy_dir}) and copy_url "
                f"({copy_url})."
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
        self.copy_repo = copy_repo
        self.copy_dir = copy_dir
        self.copy_dir_original = copy_dir
        self.copy_url = copy_url
        self.copy_branch = copy_branch
        self.docker_auto_workdir = docker_auto_workdir
        self.reqs_py = reqs_py
        self.path_map = path_map
        if self.path_map:
            # on deserialize pathmaps are strings but they may also be strings when
            # being passed from the user.
            def auto_detect_deserialize(e):
                # TODO: find a better way to discover this.
                if e.find("pathsep") >= 0:
                    return imagepath.Path.from_dockerhost_encoded(e)
                return e

            self.path_map = {
                auto_detect_deserialize(external): internal
                for external, internal in self.path_map.items()
            }
        self.git_sha = git_sha or os.getenv("CONDUCTO_GIT_SHA")

        self.pre_built = pre_built

        self._env_vars = {}

        if self.copy_repo:
            # `copy_repo` automatically copies the whole repo. If environment
            # variables are set it will use them; otherwise it copies the local
            # contents of the repo.
            if self.copy_branch or "CONDUCTO_GIT_BRANCH" in os.environ:
                self.copy_branch = self.copy_branch or os.environ["CONDUCTO_GIT_BRANCH"]
                self.copy_url = self.copy_url or os.getenv("CONDUCTO_GIT_URL")
                if self.copy_repo is True:
                    # The value `True` means "auto-detect", so go ahead and auto-detect.
                    if "CONDUCTO_GIT_REPO" in os.environ:
                        self.copy_repo = os.environ["CONDUCTO_GIT_REPO"]
                        # TODO: Is there a way to set `path_map` here? If run in PR mode
                        # then we'd have to use named shares. Get help setting this up.
                    else:
                        self.copy_repo = imagepath.Path._get_git_repo(
                            _non_conducto_dir()
                        )

                    if "CONDUCTO_GIT_REPO_ROOT" in os.environ:
                        repo_root = os.environ["CONDUCTO_GIT_REPO_ROOT"]
                    else:
                        # Set the path_map to show that this repo is at COPY_DIR
                        repo_root = imagepath.Path._get_git_root(_non_conducto_dir())

                    const_ee = constants.ExecutionEnv
                    if const_ee.value() in const_ee.external:
                        origin = imagepath.Path._get_git_origin(repo_root)
                        sharename = imagepath.Path._sanitize_git_origin(origin)
                        Image.share_directory(sharename, repo_root)

                    repo_root = self.get_contextual_path(repo_root)

                    if not self.path_map:
                        self.path_map = {}
                    self.path_map.setdefault(repo_root, dockerfile_mod.COPY_DIR)
                    self.context = repo_root
            else:
                # In normal mode, `copy_repo` resolves to `copy_dir`. Record the repo
                # location here.
                if self.copy_dir is None:
                    if "CONDUCTO_GIT_REPO_ROOT" in os.environ:
                        self.copy_dir = self.get_contextual_path(
                            os.environ["CONDUCTO_GIT_REPO_ROOT"]
                        )
                    else:
                        outside_dir = _non_conducto_dir()
                        contextual_path = self.get_contextual_path(outside_dir)
                        gitroot = contextual_path.get_git_root()
                        if not gitroot:
                            raise ValueError(
                                f"Could not find Git root for {outside_dir}."
                            )
                        self.copy_dir = gitroot.as_docker_host_path()
                self.context = self.copy_dir

            # Set CONDUCTO_GIT_SHA later because we have to run a command to do it
            self._env_vars["CONDUCTO_GIT_REPO_ROOT"] = dockerfile_mod.COPY_DIR
            self._env_vars["CONDUCTO_GIT_REPO"] = self.copy_repo
            if self.copy_branch:
                self._env_vars["CONDUCTO_GIT_BRANCH"] = self.copy_branch

        if not self.pre_built:
            if self.dockerfile is not None:
                if self.context is None:
                    self.context = os.path.dirname(self.dockerfile)
                self.dockerfile = self.get_contextual_path(self.dockerfile)

            if self.context is not None:
                self.context = self.get_contextual_path(self.context)

            if self.copy_dir is not None:
                self.copy_dir = self.get_contextual_path(self.copy_dir)
                self.path_map = {self.copy_dir: dockerfile_mod.COPY_DIR}

        if self.path_map:
            self.path_map = {
                self.get_contextual_path(external): internal
                for external, internal in self.path_map.items()
            }

        self.history = [HistoryEntry(Status.PENDING)]

        if self.pre_built:
            self.history.append(HistoryEntry(Status.DONE, finish=True))

        self._make_fut: typing.Optional[asyncio.Future] = None

        self._cloud_tag_convert = None
        self._pipeline_id = os.getenv("CONDUCTO_PIPELINE_ID")

    def __eq__(self, other):
        return isinstance(other, Image) and self.to_dict() == other.to_dict()

    @staticmethod
    def _serialize_path(p):
        return p._id() if isinstance(p, imagepath.Path) else p

    @staticmethod
    def _serialize_pathmap(pathmap):
        if pathmap:
            return {p.linear(): v for p, v in pathmap.items()}
        return None

    # hack to get this to serialize
    @property
    def _id(self):
        try:
            return self.to_dict()
        except:
            # NOTE:  when there is an error in to_dict, json.encode throws a
            # really unhelpful "ValueError: Circular reference detected"
            print(traceback.format_exc())
            raise

    def to_dict(self):
        return {
            "name": self.name,
            "image": self.image,
            "dockerfile": self._serialize_path(self.dockerfile),
            "docker_build_args": self.docker_build_args,
            "docker_auto_workdir": self.docker_auto_workdir,
            "context": self._serialize_path(self.context),
            "copy_repo": self.copy_repo,
            "copy_dir": self._serialize_path(self.copy_dir),
            "copy_url": self.copy_url,
            "copy_branch": self.copy_branch,
            "reqs_py": self.reqs_py,
            "path_map": self._serialize_pathmap(self.path_map),
            "pre_built": self.pre_built,
            "git_sha": self.git_sha,
        }

    def to_raw_image(self):
        return {
            "image": self.image,
            "dockerfile": self._serialize_path(self.dockerfile),
            "docker_build_args": self.docker_build_args,
            "docker_auto_workdir": self.docker_auto_workdir,
            "context": self._serialize_path(self.context),
            "copy_repo": self.copy_repo,
            "copy_dir": self._serialize_path(self.copy_dir),
            "copy_url": self.copy_url,
            "copy_branch": self.copy_branch,
            "reqs_py": self.reqs_py,
            "path_map": self._serialize_pathmap(self.path_map),
        }

    @staticmethod
    def get_contextual_path(p, named_shares=True):
        """
        Take a path as a string and returns a `conducto.Path`.  Optionally
        searches for named shares as recognized from the root.

        :result: `conducto.Path`
        """
        if isinstance(p, imagepath.Path):
            return p
        if isinstance(p, dict) and "pathsep" in p:
            return imagepath.Path.from_dockerhost_encoded(p)

        if constants.ExecutionEnv.value() in constants.ExecutionEnv.manager_all:
            # Note:  This can run in the worker in Lazy nodes.
            return imagepath.Path.from_localhost(p)

        op = os.path

        # Translate relative paths as starting from the file where they were defined.
        if not op.isabs(p):
            p = op.join(_non_conducto_dir(), p)
        p = op.realpath(p)

        # Apply context specified from outside this container. Needed for recursive
        # co.Lazy calls inside an Image with ".path_map".
        path_map_text = os.getenv("CONDUCTO_PATH_MAP")
        if path_map_text:
            path_map = json.loads(path_map_text)
            for external, internal in path_map.items():
                if p.startswith(internal):
                    external = imagepath.Path.from_dockerhost_encoded(external)

                    # we are in docker, so this append is always a unix path tail
                    tail = p[len(internal) :].lstrip("/")
                    return external.append(tail, sep="/")

        # If ".copy_dir" wasn't set, find the git root so that we could possibly use this
        # inside an Image with ".copy_url" set.
        elif op.exists(p):
            result = imagepath.Path.from_localhost(p)
            result = result.mark_git_root()
        else:
            result = imagepath.Path.from_localhost(p)

        if named_shares:
            auto_reg_path = None
            auto_reg_name = None

            compare = result.as_docker_host_path()

            dirsettings = conducto.api.dirconfig_detect(p)
            if dirsettings and "registered" in dirsettings:
                auto_reg_name = dirsettings["registered"]
                auto_reg_path = dirsettings["dir-path"]

            # iterate through named shares looking for a match in this org
            if auto_reg_path is None:
                conf = conducto.api.Config()
                shares = conf.get_named_share_mapping(conf.default_profile)

                def enum():
                    for name, paths in shares.items():
                        for path in paths:
                            yield name, imagepath.Path.from_dockerhost(path)

                for name, path in enum():
                    if compare.is_subdir_of(path):
                        # bingo, we have recognized this as a share for your org
                        auto_reg_name = name
                        auto_reg_path = path.to_docker_host()
                        break

            if auto_reg_name:
                # return an annotated result with the discovered path
                result = compare.mark_named_share(auto_reg_name, auto_reg_path)

        return result

    @staticmethod
    def share_directory(name, relative):
        path = Image.get_contextual_path(relative, named_shares=False)
        config = conducto.api.Config()
        config.register_named_share(config.default_profile, name, path)

    # alias for back compat
    register_directory = share_directory

    async def sha1(self):
        if self.git_sha is not None:
            return self.git_sha

        if self.copy_branch:
            branch = self.copy_branch
            url = self.copy_url
            if url is None:
                if self.copy_repo is None:
                    raise ValueError(
                        f"Specified copy_branch={self.copy_branch} but did "
                        f"not specify copy_url or copy_repo.\nImage: {self.to_dict()}"
                    )
                url = await conducto.api.AsyncGit().url(self.copy_repo)
            out, _err = await async_utils.run_and_check(
                "git", "ls-remote", url, f"refs/heads/{branch}"
            )
            sha1 = out.decode().strip().split("\t")[0]
            if not sha1:
                raise ValueError(
                    f"Cannot find branch named {branch}.\nImage: {self.to_dict()}"
                )
            self.git_sha = sha1
        else:
            root = self.copy_dir.to_docker_mount()
            out, err = await async_utils.run_and_check(
                "git", "-C", root, "rev-parse", "HEAD"
            )
            self.git_sha = out.decode().strip()
        return self.git_sha

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

    async def _make_generator(self, push_to_cloud, callback, force_rebuild=False):
        """
        Generator that pulls/builds/extends/pushes this Image.
        """
        if self.history and self.history[-1].end is None:
            self.history[-1].finish()
            if callback:
                await callback(Status.PENDING, Status.DONE, self.history[-1])

        # Pull the image if needed
        if self.image and "/" in self.image:
            async with self._new_status(Status.PULLING, callback) as st:
                if force_rebuild or not await self._image_exists(self.image):
                    await callback()
                    out, err = await async_utils.run_and_check(
                        "docker", "pull", self.image
                    )
                    st.finish(out, err)
                else:
                    st.finish("Image already pulled")
        yield

        # Build the image if needed
        if self.needs_building():
            async with self._new_status(Status.BUILDING, callback) as st:
                if force_rebuild or not await self._image_exists(self.name_built):
                    await callback()
                    out, err = await self._build()
                    st.finish(out, err)
                else:
                    st.finish("Dockerfile already built")
        yield

        # If needed, copy files into the image and install packages
        if self.needs_completing():
            async with self._new_status(Status.COMPLETING, callback) as st:
                if force_rebuild or not await self._image_exists(self.name_complete):
                    await callback()
                    out, err = await self._complete()
                    st.finish(out, err)
                else:
                    st.finish("Code and/or Python libraries already installed.")
        yield

        async with self._new_status(Status.EXTENDING, callback) as st:
            if force_rebuild or not await self._image_exists(self.name_local_extended):
                await callback()
                out, err = await self._extend()
                st.finish(out, err)
            else:
                st.finish("Conducto toolchain already added")
        yield

        if push_to_cloud:
            async with self._new_status(Status.PUSHING, callback) as st:
                await callback()
                out, err = await self._push()
                st.finish(out, err)

        self.history.append(HistoryEntry(Status.DONE, finish=True))
        if callback:
            await callback(Status.DONE, Status.DONE, self.history[-1])

    async def _image_exists(self, image):
        try:
            await async_utils.run_and_check("docker", "image", "inspect", image)
        except subprocess.CalledProcessError:
            return False
        else:
            return True

    @asynccontextmanager
    async def _new_status(self, status, callback):
        entry = HistoryEntry(status)
        self.history.append(entry)
        try:
            yield entry
        except subprocess.CalledProcessError as e:
            entry.finish(e.stdout, e.stderr)
            if callback:
                await callback(status, Status.ERROR, entry)
            self.history.append(HistoryEntry(Status.ERROR, finish=True))
            if callback:
                await callback(Status.ERROR, Status.ERROR, self.history[-1])
            raise
        except (asyncio.CancelledError, concurrent.futures.CancelledError):
            entry.finish(None, None)
            if callback:
                await callback(status, Status.CANCELLED, entry)
            self.history.append(HistoryEntry(Status.CANCELLED, finish=True))
            if callback:
                await callback(Status.ERROR, Status.ERROR, self.history[-1])
            raise
        except Exception:
            entry.finish(None, traceback.format_exc())
            if callback:
                await callback(status, Status.ERROR, entry)
            self.history.append(HistoryEntry(Status.ERROR, finish=True))
            if callback:
                await callback(Status.ERROR, Status.ERROR, self.history[-1])
            raise
        else:
            if not entry.end:
                entry.finish()
            if callback:
                await callback(status, None, entry)

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
            self.dockerfile.to_docker_mount(),
            self.context.to_docker_mount(),
        ]

        out, err = await async_utils.run_and_check(
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
        # If copying a whole repo, or if getting a remote Git repo, get the commit SHA.
        if self.copy_repo or self.copy_branch or self.copy_url:
            # Set CONDUCTO_GIT_SHA now that it's needed. We couldn't do it before
            # because it could be expensive to compute (could involve a URL fetch), so
            # we want to make sure it is only done once per Image.
            self._env_vars["CONDUCTO_GIT_SHA"] = await self.sha1()

        # Create dockerfile from stdin. Replace "-f <dockerfile> <copy_dir>"
        # with "-"
        if self.copy_dir:
            build_args = [
                "-f",
                "-",
                self.copy_dir.to_docker_mount(),
            ]
        else:
            build_args = ["-"]
        build_args += ["--build-arg", f"CONDUCTO_CACHE_BUSTER={uuid.uuid4()}"]
        text = await dockerfile_mod.text_for_build_dockerfile(
            self.name_built,
            self.reqs_py,
            self.copy_dir,
            self.copy_repo,
            self.copy_url,
            self.copy_branch,
            self.docker_auto_workdir,
            self._env_vars,
        )

        # Only in test setting, if the conducto image is used, pull it!
        if (
            self.reqs_py
            and "conducto" in self.reqs_py
            and os.environ.get("CONDUCTO_DEV_REGISTRY")
        ):
            pull_image = True

            tag = conducto.api.Config().get_image_tag()
            registry = os.environ.get("CONDUCTO_DEV_REGISTRY")
            conducto_image = f"{registry}/conducto:{tag}"

            # If this is dev/test, we may or may not have the image
            # locally, decline the pull accordingly.
            try:
                await async_utils.run_and_check("docker", "inspect", conducto_image)
                pull_image = False
            except subprocess.CalledProcessError:
                pass

            if pull_image and conducto_image not in Image._PULLED_IMAGES:
                await async_utils.run_and_check("docker", "pull", conducto_image)
                Image._PULLED_IMAGES.add(conducto_image)

        out, err = await async_utils.run_and_check(
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
            self.name_complete
        )
        if "/" in worker_image:
            pull_worker = True
            if os.environ.get("CONDUCTO_DEV_REGISTRY"):
                # If this is dev/test, we may or may not have the image
                # locally, decline the pull accordingly.
                try:
                    await async_utils.run_and_check("docker", "inspect", worker_image)
                    pull_worker = False
                except subprocess.CalledProcessError:
                    pass
            if pull_worker and worker_image not in Image._PULLED_IMAGES:
                await async_utils.run_and_check("docker", "pull", worker_image)
                Image._PULLED_IMAGES.add(worker_image)

        profile = conducto.api.Config().default_profile
        pipeline_id = os.getenv("CONDUCTO_PIPELINE_ID", self._pipeline_id)

        out, err = await async_utils.run_and_check(
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
        cloud_tag = self.name_cloud_extended
        if self._cloud_tag_convert and self.is_cloud_building():
            cloud_tag = self._cloud_tag_convert(cloud_tag)
        await async_utils.run_and_check(
            "docker", "tag", self.name_local_extended, cloud_tag
        )
        out, err = await async_utils.run_and_check("docker", "push", cloud_tag)
        return out, err

    def is_cloud_building(self):
        return not self.copy_dir and not self.dockerfile

    def needs_building(self):
        return self.dockerfile is not None

    def needs_completing(self):
        return (
            self.copy_dir is not None
            or self.copy_repo is not None
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
            print(f"\r{log.Control.ERASE_LINE}{line}", end=".")

    # Run all the builds concurrently.
    # TODO: limit simultaneous builds using an asyncio.Semaphore
    futs = [img.make(push_to_cloud, callback=_print_status) for img in images.values()]

    asyncio.get_event_loop().run_until_complete(asyncio.gather(*futs))
    print(f"\r{log.Control.ERASE_LINE}", end="")


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

    @classmethod
    def from_json(cls, values: dict):
        self = cls(values["status"])
        for k, v in values.items():
            setattr(self, k, v)
        return self

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


def _non_conducto_dir():
    """
    Walk the stack. The first file that's not in the Conducto dir is the one the user
    called this from.
    """
    op = os.path
    if Image._CONTEXT is not None:
        return op.dirname(op.abspath(Image._CONTEXT))
    for frame, _lineno in traceback.walk_stack(None):
        filename = frame.f_code.co_filename
        if not filename.startswith(_conducto_dir):
            return op.dirname(filename)


_conducto_dir = os.path.dirname(os.path.dirname(__file__)) + os.path.sep
