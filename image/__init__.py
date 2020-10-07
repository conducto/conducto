import asyncio
import contextlib
import concurrent.futures
import functools
import hashlib
import json
import os
import subprocess
import sys
import time
import traceback
import typing

from conducto.shared import async_utils, constants, imagepath, types as t
import conducto
from . import dockerfile as dockerfile_mod, names

CONST_EE = constants.ExecutionEnv

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
    CLONING = "cloning"
    PULLING = "pulling"
    BUILDING = "building"
    INSTALLING = "installing"
    COPYING = "copying"
    EXTENDING = "extending"
    PUSHING = "pushing"
    DONE = "done"
    ERROR = "error"
    CANCELLED = "cancelled"
    order = [
        PENDING,
        QUEUED,
        CLONING,
        PULLING,
        BUILDING,
        INSTALLING,
        COPYING,
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

        **copy_dir mode**: Normal use of this parameter uses local code, so it sets
        `copy_dir` to point to the root of the Git repo of the calling code.

        **copy_url mode**: Specify `copy_branch` to use a remote repository. This is
        commonly done for CI/CD. When specified, `copy_url` will be auto-populated.

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
    :param reqs_packages: List of packages to install with the appropriate Linux package
        manager for this image's flavor.
    :param reqs_docker: `bool` for whether to install Docker.
    :param shell: Which shell to use in this container. Defaults to `co.Image.AUTO` to
        auto-detect. `AUTO` will prefer `/bin/bash` when available, and fall back to
        `/bin/sh` otherwise.
    :param name: Name this `Image` so other Nodes can reference it by name. If
        no name is given, one will automatically be generated from a list of
        our favorite Pokemon. I choose you, angry-bulbasaur!
    """

    _PULLED_IMAGES = set()
    _CONTEXT = None
    _CLONE_LOCKS = {}
    _COMPLETED_CLONES = set()

    AUTO = "__auto__"

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
        reqs_packages=None,
        reqs_docker=False,
        path_map=None,
        shell=AUTO,
        name=None,
        git_urls=None,
        **kwargs,
    ):

        # TODO: remove pre_built back-compatibility for sept 9 changes
        kwargs.pop("pre_built", None)
        kwargs.pop("git_sha", None)
        if len(kwargs):
            raise ValueError(f"unknown args: {','.join(kwargs)}")

        if name is None:
            name = names.NameGenerator.name()

        self.name = name

        if image and dockerfile:
            raise ValueError(
                f"Cannot specify both image ({image}) and dockerfile ({dockerfile})"
            )
        if not image and not dockerfile:
            # Default to user's current version of python, if none is specified
            image = f"python:{sys.version_info[0]}.{sys.version_info[1]}-slim"
            if not reqs_py:
                reqs_py = ["conducto"]
        if copy_dir and copy_url:
            raise ValueError(
                f"Must not specify copy_dir ({copy_dir}) and copy_url ({copy_url})."
            )
        if copy_url and not copy_branch:
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
        self.reqs_packages = reqs_packages
        self.reqs_docker = reqs_docker
        self.path_map = path_map or {}
        self.shell = shell
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

        if self.copy_repo:
            ### Step 1: Infer repo_root and repo_url

            # If there is an entry in the path_map that maps to COPY_LOCATION, then the
            # key must be the external imagepath for the repo root, with all appropriate
            # metadata.
            repo_root: typing.Optional[imagepath.Path] = None
            if CONST_EE.value() in CONST_EE.manager_all:
                # Which path_map to use? In a manager, this image has been deserialized.
                # Anything with copy_repo=True will be encoded in self.path_map.
                pm = self.path_map
            elif CONST_EE.value() in CONST_EE.worker_all:
                # In a worker, the current contents of COPY_LOCATION come from a
                # previously-defined Image with copy_repo=True. Inherit those values.
                pm = _get_path_map()
            elif CONST_EE.value() == CONST_EE.EXTERNAL:
                # Externally, there's no previously-defined Image to use for context.
                pm = {}
            else:
                raise EnvironmentError(
                    f"Don't know what path_map to use for ExecutionEnvironment={CONST_EE.value()}"
                )
            for k, v in pm.items():
                if v == constants.ConductoPaths.COPY_LOCATION:
                    repo_root = k

            # If there's no matching entry in the path_map, autodetect the repo root.
            if not repo_root:
                repo_root = self._detect_repo_root()

            # Extract repo_url and repo_branch from the marked Git repo in repo_root
            mark = repo_root.get_git_marks()[-1]
            repo_url = mark.get("url")
            repo_branch = mark.get("branch")

            ### Step 1a: Augment repo_root

            # `git_urls` is a narrowly focused parameter that specifies multiple
            # URLs by which this repo can be cloned. A user could possibly know
            # this repo by any of them, so we need to add all of them to
            # repo_root as named shares.
            if git_urls:
                for url in git_urls:
                    sanitized = imagepath.Path._sanitize_git_url(url)
                    repo_root = repo_root.mark_named_share(sanitized, "")

            ### Step 2: Use repo_root and repo_url to set other variables
            if self.copy_branch and repo_branch and self.copy_branch != repo_branch:
                raise ValueError(
                    f"copy_branch ({self.copy_branch}) does not match branch inherited "
                    f"from path_map ({repo_branch}"
                )
            self.copy_branch = self.copy_branch or repo_branch

            # If `copy_branch` is set, then use copy_url on the Git URL. Otherwise use
            # copy_dir on the file system root
            if self.copy_branch:
                self.copy_url = repo_url
            else:
                self.copy_dir = repo_root

            # Once inferred, set this in `path_map`
            repo_root = self._get_contextual_path_helper(repo_root)
            self.path_map[repo_root] = constants.ConductoPaths.COPY_LOCATION

            # Set context in case of any Dockerfiles
            if not self.context:
                self.context = repo_root

            # In non-headless (headful?) EXTERNAL mode, copy_repo=True should attempt to
            # share the Git root according to its origin url.
            if (
                repo_url
                and CONST_EE.value() == CONST_EE.EXTERNAL
                and not CONST_EE.headless()
            ):
                sharename = imagepath.Path._sanitize_git_url(repo_url)
                Image.share_directory(sharename, repo_root)

        if self.dockerfile:
            self.dockerfile = self._get_contextual_path_helper(self.dockerfile)
            if not self.context:
                one_level_up = os.path.dirname(self.dockerfile.to_docker_host())
                self.context = self._get_contextual_path_helper(one_level_up)

        if self.context:
            self.context = self._get_contextual_path_helper(self.context)

        if self.copy_dir:
            self.copy_dir = self._get_contextual_path_helper(self.copy_dir)
            self.path_map[self.copy_dir] = constants.ConductoPaths.COPY_LOCATION

        if self.path_map:
            self.path_map = {
                self._get_contextual_path_helper(external): internal
                for external, internal in self.path_map.items()
            }

        self.history = [HistoryEntry(Status.PENDING)]

        self._make_fut: typing.Optional[asyncio.Future] = None

        self._cloud_tag_convert = None
        self._pipeline_id = os.getenv("CONDUCTO_PIPELINE_ID")

        # Check for compatibility with Headless/Agent-less mode
        if CONST_EE.headless() and not self.is_cloud_building():
            raise ValueError(
                "If in headless mode (like from a Git webhook), image must be cloud-"
                "buildable. Cannot use copy_dir. If using dockerfile or copy_repo, "
                "then copy_branch must be set so that the contents come from Git "
                "instead of the local file system."
            )

    def __eq__(self, other):
        return isinstance(other, Image) and self.to_dict() == other.to_dict()

    def _detect_repo_root(self):
        if CONST_EE.headless():
            # In headless mode, the repo_root must be a bare Git repo that
            # is not rooted in any filesystem.
            git_root = {
                "type": "git",
                "url": self.copy_url,
                "branch": self.copy_branch,
            }
            return imagepath.Path.from_marked_root(git_root, "")
        else:
            # If not headless, then look up the Git root from the filesystem.
            outside_dir = _non_conducto_dir()
            git_root = imagepath.Path._get_git_root(outside_dir)
            if not git_root:
                raise ValueError(
                    f"copy_repo=True was specified, but could not find Git "
                    f"root for {outside_dir}."
                )

            git_url = imagepath.Path._get_git_origin(git_root)
            return self.get_contextual_path(
                git_root, branch=self.copy_branch, url=self.copy_url or git_url
            ).to_docker_host_path()

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
    def id(self):
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
            "reqs_packages": self.reqs_packages,
            "reqs_docker": self.reqs_docker,
            "path_map": self._serialize_pathmap(self.path_map),
            "shell": self.shell,
        }

    def to_update_dict(self):
        """
        Return a dict of variables to be updated when the Agent is done building this
        Image. When `shell` is set to AUTO it gets updated during the build process. The
        Agent calls this method to report the changes back to the Manager so that the
        Worker can use the auto-determined `shell` when launching Tasks.
        """
        return {"shell": self.shell}

    def _get_image_tag(self):
        # Use the serialization of the image. Omit 'name' because there may be several
        # different named images that share a single underlying Docker image. Omit
        # 'shell' because it can change during the build process if the user specifies
        # AUTO.
        d = self.to_dict()
        del d["name"]
        del d["shell"]
        key = json.dumps(d).encode()
        return hashlib.md5(key).hexdigest()

    def _get_contextual_path_helper(
        self, p: typing.Union[imagepath.Path, dict, str], **kwargs
    ) -> imagepath.Path:
        return self.get_contextual_path(
            p, branch=self.copy_branch, url=self.copy_url, **kwargs
        )

    @staticmethod
    def get_contextual_path(
        p: typing.Union[imagepath.Path, dict, str],
        *,
        named_shares=True,
        branch=None,
        url=None,
    ) -> imagepath.Path:
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
        for external, internal in _get_path_map().items():
            if p.startswith(internal):
                # we are in docker, so this append is always a unix path tail
                tail = p[len(internal) :].lstrip("/")

                # It's okay to short-circuit here instead of adding named shares. The
                # entry from the external path_map must have already had those done, so
                # returning here just reuses those previously-inferred values.
                return external.append(tail, sep="/")

        # If ".copy_dir" wasn't set, find the git root so that we could possibly use this
        # inside an Image with ".copy_url" set.
        if op.exists(p):
            result = imagepath.Path.from_localhost(p)
            result = result.mark_git_root(branch=branch, url=url)
        else:
            result = imagepath.Path.from_localhost(p)

        # this is needful for windows & wsl1 (a no-op otherwise)
        result = result.to_docker_host_path()

        if named_shares:
            # iterate through named shares looking for a match in this org
            conf = conducto.api.Config()
            shares = conf.get_named_share_mapping(conf.default_profile)

            for name, paths in shares.items():
                for path in paths:
                    path = imagepath.Path.from_dockerhost(path)
                    if result.is_subdir_of(path):
                        # We have recognized this as a share, so annotate result
                        # with the discovered path.
                        result = result.mark_named_share(name, path.to_docker_host())

        return result

    @staticmethod
    def share_directory(name, relative):
        path = Image.get_contextual_path(relative, named_shares=False)
        config = conducto.api.Config()
        config.register_named_share(config.default_profile, name, path)

    # alias for back compat
    register_directory = share_directory

    async def _sha1(self):
        if self.needs_cloning():
            root = self._get_clone_dest()
        else:
            root = self.copy_dir.to_docker_mount(pipeline_id=self._pipeline_id)

        out, err = await async_utils.run_and_check(
            "git", "-C", root, "rev-parse", "HEAD"
        )
        return out.decode().strip()

    @property
    def name_built(self):
        if self.needs_building():
            return f"conducto_built:{self._pipeline_id}_{self._get_image_tag()}"
        else:
            return self.image

    @property
    def name_installed(self):
        if self.needs_installing():
            return f"conducto_installed:{self._pipeline_id}_{self._get_image_tag()}"
        else:
            return self.name_built

    @property
    def name_copied(self):
        if self.needs_copying():
            return f"conducto_copied:{self._pipeline_id}_{self._get_image_tag()}"
        else:
            return self.name_installed

    @property
    def name_local_extended(self):
        return f"conducto_extended:{self._pipeline_id}_{self._get_image_tag()}"

    @property
    def name_cloud_extended(self):
        from .. import api

        if self._pipeline_id is None:
            raise ValueError("Must specify pipeline_id before pushing to cloud")
        docker_domain = api.Config().get_docker_domain()
        return f"{docker_domain}/{self._pipeline_id}:{self._get_image_tag()}"

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

        # Clone the repo if needed
        if self.needs_cloning():
            async with self._new_status(Status.CLONING, callback) as st:
                if force_rebuild or not self._clone_complete():
                    await callback()
                    out, err = await self._clone()
                    st.finish(out, err)
                else:
                    st.finish("Using cached clone")
        yield

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

        # Install packages if needed
        if self.needs_installing():
            async with self._new_status(Status.INSTALLING, callback) as st:
                if force_rebuild or not await self._image_exists(self.name_installed):
                    await callback()
                    out, err = await self._install()
                    st.finish(out, err)
                else:
                    st.finish("Python libraries already installed.")
        yield

        # Copy files if needed
        if self.needs_copying():
            async with self._new_status(Status.COPYING, callback) as st:
                if force_rebuild or not await self._image_exists(self.name_copied):
                    await callback()
                    out, err = await self._copy()
                    st.finish(out, err)
                else:
                    st.finish("Code already copied.")
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

        self._mark_done()
        if callback:
            await callback(Status.DONE, Status.DONE, self.history[-1])

    def _mark_done(self):
        if self.history and self.history[-1].end is None:
            self.history[-1].finish()
        if not self.history or self.history[-1].status != Status.DONE:
            self.history.append(HistoryEntry(Status.DONE, finish=True))

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

    def needs_cloning(self):
        return bool(self.copy_branch)

    def _get_clone_dest(self):
        return constants.ConductoPaths.git_clone_dest(
            pipeline_id=self._pipeline_id, url=self.copy_url, branch=self.copy_branch
        )

    def _get_clone_lock(self, dest):
        if dest in Image._CLONE_LOCKS:
            lock = Image._CLONE_LOCKS[dest]
        else:
            lock = Image._CLONE_LOCKS[dest] = asyncio.Lock()
        return lock

    def _clone_complete(self):
        """
        Check whether this clone has completed. Other steps check doneness by looking
        for the existence of an image name. That doesn't work for clone() because it
        first creates the directory and then populates it, so there's no easy
        filesystem-level answer for when it has finished. Keep a flag as a workaround,
        keyed by clone dest.
        """
        dest = self._get_clone_dest()
        return dest in Image._COMPLETED_CLONES

    async def _clone(self):
        from conducto.integrations import git

        # Only one clone() can run at a time on a single directory
        dest = self._get_clone_dest()
        real_url = git.clone_url(self.copy_url)
        fake_url = git.clone_url(self.copy_url, token=t.Token("{__conducto_token__}"))

        async with self._get_clone_lock(dest):
            outputs = []
            if os.path.exists(dest):
                await async_utils.run_and_check(
                    "git", "-C", dest, "config", "remote.origin.url", real_url
                )
                outputs.append(
                    await async_utils.run_and_check("git", "-C", dest, "fetch")
                )
                outputs.append(
                    await async_utils.run_and_check(
                        "git",
                        "-C",
                        dest,
                        "reset",
                        "--hard",
                        f"origin/{self.copy_branch}",
                    )
                )
            else:
                outputs.append(
                    await async_utils.run_and_check(
                        "git",
                        "clone",
                        "--single-branch",
                        "--branch",
                        self.copy_branch,
                        real_url,
                        dest,
                    )
                )

        outputs.append(
            await async_utils.run_and_check(
                "git", "-C", dest, "config", "remote.origin.url", fake_url,
            )
        )
        out = b"\n\n".join(o for o, e in outputs if o)
        err = b"\n\n".join(e for o, e in outputs if e)

        # Mark that this has been finished
        Image._COMPLETED_CLONES.add(dest)
        return out, err

    async def _build(self):
        """
        Build this Image's `dockerfile`. If copy_* or reqs_* are passed, then additional
        code or packages will be added in a later step.
        """
        assert self.dockerfile is not None
        # Build the specified dockerfile
        if self.needs_cloning():
            context = self.context.to_docker_mount(gitroot=self._get_clone_dest())
            dockerfile = self.dockerfile.to_docker_mount(gitroot=self._get_clone_dest())
        else:
            context = self.context.to_docker_mount(pipeline_id=self._pipeline_id)
            dockerfile = self.dockerfile.to_docker_mount(pipeline_id=self._pipeline_id)

        build_args = []
        if self.docker_build_args is not None:
            for k, v in self.docker_build_args.items():
                build_args += ["--build-arg", f"{k}={v}"]

        out, err = await async_utils.run_and_check(
            "docker",
            "build",
            "-t",
            self.name_built,
            "--label",
            "com.conducto.build",
            "--label",
            "com.conducto",
            *build_args,
            "-f",
            dockerfile,
            context,
        )
        return out, err

    async def _install(self):
        # Create dockerfile from stdin. Replace "-f <dockerfile> <copy_dir>"
        # with "-"
        text = await dockerfile_mod.text_for_install(
            self.name_built, self.reqs_py, self.reqs_packages, self.reqs_docker
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
            self.name_installed,
            "--label",
            "com.conducto.install",
            "--label",
            "com.conducto",
            "-",
            input=text.encode(),
        )
        return out, err

    async def _copy(self):
        env = {}
        if self.copy_url:
            env["CONDUCTO_GIT_URL"] = self.copy_url
        if self.copy_branch:
            env["CONDUCTO_GIT_BRANCH"] = self.copy_branch

        # If copying a whole repo, or if getting a remote Git repo, get the commit SHA.
        if self.copy_repo or self.needs_cloning():
            env["CONDUCTO_GIT_SHA"] = await self._sha1()

        if self.copy_dir:
            context = self.copy_dir.to_docker_mount(pipeline_id=self._pipeline_id)
        else:
            context = self._get_clone_dest()

        # Create dockerfile and dockerignore. For details on why we do it this way, see:
        #   https://github.com/moby/moby/issues/12886
        dockerfile_text = dockerfile_mod.text_for_copy(
            self.name_installed, self.docker_auto_workdir, env
        )
        dockerignore_text = dockerfile_mod.dockerignore_for_copy(
            context, preserve_git=self.copy_repo or self.needs_cloning()
        )
        dockerfile_path = f"/tmp/{self.name}/Dockerfile"
        dockerignore_path = f"/tmp/{self.name}/Dockerfile.dockerignore"
        os.makedirs(f"/tmp/{self.name}", exist_ok=True)
        with open(dockerfile_path, "w") as f:
            f.write(dockerfile_text)
        with open(dockerignore_path, "w") as f:
            f.write(dockerignore_text)

        # Run the command
        out, err = await async_utils.run_and_check(
            "docker",
            "build",
            "-t",
            self.name_copied,
            "--label",
            "com.conducto.copy",
            "--label",
            "com.conducto",
            "-f",
            dockerfile_path,
            context,
            env={**os.environ, "DOCKER_BUILDKIT": "1"},
        )
        return out, err

    async def _extend(self):
        if self.shell == Image.AUTO:
            self.shell = await dockerfile_mod.get_shell(self.name_copied)

        # Writes Dockerfile that extends user-provided image.
        text, worker_image = await dockerfile_mod.text_for_extend(self.name_copied)
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
            "com.conducto",
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
        """
        Check whether this image can be built in a cloud manager's RD. If it requires
        resources from a local machine -- such as copy_dir or a non-Git-based dockerfile
        -- then it cannot do this and needs an Agent to be built.
        """
        if self.copy_dir:
            return False
        if self.dockerfile and not self.copy_branch:
            return False
        return True

    def needs_building(self):
        return self.dockerfile is not None

    def needs_installing(self):
        return self.reqs_py or self.reqs_packages or self.reqs_docker

    def needs_copying(self):
        return self.copy_dir is not None or self.copy_url is not None


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


@functools.lru_cache(1)
def _get_path_map() -> typing.Dict[imagepath.Path, str]:
    if "CONDUCTO_PATH_MAP" in os.environ:
        d = json.loads(os.environ["CONDUCTO_PATH_MAP"])
        return {
            imagepath.Path.from_dockerhost_encoded(external): internal
            for external, internal in d.items()
        }
    return {}


_conducto_dir = os.path.dirname(os.path.dirname(__file__)) + os.path.sep
