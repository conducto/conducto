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
import uuid

import conducto.internal.host_detection as hostdet
from conducto.shared import async_utils, log
from .. import pipeline
from . import dockerfile as dockerfile_mod, names


def relpath(path):
    ctxpath = Image.get_contextual_path(path)
    if hostdet.is_wsl():
        import conducto.internal.build as cib

        ctxpath = cib._split_windocker(ctxpath)
    elif hostdet.is_windows():
        ctxpath = hostdet.windows_docker_path(ctxpath)
    return f"__conducto_path:{ctxpath}:endpath__"


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
    PATH_PREFIX = ""

    def __init__(
        self,
        image=None,
        *,
        dockerfile=None,
        context=None,
        copy_dir=None,
        copy_url=None,
        copy_branch=None,
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
            image = f"python:{sys.version_info[0]}.{sys.version_info[1]}"
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
        self.context = context
        self.copy_dir = copy_dir
        self.copy_url = copy_url
        self.copy_branch = copy_branch
        self.reqs_py = reqs_py
        self.path_map = path_map

        self.pre_built = pre_built

        if not self.pre_built:
            if dockerfile is not None:
                self.dockerfile = self.get_contextual_path(dockerfile)
                if context is None:
                    context = os.path.dirname(self.dockerfile)
                self.context = self.get_contextual_path(context)

            if copy_dir is not None:
                self.copy_dir = self.get_contextual_path(copy_dir)
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
            "context": self.context,
            "copy_dir": self.copy_dir,
            "copy_url": self.copy_url,
            "copy_branch": self.copy_branch,
            "reqs_py": self.reqs_py,
            "path_map": self.path_map,
        }

    @staticmethod
    def get_contextual_path(p):
        op = os.path

        # Translate relative paths as starting from the file where they were defined.
        if not op.isabs(p):
            stack = traceback.extract_stack()
            from_file = stack[-3].filename
            pipeline = op.join(op.dirname(op.dirname(__file__)), "pipeline.py")
            if op.realpath(from_file) == op.realpath(pipeline):
                from_file = stack[-4].filename
            from_dir = op.dirname(from_file)
            p = op.realpath(op.join(from_dir, p))

        # Apply context specified from outside this container. Needed for recursive
        # co.lazy_py calls inside an Image with ".path_map".
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
        except FileNotFoundError as e:
            # log, but essentially pass
            log.debug("no git installation found, skipping directory indication")
        return result

    @property
    def name_built(self):
        if self.needs_building():
            key = json.dumps(self.to_raw_image()).encode()
            return "conducto_built:" + hashlib.md5(key).hexdigest()
        else:
            return self.image

    @property
    def name_complete(self):
        if self.needs_completing():
            key = json.dumps(self.to_raw_image()).encode()
            return "conducto_complete:" + hashlib.md5(key).hexdigest()
        else:
            return self.name_built

    @property
    def name_local_extended(self):
        return (
            "conducto_extended:" + hashlib.md5(self.name_complete.encode()).hexdigest()
        )

    @property
    def name_cloud_extended(self):
        return (
            f"263615699688.dkr.ecr.us-east-2.amazonaws.com/{self.name_local_extended}"
        )

    @property
    def cloud_buildable(self):
        return self.copy_dir is None

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
        """
        Generator that pulls/builds/extends/pushes this Image.
        """
        assert len(self.history) == 1 and self.history[0].status == Status.PENDING
        self.history[0].finish()

        # Pull the image if needed
        if self.image and "/" in self.image:
            with self._new_status(Status.PULLING) as st:
                callback()
                out, err = await async_utils.run_and_check("docker", "pull", self.image)
                st.finish(out, err)

        # Build the image if needed
        if self.needs_building():
            with self._new_status(Status.BUILDING) as st:
                callback()
                out, err = await self._build()
                st.finish(out, err)

        # If needed, copy files into the image and install packages
        if self.needs_completing():
            with self._new_status(Status.COMPLETING) as st:
                callback()
                out, err = await self._complete()
                st.finish(out, err)

        with self._new_status(Status.EXTENDING) as st:
            callback()
            out, err = await self._extend()
            st.finish(out, err)

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
        build_args = [
            "-f",
            Image.PATH_PREFIX + self.dockerfile,
            Image.PATH_PREFIX + self.context,
        ]
        out, err = await async_utils.run_and_check(
            "docker",
            "build",
            "-t",
            self.name_built,
            "--label",
            "conducto",
            *build_args,
        )
        return out, err

    async def _complete(self):
        # Create dockerfile from stdin. Replace "-f <dockerfile> <copy_dir>"
        # with "-"
        if self.copy_dir:
            build_args = ["-f", "-", Image.PATH_PREFIX + self.copy_dir]
        else:
            build_args = ["-"]
        build_args += ["--build-arg", f"CONDUCTO_CACHE_BUSTER={uuid.uuid4()}"]
        text = await dockerfile_mod.text_for_build_dockerfile(
            self.name_built, self.reqs_py, self.copy_url, self.copy_branch
        )

        out, err = await async_utils.run_and_check(
            "docker",
            "build",
            "-t",
            self.name_complete,
            "--label",
            "conducto",
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
            await dockerfile_mod.pull_conducto_worker(worker_image)
        out, err = await async_utils.run_and_check(
            "docker",
            "build",
            "-t",
            self.name_local_extended,
            "--label",
            "conducto",
            "-",
            input=text.encode(),
        )
        return out, err

    async def _push(self):
        # If push_to_cloud, tag the local image and push it
        await async_utils.run_and_check(
            "docker", "tag", self.name_local_extended, self.name_cloud_extended
        )
        out, err = await async_utils.run_and_check(
            "docker", "push", self.name_cloud_extended
        )
        return out, err

    def needs_building(self):
        return self.dockerfile is not None

    def needs_completing(self):
        return (
            self.copy_dir is not None
            or self.copy_url is not None
            or self.reqs_py is not None
        )


def make_all(node: "pipeline.Node", push_to_cloud):
    images = {}
    for n in node.stream():
        if n.user_set["image_name"]:
            img = n.repo[n.user_set["image_name"]]
            img.pre_built = True
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
    futs = [
        img.make(push_to_cloud=push_to_cloud, callback=_print_status)
        for img in images.values()
    ]

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
