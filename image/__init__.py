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

from conducto.shared import async_utils, log
from .. import pipeline
from . import dockerfile, names


def relpath(path):
    ctxpath = Image.get_contextual_path(path)
    return f"__conducto_path:{ctxpath}:endpath__"


class Status:
    PENDING = "pending"
    QUEUED = "queued"
    PULLING = "pulling"
    BUILDING = "building"
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
        context_url=None,
        context_branch=None,
        reqs_py=None,
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
        if context is not None and context_url is not None:
            raise ValueError(
                f"Cannot specify both context ({context}) "
                f"and context_url ({context_url})"
            )
        if context_url is not None and context_branch is None:
            raise ValueError(
                f"If specifying context_url ({context_url}) must "
                f"also specify context_branch ({context_branch})"
            )

        self.image = image
        self.dockerfile = dockerfile
        self.context = context
        self.context_url = context_url
        self.context_branch = context_branch
        self.reqs_py = reqs_py

        self.pre_built = pre_built

        if not self.pre_built:
            if image is not None:
                if context is not None:
                    self.context = self.get_contextual_path(context)
            elif dockerfile is not None:
                self.dockerfile = self.get_contextual_path(dockerfile)
                assert os.path.isfile(
                    Image.PATH_PREFIX + self.dockerfile
                ), f"Image(dockerfile={dockerfile}) must point to a file"
                if context is None:
                    context = os.path.dirname(self.dockerfile)
                self.context = self.get_contextual_path(context)

        self._py_binary = None
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
            "context_url": self.context_url,
            "context_branch": self.context_branch,
            "reqs_py": self.reqs_py,
            "pre_built": self.pre_built,
        }

    def to_raw_image(self):
        return {
            "image": self.image,
            "dockerfile": self.dockerfile,
            "context": self.context,
            "context_url": self.context_url,
            "context_branch": self.context_branch,
            "reqs_py": self.reqs_py,
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
        # do.lazy_py calls inside an Image with ".context".
        ctx = os.getenv("CONDUCTO_CONTEXT")
        if ctx:
            external, internal = ctx.split(":", -1)
            if p.startswith(internal):
                p = p.replace(internal, external, 1)

        # If ".context" wasn't set, find the git root so that we could possibly use this
        # inside an Image with ".context_url" set.
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
    def name_local_extended(self):
        return "conducto_extended:" + hashlib.md5(self.name_built.encode()).hexdigest()

    @property
    def name_cloud_extended(self):
        return (
            f"263615699688.dkr.ecr.us-east-2.amazonaws.com/{self.name_local_extended}"
        )

    @property
    def cloud_buildable(self):
        return self.context is None

    @property
    def status(self):
        return self.history[-1].status

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
            self.history.append(HistoryEntry(Status.ERROR, finish=True))
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
        "Build" this image. If a specific image is named, pull it to local.
        Otherwise, run 'docker build', possibly auto-generating a Dockerfile.
        """
        if self.dockerfile is not None:
            # Build the specified dockerfile
            build_args = [
                "-f",
                Image.PATH_PREFIX + self.dockerfile,
                Image.PATH_PREFIX + self.context,
            ]
            stdin = None
        else:
            # Create dockerfile from stdin. Replace "-f <dockerfile> <context>"
            # with "-"
            if self.context:
                build_args = ["-f", "-", Image.PATH_PREFIX + self.context]
            else:
                build_args = ["-"]
            lines = dockerfile.lines_for_build_dockerfile(
                self.image, self.reqs_py, self.context_url, self.context_branch
            )
            stdin = ("\n".join(lines)).encode()

        out, err = await async_utils.run_and_check(
            "docker", "build", "-t", self.name_built, *build_args, input=stdin
        )
        return out, err

    async def _extend(self):
        # Writes Dockerfile that extends user-provided image.
        lines = dockerfile.lines_for_extend_dockerfile(self.name_built)
        text = "\n".join(lines).encode()
        out, err = await async_utils.run_and_check(
            "docker", "build", "-t", self.name_local_extended, "-", input=text
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
        return (
            self.dockerfile is not None
            or self.context is not None
            or self.context_url is not None
            or self.reqs_py is not None
        )


def make_all(node: "pipeline.Node", push_to_cloud):
    images = {}
    for n in node.stream():
        if n.user_set["image_name"]:
            img = n.repo[n.user_set["image_name"]]
            img.pre_built = True
            if img.name_built not in images:
                images[img.name_built] = img

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
