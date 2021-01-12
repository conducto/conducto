import contextlib
import concurrent.futures

import os
import sys
import traceback
import typing
import itertools

from conducto.shared import (
    async_utils,
    client_utils,
    types as t,
    log,
)
import conducto
from . import dockerfile as dockerfile_mod, names

from conducto.shared import constants, imagepath
from . import names

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

    def finalize(self):
        from .internal_image import Image as IImage

        image_shells, self.images = self.images, {}
        for img in image_shells.values():
            self.add(IImage(**img.to_dict()))


class Image:
    """
    :param image:  Specify the base image to start from. Code can be added with
        various context* variables, and packages with reqs_* variables.
    :type image: `str`

    :param dockerfile:  Use instead of :code:`image` and pass a path to a Dockerfile.
        Relative paths are evaluated starting from the file where this code is
        written. Unless :code:`context` is specified, it uses the directory of the
        Dockerfile as the build context
    :type dockerfile: `str`

    :param docker_build_args: Dict mapping names of arguments to
        :code:`docker --build-args` to values
    :type docker_build_args: `dict`

    :param docker_auto_workdir: Set the work-dir to the destination of
        :code:`copy_dir`. Default: :code:`True`
    :type docker_auto_workdir: `bool`

    :param context:  Use this to specify a custom docker build context when
        using :code:`dockerfile`.
    :type context: `str`

    :param copy_repo:  Set to `True` to automatically copy the entire current Git repo
        into the Docker image. Use this so that a single Image definition can either use
        local code or can fetch from a remote repo.
        **copy_dir mode**: Normal use of this parameter uses local code, so it sets
        `copy_dir` to point to the root of the Git repo of the calling code.
        **copy_url mode**: Specify `copy_branch` to use a remote repository. This is
        commonly done for CI/CD. When specified, `copy_url` will be auto-populated.
    :type copy_repo: `bool`

    :param copy_dir:  Path to a directory. All files in that directory (and its
        subdirectories) will be copied into the generated Docker image.
    :type copy_dir: `str`

    :param copy_url:  URL to a Git repo. Conducto will clone it and copy its
        contents into the generated Docker image. Authenticate to private
        GitHub repos with a URL like `https://{user}:{token}@github.com/...`.
        See secrets for more info on how to store this securely. Must also
        specify copy_branch.
    :type copy_url: `str`

    :param copy_branch:  A specific branch name to clone. Required if using copy_url.
    :type copy_branch: `str`

    :param path_map:  Dict that maps external_path to internal_path. Needed for
        live debug and for passing callables to :py:class:`Exec` & :py:class:`Lazy`.
        It can be inferred from :code:`copy_dir`, :code:`copy_url`, or :code:`copy_repo`;
        if not using one of those, you must specify :code:`path_map` explicitly. This
        typically happens when a user-generated Dockerfile copies the code into the image.
    :type path_map: `None`

    :param reqs_py:  List of Python packages for Conducto to :code:`pip install` into
        the generated Docker image.
    :type reqs_py: `List[str]`

    :param reqs_packages: List of packages to install with the appropriate Linux package
        manager for this image's flavor.
    :type reqs_packages: `List[str]`

    :param reqs_docker: If :code:`True`, install Docker during build time.
    :type reqs_docker: `bool`

    :param shell: Which shell to use in this container. Defaults to :code:`co.Image.AUTO` to
        auto-detect. :code:`AUTO` will prefer :code:`/bin/bash` when available, and fall back to
        :code:`/bin/sh` otherwise.
    :type shell: `str`

    :param name: Name this `Image` so other Nodes can reference it by name. If
        no name is given, one will automatically be generated from a list of
        our favorite Pokemon. I choose you, angry-bulbasaur!
    :type name: `str`

    :param instantiation_directory: The directory of the file in which this image object was created. This is
        used to determine where relative paths passed into co.Image are relative from. This is
        automatically populated internally by conducto.
    :type instantiation_directory: `str`
    """

    _CONTEXT = None

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
        reqs_npm=None,
        reqs_packages=None,
        reqs_docker=False,
        path_map=None,
        shell=AUTO,
        name=None,
        git_urls=None,
        instantiation_directory=None,
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
        self.copy_url = copy_url
        self.image = image
        self.dockerfile = dockerfile
        self.docker_build_args = docker_build_args
        self.context = context
        self.copy_repo = copy_repo
        self.copy_dir = copy_dir
        self.copy_url = copy_url
        self.copy_branch = copy_branch
        self.docker_auto_workdir = docker_auto_workdir
        self.reqs_py = reqs_py
        self.reqs_npm = reqs_npm
        self.reqs_packages = reqs_packages
        self.reqs_docker = reqs_docker
        self.path_map = path_map or {}
        self.shell = shell
        self.git_urls = git_urls

        self.instantiation_directory = instantiation_directory or _non_conducto_dir()

    def __eq__(self, other):
        return isinstance(other, Image) and self.to_dict() == other.to_dict()

    @staticmethod
    def _serialize_path(p):
        return p._id() if isinstance(p, imagepath.Path) else p

    @staticmethod
    def _serialize_pathmap(pathmap):
        if pathmap:
            return {
                p if isinstance(p, str) else p.linear(): v for p, v in pathmap.items()
            }
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
            "reqs_npm": self.reqs_npm,
            "reqs_packages": self.reqs_packages,
            "reqs_docker": self.reqs_docker,
            "path_map": self._serialize_pathmap(self.path_map),
            "shell": self.shell,
            "instantiation_directory": self.instantiation_directory,
        }

    # Note: these methods are not needed in non-python implementations
    # co.Lazy(function) is not a thing in other languages
    # and conducto share-directory should be called instead writing an Image.share_directory

    @staticmethod
    def get_contextual_path(
        p: typing.Union[imagepath.Path, dict, str],
        *,
        named_shares=True,
        branch=None,
        url=None,
    ) -> imagepath.Path:
        from conducto.image.internal_image import Image as IImage

        class HackImage:
            instantiation_directory = _non_conducto_dir()

        return IImage.get_contextual_path(
            HackImage(), p, named_shares=named_shares, branch=branch, url=url
        )

    @staticmethod
    def share_directory(name, relative):
        import conducto
        from conducto.image.internal_image import Image as IImage

        path = Image.get_contextual_path(relative, named_shares=False)
        config = conducto.api.Config()
        config.register_named_share(config.default_profile, name, path)

    register_directory = share_directory


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
