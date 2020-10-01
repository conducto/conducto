import collections
import functools
import json
import os
import re
import subprocess
import typing
from conducto.shared import constants, log

Sharedpath = collections.namedtuple("Sharedpath", ["name", "hint", "tail"])

# NOTE:  this is imported in the functions needed since it is not permissible
# in the code shared with the worker.
# import conducto.internal.host_detection as hostdet


class Path:
    """
    This represents a path to a file or directory available to a pipeline
    worker with annotations at named mount or git repo boundaries.  We expect
    some syntactic similarity to string handling and this object guarantees
    immutability.

    It handles platform variations and translations among the following:

    - linux docker host
    - windows docker host
    - wsl on windows docker host
    - worker container on linux/windows docker host
    - agent or manager on linux/windows docker host
    """

    PATH_PREFIX = ""

    def __init__(self):
        # _type is one of internal, external, dockerhost, exportrooted
        self._type = None
        self._pathsep = None
        self._value = None
        # ordered by index list of tuples
        #   [(i1, marktype), (i2, marktype), ...]
        # marktype may be (named) export or git root
        self._marks = []

    def __repr__(self):
        # This is only for diagnostic prints
        return f"""{{
    "path": "{self._value}",
    "pathsep": "{self._pathsep}",
    "marks": {json.dumps(self._marks)}
}}"""

    @classmethod
    def from_dockerhost_encoded(cls, s: dict) -> "Path":
        if isinstance(s, str) and s.startswith("{") and s.endswith("}"):
            s = json.loads(s)

        if not isinstance(s, dict):
            raise RuntimeError(f"string {s} not recognized as an encoded path")

        segs = s["path"]
        assert len(s["pathsep"]) == 1

        dirsegs = [""] if s["pathsep"] == "/" else []
        marks = []
        for seg in segs:
            if isinstance(seg, dict):
                prefix = s["pathsep"].join(dirsegs)
                marks.append((len(prefix), seg))
            else:
                dirsegs.append(seg)

        self = cls()
        self._type = "dockerhost"
        self._pathsep = s["pathsep"]
        self._value = s["pathsep"].join(dirsegs)
        self._marks = marks
        return self

    def _id(self):
        pathsep = self._pathsep

        signal = {"type": "endpath"}
        segs = []
        beginnings = [(0, {"type": "startpath"})] + self._marks
        endings = self._marks + [(len(self._value), signal)]
        for m1, m2 in zip(beginnings, endings):
            chunk = self._value[m1[0] : m2[0]]
            if chunk != "":
                segs += list(chunk.lstrip(pathsep).split(pathsep))

            if m2[1]["type"] != signal["type"]:
                segs.append(m2[1])

        return {"pathsep": pathsep, "path": segs}

    def linear(self):
        return json.dumps(self._id())

    def is_git_branch_rooted(self):
        for m in self._marks:
            if m[1]["type"] == "git" and m[1].get("branch", None):
                return True

    def to_docker_mount(self, *, pipeline_id=None, gitroot=None) -> str:
        # to be used for building
        # This converts a path from a serialization to the manager or the host
        # machine which created this serialization.  It is will return a valid path
        # supposing that the host machine's directory structure matches what was at
        # the point of pipeline creation.  No information required from the user as
        # in resolve_named_share.

        if constants.ExecutionEnv.value() in constants.ExecutionEnv.manager_all:
            # TODO:  I think this is a little ambiguous to call this "docker
            # mount" because this is really about building inside the container
            if self.is_git_branch_rooted() or gitroot:
                mark = [m for m in self._marks if m[1]["type"] == "git"][-1]
                if gitroot is None:
                    gitroot = constants.ConductoPaths.git_clone_dest(
                        pipeline_id=pipeline_id,
                        url=mark[1].get("url"),
                        branch=mark[1].get("branch"),
                    )
                # git-rooted paths have a leading slash bug
                gittail = self._value[mark[0] + 1 :]

                if self._pathsep == "\\":
                    gittail = gittail.replace("\\", "/")

                return os.path.join(gitroot, gittail)
            else:
                hostpath = self.to_docker_host()

                if self._pathsep == "\\":
                    hostpath = self._windows_docker_path(hostpath)

                # host `/` is mounted at `/mnt/external`
                return Path.PATH_PREFIX + hostpath
        else:
            return self.to_worker_mount()

    def to_worker_mount(self) -> str:
        # This converts a path from a serialization to what a worker will see when
        # running it. It will return a valid path supposing that the host machine's
        # directory structure matches what was at the point of pipeline creation.  No
        # information required from the user as in resolve_named_share.

        import conducto.internal.host_detection as hostdet

        hostpath = self.to_docker_host()

        # this results in a unix host path ...
        if hostdet.is_wsl1() or hostdet.is_windows():
            # ... or a docker-friendly windows path
            hostpath = self._windows_docker_path(hostpath)

        return hostpath

    @staticmethod
    def to_unix_relpath(parent, child):
        """
        Compute the relative path assuming that `child.is_subdir_of(parent)`
        returns True.
        """

        ptail, ctail = Path.find_last_common_mark(parent, child)
        assert ctail.startswith(ptail)
        return ctail[len(ptail) :].lstrip(child._pathsep).replace(child._pathsep, "/")

    def to_docker_host(self) -> str:
        if self._type == "dockerhost":
            return self._value

        # probably also want to support in-container paths by way of docker
        # mount analysis (as in get_external_conducto_dir)
        assert (
            self._type == "external"
        ), f"cannot convert path type '{self._type}' to docker_host string"

        import conducto.internal.host_detection as hostdet

        if hostdet.is_wsl1():
            return self._wsl_to_windows(self._value)
        else:
            return self._value

    def to_external(self) -> str:
        if self._type == "external":
            return self._value

        assert (
            self._type == "dockerhost"
        ), f"cannot convert path type '{self._type}' to external path string"

        import conducto.internal.host_detection as hostdet

        if hostdet.is_wsl1():
            return self._windows_to_wsl(self._value)
        else:
            return self._value

    @staticmethod
    def _wsl_to_windows(p) -> str:
        # circular import prevents top level
        import conducto.api as co_api

        proc = subprocess.run(["wslpath", "-w", p], stdout=subprocess.PIPE)
        winpath = proc.stdout.decode("utf-8").strip()
        if winpath.startswith(r"\\") or winpath[1] != ":":
            raise co_api.WSLMapError(
                f"The context path {p} is not on a Windows drive accessible to Docker.  All image contexts paths must resolve to a location on the Windows file system."
            )
        return winpath

    @staticmethod
    def _windows_to_wsl(p) -> str:
        proc = subprocess.run(["wslpath", "-u", p], stdout=subprocess.PIPE)
        return proc.stdout.decode("utf-8").strip()

    @staticmethod
    def _windows_docker_path(path) -> str:
        """
        Returns the windows path with forward slashes.  This is the format docker
        wants in the -v switch.
        """
        winpath = path.replace("\\", "/")
        return f"/{winpath[0].lower()}{winpath[2:]}"

    def to_docker_host_path(self) -> "Path":
        if self._type == "dockerhost":
            return self

        if self._type == "external":
            # convert with marks

            # worker shared code
            import conducto.internal.host_detection as hostdet

            if hostdet.is_wsl1():
                if len(self._marks):
                    firstmark = self._marks[0][0]
                else:
                    firstmark = len(self._value)

                markpref = self._value[:firstmark]
                winpath = self._wsl_to_windows(markpref)

                delta = len(winpath) - len(markpref)

                r = self.__class__()
                r._type = "dockerhost"
                r._pathsep = "\\"
                r._value = winpath + self._value[firstmark:].replace("/", "\\")
                r._marks = [(m[0] + delta, m[1]) for m in self._marks]
                return r
            else:
                r = self._dupe()
                r._type = "dockerhost"
                return r

    @classmethod
    def from_localhost(cls, s: str) -> "Path":
        import conducto.internal.host_detection as hostdet

        self = cls()
        self._type = "external"
        self._pathsep = "/" if not hostdet.is_windows() else "\\"
        self._value = s
        return self

    @classmethod
    def from_dockerhost(cls, s: str) -> "Path":
        import conducto.internal.host_detection as hostdet

        win_dockerhost = (
            hostdet.is_windows() or hostdet.is_wsl1() or os.environ.get("WINDOWS_HOST")
        )

        self = cls()
        self._type = "dockerhost"
        self._pathsep = "/" if not win_dockerhost else "\\"
        self._value = s
        return self

    @classmethod
    def from_marked_root(cls, rootdict, tail: str) -> "Path":
        # NOTE:  Windows users may specify either leaning slash in their
        # `.conducto.cfg` (particularly if they have unix peers).  However a
        # marked root is always materialized as a real path in a unix
        # environment.  The code here figures accordingly.

        self = cls()
        self._type = "dockerhost"
        self._pathsep = "/"
        self._value = tail.replace("\\", "/")
        self._marks = [(0, rootdict)]
        return self

    @staticmethod
    def find_last_common_mark(path1, path2):
        path1_tail = path1._value
        path2_tail = path2._value
        for path2_idx, path2_mark in reversed(path2._marks):
            found_match = False
            for path1_idx, path1_mark in reversed(path1._marks):
                if path2_mark == path1_mark:
                    found_match = True
                    break
            if found_match:
                path1_tail = path1._value[path1_idx:].lstrip(path1._pathsep)
                path2_tail = path2._value[path2_idx:].lstrip(path2._pathsep)
                break

        return path1_tail, path2_tail

    def is_subdir_of(self, parent: "Path") -> bool:
        import conducto.internal.host_detection as hostdet

        # WSL on windows is the only context in which external & dockerhost are not synonyms
        safe_host_aliasing = {self._type, parent._type} == {
            "external",
            "dockerhost",
        } and not hostdet.is_wsl1()

        assert (
            self._type == parent._type or safe_host_aliasing
        ), f"cannot compare path domains {self._type} and {parent._type}"

        assert (
            self._pathsep == parent._pathsep
        ), "cannot compare paths with different slashes"

        # find common marks -- either gitroot or shares -- and only compare tails
        self_tail, parent_tail = Path.find_last_common_mark(self, parent)

        if self._pathsep == "\\":
            import ntpath

            cleanpath = parent_tail.rstrip("/\\")
            try:
                return cleanpath == ntpath.commonpath([parent_tail, self_tail])
            except ValueError:
                # https://docs.python.org/3/library/os.path.html
                # if paths contain both absolute and relative pathnames,
                # the paths are on the different drives or if paths is empty.
                # then ValueError will be raised, lets just return false in this case
                log.debug(
                    f"commonpath raised value error for the following paths: {parent_tail}, {self_tail}"
                )
                return False
        else:
            import posixpath

            cleanpath = parent_tail.rstrip("/")
            common = posixpath.commonpath([parent_tail, self_tail])
            return os.path.normpath(cleanpath) == os.path.normpath(common)

    def append(self, tail, sep) -> "Path":
        if self._pathsep == "\\":
            if sep == "/":
                tail = tail.replace("/", "\\")
        else:
            if sep == "\\":
                tail = tail.replace("\\", "/")

        x = self._dupe()
        # empty strings get a trailing slash on an os.path.join, not a fan
        if tail != "":
            if self._pathsep == "\\":
                import ntpath

                x._value = ntpath.join(x._value, tail)
            else:
                import posixpath

                x._value = posixpath.join(x._value, tail)
        return x

    def _dupe(self) -> "Path":
        r = self.__class__()
        r._type = self._type
        r._pathsep = self._pathsep
        r._value = self._value
        r._marks = self._marks
        return r

    def _dupe_new_mark(self, newmark) -> "Path":
        r = self.__class__()
        r._type = self._type
        r._pathsep = self._pathsep
        r._value = self._value
        if newmark not in self._marks:
            r._marks = self._marks[:] + [newmark]
            r._marks.sort(key=lambda x: x[0])
        return r

    def mark_named_share(self, name, path) -> "Path":
        return self._dupe_new_mark((len(path), {"type": "shared", "name": name}))

    def mark_git_root(self, *, branch=None, url=None) -> "Path":
        assert self._type == "external"

        p = self._value
        if os.path.isfile(p):
            dirname = os.path.dirname(p)
        else:
            dirname = p
        git_root = Path._get_git_root(dirname)
        if git_root:
            d = {"type": "git"}
            if branch is not None:
                d["branch"] = branch
            if url is not None:
                d["url"] = url
            return self._dupe_new_mark((len(git_root), d))
        else:
            return self

    def get_git_root(self) -> typing.Optional["Path"]:
        for mark in self._marks:
            if mark[1]["type"] == "git":
                r = self._dupe()
                r._value = self._value[: mark[0]]
                r._marks = [m for m in self._marks if m[0] <= mark[0]]
                return r
        return None

    def get_declared_shared(self) -> typing.Optional[Sharedpath]:
        for mark in self._marks:
            if mark[1]["type"] == "shared":
                name = mark[1]["name"]
                hint, tail = self._value[: mark[0]], self._value[mark[0] :]
                return Sharedpath(name, hint, tail)

    def get_git_marks(self) -> typing.List[dict]:
        return [m for idx, m in self._marks if m["type"] == "git"]

    @staticmethod
    @functools.lru_cache(maxsize=None)
    def _get_git_root(dirpath) -> typing.Optional[str]:
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
    @functools.lru_cache(maxsize=None)
    def _get_git_origin(dirpath) -> typing.Optional[str]:
        result = None

        try:
            PIPE = subprocess.PIPE
            args = ["git", "-C", dirpath, "config", "--get", "remote.origin.url"]
            out, err = subprocess.Popen(args, stdout=PIPE, stderr=PIPE).communicate()

            nongit = "fatal: not a git repository"
            if not err.decode("utf-8").rstrip().startswith(nongit):
                return out.decode("utf-8").strip()
        except FileNotFoundError:
            # log, but essentially pass
            log.debug("no git installation found, skipping directory indication")
        return result

    @staticmethod
    def _sanitize_git_url(url) -> str:
        return re.sub("[^a-zA-Z0-9@/.]", "-", url)

    def resolve_named_share(self) -> "Path":
        import conducto.internal.host_detection as hostdet
        import conducto.api as co_api

        # This converts a path from a serialization to a host machine.  It may or
        # may not the same host machine as the one that made the serialization and
        # it may need to be interactive with the user.

        if constants.ExecutionEnv.value() != constants.ExecutionEnv.EXTERNAL:
            raise RuntimeError(
                "this is an interactive function and has no manager (or worker) implementation"
            )

        # TODO: There may be multiple possible shares: the GitHub webhook includes
        # shares for all possible URLs that clone the repo. One of them should match,
        # depending on how the user actually cloned the repo (e.g., 'git@github.com:',
        # 'https://', or 'git://').
        #
        # Also the user can share the same path by multiple names, and they should all
        # be considered.
        parsed = self.get_declared_shared()

        if not parsed:
            return self

        # TODO:  we do not know which way the slashes are leaning in the
        # incoming path.   It is certainly possible to launch a debug
        # session on linux (respectively windows) from a pipeline
        # originally launched on windows (respectively linux).  Right this
        # moment, I will assume that debug and launch platform match.
        if hostdet.is_windows() or hostdet.is_wsl1():
            tailsep = "\\"
        else:
            tailsep = "/"

        conf = co_api.Config()
        shares = conf.get_named_share_paths(conf.default_profile, parsed.name)

        if len(shares) == 1:
            sharepath = Path.from_dockerhost(shares[0])
            sharepath = sharepath.mark_named_share(parsed.name, shares[0])
            result = sharepath.append(parsed.tail.strip(tailsep), sep=tailsep)
        elif len(shares) == 0:
            # ask
            print(
                f"The named share {parsed.name} is not known on this host. Enter a path for that share (blank to abort)."
            )
            path = input("path:  ")
            if path == "":
                raise RuntimeError("error sharing directories")
            conf.register_named_share(conf.default_profile, parsed.name, path)

            sharepath = Path.from_dockerhost(path)
            sharepath = sharepath.mark_named_share(parsed.name, path)
            result = sharepath.append(parsed.tail.strip(tailsep), sep=tailsep)
        elif parsed.hint in shares:
            sharepath = Path.from_dockerhost(parsed.hint)
            sharepath = sharepath.mark_named_share(parsed.name, parsed.hint)
            result = sharepath.append(parsed.tail.strip(tailsep), sep=tailsep)
        else:
            # disambiguate by asking which
            print(
                f"The named share {parsed.name} is associated with multiple directories on this host. Select one or enter a new directory."
            )
            for index, mpath in enumerate(shares):
                print(f"\t[{index+1}] {mpath}")
            print("\t[new] Enter a new path")

            def safe_int(s):
                try:
                    return int(s)
                except ValueError:
                    return None

            while True:
                sel = input(f"path [1-{len(shares)}, new]:  ")
                if sel == "":
                    raise RuntimeError("error shareing directories")
                if sel == "new" or safe_int(sel) is not None:
                    break

            if sel == "new":
                path = input("path:  ")
                conf.register_named_share(conf.default_profile, parsed.name, path)
            else:
                path = shares[int(sel) - 1]

            sharepath = Path.from_dockerhost(path)
            sharepath = sharepath.mark_named_share(parsed.name, path)
            result = sharepath.append(parsed.tail.strip(tailsep), sep=tailsep)

        return result
