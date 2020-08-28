import os
import re
import json
import collections
import functools
import subprocess
from conducto.shared import constants, log

# NOTE:  this is imported in the functions needed since it is not permissible
# in the code shared with the worker.
# import conducto.internal.host_detection as hostdet


def parse_registered_path(path):
    # TODO remove late August 2020 after everybody has the new path code
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
        self._value = None
        # ordered by index list of tuples
        #   [(i1, marktype), (i2, marktype), ...]
        # marktype may be (named) export or git root
        self._marks = []

    def __repr__(self):
        # This is only for diagnostic prints
        return f"""{{
    "path": "{self._value}",
    "marks": {json.dumps(self._marks)}
}}"""

    @classmethod
    def from_dockerhost_encoded(cls, s: dict) -> "Path":
        if isinstance(s, str) and s.startswith("{") and s.endswith("}"):
            s = json.loads(s)

        if not isinstance(s, dict):
            # TODO remove compatibility shim

            regpath = parse_registered_path(s)
            self = cls()
            self._type = "dockerhost"
            if regpath != None:
                self._value = regpath.hint + regpath.tail
                self._marks = [(len(regpath.hint), f"shared:name={regpath.name}")]
            else:
                self._value = s
            return self

            # raise RuntimeError(f"string {s} not recognized as an encoded path")

        segs = s["path"]
        assert len(s["pathsep"]) == 1

        dirsegs = [""] if s["pathsep"] == "/" else []
        marks = []
        for seg in segs:
            if isinstance(seg, dict):
                prefix = s["pathsep"].join(dirsegs)
                if seg["type"] == "git":
                    marks.append((len(prefix), "git"))
                elif seg["type"] == "shared":
                    marks.append((len(prefix), f"shared:name={seg['name']}"))
                else:
                    raise RuntimeError(f"segment type {seg['type']} is unknown")
            else:
                dirsegs.append(seg)

        self = cls()
        self._type = "dockerhost"
        self._value = s["pathsep"].join(dirsegs)
        self._marks = marks
        return self

    def _id(self):
        import conducto.internal.host_detection as hostdet

        windockerhost = os.getenv("WINDOWS_HOST") or hostdet.is_wsl1()
        is_win_host = self._type == "dockerhost" and windockerhost
        is_win_env = self._type == "external" and hostdet.is_windows()
        is_win_pathhack = (
            self._value[1] == ":"
        )  # TODO - WINDOWS_HOST is not passed down to worker

        pathsep = "\\" if is_win_host or is_win_env or is_win_pathhack else "/"

        signal = "endpath:null"
        segs = []
        beginnings = [(0, "start")] + self._marks
        endings = self._marks + [(len(self._value), signal)]
        for m1, m2 in zip(beginnings, endings):
            chunk = self._value[m1[0] : m2[0]]
            if chunk != "":
                segs += list(chunk.lstrip(pathsep).split(pathsep))

            if m2[1].startswith("shared:"):
                n = m2[1][len("shared:name=") :]
                segs.append({"type": "shared", "name": n})
            elif m2[1].startswith("git"):
                segs.append({"type": "git"})
            elif m2[1] == signal:
                pass

        return {"pathsep": pathsep, "path": segs}

    def linear(self):
        return json.dumps(self._id())

    def to_docker_mount(self):
        # to be used for building
        # This converts a path from a serialization to the manager or the host
        # machine which created this serialization.  It is will return a valid path
        # supposing that the host machine's directory structure matches what was at
        # the point of pipeline creation.  No information required from the user as
        # in resolve_named_share.
        import conducto.internal.host_detection as hostdet

        if constants.ExecutionEnv.value() in constants.ExecutionEnv.manager_all:
            # TODO:  I think this is a little ambiguous to call this "docker
            # mount" because this is really about building inside the container
            hostpath = self.to_docker_host()

            if os.getenv("WINDOWS_HOST"):
                hostpath = self._windows_docker_path(hostpath)

            # host `/` is mounted at `/mnt/external`
            return Path.PATH_PREFIX + hostpath
        else:
            hostpath = self.to_docker_host()

            # this results in a unix host path ...
            if hostdet.is_wsl1() or hostdet.is_windows():
                # ... or a docker-friendly windows path
                hostpath = self._windows_docker_path(hostpath)
            return hostpath

    def to_docker_host(self):
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

    def to_external(self):
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
    def _wsl_to_windows(p):
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
    def _windows_to_wsl(p):
        proc = subprocess.run(["wslpath", "-u", p], stdout=subprocess.PIPE)
        return proc.stdout.decode("utf-8").strip()

    @staticmethod
    def _windows_docker_path(path):
        """
        Returns the windows path with forward slashes.  This is the format docker
        wants in the -v switch.
        """
        winpath = path.replace("\\", "/")
        return f"/{winpath[0].lower()}{winpath[2:]}"

    def as_docker_host_path(self):
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
                r._value = winpath + self._value[firstmark:].replace("/", "\\")
                r._marks = [(m[0] + delta, m[1]) for m in self._marks]
                return r
            else:
                r = self._dupe()
                r._type = "dockerhost"
                return r

    def to_container(self):
        pass

    @classmethod
    def from_localhost(cls, s: str) -> "Path":
        self = cls()
        self._type = "external"
        self._value = s
        return self

    @classmethod
    def from_dockerhost(cls, s: str) -> "Path":
        self = cls()
        self._type = "dockerhost"
        self._value = s
        return self

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

        windockerhost = os.getenv("WINDOWS_HOST") or hostdet.is_wsl1()
        is_win_host = self._type == "dockerhost" and windockerhost
        is_win_env = self._type in ("dockerhost", "external") and hostdet.is_windows()
        is_win_pathhack = (
            self._value[1] == ":"
        )  # TODO - WINDOWS_HOST is not passed down to worker
        if is_win_host or is_win_env or is_win_pathhack:
            import ntpath

            cleanpath = parent._value.rstrip("/\\")
            try:
                return cleanpath == ntpath.commonpath([parent._value, self._value])
            except ValueError:
                # https://docs.python.org/3/library/os.path.html
                # if paths contain both absolute and relative pathnames,
                # the paths are on the different drives or if paths is empty.
                # then ValueError will be raised, lets just return false in this case
                log.debug(
                    f"commonpath raised value error for the following paths: {parent._value}, {self._value}"
                )
                return False
        else:
            import posixpath

            cleanpath = parent._value.rstrip("/")
            return cleanpath == posixpath.commonpath([parent._value, self._value])

    def append(self, tail, sep):
        import conducto.internal.host_detection as hostdet

        windockerhost = os.getenv("WINDOWS_HOST") or hostdet.is_wsl1()
        is_win_host = self._type == "dockerhost" and windockerhost
        is_win_env = self._type == "external" and hostdet.is_windows()
        is_win_pathhack = (
            self._value[1] == ":"
        )  # TODO - WINDOWS_HOST is not passed down to worker
        if is_win_host or is_win_env or is_win_pathhack:
            if sep == "/":
                tail = tail.replace("/", "\\")
        else:
            if sep == "\\":
                tail = tail.replace("\\", "/")

        x = self._dupe()
        # empty strings get a trailing slash on an os.path.join, not a fan
        if tail != "":
            if is_win_host or is_win_env or is_win_pathhack:
                import ntpath

                x._value = ntpath.join(x._value, tail)
            else:
                import posixpath

                x._value = posixpath.join(x._value, tail)
        return x

    def _dupe(self):
        r = self.__class__()
        r._type = self._type
        r._value = self._value
        r._marks = self._marks
        return r

    def _dupe_new_mark(self, newmark):
        r = self.__class__()
        r._type = self._type
        r._value = self._value
        r._marks = self._marks[:] + [newmark]
        r._marks.sort(key=lambda x: x[0])
        return r

    def mark_named_share(self, name, path):
        return self._dupe_new_mark((len(path), f"shared:name={name}"))

    def mark_git_root(self):
        assert self._type == "external"

        p = self._value
        if os.path.isfile(p):
            dirname = os.path.dirname(p)
        else:
            dirname = p
        git_root = Path._get_git_root(dirname)
        if git_root:
            return self._dupe_new_mark((len(git_root), "git"))
        else:
            return self

    def get_git_root(self):
        for mark in self._marks:
            if mark[1].startswith("git"):
                r = self._dupe()
                r._value = self._value[: mark[0]]
                r._marks = [m for m in self._marks if m[0] <= mark[0]]
                return r
        return None

    def get_declared_shared(self):
        for mark in self._marks:
            if mark[1].startswith("shared:"):
                # TODO not sure about this syntax yet, but this is way it is for now.
                assert mark[1].startswith("shared:name=")

                sharedpath = collections.namedtuple(
                    "sharedpath", ["name", "hint", "tail"]
                )

                name = mark[1][len("shared:name=") :]
                hint, tail = self._value[: mark[0]], self._value[mark[0] :]
                return sharedpath(name, hint, tail)

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
    @functools.lru_cache(maxsize=None)
    def _get_git_origin(dirpath):
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
    @functools.lru_cache(maxsize=None)
    def _get_git_repo(dirpath):
        result = None

        try:
            PIPE = subprocess.PIPE
            args = ["git", "-C", dirpath, "remote", "-v"]
            out, err = subprocess.Popen(args, stdout=PIPE, stderr=PIPE).communicate()

            nongit = "fatal: not a git repository"
            if not err.decode("utf-8").rstrip().startswith(nongit):
                m = re.search(r"([\w\-_]+)\.git", out.decode("utf-8"))
                if m:
                    result = m.group(1)
        except FileNotFoundError:
            # log, but essentially pass
            log.debug("no git installation found, skipping directory indication")
        return result

    @staticmethod
    def _sanitize_git_origin(repo):
        return re.sub("[^a-zA-Z0-9@/.-]", "-", repo)

    def resolve_named_share(self):
        import conducto.internal.host_detection as hostdet
        import conducto.api as co_api

        # This converts a path from a serialization to a host machine.  It may or
        # may not the same host machine as the one that made the serialization and
        # it may need to be interactive with the user.

        if constants.ExecutionEnv.value() not in constants.ExecutionEnv.external:
            raise RuntimeError(
                "this is an interactive function and has no manager (or worker) implementation"
            )

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
            sharepath.mark_named_share(parsed.name, shares[0])
            result = sharepath.append(parsed.tail.strip(tailsep), sep=tailsep)
        elif len(shares) == 0:
            # ask
            print(
                f"The named share {parsed.name} is not known on this host. Enter a path for that share (blank to abort)."
            )
            path = input("path:  ")
            if path == "":
                raise RuntimeError("error shareing directories")
            conf.register_named_share(conf.default_profile, parsed.name, path)

            sharepath = Path.from_dockerhost(path)
            sharepath.mark_named_share(parsed.name, path)
            result = sharepath.append(parsed.tail.strip(tailsep), sep=tailsep)
        elif parsed.hint in shares:
            sharepath = Path.from_dockerhost(parsed.hint)
            sharepath.mark_named_share(parsed.name, parsed.hint)
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
            sharepath.mark_named_share(parsed.name, path)
            result = sharepath.append(parsed.tail.strip(tailsep), sep=tailsep)

        return result
