import base64
import collections
import functools
import gzip
import shlex
import inspect
import json
import os
import pprint
import re
import traceback
import typing
import inspect

from .shared import constants, log, types as t, imagepath, resource_validation as rv
from . import api, callback, image as image_mod
import inspect

State = constants.State


class TreeError(Exception):
    pass


def jsonable(obj):
    try:
        json.dumps(obj)
        return True
    except TypeError:
        return False


def load_node(kwargs):
    use_cls = {"Exec": Exec, "Serial": Serial, "Parallel": Parallel}[kwargs["type"]]
    return use_cls(**{k: v for k, v in kwargs.items() if k in KEY_PARAMS})


class Node:
    """
    The node classes :py:class:`Exec`, :py:class:`Serial` and
    :py:class:`Parallel` all derive from this class.  The parameters here apply
    directly to `Exec` nodes and as defaults on `Serial` and `Parallel` for the
    sub-nodes.

    :param cpu: Number of CPUs to allocate to the Node. Must be >0 if assigned.
        Default: :code:`1`
    :type cpu: `float`

    :param mem: GB of memory to allocate to the Node. Must be >0 if assigned.
        Default: :code:`2`
    :type mem: `float`

    :param requires_docker: If True, enable the Node to use. Default: :code:`False`
    :type requires_docker: `bool`

    :param env: Mapping containing environment variables as its keys and values.
        Default: :code:`{}`
    :type env: `dict`

    :param image: Run Node in container using the given :py:class:`Image`
        or image identified by name in Docker.
    :type image: :py:class:`Image` or `str`.

    :param image_name: Reference an :py:class:`Image` by
        name instead of passing it explicitly. The Image must have been
        registered with :py:func:`Node.register_image`.
    :type image_name: `str`

    :param container_reuse_context: See :ref:`Running Exec nodes` for details. Note this
        has special inheritance rules when propagating to child nodes.

    :param skip: If False the Node will be run normally. If True execution will pass
        over it and it will not be run. Default: :code:`False`
    :type skip: `bool`

    :param suppress_errors: If True the Node will go to the Done state when finished,
        even if some children have failed. If False, any failed children will cause
        it to go to the Error state. Default: :code:`False`
    :type suppress_errors: `bool`

    :param max_time: An int or float value of seconds, or a duration string, representing
        the maximum time a Node may take to complete successfully. If a Node exceeds this
        time, it will be killed. The duration string must be a positive decimal with a
        suffix of 's', 'm', 'h', or 'd', indicating seconds, minutes, hours, or days
        respectively. Default: :code:`'4h'`
    :type max_time: `int` or `float` or `str`

    :param max_concurrent: If set it limits the number of
        descendant Exec nodes that can run concurrently. Only applies to
        :py:class:`Serial` and :py:class:`Parallel` nodes.
        Default: :code:`None`
    :type max_concurrent: `int`

    :param docker_run_args: Additional arguments to pass to
        :code:`docker run` when starting the container that runs the Exec node.
        Default: :code:`None`

        Caution: the arguments specified here are passed directly to :code:`docker run`.
        They may cause the command to fail, in which case affected Exec nodes will
        not run. If you create orphaned Docker containers, clean them up with
        :code:`docker container list` and :code:`docker kill`. Use with care.

        **Only allowed for local mode**
    :type docker_run_args: `str` or `List[str]`

    :param name: If creating Node inside a context manager, you may pass :code:`name=...`
        instead of using normal dict assignment. Default: :code:`None`
    :type name: `str`

    All of these arguments, except for :code:`name`, may be set in the Node
    constructor or later. For example, :code:`n = co.Parallel(cpu=2)` and

    .. code-block::

        n = co.Parallel()
        n.cpu = 2

    are equivalent.

    :ivar name: Immutable. The name of this Node must be unique among sibling
        Nodes. It is most commonly set through dict assignment with
        :code:`parent['nodename'] = co.Parallel()`. It may also be set in the
        constructor with :code:`co.Parallel(name='nodename')` if you're using
        another Node as a context manager. It may not contain a :code:`/`, as
        :code:`/` is reserved as the path separator.
    """

    # We define __iter__ as None, so that python will error early if Node is used in a
    # loop and not fall back to using __getitem__, which cannot accept python 2.2 style
    # iteration.
    __iter__ = None

    # Enum regarding skip statuses. The naming is awkward but intentional:
    # 'skip' is the namespace, but we should phrase the terms in the positive,
    # i.e., how many are running.
    SKIP_RUN_NONE = 0
    SKIP_RUN_SOME = 1
    SKIP_RUN_ALL = 2

    # In AWS cloud mode, mem and cpu must fit on an EC2 instance (in EC2
    # mode), and must be one of allowed pairings (in FARGATE mode).
    DEFAULT_MEM = 2
    DEFAULT_CPU = 1
    DEFAULT_GPU = 0

    sys_default = {
        "cpu": DEFAULT_CPU,
        "gpu": DEFAULT_GPU,
        "mem": DEFAULT_MEM,
        "container_id": -1,
        "requires_docker": False,
        "max_time": "4h",
    }

    _CONTEXT_STACK = []

    _NUM_FILE_AND_LINE_CALLS = 0
    _MAX_FILE_AND_LINE_CALLS = 50000
    _PATH_MAP = json.loads(os.getenv("CONDUCTO_PATH_MAP", "{}"))
    _PATH_MAP = {
        imagepath.Path.from_dockerhost_encoded(k): v for k, v in _PATH_MAP.items()
    }

    if api.Config().get("config", "force_debug_info") or t.Bool(
        os.getenv("CONDUCTO_FORCE_DEBUG_INFO")
    ):
        _MAX_FILE_AND_LINE_CALLS = 10 ** 20

    __slots__ = (
        "_name",
        "id",
        "_id",
        "user_set",
        "_root",
        "pipeline_id",
        "token",
        "parent",
        "children",
        "_callbacks",
        "suppress_errors",
        "max_time",
        "max_concurrent",
        "container_reuse_context",
        "env",
        "doc",
        "title",
        "tags",
        "file",
        "line",
        "_repo",
        "_autorun",
        "_sleep_when_done",
        "callback_data",
    )

    def __init__(
        self,
        *,
        env=None,
        skip=False,
        name=None,
        cpu=None,
        gpu=None,
        mem=None,
        requires_docker=None,
        suppress_errors=False,
        max_time: typing.Union[int, float, str] = None,
        max_concurrent=None,
        container_reuse_context=None,
        same_container=constants.SameContainer.INHERIT,  # deprecated
        image: typing.Union[str, image_mod.Image] = None,
        image_name=None,
        docker_run_args=None,
        doc=None,
        title=None,
        tags: typing.Iterable = None,
        file=None,
        line=None,
        callback_data=None,
    ):

        self.parent = None
        self._root = self
        self.children = {}
        self._callbacks = []
        self.token = None

        self._repo = image_mod.Repository()

        # These are only to be set on the root node, and only by co.main().
        self._autorun = None
        self._sleep_when_done = None

        # default all user-settable parameters
        self.user_set = {
            "skip": False,
            "cpu": None,
            "gpu": None,
            "mem": None,
            "requires_docker": None,
            "docker_run_args": None,
        }
        self.image = None
        self.env = {}
        self.doc = None
        self.title = None
        self.tags = None
        self._name = "/"
        self.suppress_errors = False
        self.max_time = None
        self.max_concurrent = None
        self.container_reuse_context = None
        self.callback_data = callback_data

        # prefer same_container only if it is set and container_reuse_context is not
        if same_container is not constants.SameContainer.INHERIT:
            if container_reuse_context is None:
                container_reuse_context = same_container
            # throw if both are set
            else:
                raise ValueError(
                    "same_container is deprecated in favor of container_reuse_context, please don't use both."
                )
        if file is None:
            self.file, self.line = self._get_file_and_line()

        self.set(
            env=env,
            skip=skip,
            name=name,
            cpu=cpu,
            gpu=gpu,
            mem=mem,
            docker_run_args=docker_run_args,
            requires_docker=requires_docker,
            suppress_errors=suppress_errors,
            max_time=max_time,
            max_concurrent=max_concurrent,
            container_reuse_context=container_reuse_context,
            image=image,
            image_name=image_name,
            doc=doc,
            title=title,
            tags=tags,
            file=file,
            line=line,
        )

    def set(
        self,
        *,
        env=None,
        skip=None,
        name=None,
        cpu=None,
        gpu=None,
        mem=None,
        requires_docker=None,
        suppress_errors=None,
        max_time: typing.Union[int, float, str] = None,
        max_concurrent=None,
        container_reuse_context=None,
        image: typing.Union[str, image_mod.Image] = None,
        image_name=None,
        docker_run_args=None,
        doc=None,
        title=None,
        tags: typing.Iterable = None,
        file=None,
        line=None,
    ):
        """
        Set params on an already created node with args that would typically go
        in the constructor. This is relevant when a node has already been
        constructed with a function and its args, and no node args have yet
        been specified.
        """
        assert image_name is None or image is None, "can only specify one image"

        if skip is not None:
            self.user_set["skip"] = skip
        if cpu is not None:
            self.user_set["cpu"] = cpu
        if gpu is not None:
            self.user_set["gpu"] = gpu
        if mem is not None:
            self.user_set["mem"] = mem
        if requires_docker is not None:
            self.user_set["requires_docker"] = requires_docker
        if image_name is not None:
            self.user_set["image_name"] = image_name
        if docker_run_args is not None:
            self.user_set["docker_run_args"] = rv.docker_run_args(docker_run_args)

        if image is not None:
            self.image = image
        if env is not None:
            # explicitly copy the current env here so that this is not mutated
            # by modifying the passed dict later
            self.env = env.copy()
        if doc is not None:
            self.doc = doc
        if title is not None:
            self.title = title
        if tags is not None:
            self.tags = self.sanitize_tags(tags)

        if name is not None:
            if not Node._CONTEXT_STACK:
                raise ValueError(
                    f"Cannot assign name={name} outside of a context manager."
                )
            if "/" in name:
                raise ValueError(
                    f"Disallowed character in name, may not use '/': {name}"
                )
            parent = Node._CONTEXT_STACK[-1]
            parent[name] = self

        if suppress_errors is not None:
            self.suppress_errors = suppress_errors
        if max_time is not None:
            self.max_time = max_time
        if max_concurrent is not None:
            if max_concurrent == 0:
                raise Exception("Cannot specify no concurrent nodes")
            if type(Node) == Exec:
                raise Exception("Cannot specify max_concurrent on an Exec node")
            self.max_concurrent = max_concurrent

        if container_reuse_context is not None:
            self.container_reuse_context = container_reuse_context
        if file is not None:
            self.file = file
            self.line = line

    def __enter__(self):
        Node._CONTEXT_STACK.append(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if Node._CONTEXT_STACK[-1] is not self:
            raise Exception(
                f"Node context error: {repr(Node._CONTEXT_STACK[-1])} is not {repr(self)}"
            )
        Node._CONTEXT_STACK.pop()

    def __str__(self):
        """
        The full path of Node, computed by joining the names of this Node's ancestry with `/`.

        .. code-block:: python

           import conducto as co
           x = co.Parallel()
           x["foo"] = y = co.Parallel()
           x["foo/bar"] = z = co.Exec("echo foobar")

           print(f"x.name={x.name}  str(x)={x}")
           # x.name=/  str(x) = /
           print(f"y.name={y.name}  str(y)={y}")
           # y.name=foo  str(y) = /foo
           print(f"z.name={z.name}  str(z)={z}")
           # z.name=bar  str(z) = /foo/bar
           for node in x.stream():
               print(str(node))
           # /
           # /foo
           # /foo/bar
        """
        name = []
        cur = self
        while cur:
            name.append(cur.name)
            cur = cur.parent
        return "/".join(name[::-1]).replace("//", "/")

    @property
    def name(self):
        return self._name

    @property
    def repo(self):
        return self.root._repo

    @property
    def mem(self):
        return self.user_set["mem"]

    @property
    def gpu(self):
        return self.user_set["gpu"]

    @property
    def cpu(self):
        return self.user_set["cpu"]

    @property
    def docker_run_args(self):
        return self.user_set.get("docker_run_args")

    @property
    def requires_docker(self):
        return self.user_set.get("requires_docker")

    @property
    def skip(self):
        return self.user_set.get("skip", False)

    @mem.setter
    def mem(self, val):
        self.user_set["mem"] = val

    @gpu.setter
    def gpu(self, val):
        self.user_set["gpu"] = val

    @cpu.setter
    def cpu(self, val):
        self.user_set["cpu"] = val

    @docker_run_args.setter
    def docker_run_args(self, val):
        self.user_set["docker_run_args"] = val

    @property
    def image(self) -> typing.Optional[image_mod.Image]:
        if self.image_name is None:
            return None
        return self.repo[self.image_name]

    @property
    def image_name(self):
        return self.get_inherited_attribute("image_name")

    @image.setter
    def image(self, val):
        if val is None:
            self.user_set["image_name"] = None
            return
        if isinstance(val, str):
            val = image_mod.Image(val)
        if isinstance(val, image_mod.Image):
            self.repo.add(val)
            self.user_set["image_name"] = val.name
        else:
            raise ValueError(f"Unknown type for Node.image: {repr(val)}")

    @requires_docker.setter
    def requires_docker(self, val: bool):
        self.user_set["requires_docker"] = val

    @skip.setter
    def skip(self, val: bool):
        self.user_set["skip"] = val

    def register_image(self, image: image_mod.Image):
        """
        Register a named Image for use by descendant Nodes that specify
        image_name. This is especially useful with lazy pipeline creation to
        ensure that the correct base image is used.
        """
        self.repo.add(image)

    def on_done(self, cback):
        assert isinstance(cback, callback.base)
        self._callbacks.append((State.DONE, cback))

    def on_error(self, cback):
        assert isinstance(cback, callback.base)
        self._callbacks.append((State.ERROR, cback))

    def on_queued(self, cback):
        assert isinstance(cback, callback.base)
        self._callbacks.append((State.QUEUED, cback))

    def on_running(self, cback):
        assert isinstance(cback, callback.base)
        self._callbacks.append((State.RUNNING, cback))

    def on_killed(self, cback):
        assert isinstance(cback, callback.base)
        self._callbacks.append((State.WORKER_ERROR, cback))

    def on_state_change(self, cback):
        assert isinstance(cback, callback.base)
        self._callbacks.append(("stateChange", cback))

    def _pull(self):
        # these are finalized upon serialization
        hash_this = "/" + self.name
        self._id = (
            constants.Hashing.hash(hash_this, self.parent._id) if self.parent else 1
        )
        self.id = constants.Hashing.encode(self._id)

    # get root with path compression
    @property
    def root(self):
        if self._root != self:
            self._root = self._root.root
        return self._root

    def __setitem__(self, name, node):
        if "/" in name:
            path, new = name.rsplit("/", 1)
            self[path][new] = node
            return
        if not name:
            raise ValueError("Name cannot be empty")
        if name in self.children or node.root == self.root or node.root != node:
            raise TreeError(
                f"Adding node {name} violates the integrity of the pipeline"
            )
        self.children[name] = node

        self.repo.merge(node.repo)

        node.parent = self
        node._root = self.root
        node._name = name

    def __getitem__(self, item: str):
        if not isinstance(item, str):
            raise TypeError(
                f"Node names must be strings. Expected object of type str, got {repr(item)}"
            )
        # Absolute paths start with a '/' and begin at the root
        if item.startswith("/"):
            current = self.root
        else:
            current = self
        for i in item.split("/"):
            # Ignore consecutive delimiters: 'a/b//c' == 'a/b/c'
            if not i:
                continue

            # Find the referenced child and iterate
            current = current.children[i]
        return current

    def __contains__(self, item):
        try:
            self[item]
        except KeyError:
            return False
        else:
            return True

    def describe(self):
        output = {
            **self.user_set,
            **{"__env__" + key: value for key, value in self.env.items()},
            "id": self,
            "callbacks": [(event, cb.to_literal()) for event, cb in self._callbacks],
            "type": self.__class__.__name__,
            "file": self.file,
            "line": self.line,
        }
        if self.doc:
            output["doc"] = self.doc
        if self.title:
            output["title"] = self.title
        if self.tags:
            output["tags"] = self.tags
        if self.container_reuse_context is not None:
            output["container_reuse_context"] = self.container_reuse_context
        if self.suppress_errors:
            output["suppress_errors"] = self.suppress_errors
        if self.max_time:
            output["max_time"] = self.max_time
        if self.max_concurrent:
            output["max_concurrent"] = self.max_concurrent
        if self.callback_data:
            output["callback_data"] = self.callback_data
        if isinstance(self, Serial):
            output["stop_on_error"] = self.stop_on_error
        if isinstance(self, Exec):
            output["command"] = self.command
        return output

    @staticmethod
    def deserialize(string) -> "Node":
        string = gzip.decompress(base64.b64decode(string))
        data = json.loads(string)
        nodes = {n["id"]: load_node(n) for n in data["nodes"]}

        for i in data["nodes"]:
            for event, cb_literal in i.get("callbacks", []):
                cb = callback._parse(cb_literal)
                nodes[i["id"]]._callbacks.append((event, cb))

        for parent, child, name in data["edges"]:
            nodes[parent][name] = nodes[child]

        root = nodes[data["nodes"][0]["id"]]

        # initialize the repo here because we update its attributes in _pull()
        root._repo = image_mod.Repository()
        for name, image in data["images"].items():
            image = image_mod.Image(**image)
            root._repo.add(image)

        root.token = data.get("token")
        root._sleep_when_done = data.get("sleep_when_done", False)
        root._autorun = data.get("autorun")

        return root

    def serialize(self, pretty=False):

        res = {
            "edges": [],
            "nodes": [],
            "images": self.repo.images,
            "token": self.token,
            "autorun": self._autorun,
            "sleep_when_done": self._sleep_when_done,
            "agent_only": t.Bool(os.getenv("CONDUCTO_AGENT_ONLY")),
        }
        queue = collections.deque([self])
        while queue:
            node = queue.popleft()
            node._pull()
            res["nodes"].append(
                {k: v for k, v in node.describe().items() if v is not None}
            )

            for name, child in node.children.items():
                queue.append(child)
                res["edges"].append([node, child, name])

        class NodeEncoder(json.JSONEncoder):
            def default(self, o):
                if hasattr(o, "id"):
                    return o.id
                return o

        output = json.dumps(res, cls=NodeEncoder)
        if pretty:
            return pprint.pformat(json.loads(output), width=200)
        return base64.b64encode(
            gzip.compress(output.encode(), compresslevel=3)
        ).decode()

    # returns a stream in topological order
    def stream(self, reverse=False):
        """
        Iterate through the nodes
        """

        def _fwd():
            stack = [self]
            while stack:
                yld = stack.pop()
                yield yld
                stack.extend(list(yld.children.values())[::-1])

        def _bwd():
            stack = [[self, True]]
            while stack:
                while stack[-1][1]:
                    stack[-1][1] = False
                    for i in stack[-1][0].children.values():
                        stack.append([i, True])
                yield stack.pop()[0]

        if reverse:
            return _bwd()
        else:
            return _fwd()

    def get_inherited_attribute(self, attr):
        node = self
        while node is not None:
            v = node.user_set[attr]
            if v is not None:
                return v
            else:
                node = node.parent
        if attr in self.sys_default:
            return self.sys_default[attr]
        return None

    def launch_local(
        self, use_shell=True, retention=7, run=False, sleep_when_done=False
    ):
        """
        Launch directly from python.

        :param use_shell: If True connect to the running pipeline using
            the shell UI. Otherwise just launch the pipeline and
            then exit. Default: :code:`True`
        :type use_shell: `bool`

        :param retention: Once the pipeline is put to sleep, its logs and
            :ref:`data` will be deleted after `retention` days of inactivity.
            Until then it can be woken up and interacted with. Default: :code:`7`
        :type retention: `int`

        :param run: If True the pipeline will run immediately upon launching.
            Otherwise it will stay Pending until the user starts it. Default: :code:`False`
        :type run: `bool`

        :param sleep_when_done: If True the pipeline will sleep -- manager
            exits with recoverable state -- when the root node successfully
            gets to the Done state. Default: :code:`False`
        :type sleep_when_done: `bool`
        """

        # TODO:  Do we want these params? They seem sensible and they were documented at one point.
        # :param tags: If specified, should be a list of strings. The app lets you filter programs based on these tags.
        # :param title: Title to show in the program list in the app. If unspecified, the title will be based on the command line.

        self._build(
            build_mode=constants.BuildMode.LOCAL,
            shell=use_shell,
            retention=retention,
            run=run,
            sleep_when_done=sleep_when_done,
        )

    # optimized build that directly calls build.build,
    # no additional subprocesses, serialization is only performed once on the node
    def _build_optim(
        self,
        build_mode=constants.BuildMode.LOCAL,
        shell=False,
        app=False,
        retention=7,
        run=False,
        sleep_when_done=False,
        public=False,
    ):
        if self.image is None:
            self.image = image_mod.Image(name="conducto-default")

        self._autorun = run
        self._sleep_when_done = sleep_when_done
        from conducto.internal import build

        build.build(
            self,
            build_mode,
            use_shell=shell,
            use_app=app,
            retention=retention,
            is_public=public,
        )

    # emulate node.build from another language
    # this does not return the pipeline id, and incurs additional overhead
    # this is how .build in another language should be implemented
    def _build_multilang(
        self,
        build_mode=constants.BuildMode.LOCAL,
        shell=False,
        app=False,
        retention=7,
        run=False,
        sleep_when_done=False,
        public=False,
    ):
        if self.image is None:
            self.image = image_mod.Image(name="conducto-default")

        self._autorun = run
        self._sleep_when_done = sleep_when_done

        args = ["conducto", "build"]
        if shell:
            args.append("--shell")
        if app:
            args.append("--app")
        if build_mode == constants.BuildMode.LOCAL:
            args.append("--local")
        else:
            args.append("--cloud")
        args.append(f"--retention={retention}")
        if public:
            args.append("--public")

        import subprocess

        subprocess.run(args, input=self.serialize().encode("utf-8"))

    def _build(
        self,
        build_mode=constants.BuildMode.LOCAL,
        shell=False,
        app=False,
        retention=7,
        run=False,
        sleep_when_done=False,
        public=False,
    ):
        build_func = (
            self._build_multilang
            if os.getenv("CONDUCTO_EMULATE_MULTILANG")
            else self._build_optim
        )
        build_func(
            build_mode=build_mode,
            shell=shell,
            app=app,
            retention=retention,
            run=run,
            sleep_when_done=sleep_when_done,
            public=public,
        )

    def check_images(self):
        for node in self.stream():
            if isinstance(node, Exec):
                node.expanded_command()

    def pretty(self, strict=True):
        buf = []
        self._pretty("", "", "", buf, strict)
        return "\n".join(buf)

    def _pretty(self, node_prefix, child_prefix, index_str, buf, strict):
        """
        Draw pretty representation of the node pipeline, using ASCII box-drawing
        characters.

        For example:
          /
          ├─1 First
          │ ├─ Parallel1   "echo 'I run first"
          │ └─ Parallel2   "echo 'I also run first"
          └─2 Second   "echo 'I run last.'"
        """
        if isinstance(self, Exec):
            node_str = f"{log.format(self.name, color='cyan')}   {self.expanded_command(strict)}"
            node_str = node_str.strip().replace("\n", "\\n")
        else:
            node_str = log.format(self.name, color="blue")
        buf.append(f"{node_prefix}{index_str}{node_str}")
        length_of_length = len(str(len(self.children) - 1))
        for i, node in enumerate(self.children.values()):
            if isinstance(self, Parallel):
                new_index_str = " "
            else:
                new_index_str = f"{str(i).zfill(length_of_length)} "

            if i == len(self.children) - 1:
                this_node_prefix = f"{child_prefix}└─"
                this_child_prefix = f"{child_prefix}  "
            else:
                this_node_prefix = f"{child_prefix}├─"
                this_child_prefix = f"{child_prefix}│ "
            node._pretty(
                this_node_prefix, this_child_prefix, new_index_str, buf, strict
            )

    @staticmethod
    def sanitize_tags(val):
        if val is None:
            return val
        elif isinstance(val, (bytes, str)):
            return [val]
        elif isinstance(val, (list, tuple, set)):
            for v in val:
                if not isinstance(v, (bytes, str)):
                    raise TypeError(f"Expected list of strings, but got {repr(v)}")
            return val
        else:
            raise TypeError(f"Cannot convert {repr(val)} to list of strings.")

    @staticmethod
    def _get_file_and_line():
        if Node._NUM_FILE_AND_LINE_CALLS > Node._MAX_FILE_AND_LINE_CALLS:
            return None, None
        Node._NUM_FILE_AND_LINE_CALLS += 1

        for frame, lineno in traceback.walk_stack(None):
            filename = frame.f_code.co_filename
            if not filename.startswith(_conducto_dir):
                if not _isabs(filename):
                    filename = _abspath(filename)

                for external, internal in Node._PATH_MAP.items():
                    if filename.startswith(internal):
                        filename = filename.replace(
                            internal, external.to_docker_host(), 1
                        )
                return filename, lineno

        return None, None

    @staticmethod
    def force_debug_info(val):
        if val:
            Node._MAX_FILE_AND_LINE_CALLS = 10 ** 30
        else:
            Node._MAX_FILE_AND_LINE_CALLS = 10 ** 4


class Exec(Node):
    """
    A node that contains an executable command

    :param command: A shell command to execute or a python callable
    :type command: `str`

    If a Python callable is specified for the command the `args` and `kwargs`
    are serialized and a `conducto` command line is constructed to launch the
    function for that node in the pipeline.
    """

    __slots__ = ("command",)

    def __init__(self, command, *args, **kwargs):
        if callable(command):
            self._validate_args(command, *args, **kwargs)
            from .glue import method

            wrapper = method.Wrapper(command)
            command = wrapper.to_command(*args, **kwargs)
            kwargs = wrapper.get_exec_params(*args, **kwargs)
            args = []

        if args:
            raise ValueError(
                f"Only allowed arg is command. Got:\n  command={repr(command)}\n  args={args}\n  kwargs={kwargs}"
            )

        super().__init__(**kwargs)

        # Instance variables
        self.command = log.unindent(command)

    # Validate arguments for the given function without calling it. This is useful for
    # raising early errors on `co.Lazy()` or `co.Exec(func, *args, **kwargs).
    @staticmethod
    def _validate_args(func, *args, **kwargs):
        params = inspect.signature(func).parameters
        hints = typing.get_type_hints(func)
        if isinstance(func, staticmethod):
            function = func.__func__
        else:
            function = func

        # TODO: can target function have a `*args` or `**kwargs` in the signature? If
        # so, handle it.
        invalid_params = [
            (name, str(param.kind))
            for name, param in params.items()
            if param.kind != inspect.Parameter.POSITIONAL_OR_KEYWORD
        ]
        if invalid_params:
            raise TypeError(
                f"Unsupported parameter types of "
                f"{function.__name__}: {invalid_params} - "
                f"Only {str(inspect.Parameter.POSITIONAL_OR_KEYWORD)} is allowed."
            )

        # this will also validate against too-many or too-few arguments
        call_args = inspect.getcallargs(function, *args, **kwargs)
        for name, arg_value in call_args.items():
            if name in hints:
                # If there is a type hint, use the output of `typing.get_type_hints`. It
                # infers typing.Optional when default is None, and it handles forward
                # references.
                param_type = hints[name]
            else:
                # If
                param_type = params[name].annotation
            if not t.is_instance(arg_value, param_type):
                raise TypeError(
                    f"Argument {name}={arg_value} {type(arg_value)} for "
                    f"function {function.__name__} is not compatible "
                    f"with expected type: {param_type}"
                )

    def delete_child(self, node):
        raise NotImplementedError("Exec nodes have no children")

    def append_child(self, node):
        raise NotImplementedError("Exec nodes have no children")

    def resolve_intermediate_paths(self):
        if "__conducto_intermediate_path" in self.command:
            prefix = "__conducto_intermediate_path:"
            suffix = ":endpath__"

            intermediate_path = re.search(
                f"{prefix}(.*?){suffix}", self.command
            ).group()

            abspath = intermediate_path[len(prefix) : -len(suffix)]
            ctxpath = image_mod.Image.get_contextual_path(abspath)

            serialized_path = f"__conducto_path:{ctxpath.linear()}:endpath__"
            self.command = self.command.replace(intermediate_path, serialized_path)

    def expanded_command(self, strict=True):
        if "__conducto_path:" in self.command:
            img = self.image

            if img is None:
                if strict:
                    raise ValueError(
                        "Node references code inside a container but no image is specified\n"
                        f"  Node: {self}"
                    )
                else:
                    return self.command

            copy_loc = constants.ConductoPaths.COPY_LOCATION

            def repl(match):
                path = match.group(1)
                path_map = dict(img.path_map) if img.path_map is not None else {}

                path = imagepath.Path.from_dockerhost_encoded(path)

                for external, internal in path_map.items():
                    # external is already an imagepath.Path

                    # For each element of path_map, see if the external path matches
                    if not path.is_subdir_of(external):
                        continue

                    relative = imagepath.Path.to_unix_relpath(external, path)

                    # If so, calculate the corresponding internal path
                    internal = os.path.normpath(internal.rstrip("/"))
                    new_path = os.path.join(internal, relative)

                    # As a convenience, if we `docker_auto_workdir` then we know the workdir and
                    # we can shorten the path
                    if img.docker_auto_workdir and new_path.startswith(copy_loc):
                        return shlex.quote(os.path.relpath(new_path, copy_loc))
                    else:
                        # Otherwise just return an absolute path.
                        return shlex.quote(new_path)

                raise ValueError(
                    f"Node references local code but the Image doesn't have enough information to infer the corresponding path inside the container.\n"
                    f"Expected '.copy_dir', '.copy_url' inside a Git directory, or 'path_map'."
                    f"  Node: {self}\n"
                    f"  Image: {img.to_dict()}"
                )

            return re.sub("__conducto_path:(.*?):endpath__", repl, self.command)
        else:
            return self.command


class Parallel(Node):
    """
    Node that has child Nodes and runs them at the same time.
    Same interface as :py:class:`Node`.
    """

    __slots__ = []
    pass


class Serial(Node):
    """
    Node that has child Nodes and runs them one after another. Same interface as
    :py:class:`Node`, plus the following:

    :param stop_on_error: If True the :code:`Serial` will Error when one of its
        children Errors, leaving subsequent children Pending. If `False` and a
        child Errors the :code:`Serial` will still run the rest of its children
        and then Error. Default: :code:`True`
    :type stop_on_error: `bool`
    """

    __slots__ = ["stop_on_error"]

    def __init__(
        self,
        *,
        env=None,
        skip=False,
        name=None,
        cpu=None,
        gpu=None,
        mem=None,
        requires_docker=None,
        stop_on_error=True,
        suppress_errors=False,
        max_time=None,
        max_concurrent=None,
        container_reuse_context=None,
        same_container=constants.SameContainer.INHERIT,  # deprecated
        image: typing.Union[str, image_mod.Image] = None,
        image_name=None,
        docker_run_args=None,
        doc=None,
        title=None,
        tags: typing.Iterable = None,
        file=None,
        line=None,
    ):
        super().__init__(
            env=env,
            skip=skip,
            name=name,
            cpu=cpu,
            gpu=gpu,
            mem=mem,
            requires_docker=requires_docker,
            suppress_errors=suppress_errors,
            max_time=max_time,
            max_concurrent=max_concurrent,
            container_reuse_context=container_reuse_context,
            same_container=same_container,
            image=image,
            image_name=image_name,
            docker_run_args=docker_run_args,
            doc=doc,
            title=title,
            tags=tags,
            file=file,
            line=line,
        )
        self.stop_on_error = stop_on_error


_abspath = functools.lru_cache(1000)(os.path.abspath)
_isabs = functools.lru_cache(1000)(os.path.isabs)
_conducto_dir = os.path.dirname(__file__) + os.path.sep

KEY_PARAMS = set(inspect.signature(Serial.__init__).parameters) | {"command"}
