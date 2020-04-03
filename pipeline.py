import base64
import collections
import gzip
import itertools
import json
import os
import re
import typing

from .shared import constants, log
from . import callback, image as image_mod

State = constants.State


class TreeError(Exception):
    pass


def jsonable(obj):
    try:
        json.dumps(obj)
        return True
    except TypeError:
        return False


def load_node(**kwargs):
    if kwargs["type"] == "Exec":
        return Exec(**kwargs)
    elif kwargs["type"] == "Serial":
        return Serial(**kwargs)
    elif kwargs["type"] == "Parallel":
        return Parallel(**kwargs)
    else:
        raise TypeError("Type {} not a valid node type".format(kwargs["type"]))


class Node:

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

    _CONTEXT_STACK = []

    __slots__ = (
        "_name",
        "id",
        "id_root",
        "user_set",
        "_root",
        "pipeline_id",
        "id_generator",
        "result",
        "token",
        "parent",
        "children",
        "_callbacks",
        "suppress_errors",
        "same_container",
        "env",
        "doc",
        "_repo",
        "_autorun",
        "_sleep_when_done",
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
        same_container=constants.SameContainer.INHERIT,
        image: typing.Union[str, image_mod.Image] = None,
        image_name=None,
        doc=None,
    ):
        self.id_generator, self.id_root = itertools.count(), self
        self.id = None

        self.parent = None
        self._root = self
        self.children = {}
        self._callbacks = []
        self.token = None

        assert image_name is None or image is None, "can only specify one image"

        self._repo = image_mod.Repository()
        # store actual values of each attribute
        self.user_set = {
            "skip": skip,
            "cpu": cpu,
            "gpu": gpu,
            "mem": mem,
            "requires_docker": requires_docker,
        }

        if image:
            self.image = image
        else:
            self.user_set["image_name"] = image_name

        self.env = env or {}

        self.doc = doc

        self.result = {}
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
        else:
            self._name = "/"

        self.suppress_errors = suppress_errors
        self.same_container = same_container

        # These are only to be set on the root node, and only by co.main().
        self._autorun = None
        self._sleep_when_done = None

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
    def _id(self):
        return self.id

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

    def _pull(self):
        if self.id is None or self.root != self.id_root:
            self.id_root = self.root
            self.id = next(self.root.id_generator)

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
        if name in self.children or node.root == self.root or node.root != node:
            raise TreeError(
                f"Adding node {name} violates the integrity of the pipeline"
            )
        self.children[name] = node

        self.repo.merge(node.repo)

        node.parent = self
        node._root = self.root
        node._name = name

    def __getitem__(self, item):
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
        return {
            **self.user_set,
            **{"__env__" + key: value for key, value in self.env.items()},
            **{
                "id": self,
                "callbacks": [
                    (event, cb.to_literal()) for event, cb in self._callbacks
                ],
                "type": self.__class__.__name__,
                "suppress_errors": getattr(self, "suppress_errors", False),
                "same_container": getattr(
                    self, "same_container", constants.SameContainer.INHERIT
                ),
            },
            **({"doc": self.doc} if self.doc else {}),
            **(
                {"stop_on_error": self.stop_on_error}
                if isinstance(self, Serial)
                else {}
            ),
            **({"command": self.command} if isinstance(self, Exec) else {}),
        }

    def serialize(self, pretty=False):
        def validate_env(node):
            for key, value in node.env.items():
                if not isinstance(key, str):
                    raise TypeError(
                        f"{node} has {type(key).__name__} in env key when str is required"
                    )
                if not isinstance(value, str):
                    raise TypeError(
                        f"{node} has {type(value).__name__} in env value for {key} when str is required"
                    )

        res = {
            "edges": [],
            "nodes": [],
            "images": self.repo.images,
            "token": self.token,
            "autorun": self._autorun,
            "sleep_when_done": self._sleep_when_done,
        }
        queue = collections.deque([self])
        while queue:
            node = queue.popleft()
            validate_env(node)
            node._pull()
            res["nodes"].append(
                {k: v for k, v in node.describe().items() if v is not None}
            )

            for name, child in node.children.items():
                queue.append(child)
                res["edges"].append([node, child, name])

        class NodeEncoder(json.JSONEncoder):
            def default(self, o):
                try:
                    return o._id
                except AttributeError:
                    return o

        if pretty:
            import pprint

            return pprint.pformat(res)
        output = json.dumps(res, cls=NodeEncoder)
        return base64.b64encode(
            gzip.compress(output.encode(), compresslevel=3)
        ).decode()

    @staticmethod
    def deserialize(string):
        string = gzip.decompress(base64.b64decode(string))
        data = json.loads(string)
        nodes = {i["id"]: load_node(**i) for i in data["nodes"]}

        for i in data["nodes"]:
            for event, cb_literal in i.get("callbacks", []):
                cb, cb_args = cb_literal
                kwargs = {
                    k: nodes[cb_args[k]] for k in cb_args.get("__node_args__", [])
                }
                cb = callback.base(cb, **kwargs)
                nodes[i["id"]]._callbacks.append((event, cb))

        for parent, child, name in data["edges"]:
            nodes[parent][name] = nodes[child]

        root = nodes[data["nodes"][0]["id"]]
        root.token = data.get("token")
        root._autorun = data.get("autorun", False)
        root._sleep_when_done = data.get("sleep_when_done", False)
        return root

    # returns a stream in topological order
    def stream(self, reverse=False):
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
        return None

    def launch_local(
        self,
        tags=None,
        title=None,
        use_shell=True,
        retention=7,
        run=False,
        sleep_when_done=False,
        prebuild_images=False,
    ):
        self._build(
            build_mode=constants.BuildMode.LOCAL,
            tags=tags,
            title=title,
            use_shell=use_shell,
            retention=retention,
            run=run,
            sleep_when_done=sleep_when_done,
            prebuild_images=prebuild_images,
        )

    def launch_cloud(
        self,
        tags=None,
        title=None,
        use_shell=True,
        retention=7,
        run=False,
        sleep_when_done=False,
        prebuild_images=False,
    ):
        self._build(
            build_mode=constants.BuildMode.DEPLOY_TO_CLOUD,
            tags=tags,
            title=title,
            use_shell=use_shell,
            retention=retention,
            run=run,
            sleep_when_done=sleep_when_done,
            prebuild_images=prebuild_images,
        )

    def _build(
        self,
        build_mode=constants.BuildMode.LOCAL,
        tags=None,
        title=None,
        use_shell=True,
        prebuild_images=False,
        retention=7,
        run=False,
        sleep_when_done=False,
    ):
        if self.image is None:
            self.image = image_mod.Image()

        if build_mode != constants.BuildMode.LOCAL or prebuild_images:
            image_mod.make_all(
                self, push_to_cloud=build_mode != constants.BuildMode.LOCAL
            )

        self._autorun = run
        self._sleep_when_done = sleep_when_done

        from conducto.internal import build

        return build.build(self, build_mode, tags, title, use_shell, retention)

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


class Exec(Node):
    """
    A node that contains an executable command
    """

    __slots__ = ("command",)

    def __init__(self, command, **kwargs):
        super().__init__(**kwargs)

        # Instance variables
        self.command = command

    def delete_child(self, node):
        raise NotImplementedError("Exec nodes have no children")

    def append_child(self, node):
        raise NotImplementedError("Exec nodes have no children")

    def expanded_command(self, strict=True):
        if "__conducto" in self.command:
            img = self.image

            if not strict and img is None:
                return self.command

            copy_dir = img.copy_dir

            if "//" in self.command and copy_dir is None and img.copy_url:
                copy_dir = (
                    re.search("__conducto_path:(.*?):endpath__", self.command)
                    .group(1)
                    .split("//")[0]
                )

            def repl(match):
                if copy_dir is None:
                    raise ValueError(
                        f"Node must be in Image with .copy_dir or .copy_url set. Node={self}. Image={img.to_dict()}"
                    )
                return os.path.relpath(match.group(1), copy_dir)

            return re.sub("__conducto_path:(.*?):endpath__", repl, self.command)
        else:
            return self.command


class Parallel(Node):
    "A list of commands executed in Parallel - a branch node."
    pass


class Serial(Node):
    """
    A list of commands executed in Serial, kind of like "begin" in Scheme.
    Its children get executed one after another.
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
        same_container=constants.SameContainer.INHERIT,
        image: typing.Union[str, image_mod.Image] = None,
        image_name=None,
        doc=None,
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
            same_container=same_container,
            image=image,
            image_name=image_name,
            doc=doc,
        )
        self.stop_on_error = stop_on_error
