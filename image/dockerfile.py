import json
import os
import packaging.version
import re
import shlex
import subprocess
import typing

from .. import api
from .._version import __version__
from ..shared import async_utils, constants


_ROOT_USER_STRS = {"", "0", "root"}


async def text_for_install(image, reqs_py, reqs_packages, reqs_docker, reqs_npm):
    lines = [f"FROM {image}"]

    linux_flavor, linux_version, linux_name = None, None, None

    if linux_flavor is None:
        (
            linux_flavor,
            linux_version,
            linux_name,
        ) = await _get_linux_flavor_and_version(image)

    def lines_append_once(cmd):
        nonlocal lines

        if cmd not in lines:
            lines.append(cmd)

    # All installs should be done as root, so set uid=0 if needed
    user_str = await _get_user_str(image)
    if user_str not in _ROOT_USER_STRS:
        lines.append("USER 0")

    # Install any packages the user requests
    if reqs_packages:
        package_str = " ".join(shlex.quote(p) for p in reqs_packages)
        if _is_debian(linux_flavor):
            lines_append_once("RUN apt-get update")
            lines.append(f"RUN apt-get install -y {package_str}")
        elif _is_alpine(linux_flavor):
            lines_append_once("RUN apk update")
            lines.append(f"RUN apk add --update {package_str}")
        elif _is_centos(linux_flavor):
            lines_append_once("RUN yum update -y")
            lines.append(f"RUN yum install -y {package_str}")
        elif _is_fedora(linux_flavor):
            lines_append_once("RUN dnf update -y")
            lines.append(f"RUN dnf install -y {package_str}")
        else:
            raise ValueError(
                f"Don't know how to install packages for linux_flavor={repr(linux_flavor)}"
            )

    # Install docker if needed
    if reqs_docker:
        lines.append("RUN curl -sSL https://get.docker.com/ | sh")
        if _is_debian(linux_flavor):
            lines_append_once("RUN apt-get update")
            lines.append("RUN apt-get install -y openssh-server")
        elif _is_alpine(linux_flavor):
            lines.append("RUN apk add openssh")
        elif _is_centos(linux_flavor):
            lines.append("RUN yum install -y openssh-clients")
        elif _is_fedora(linux_flavor):
            lines.append("RUN dnf install -y openssh-clients")
        else:
            raise ValueError(
                f"Don't know how to install ssh for linux_flavor={repr(linux_flavor)}"
            )

    # Install python packages with pip
    if reqs_py:
        py_binary, _py_version, pip_binary = await get_python_version(image)
        if not pip_binary:
            # install pip as per distro
            if _is_debian(linux_flavor):
                lines_append_once("RUN apt-get update")
                # this will install python too
                lines.append("RUN apt-get install -y python3-pip")
                py_binary = "python3"
            elif _is_alpine(linux_flavor):
                lines_append_once("RUN apk update")
                # this will install pip3 as well
                lines.append("RUN apk add --update python3 py3-pip")
                py_binary = "python3"
            elif _is_centos(linux_flavor):
                lines_append_once("RUN yum update -y")
                lines.append("RUN yum install -y python36")
                py_binary = "python3"
            elif _is_fedora(linux_flavor):
                lines_append_once("RUN dnf update -y")
                lines.append("RUN dnf install -y python38")
                py_binary = "python3"
            else:
                raise ValueError(
                    f"Don't know how to install pip for linux_flavor={repr(linux_flavor)}"
                )

        if py_binary is None:
            raise Exception(
                f"Cannot find suitable python in {image} for installing {reqs_py}"
            )

        # Upgrade pip to use the "--use-feature" input
        lines.append(f"RUN {py_binary} -m pip install --upgrade pip")

        non_conducto_reqs_py = [r for r in reqs_py if r != "conducto"]
        if non_conducto_reqs_py:
            # Don't use the 2020 resolver with non conducto reqs
            lines.append(
                f"RUN {py_binary} -m pip install " + " ".join(non_conducto_reqs_py)
            )

        if "conducto" in reqs_py:
            tag = api.Config().get_image_tag()
            if tag:
                conducto_image = "conducto"
                registry = os.environ.get("CONDUCTO_DEV_REGISTRY")
                if registry:
                    conducto_image = f"{registry}/{conducto_image}"
                conducto_image = f"{conducto_image}:{tag}"
                lines.append(
                    f"COPY --from={conducto_image} /tmp/conducto /tmp/conducto"
                )
                # Use the 2020 resolver with conducto reqs
                lines.append(f"RUN {py_binary} -m pip install -e /tmp/conducto")
            else:
                # Use the 2020 resolver with conducto reqs
                lines.append(f"RUN {py_binary} -m pip install conducto=={__version__}")

    # Install npm packages
    if reqs_npm:
        # TODO: check for and install node
        lines.append(f"RUN mkdir -p {constants.ConductoPaths.COPY_LOCATION}")
        if reqs_npm is True:
            lines.append(f"RUN cd {constants.ConductoPaths.COPY_LOCATION} && npm i")
        else:
            non_conducto_reqs_npm = [i for i in reqs_npm if i != "conducto"]
            if "conducto" in reqs_npm and "esm" not in reqs_npm:
                non_conducto_reqs_npm.append("esm")

            lines.append(
                f"RUN cd {constants.ConductoPaths.COPY_LOCATION} && npm i "
                + " ".join(i for i in reqs_npm if i != "conducto")
            )

    if type(reqs_npm) == list and "conducto" in reqs_npm:
        tag = api.Config().get_image_tag()
        if tag:
            conducto_image = "conductojs"
            registry = os.environ.get("CONDUCTO_DEV_REGISTRY")
            if registry:
                conducto_image = f"{registry}/{conducto_image}"
            conducto_image = f"{conducto_image}:{tag}"
            lines.append(
                f"COPY --from={conducto_image} /tmp/conductojs /tmp/conductojs"
            )
            lines.append(f"RUN cd /tmp/conductojs && npm i")

            lines.append(
                f"RUN cd {constants.ConductoPaths.COPY_LOCATION} && npm i /tmp/conductojs"
            )
        else:
            # need to make an actual node package
            lines.append(f"RUN npm i conducto")

    # Reset the user to its original value, if needed
    if user_str not in _ROOT_USER_STRS:
        lines.append(f"USER {user_str}")

    return "\n".join(lines)


def text_for_copy(image, docker_auto_workdir, env_vars):
    lines = [
        f"FROM {image}",
        f"COPY . {constants.ConductoPaths.COPY_LOCATION}",
    ]

    if docker_auto_workdir:
        lines.append(f"WORKDIR {constants.ConductoPaths.COPY_LOCATION}")

    if env_vars:
        env_str = " ".join(_escape(f"{k}={v}") for k, v in env_vars.items())
        lines.append(f"ENV {env_str}")
    return "\n".join(lines)


def dockerignore_for_copy(context, preserve_git):
    path = os.path.join(context, ".dockerignore")
    if not os.path.exists(path):
        # TODO: read the .gitignore and convert to a .dockerignore
        # https://zzz.buzz/2018/05/23/differences-of-rules-between-gitignore-and-dockerignore/
        return ""

    # Read the existing dockerfile
    with open(path, "r") as f:
        text = f.read()

    # Add an anti-ignore line to the end of the file if we need to preserve Git
    if preserve_git:
        text = text.strip() + "\n" + "!**/.git"

    return text


async def text_for_extend_rust(user_image, labels):
    # Copy the conducto-worker files at the end to preserve the caching of the
    # python install
    tag = "musl-x64"
    image = "conducto/worker"
    config = api.Config()
    dev_tag = config.get_image_tag()
    if dev_tag is not None:
        image = f"worker-dev"
        tag += f"-{dev_tag}"
        registry = os.environ.get("CONDUCTO_DEV_REGISTRY")
        if registry:
            image = f"{registry}/{image}"
    elif os.environ.get("CONDUCTO_USE_TEST_IMAGES"):
        tag += "-test"
    worker_image = f"{image}:{tag}"

    lines = [f"FROM {user_image}"]
    lines.append(f"COPY --from={worker_image} /conducto-worker /")
    return "\n".join(lines), worker_image


async def text_for_extend(user_image, labels):
    lines = [f"FROM {user_image}"]

    linux_flavor, linux_version, linux_name = await _get_linux_flavor_and_version(
        user_image
    )

    # Determine python version and binary.
    acceptable_binary, pyvers, _pip_binary = await get_python_version(user_image)

    default_python = None
    if pyvers is not None:
        # Use `type` instead of `which` because the base Fedora and CentOS images don't
        # have `/usr/bin/which` while `type` is a widely supported shell builtin.
        out, err = await async_utils.run_and_check(
            "docker",
            "run",
            "--rm",
            "--entrypoint",
            "sh",
            user_image,
            "-c",
            f"type {acceptable_binary}",
        )
        line = out.decode().splitlines()[0]
        _, default_python = _parse_type_command(line)

    user_str = await _get_user_str(user_image)
    if user_str not in _ROOT_USER_STRS:
        lines.append("USER 0")

    if _is_debian(linux_flavor):
        if linux_version == "10" or "buster" in linux_name:  # buster
            suffix = "slim-buster"
        elif linux_version == "9" or "stretch" in linux_name:  # stretch
            suffix = "slim-stretch"
        else:
            raise UnsupportedPythonException(
                "Cannot figure out how to install Conducto toolchain. "
                f"Unsupported Python version {pyvers} for "
                f"linux_flavor={linux_flavor}, "
                f"linux_version={linux_version}, "
                f"linux_name={linux_name}"
            )
        if pyvers is None:
            lines.append("RUN apt-get update")
            lines.append(f"RUN apt-get install -y python3")
            install_command = "apt-get update && apt-get install -y python3"
            _default_python, pyvers, _pip_binary = await get_python_version(
                user_image, install_command
            )
            default_python = "/usr/bin/python3"
    elif _is_alpine(linux_flavor):
        suffix = "alpine"
        if pyvers is None:
            lines.append("RUN apk update")
            lines.append(f"RUN apk add --update python3")
            pyvers = (3, 8)
            default_python = "/usr/bin/python3"
    elif _is_centos(linux_flavor):
        suffix = "centos"
        if pyvers is None:
            lines.append("RUN yum install -y python36")
            pyvers = (3, 6)
            default_python = "/usr/bin/python3"
    elif _is_fedora(linux_flavor):
        suffix = "fedora"
        if pyvers is None:
            lines.append("RUN dnf install -y python3.7")
            pyvers = (3, 7)
            default_python = "/usr/bin/python3"
    else:
        raise UnsupportedLinuxException(f"Unsupported Linux version {linux_flavor}.")

    # Copy the conducto-worker files at the end to preserve the caching of the
    # python install
    tag = f"{pyvers[0]}.{pyvers[1]}-{suffix}"
    image = "conducto/worker"
    config = api.Config()
    dev_tag = config.get_image_tag()
    if dev_tag is not None:
        image = f"worker-dev"
        tag += f"-{dev_tag}"
        registry = os.environ.get("CONDUCTO_DEV_REGISTRY")
        if registry:
            image = f"{registry}/{image}"
    elif os.environ.get("CONDUCTO_USE_TEST_IMAGES"):
        tag += "-test"
    worker_image = f"{image}:{tag}"

    # Write to /tmp instead of /opt because if you don't have root access inside the
    # container, /tmp is usually still writable whereas /opt may not be.
    lines.append(f"COPY --from={worker_image} /opt/conducto_venv /tmp/conducto_venv")
    lines.append(f"RUN ln -sf {default_python} /tmp/conducto_venv/bin/python3")
    lines.append(
        "RUN /tmp/conducto_venv/bin/python3 -m conducto_worker --version  "
        "# Image building should fail if conducto_worker can't be imported"
    )
    if user_str not in _ROOT_USER_STRS:
        lines.append(f"USER {user_str}")

    # Record the old entrypoint and clear it in the new image. For more info, see:
    # https://docs.docker.com/engine/reference/builder/#entrypoint
    entrypoint = await _get_entrypoint(user_image)
    if entrypoint:
        lines.append("ENTRYPOINT []")
        if isinstance(entrypoint, list):
            entrypoint = " ".join(shlex.quote(s) for s in entrypoint)
        lines.append(f"ENV ENTRYPOINT={shlex.quote(entrypoint)}")

    lines.append(f"LABEL {' '.join(labels)}")

    return "\n".join(lines), worker_image


class LowPException(Exception):
    pass


class UnsupportedLinuxException(Exception):
    pass


class UnsupportedPythonException(Exception):
    pass


class UnsupportedShellException(Exception):
    pass


class UnsupportedTypeOutputException(Exception):
    pass


# Note: we don't need caching here except on get_python_version and get_shell
# because we already have caching mechanisms above
# each image gets their .build called once, and everything below _get_python_info
# is called just once per name_copied
async def get_shell(user_image):
    cache = get_shell._cache = getattr(get_shell, "cache", {})
    if user_image in cache:
        return cache[user_image]

    shells = ["bash", "ash", "zsh", "sh"]
    subcommands = [f"(type {s} 2>/dev/null)" for s in shells]
    command = " || ".join(cmd for cmd in subcommands)
    try:
        out, _err = await async_utils.run_and_check(
            "docker", "run", "--rm", "--entrypoint", "sh", user_image, "-c", command
        )
    except subprocess.CalledProcessError:
        pass
    else:
        for line in out.decode().splitlines():
            sh, location = _parse_type_command(line)
            if location is not None:
                cache[user_image] = location
                return location

    raise UnsupportedShellException(
        f"Could not auto-determine correct shell to use. Tried {shells} but none "
        f"were available."
    )


async def get_python_version(user_image, install_command=None):
    cache = get_python_version._cache = getattr(get_python_version, "cache", {})
    cache_key = (user_image, install_command)
    if cache_key in cache:
        return cache[cache_key]

    result = await _get_python_info(user_image, install_command)

    cache[cache_key] = result
    return result


async def _get_python_info(user_image, install_command):
    """
    Gets python version within a Docker image.
    Args:
        user_image: image name
        python_binary: Python binary (e.g. python3)
    Returns:
        packaging.version.Version object
    Raises:
        subprocess.CalledProcessError if binary doesn't exist
        LowPException if version is too low
    """

    # Construct a single command to check all the python versions and pip
    # version in one shot. Calling `docker run` has a nontrivial startup time,
    # so batching these commands noticeably reduces the latency of building an
    # Image. Order these from most recent python version to least recent,
    # because we prefer newer versions.
    py_binaries = [
        "python3.8",
        "python3.7",
        "python3.6",
        "python3.5",
        "python3",
        "python",  # Put this last because it can get python 2
    ]
    py_subcommands = []
    for b in py_binaries:
        py_subcommands.append(f"({b} --version 2>/dev/null && echo {b})")
    py_command = " || ".join(py_subcommands)

    pip_command = "type pip3|| type pip"

    sep = "__conducto_start__"

    command = f"echo {sep} && (({py_command}) && ({pip_command}))"
    if install_command is not None:
        command = f"{install_command} && {command}"

    docker_command = [
        "docker",
        "run",
        "--rm",
        "--entrypoint",
        "sh",
        user_image,
        "-c",
        command,
    ]
    out, err = await async_utils.run_and_check(*docker_command, stop_on_error=False)
    out = out.decode("utf-8").strip()
    out = out.split(sep, 1)[-1].strip()

    if not out:
        return None, None, None

    lines = out.splitlines()
    if len(lines) not in (2, 3, 4):
        raise UnsupportedPythonException(
            f"Cannot parse output of Python-detection command. output:\n{out}"
        )

    version_line = lines[0].strip()
    version_str = re.sub(r"^.*Python\s+", r"", version_line, re.IGNORECASE, re.DOTALL)
    py_binary = lines[1]

    # Some weird versions cannot be parsed by packaging.version.
    if version_str.endswith("+"):
        version_str = version_str[:-1]
    try:
        py_version = packaging.version.Version(version_str)
    except Exception as e:
        raise Exception(
            f"Cannot get version from version string={version_str}\n"
            f"Original version line={version_line}\n"
            f"Docker command={' '.join(docker_command)}\n"
            f"Exception={e}"
        )
    if py_version < packaging.version.Version("3.5"):
        return None, None, None

    # Parse the `pip` lines. Only need to consider the last line, because the previous
    # ones will have failed.
    if len(lines) > 2:
        pip_binary, _pip_location = _parse_type_command(lines[-1])
        if _pip_location is None:
            pip_binary = None
    else:
        pip_binary = None

    return py_binary, py_version.release[0:2], pip_binary


async def _get_pip_version(user_image, pip_binary) -> packaging.version.Version:
    """Gets pip version within a Docker image.
    Args:
        user_image: image name
        pip_binary: Python binary (e.g. pip3)
    Returns:
        packaging.version.Version object
    Raises:
        subprocess.CalledProcessError if binary doesn't exist
    """

    out, err = await async_utils.run_and_check(
        "docker", "run", "--rm", "--entrypoint", pip_binary, user_image, "--version"
    )

    # we don't really care about the pip version, but I retain it here for
    # similarity with the python version of this function.
    pip_version = re.search(r"^pip ([0-9.]+)", out.decode("utf-8"))
    pip_version = packaging.version.Version(pip_version.group(1))
    return pip_version


async def _get_linux_flavor_and_version(user_image):
    out, err = await async_utils.run_and_check(
        "docker",
        "run",
        "--rm",
        "--entrypoint",
        "sh",
        user_image,
        "-c",
        "cat /etc/*-release",
    )
    out = out.decode("utf-8").strip()

    flavor = None
    version = None
    pretty_name = None

    # Now we filter out only lines that start with ID= & VERSION_ID= for flavor
    # and version respectively.
    outlines = [line.strip() for line in out.split("\n")]
    for line in outlines:
        if line.startswith("ID="):
            flavor = line[len("ID=") :].strip().strip('"')
        if line.startswith("VERSION_ID="):
            version = line[len("VERSION_ID=") :].strip().strip('"')
        if line.startswith("PRETTY_NAME="):
            pretty_name = line[len("PRETTY_NAME=") :].strip().strip('"')

    return flavor, version, pretty_name


async def _get_user_str(image_name):
    out, _err = await async_utils.run_and_check(
        "docker", "inspect", "--format", "{{json .}}", image_name
    )
    d = json.loads(out)
    return d["Config"]["User"]


async def _get_entrypoint(image_name):
    cache = _get_entrypoint._cache = getattr(_get_entrypoint, "cache", {})
    if image_name in cache:
        return cache[image_name]

    out, _err = await async_utils.run_and_check(
        "docker", "inspect", "--format", "{{json .}}", image_name
    )
    d = json.loads(out)
    cache[image_name] = result = d["Config"]["Entrypoint"]
    return result


def _escape(s):
    """
    Return an escaped version of the string *s* for a Dockerfile ENV statement.
    """

    return re.sub(r"([^\w@%+=:,./-])", r"\\\1", s, re.ASCII)


def _is_debian(linux_flavor):
    return re.search(r".*(ubuntu|debian)", linux_flavor)


def _is_alpine(linux_flavor):
    return re.search(r".*alpine", linux_flavor)


def _is_centos(linux_flavor):
    return re.search(r".*(rhel|centos)", linux_flavor)


def _is_fedora(linux_flavor):
    return re.search(r".*fedora", linux_flavor)


def _parse_type_command(line) -> typing.Tuple[str, typing.Optional[str]]:
    """
    Parse the output of the `type` command.

    `type` has a few possible outputs:
    - "python: not found"
    - "python is /usr/bin/python"
    - "echo is a shell builtin"

    There are more, but we only need to parse the first two
    """
    m = re.search("^(.*): not found$", line)
    if m:
        return m.group(1), None

    m = re.search("^(.*?) is (/.*)$", line)
    if m:
        return m.group(1), m.group(2)

    raise UnsupportedTypeOutputException(line)
