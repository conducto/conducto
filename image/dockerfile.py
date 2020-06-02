import json
import packaging.version
import re
import subprocess
import os

from .. import api
from .._version import __version__

COPY_DIR = "/mnt/conducto"


async def text_for_build_dockerfile(
    image, reqs_py, copy_dir, copy_url, copy_branch, docker_auto_workdir, exec_
):
    lines = [f"FROM {image}"]

    if reqs_py:
        py_binary, _py_version, pip_binary = await get_python_version(image, exec_)

        if reqs_py and not pip_binary:
            # install pip as per distro
            (
                linux_flavor,
                linux_version,
                linux_name,
            ) = await _get_linux_flavor_and_version(image, exec_)

            uid = await _get_uid(image, exec_)

            lines.append("USER 0")
            if _is_debian(linux_flavor):
                lines.append("RUN apt-get update")
                # this will install python too
                lines.append(f"RUN apt-get install -y python3-pip")
                py_binary = "python3"
            elif _is_alpine(linux_flavor):
                lines.append("RUN apk update")
                # this will install pip3 as well
                lines.append(f"RUN apk add --update python3")
                py_binary = "python3"
            elif _is_centos(linux_flavor):
                lines.append("RUN yum update -y")
                lines.append("RUN yum install -y python36")
                py_binary = "python3"
            elif _is_fedora(linux_flavor):
                lines.append("RUN dnf update -y")
                lines.append("RUN dnf install -y python38")
                py_binary = "python3"
            lines.append(f"USER {uid}")

        if py_binary is None:
            raise Exception(
                f"Cannot find suitable python in {image} for installing {reqs_py}"
            )

        non_conducto_reqs_py = [r for r in reqs_py if r != "conducto"]
        if non_conducto_reqs_py:
            lines.append(
                f"RUN {py_binary} -m pip install " + " ".join(non_conducto_reqs_py)
            )

        if "conducto" in reqs_py:
            tag = api.Config().get_image_tag()
            if tag:
                image = "conducto"
                registry = os.environ.get("CONDUCTO_DEV_REGISTRY")
                if registry:
                    image = f"{registry}/{image}"
                image = f"{image}:{tag}"
                lines.append(f"COPY --from={image} /tmp/conducto /tmp/conducto")
                lines.append(f"RUN {py_binary} -m pip install -e /tmp/conducto")
            else:
                lines.append(f"RUN {py_binary} -m pip install conducto=={__version__}")

    if copy_dir or copy_url:
        if docker_auto_workdir:
            lines.append(f"WORKDIR {COPY_DIR}")

        if copy_url:
            lines.append("ARG CONDUCTO_CACHE_BUSTER")
            lines.append(f"RUN echo $CONDUCTO_CACHE_BUSTER")
            lines.append(
                f"RUN git clone --single-branch --branch {copy_branch} {copy_url} {COPY_DIR}"
            )
        else:
            lines.append(f"COPY . {COPY_DIR}")

    return "\n".join(lines)


async def text_for_extend_dockerfile(user_image, exec_):
    lines = [f"FROM {user_image}"]

    linux_flavor, linux_version, linux_name = await _get_linux_flavor_and_version(
        user_image, exec_
    )

    # Determine python version and binary.
    acceptable_binary, pyvers, _pip_binary = await get_python_version(user_image, exec_)

    default_python = None
    if pyvers is not None:
        if _is_fedora(linux_flavor) or _is_centos(linux_flavor):
            # The base Fedora and CentOS images don't have 'which' for some reason. A
            # workaround is to use python to print sys.executable.
            which_python, _ = await exec_(
                "docker",
                "run",
                "--rm",
                user_image,
                acceptable_binary,
                "-c",
                "import sys; print(sys.executable)",
            )
            default_python = which_python.decode("utf8").strip()
        else:
            which_python, _ = await exec_(
                "docker", "run", "--rm", user_image, "which", acceptable_binary
            )
            default_python = which_python.decode("utf8").strip()

    uid = await _get_uid(user_image, exec_)
    lines.append("USER 0")

    if _is_debian(linux_flavor):
        if linux_version == "10" or "buster" in linux_name:  # buster
            suffix = "slim-buster"
        elif linux_version == "9" or "stretch" in linux_name:  # stretch
            suffix = "slim-stretch"
        else:
            raise UnsupportedPythonException(
                f"Unsupported Python version {pyvers} for "
                f"linux_flavor={linux_flavor}, "
                f"linux_version={linux_version}, "
                f"linux_name={linux_name}"
            )
        if pyvers is None:
            lines.append("RUN apt-get update")
            lines.append(f"RUN apt-get install -y python3.7")
            pyvers = (3, 7)
            default_python = "/usr/bin/python3.7"
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
    lines.append(f"USER {uid}")
    return "\n".join(lines), worker_image


class LowPException(Exception):
    pass


class UnsupportedLinuxException(Exception):
    pass


class UnsupportedPythonException(Exception):
    pass


# Note: we don't need caching here except on get_python_version
# because we already have caching mechanisms above
# each image gets their .build called once, and everything below _get_python_version
# is called just once per name_complete


async def get_python_version(user_image, exec_):
    cache = get_python_version._cache = getattr(get_python_version, "cache", {})
    if user_image in cache:
        return cache[user_image]

    pyresults = [None, None]

    for binary in [
        "python",
        "python3",
        "python3.5",
        "python3.6",
        "python3.7",
        "python3.8",
    ]:
        try:
            version = await _get_python_version(user_image, binary, exec_)
            pyresults = [binary, version.release[0:2]]
            break
        except (
            subprocess.CalledProcessError,
            LowPException,
            packaging.version.InvalidVersion,
        ):
            pass

    pipresults = None
    # if no python, there is no point in looking for pip
    if pyresults[0]:
        for binary in ["pip", "pip3"]:
            try:
                # we are only interested here in known the binary
                # name that responds with-out shell error
                await _get_pip_version(user_image, binary, exec_)
                pipresults = binary
                break
            except subprocess.CalledProcessError:
                pass

    result = pyresults[0], pyresults[1], pipresults
    cache[user_image] = result
    return result


async def _get_python_version(
    user_image, python_binary, exec_
) -> packaging.version.Version:
    """Gets python version within a Docker image.
    Args:
        user_image: image name
        python_binary: Python binary (e.g. python3)
    Returns:
        packaging.version.Version object
    Raises:
        supprocess.CalledProcessError if binary doesn't exist
        LowPException if version is too low
    """

    out, err = await exec_(
        "docker",
        "run",
        "--rm",
        user_image,
        python_binary,
        "--version",
        stop_on_error=False,
    )
    out = out.decode("utf-8")

    python_version = re.sub(r"^Python\s+", r"", out, re.IGNORECASE,).strip()
    python_version = packaging.version.Version(python_version)
    if python_version < packaging.version.Version("3.5"):
        raise LowPException("")
    return python_version


async def _get_pip_version(user_image, pip_binary, exec_) -> packaging.version.Version:
    """Gets pip version within a Docker image.
    Args:
        user_image: image name
        pip_binary: Python binary (e.g. pip3)
    Returns:
        packaging.version.Version object
    Raises:
        supprocess.CalledProcessError if binary doesn't exist
    """

    out, err = await exec_(
        "docker", "run", "--rm", user_image, pip_binary, "--version",
    )

    # we don't really care about the pip version, but I retain it here for
    # similarity with the python version of this function.
    pip_version = re.search(r"^pip ([0-9.]+)", out.decode("utf-8"))
    pip_version = packaging.version.Version(pip_version.group(1))
    return pip_version


async def _get_linux_flavor_and_version(user_image, exec_):
    out, err = await exec_(
        "docker", "run", "--rm", user_image, "sh", "-c", "cat /etc/*-release",
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


async def _get_uid(image_name, exec_):
    out, _err = await exec_("docker", "inspect", "--format", "{{json .}}", image_name)
    d = json.loads(out)
    uid_str = d["Config"]["User"]
    if uid_str == "":
        uid = 0
    else:
        uid = int(uid_str)
    return uid


def _is_debian(linux_flavor):
    return re.search(r".*(ubuntu|debian)", linux_flavor)


def _is_alpine(linux_flavor):
    return re.search(r".*alpine", linux_flavor)


def _is_centos(linux_flavor):
    return re.search(r".*(rhel|centos)", linux_flavor)


def _is_fedora(linux_flavor):
    return re.search(r".*fedora", linux_flavor)
