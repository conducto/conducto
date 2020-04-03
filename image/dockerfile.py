import asyncio
import functools
import packaging.version
import re
import subprocess

from conducto.shared import async_utils, client_utils
from .. import api
from .._version import __version__

COPY_DIR = "/mnt/context"


async def text_for_build_dockerfile(image, reqs_py, copy_url, copy_branch):
    lines = [f"FROM {image}"]

    if reqs_py:
        py_binary, _py_version = await get_python_version(image)
        if py_binary is None:
            raise Exception(
                f"Cannot find suitable python in {image} for installing {reqs_py}"
            )

        non_conducto_reqs_py = [r for r in reqs_py if r != "conducto"]
        if non_conducto_reqs_py:
            lines.append("RUN pip install " + " ".join(non_conducto_reqs_py))

        if "conducto" in reqs_py:
            if api.Config().get("dev", "who"):
                image = "conducto"
                tag = api.Config().get_image_tag(default=None)
                if tag is not None:
                    image = f"{image}:{tag}"
                lines.append(f"COPY --from={image} /tmp/conducto /tmp/conducto")
                lines.append(f"RUN {py_binary} -m pip install -e /tmp/conducto")
            else:
                lines.append(f"RUN {py_binary} -m pip install conducto=={__version__}")

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


async def text_for_extend_dockerfile(user_image):
    lines = [f"FROM {user_image}"]

    # TODO: Use Docker commands instead of RUN where possible.
    linux_flavor, linux_version = await _get_linux_flavor_and_version(user_image)

    # Determine python version and binary.
    acceptable_binary, pyvers = await get_python_version(user_image)

    default_python = None
    if pyvers is not None:
        which_python, _ = await async_utils.run_and_check(
            "docker", "run", "--rm", user_image, "which", acceptable_binary
        )
        default_python = which_python.decode("utf8").strip()

    if _is_debian(linux_flavor):
        if linux_version == "10":  # buster
            suffix = "slim-buster"
        elif linux_version == "9":  # stretch
            suffix = "slim-stretch"
        else:
            raise UnsupportedPythonException(f"Unsupported Python version {pyvers}")
        if pyvers is None:
            lines.append("RUN apt-get update")
            lines.append(f"RUN apt-get install -y python3.7-dev")
            pyvers = (3, 7)
            default_python = "/usr/bin/python3.7"
    elif _is_alpine(linux_flavor):
        suffix = "alpine"
        if pyvers is None:
            lines.append("RUN apk update")
            lines.append(f"RUN apk add --update python3-dev")
            pyvers = (3, 8)
            default_python = "/usr/bin/python3"
    else:
        raise UnsupportedLinuxException(f"Unsupported Linux version {linux_flavor}.")

    # Copy the conducto-worker files at the end to preserve the caching of the
    # python install
    tag = f"{pyvers[0]}.{pyvers[1]}-{suffix}"
    image = "conducto/worker"
    dev_tag = api.Config().get_image_tag()
    if dev_tag is not None:
        image = f"worker-dev"
        tag += f"-{dev_tag}"
    worker_image = f"{image}:{tag}"
    lines.append(f"COPY --from={worker_image} /opt/conducto_venv /opt/conducto_venv")
    lines.append(f"RUN ln -sf {default_python} /opt/conducto_venv/bin/python3")
    return "\n".join(lines), worker_image


class LowPException(Exception):
    pass


class UnsupportedLinuxException(Exception):
    pass


class UnsupportedPythonException(Exception):
    pass


@functools.lru_cache()
def pull_conducto_worker(worker_image):
    return asyncio.ensure_future(
        async_utils.run_and_check("docker", "pull", worker_image)
    )


# Note: we don't need caching here except on get_python_version
# because we already have caching mechanisms above
# each image gets their .build called once, and everything below _get_python_version
# is called just once per name_complete


@async_utils.async_cache
async def get_python_version(user_image):

    for binary in [
        "python",
        "python3",
        "python3.5",
        "python3.6",
        "python3.7",
        "python3.8",
    ]:
        try:
            version = await _get_python_version(user_image, binary)
        except (
            subprocess.CalledProcessError,
            LowPException,
            packaging.version.InvalidVersion,
        ):
            pass
        else:
            return binary, version.release[0:2]
    return None, None


async def _get_python_version(user_image, python_binary) -> packaging.version.Version:
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

    proc = await asyncio.create_subprocess_exec(
        "docker",
        "run",
        "--rm",
        user_image,
        python_binary,
        "--version",
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )
    out, err = await proc.communicate()
    out = out.decode("utf-8")

    python_version = re.sub(r"^Python\s+", r"", out, re.IGNORECASE,).strip()
    python_version = packaging.version.Version(python_version)
    if python_version < packaging.version.Version("3.5"):
        raise LowPException("")
    return python_version


async def _get_linux_flavor_and_version(user_image):

    subp = await asyncio.create_subprocess_shell(
        f'docker run --rm {user_image} sh -c "cat /etc/*-release"',
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )
    out, err = await subp.communicate()
    out = out.decode("utf-8").strip()

    flavor = None
    version = None

    # Now we filter out only lines that start with ID= & VERSION_ID= for flavor
    # and version respectively.
    outlines = [line.strip() for line in out.split("\n")]
    for line in outlines:
        if line.startswith("ID="):
            flavor = line[len("ID=") :].strip().strip('"')
        if line.startswith("VERSION_ID="):
            version = line[len("VERSION_ID=") :].strip().strip('"')

    return flavor, version


def _is_debian(linux_flavor):
    return re.search(r".*(ubuntu|debian)", linux_flavor)


def _is_alpine(linux_flavor):
    return re.search(r".*alpine", linux_flavor)
