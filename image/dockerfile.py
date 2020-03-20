import functools
import packaging.version
import re
import subprocess

from conducto.shared import client_utils
from .. import api


def lines_for_build_dockerfile(image, reqs_py, context_url, context_branch):
    yield f"FROM {image}"
    if reqs_py:
        py_binary, _py_version = get_python_version(image)
        if py_binary is None:
            raise Exception(
                f"Cannot find suitable python in {image} for installing {reqs_py}"
            )

        if api.Config().get("dev", "who"):
            non_conducto_reqs_py = [r for r in reqs_py if r != "conducto"]
            if non_conducto_reqs_py:
                yield "RUN pip install " + " ".join(non_conducto_reqs_py)

            if "conducto" in reqs_py:
                image = "conducto"
                tag = api.Config().get_image_tag(default=None)
                if tag is not None:
                    image = f"{image}:{tag}"
                yield f"COPY --from={image} /tmp/conducto /tmp/conducto"
                yield "RUN pip install -e /tmp/conducto"
        else:
            yield "RUN pip install " + " ".join(reqs_py)

    code_dir = "/usr/conducto"
    yield f"WORKDIR {code_dir}"

    if context_url:
        yield f"ADD http://worldtimeapi.org/api/ip /tmp/time"  # cache busting
        yield f"RUN git clone --single-branch --branch {context_branch} {context_url} {code_dir}"
    else:
        yield f"COPY . {code_dir}"


def lines_for_extend_dockerfile(user_image):
    yield f"FROM {user_image}"

    # TODO: Use Docker commands instead of RUN where possible.
    linux_flavor, linux_version = _get_linux_flavor_and_version(user_image)

    # Determine python version and binary.
    acceptable_binary, pyvers = get_python_version(user_image)

    default_python = None
    if pyvers is not None:
        which_python = client_utils.subprocess_run(
            ["docker", "run", "--rm", user_image, "which", acceptable_binary]
        )
        default_python = which_python.stdout.decode("utf8").strip()

    if _is_debian(linux_flavor):
        if linux_version == "10":  # buster
            suffix = "slim-buster"
        elif linux_version == "9":  # stretch
            suffix = "slim-stretch"
        else:
            raise UnsupportedPythonException(f"Unsupported Python version {pyvers}")
        if pyvers is None:
            yield "RUN apt-get update"
            yield f"RUN apt-get install -y python3.7-dev"
            pyvers = (3, 7)
            default_python = "/usr/bin/python3.7"
    elif _is_alpine(linux_flavor):
        suffix = "alpine"
        if pyvers is None:
            yield "RUN apk update"
            yield f"RUN apk add --update python3-dev"
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
    source_image = f"{image}:{tag}"
    yield f"COPY --from={source_image} /opt/conducto_venv /opt/conducto_venv"
    yield f"RUN ln -sf {default_python} /opt/conducto_venv/bin/python3"


class LowPException(Exception):
    pass


class UnsupportedLinuxException(Exception):
    pass


class UnsupportedPythonException(Exception):
    pass


@functools.lru_cache()
def get_python_version(user_image):
    for binary in [
        "python",
        "python3",
        "python3.5",
        "python3.6",
        "python3.7",
        "python3.8",
    ]:
        try:
            version = _get_python_version(user_image, binary)
        except (
            subprocess.CalledProcessError,
            LowPException,
            packaging.version.InvalidVersion,
        ):
            pass
        else:
            return binary, version.release[0:2]
    return None, None


def _get_python_version(user_image, python_binary) -> packaging.version.Version:
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
    python_version = re.sub(
        r"^Python\s+",
        r"",
        subprocess.check_output(
            ["docker", "run", "--rm", user_image, python_binary, "--version"],
            stderr=subprocess.PIPE,
        ).decode("utf8"),
        re.IGNORECASE,
    ).strip()
    python_version = packaging.version.Version(python_version)
    if python_version < packaging.version.Version("3.5"):
        raise LowPException("")
    return python_version


def _get_linux_flavor_and_version(user_image):
    output = (
        subprocess.check_output(
            f'docker run --rm {user_image} sh -c "cat /etc/*-release" | '
            f'grep -e "^ID=" -e "^VERSION_ID="',
            shell=True,
            stderr=subprocess.PIPE,
        )
        .decode("utf8")
        .strip()
    )

    flavor = re.search(r"^ID=(.*)", output, re.M)
    flavor = flavor.groups()[0].strip().strip('"')

    version = re.search(r"^VERSION_ID=(.*)", output, re.M)
    version = version.groups()[0].strip().strip('"')

    return flavor, version


def _is_debian(linux_flavor):
    return re.search(r".*(ubuntu|debian)", linux_flavor)


def _is_alpine(linux_flavor):
    return re.search(r".*alpine", linux_flavor)
