"""
Collection of utility functions useful for launching Conducto's containers that are able
to use the host's Docker socket in order to build images and run other containers. They
must also be able to access the external .conducto directory.
"""

import os
import re
import json
import functools
import subprocess
from subprocess import DEVNULL, PIPE

from conducto.shared import async_utils, client_utils, constants, log, imagepath
import conducto.internal.host_detection as hostdet


def refresh_image(img, verbose=False):
    # If the image is local, there's nothing to refresh.
    if "/" not in img:
        return

    try:
        subprocess.check_call(["docker", "inspect", img], stdout=PIPE, stderr=PIPE)
    except subprocess.CalledProcessError:
        # Image not present so pull may take a while. Show the output of
        # 'docker pull' so user knows to wait
        print("Pulling docker image...")
        print("\033[2m", end="", flush=True)
        try:
            subprocess.run(["docker", "pull", img])
        finally:
            print("\033[0m", end="", flush=True)
    else:
        # Image is present so we pull again to make sure we have the latest code. It
        # should be fast so hide the output.
        if verbose:
            print("Refreshing docker image...", flush=True)
        subprocess.run(["docker", "pull", img], stdout=PIPE, stderr=PIPE)


@functools.lru_cache(None)
def is_windows_by_host_mnt():
    if os.getenv("WINDOWS_HOST"):
        # if this check does not work to detect your windows, you can short-circuit it
        return True

    try:
        kwargs = dict(check=True, stdout=PIPE, stderr=PIPE)
        catwin = "docker run --rm -v /:/mnt/external alpine cat /mnt/external/host_mnt/c/Windows/win.ini"
        proc = subprocess.run(catwin, shell=True, **kwargs)
        return len(proc.stderr) == 0 and len(proc.stdout) > 0
    except subprocess.CalledProcessError:
        return False


@functools.lru_cache(None)
def docker_available_drives():
    try:
        kwargs = dict(check=True, stdout=PIPE, stderr=PIPE)
        lsdrives = "docker run --rm -v /:/mnt/external alpine ls /mnt/external/host_mnt"
        proc = subprocess.run(lsdrives, shell=True, **kwargs)
        return proc.stdout.decode("utf8").split()
    except subprocess.CalledProcessError:
        import string
        from ctypes import windll  # Windows only

        # get all drives
        bitmask = windll.kernel32.GetLogicalDrives()
        drives = []
        for letter in string.ascii_uppercase:
            if bitmask & 1:
                drives.append(letter)
            bitmask >>= 1

        # filter to fixed drives
        is_fixed = lambda x: windll.kernel32.GetDriveTypeW(f"{x}:\\") == 3
        drives = [d for d in drives if is_fixed(d)]
        return [d.lower() for d in drives]


@functools.lru_cache(None)
def is_docker_desktop():
    subp = subprocess.Popen(
        "docker info --format '{{json .}}'", shell=True, stdout=PIPE, stderr=DEVNULL,
    )
    info_txt, err = subp.communicate()

    info = json.loads(info_txt)
    return info["OperatingSystem"] == "Docker Desktop"


@functools.lru_cache(None)
def get_current_container_mounts():
    subp = subprocess.Popen(
        f"docker inspect -f '{{{{ json . }}}}' {get_current_container_id()}",
        shell=True,
        stdout=PIPE,
        stderr=DEVNULL,
    )
    mount_data, err = subp.communicate()
    log.debug(f"Got {mount_data} {err}")
    if subp.returncode == 0:
        inspect = json.loads(mount_data)

        mount_list = inspect["Mounts"]

        labels = inspect["Config"]["Labels"]

        if "desktop.docker.io/wsl-distro" in labels:
            # This is WSL2 integrated distro; the actual mounts in the mount
            # list are of the form [1] which appears fairly useless for our
            # purposes.  However the list HostConfig.Binds has a different more
            # useful view with a list of strings of format
            # <Source>:<Destination>.
            # [1] /run/desktop/mnt/host/wsl/docker-desktop-bind-mounts/<distro>/<hexstring>

            re_bind = "^desktop.docker.io/(binds|mounts)/([0-9]+)/Source$"
            has_bind_labels = any(re.match(re_bind, ll) for ll in labels.keys())

            if has_bind_labels:
                # Docker Desktop 2.4
                log.debug("determining host paths via docker 2.4 mechanism")

                # The old understanding of the good news is that docker-desktop
                # includes a bunch of labels on the image to fill the need.
                mount_list = []
                for key in labels.keys():
                    m = re.match(
                        "^desktop.docker.io/(binds|mounts)/([0-9]+)/Source$", key
                    )
                    if m:
                        src = key
                        # regex match guarantees this replace is only at the end
                        dest = key.replace("Source", "Target")
                        mount_list.append(
                            {"Source": labels[src], "Destination": labels[dest]}
                        )
            else:
                # Docker Desktop 2.5
                log.debug("determining host paths via docker 2.5 mechanism")

                bindlist = inspect["HostConfig"]["Binds"]
                bindlist = [bl.rsplit(":") for bl in bindlist]
                bindmap = {bl[1].rstrip("/"): bl[0].rstrip("/") for bl in bindlist}
                log.debug(bindmap)

                for mount in mount_list:
                    if mount["Destination"] in bindmap:
                        mount["Source"] = bindmap[mount["Destination"]]
        elif is_docker_desktop():
            # if windows/mac (aka "Docker Desktop"), remove any leading '/host_mnt'
            # docker VM paths
            for mount in mount_list:
                if mount["Source"].startswith("/host_mnt/"):
                    mount["Source"] = mount["Source"].replace("/host_mnt", "", 1)

        return mount_list
    else:
        raise RuntimeError(
            r"error {subp.returncode} while getting current container mounts"
        )


def get_whole_host_mounting_flags(from_container):
    """
    Mount whole system read-only to enable rebuilding images as needed
    """
    if hostdet.is_wsl1() or hostdet.is_windows() or is_windows_by_host_mnt():
        if from_container:
            mounts = get_current_container_mounts()

            drives = []
            for mount in mounts:
                firstseg = mount["Source"].strip("/").split("/")[0]
                if len(firstseg) == 1:
                    drives.append(firstseg)

            if len(drives) == 0:
                # probably didn't mount
                if is_windows_by_host_mnt():
                    drives = docker_available_drives()
        else:
            drives = docker_available_drives()

        output = ["-e", "WINDOWS_HOST=plain"]

        for d in drives:
            # Mount whole system read-only to enable rebuilding images as needed
            mount = f"type=bind,source=/{d}/,target={constants.ConductoPaths.MOUNT_LOCATION}/{d.lower()},readonly"
            output += ["--mount", mount]
        return output
    else:
        # Mount whole system read-only to enable rebuilding images as needed
        mount = f"type=bind,source=/,target={constants.ConductoPaths.MOUNT_LOCATION},readonly"
        return ["--mount", mount]


def get_external_conducto_dir(from_container):
    # Remote base dir will be verified by container.
    result = constants.ConductoPaths.get_profile_base_dir()

    if from_container:
        # Mount to the ~/.conducto of the host machine and not of the container
        mounts = get_current_container_mounts()

        for mount in mounts:
            if result.startswith(mount["Destination"]):
                result = result.replace(mount["Destination"], mount["Source"], 1)
                log.debug(f"Mounting to {result}")
                break
    else:
        result = imagepath.Path.from_localhost(result)
        result = result.to_docker_mount()

    return result


def get_docker_dir_mount_flags():
    # WSL doesn't persist this into containers natively
    # Have to have this configured so that we can use host docker creds to pull containers
    docker_basedir = constants.ConductoPaths.get_local_docker_config_dir()
    if docker_basedir:
        return ["-v", f"{docker_basedir}:/root/.docker"]
    else:
        return []


def get_running_containers():
    args = ["docker", "container", "ls", "--format", "{{.Names}}"]
    result = client_utils.subprocess_run(args, capture_output=True)
    return result.stdout.decode().splitlines()


def get_current_container_id():
    """
    Return the full container ID of the Docker container we're in, or the empty string
    if we're not in a container
    """
    if hostdet.is_windows() or hostdet.is_mac():
        # This function assumes we never launch agents or managers from a
        # container running Windows or macOS inside.
        return ""

    # If, and only if, we find docker as a cgroup owner, parse the
    # container id from the ownership list.
    try:
        # Inside a container will look something like this:
        # 1:name=systemd:/docker/8780f2179bc7cbff4cc0972db8cffc2acf107ad06a11820c74f0165f21b98cbf
        # Outside, something like this:
        # 1:name=systemd:/init.scope
        with open("/proc/1/cgroup") as cgroup:
            # Different distros have different resource group orders, not all of which are
            # expected to be owned by docker.
            docker_lines = [
                line for line in cgroup.read().splitlines() if "docker" in line
            ]
            if len(docker_lines) > 0:
                # The docker container ID is the last token in the resource line
                return docker_lines[0].split("/")[-1].strip()
            # Docker doesn't own any resources, so we must not be in a container
            return ""
    # Allow errors to bubble up, except if we don't find the cgroups for /proc/1.
    # In that case, we can't be in a container.
    except FileNotFoundError:
        return ""


async def does_newer_version_exist():
    container_id = get_current_container_id()

    # Get the SHA256 of the current image, and the name it was run with
    out, _err = await async_utils.run_and_check(
        "docker",
        "container",
        "inspect",
        "--format",
        "{{.Image}} {{.Config.Image}}",
        container_id,
    )
    image_sha, image_name = out.decode().strip().split()

    # Pull the latest version of image_name, if needed
    if "/" in image_name:
        try:
            await async_utils.run_and_check("docker", "image", "pull", image_name)
        except client_utils.CalledProcessError as e:
            log.warn(e.stderr.decode().strip())

    # Get the current SHA of the image
    out, _err = await async_utils.run_and_check(
        "docker", "image", "inspect", "--format", "{{.Id}}", image_name
    )
    current_sha = out.decode().strip()

    # Return True if there's a difference
    return current_sha != image_sha
