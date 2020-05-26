"""
Collection of utility functions useful for launching Conducto's containers that are able
to use the host's Docker socket in order to build images and run other containers. They
must also be able to access the external .conducto directory.
"""

import functools
import os
import subprocess

from conducto.shared import async_utils, client_utils, constants, log
import conducto.internal.host_detection as hostdet


@functools.lru_cache(None)
def docker_desktop_23():
    # Docker Desktop
    try:
        kwargs = dict(check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # Docker Desktop 2.2.x
        lsdrives = "docker run --rm -v /:/mnt/external alpine ls /mnt/external/host_mnt"
        subprocess.run(lsdrives, shell=True, **kwargs)
        return False
    except subprocess.CalledProcessError:
        return True


@functools.lru_cache(None)
def docker_available_drives():
    import string

    if hostdet.is_wsl():
        kwargs = dict(check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        drives = []
        for drive in string.ascii_lowercase:
            drivedir = f"{drive}:\\"
            try:
                subprocess.run(f"wslpath -u {drivedir}", shell=True, **kwargs)
                drives.append(drive)
            except subprocess.CalledProcessError:
                pass
    else:
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


def get_whole_host_mounting_flags():
    """
    Mount whole system read-only to enable rebuilding images as needed
    """
    if hostdet.is_wsl() or hostdet.is_windows():
        drives = docker_available_drives()

        output = []
        if docker_desktop_23():
            output += ["-e", "WINDOWS_HOST=host_mnt"]
        else:
            output += ["-e", "WINDOWS_HOST=plain"]

        for d in drives:
            # Mount whole system read-only to enable rebuilding images as needed
            mount = f"type=bind,source={d}:/,target={constants.ConductoPaths.MOUNT_LOCATION}/{d.lower()},readonly"
            output += ["--mount", mount]
            return output
    else:
        # Mount whole system read-only to enable rebuilding images as needed
        mount = f"type=bind,source=/,target={constants.ConductoPaths.MOUNT_LOCATION},readonly"
        return ["--mount", mount]


def get_external_conducto_dir(is_migration):
    # Remote base dir will be verified by container.
    result = constants.ConductoPaths.get_profile_base_dir()

    if hostdet.is_wsl():
        result = os.path.realpath(result)
        result = hostdet.wsl_host_docker_path(result)
    elif hostdet.is_windows():
        result = hostdet.windows_docker_path(result)
    elif is_migration:
        # we no longer support conducto in conducto
        # instead of doing a general (and fragile) check if we are in a docker container
        # we already know that we are doing a migration, so we can grab the mount info with the name

        # Mount to the ~/.conducto of the host machine and not of the container
        import json

        subp = subprocess.Popen(
            f"docker inspect -f '{{{{ json .Mounts }}}}' {get_current_container_id()}",
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
        mount_data, err = subp.communicate()
        log.log(f"Got {mount_data} {err}")
        if subp.returncode == 0:
            mounts = json.loads(mount_data)
            for mount in mounts:
                if result.startswith(mount["Destination"]):
                    result = result.replace(mount["Destination"], mount["Source"], 1)
                    log.log(f"Mounting to {result}")
                    break

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
    result = client_utils.subprocess_run(
        "head -1 /proc/self/cgroup|cut -d/ -f3", shell=True,
    )
    return result.stdout.decode("utf-8").strip()


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
        await async_utils.run_and_check("docker", "image", "pull")

    # Get the current SHA of the image
    out, _err = await async_utils.run_and_check(
        "docker", "image", "inspect", "--format", "{{.Id}}", image_name
    )
    current_sha = out.decode().strip()

    # Return True if there's a difference
    return current_sha != image_sha
