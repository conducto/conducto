import platform
import subprocess


def is_windows():
    return platform.system().lower() == "windows"


def is_wsl():
    return "microsoft" in platform.uname().version.lower()


class WSLMapError(Exception):
    pass


class WindowsMapError(Exception):
    pass


def windows_drive_path(path):
    """
    Returns the windows path with forward slashes.  This is the format docker
    wants in the -v switch.
    """
    proc = subprocess.run(["wslpath", "-m", path], stdout=subprocess.PIPE)
    winpath = proc.stdout.decode("utf-8").strip()
    if winpath.startswith(r"\\") or winpath[1] != ":":
        raise WSLMapError(
            f"The context path {path} is not on a Windows drive accessible to Docker.  All image contexts paths must resolve to a location on the Windows file system."
        )

    return winpath


def wsl_host_docker_path(path):
    """
    Returns the windows path with forward slashes.  This is the format docker
    wants in the -v switch.
    """
    winpath = windows_drive_path(path)
    return f"/{winpath[0]}{winpath[2:]}"


def windows_docker_path(path):
    """
    Returns the windows path with forward slashes.  This is the format docker
    wants in the -v switch.
    """
    winpath = path.replace("\\", "/")
    return f"/{winpath[0]}{winpath[2:]}"
