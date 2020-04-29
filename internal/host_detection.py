import os
import sys
import platform
import subprocess


def is_windows():
    return platform.system().lower() == "windows"


def is_mac():
    return platform.system().lower() == "darwin"


def is_wsl():
    return "microsoft" in platform.uname().version.lower()


def host_exec():
    venv = os.environ.get("VIRTUAL_ENV", None)
    # woah, os.path.sep vs. os.pathsep -- gotta be kidding
    pathdirs = os.environ["PATH"].split(os.pathsep)
    dirname = os.path.dirname(sys.executable)

    if venv is not None:
        return sys.executable
    elif dirname in pathdirs:
        return os.path.basename(sys.executable)
    else:
        return sys.executable


def system_open(url):
    """
    Open the URL (or file) in the default browser (or application).
    """
    sanitized = url.replace(";", r"\;").replace("&", r"\&")
    if is_windows():
        os.startfile(url)
    elif is_wsl():
        os.system(f'powershell.exe /c start "{sanitized}"')
    elif is_mac():
        os.system(f'open "{sanitized}"')
    else:
        # assumed linux with DISPLAY variable
        if os.environ.get("DISPLAY", None):
            # Redirect output to dev-null because gui programs on linux print
            # things which look scary but have little to do with the user level
            # reality
            os.system(f'xdg-open "{sanitized}" > /dev/null 2>&1')


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
    return f"/{winpath[0].lower()}{winpath[2:]}"


def windows_docker_path(path):
    """
    Returns the windows path with forward slashes.  This is the format docker
    wants in the -v switch.
    """
    winpath = path.replace("\\", "/")
    return f"/{winpath[0].lower()}{winpath[2:]}"
