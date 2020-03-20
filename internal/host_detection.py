import platform


def is_wsl():
    return "microsoft" in platform.uname().version.lower()
