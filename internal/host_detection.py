import os
import sys
import platform


def is_windows():
    return platform.system().lower() == "windows"


def is_mac():
    return platform.system().lower() == "darwin"


def is_wsl1():
    # uname_result(system='Linux', node='...', release='4.4.0-19041-Microsoft', version='#1-Microsoft Fri Dec 06 14:06:00 PST 2019', machine='x86_64', processor='x86_64')
    uname = platform.uname()
    return "microsoft" in uname.version.lower() and "microsoft" in uname.release.lower()


def is_wsl2():
    # uname_result(system='Linux', node='...', release='4.19.104-microsoft-standard', version='#1 SMP Wed Feb 19 06:37:35 UTC 2020', machine='x86_64', processor='')
    uname = platform.uname()
    return (
        "microsoft" not in uname.version.lower()
        and "microsoft" in uname.release.lower()
    )


# 6 ways to use docker and linux on windows
# +-----------------+---------------+-----------------------------------------------------+
# | Pipeline Launch | Docker Engine |                      Comments                       |
# +-----------------+---------------+-----------------------------------------------------+
# | Windows         | Hyper-V       | current recommended conducto usage                  |
# | Windows         | WSL2          | probably future recommended conducto usage          |
# | WSL1 Distro     | Hyper-V       | shares ~/.conducto with Windows and supported       |
# | WSL1 Distro     | WSL2          | suggest to update your distro - "wsl --set-version" |
# | WSL2 Distro     | Hyper-V       | suggest to turn on docker "WSL2 based engine"       |
# | WSL2 Distro     | WSL2          | supported elegantly by docker basic installation    |
# +-----------------+---------------+-----------------------------------------------------+


def is_linux():
    return "linux" in platform.system().lower()


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


def os_name():
    if "CONDUCTO_OS" in os.environ:
        return os.environ["CONDUCTO_OS"]

    if is_mac():
        # If we want to convert this to a name, uncomment the following code:
        # major_version = platform.release().split(".")[0]
        # version_str = {
        #     "7": "Panther",
        #     "8": "Tiger",
        #     "9": "Leopard",
        #     "10": "Snow Leopard",
        #     "11": "Lion",
        #     "12": "Mountain Lion",
        #     "13": "Mavericks",
        #     "14": "Yosemite",
        #     "15": "El Capitan",
        #     "16": "Sierra",
        #     "17": "High Sierra",
        #     "18": "Mojave",
        #     "19": "Catalina"
        # }.get(major_version, "")
        # return f"Mac OS X {version_str}".strip()
        return f"macOS {platform.mac_ver()[0]}"
    elif is_windows():
        return f"Windows {platform.release()}"  # Works for 10 and Vista
    elif is_wsl1():
        return f"WSL1 {platform.release()}"
    elif is_wsl2():
        return f"WSL2 {platform.release()}"
    elif is_linux():
        if os.path.exists("/etc/os-release"):
            with open("/etc/os-release") as f:
                lines = f.read().splitlines()
            for line in lines:
                if line.startswith("PRETTY_NAME="):
                    return line.split("=", 1)[1].strip('"')
        return "Linux"
    else:
        return platform.platform(aliased=1, terse=1)


def system_open(url):
    """
    Open the URL (or file) in the default browser (or application).
    """
    sanitized = url.replace(";", r"\;").replace("&", r"\&")
    if is_windows():
        os.startfile(url)
    elif is_wsl1() or is_wsl2():
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
