import os
import random
import subprocess
import configparser
from conducto.shared import constants, log


def is_home_dir(dirname):
    homedir = os.path.expanduser("~")
    return homedir.rstrip(os.path.sep) == dirname.rstrip(os.path.sep)


def dirconfig_select(filename):
    if "CONDUCTO_PROFILE" in os.environ:
        return

    log.debug(f"auto-detecting profile from directory of {filename}")
    dirname = os.path.dirname(os.path.abspath(filename))

    def has_dcprofile(_dn):
        dcprofile = os.path.join(_dn, ".conducto", "profile")
        return os.path.exists(dcprofile)

    parent = dirname
    while True:
        if has_dcprofile(parent):
            break

        dn2 = os.path.dirname(parent)
        # comparing at length as a string prevents a wide swath of possible
        # ways this loop could go endless.
        if len(dn2) >= len(parent):
            parent = None
            break
        parent = dn2

    if parent is None:
        # no .conducto/profile found
        return

    dcprofile = os.path.join(parent, ".conducto", "profile")

    dirconfig = configparser.ConfigParser()
    dirconfig.read(dcprofile)

    section = dict(dirconfig.items("default"))

    config = Config()
    for profile in config.profile_sections():
        url = config.get(profile, "url")
        org_id = config.get(profile, "org_id")
        if url == section["url"] and org_id == section["org_id"]:
            break
    else:
        profile = None

    if profile is not None:
        os.environ["CONDUCTO_PROFILE"] = profile


def dirconfig_write(dirname, url, org_id):
    dirconfig = configparser.ConfigParser()
    dirconfig["default"] = {"url": url, "org_id": org_id}
    dotconducto = os.path.join(dirname, ".conducto")
    if not os.path.isdir(dotconducto):
        os.mkdir(dotconducto)
    dcprofile = os.path.join(dirname, ".conducto", "profile")
    with open(dcprofile, "w") as dirfile:
        dirconfig.write(dirfile)


class Config:
    class Location:
        LOCAL = "local"
        AWS = "aws"

    def __init__(self):
        self.reload()

    ############################################################
    # generic methods
    ############################################################
    def reload(self):
        configFile = self.__get_config_file()
        self.config = configparser.ConfigParser()
        self.config.read(configFile)

        # TODO: delete this convert chunk in May 2020
        if self.config.has_option("login", "token"):
            # convert old pre-profile format
            self._convert()
            # re-read
            self.config = configparser.ConfigParser()
            self.config.read(configFile)

        if "CONDUCTO_PROFILE" in os.environ:
            self.default_profile = os.environ["CONDUCTO_PROFILE"]
        else:
            self.default_profile = self.get("general", "default")

    def _convert(self):
        # TODO: remove in may 2020
        url = self.config.get("cloud", "url", fallback="https://www.conducto.com")
        token = self.config.get("login", "token", fallback="__none__")
        if token != "__none__":
            try:
                self.config.remove_option("cloud", "url")
            except:
                pass
            try:
                self.config.remove_option("login", "token")
            except:
                pass
            self.write()

            self.write_profile(url, token)

        if self.config.has_option("launch", "show_shell"):
            self.config.set(
                "general", "show_shell", self.config.get("launch", "show_shell")
            )
            self.config.remove_option("launch", "show_shell")
        if self.config.has_option("launch", "show_app"):
            self.config.set(
                "general", "show_app", self.config.get("launch", "show_app")
            )
            self.config.remove_option("launch", "show_app")

        if self.config.has_section("launch") and self.config.options("launch") == []:
            self.config.remove_section("launch")
        if self.config.has_section("cloud") and self.config.options("cloud") == []:
            self.config.remove_section("cloud")
        if self.config.has_section("login") and self.config.options("login") == []:
            self.config.remove_section("login")
        self.write()

    def get(self, section, key, default=None):
        return self.config.get(section, key, fallback=default)

    def set(self, section, key, value, write=True):
        if section not in self.config:
            self.config[section] = {}
        self.config[section][key] = value
        if write:
            self.write()

    def profile_sections(self):
        for section in self.config.sections():
            required = ["url", "org_id", "email", "token"]
            if all(self.config.has_option(section, rq) for rq in required):
                yield section

    def delete(self, section, key, write=True):
        del self.config[section][key]
        if not self.config[section]:
            del self.config[section]
        if write:
            self.write()

    def write(self):
        config_file = self.__get_config_file()
        # Create config dir if doesn't exist.
        config_dir = os.path.dirname(config_file)
        if not os.path.isdir(config_dir):
            # local import due to import loop
            import conducto.internal.host_detection as hostdet

            if hostdet.is_wsl():
                # Create .conducto directory in the window's users homedir.
                # Symlink that to the linux user's homedir.  This is
                # back-translated to a docker friendly path on docker mounting.

                fallback_error = """\
There was an error creating the conducto configuration files at ~/.conducto.
The .conducto folder must be accessible to docker and so it must be on a
Windows drive.  You can set that up manually by executing the following
commands:

    mkdir /mnt/c/Users/<winuser>/.conducto
    ln -sf /mnt/c/Users/<winuser>/.conducto ~/.conducto
"""

                try:
                    cmdline = ["wslpath", "-u", r"C:\Windows\system32\cmd.exe"]
                    proc = subprocess.run(cmdline, stdout=subprocess.PIPE)
                    cmdpath = proc.stdout.decode("utf-8").strip()

                    cmdline = [cmdpath, "/C", "echo %USERPROFILE%"]
                    proc = subprocess.run(cmdline, stdout=subprocess.PIPE)
                    winprofile = proc.stdout.decode("utf-8").strip()

                    cmdline = ["wslpath", "-u", winprofile]
                    proc = subprocess.run(cmdline, stdout=subprocess.PIPE)
                    homedir = proc.stdout.decode("utf-8").strip()

                    win_config_dir = os.path.join(homedir, ".conducto")
                    if not os.path.isdir(win_config_dir):
                        os.mkdir(win_config_dir)

                    cmdline = ["ln", "-s", win_config_dir, config_dir]
                    subprocess.run(cmdline, stdout=subprocess.PIPE)
                except subprocess.CalledProcessError as e:
                    raise RuntimeError(fallback_error)
            else:
                os.mkdir(config_dir)
        with open(config_file, "w") as config_fh:
            self.config.write(config_fh)

    ############################################################
    # specific methods
    ############################################################
    def get_url(self):
        if "CONDUCTO_URL" in os.environ and os.environ["CONDUCTO_URL"]:
            return os.environ["CONDUCTO_URL"]
        else:
            return self.get(self.default_profile, "url", "https://www.conducto.com")

    def get_token(self):
        return self.get(self.default_profile, "token")

    def get_connect_url(self, pipeline_id):
        if self.get_url().find("conducto.com") > 0:
            url = f"https://conduc.to/{pipeline_id}"
        elif self.get_url().find("test.conducto.io") > 0:
            url = f"https://test.conduc.to/{pipeline_id}"
        else:
            base = self.get_url().rstrip("/")
            url = f"{base}/app/p/{pipeline_id}"
        return url

    def get_location(self):
        if "AWS_EXECUTION_ENV" in os.environ:
            return Config.Location.AWS
        else:
            return Config.Location.LOCAL

    def get_image_tag(self, default=None):
        if os.getenv("IMAGE_TAG"):
            return os.getenv("IMAGE_TAG")
        tag = self.get("dev", "image_tag", default)
        if tag != default:
            return tag
        return self.get("docker", "image_tag", default)

    def write_profile(self, url, token, default=True):
        # ensure that [general] section is first for readability
        if not self.config.has_section("general"):
            self.config.add_section("general")

        from . import dir

        dir_api = dir.Dir()
        dir_api.url = url

        try:
            userdata = {}
            userdata = dir_api.user(token)
        except Exception as e:
            # This may be a pre-registered user in which case we leave email &
            # org_id blank
            if not str(e).startswith("No user information found."):
                raise

        # search for url & org matching
        is_new = False
        for section in self.config.sections():
            ss_url = self.config.get(section, "url", fallback=None)
            ss_org_id = self.config.get(section, "org_id", fallback=None)
            if (
                "org_id" in userdata
                and url == ss_url
                and str(userdata["org_id"]) == ss_org_id
            ):
                # re-use this one
                profile = section
                break
        else:
            is_new = True
            profile = "".join(random.choice("0123456789abcdef") for _ in range(8))

        self.set(profile, "url", url, write=False)
        if "org_id" in userdata:
            self.set(profile, "org_id", str(userdata["org_id"]), write=False)
        if "email" in userdata:
            self.set(profile, "email", userdata["email"], write=False)
        self.set(profile, "token", token, write=False)

        if default and is_new:
            self.set("general", "default", profile, write=False)
            self.default_profile = profile
        self.write()
        return profile

    ############################################################
    # helper methods
    ############################################################
    @staticmethod
    def __get_config_file():
        baseDir = constants.ConductoPaths.get_local_base_dir()
        defaultConfigFile = os.path.join(baseDir, "config")
        configFile = os.environ.get("CONDUCTO_CONFIG", defaultConfigFile)
        return os.path.expanduser(configFile)
