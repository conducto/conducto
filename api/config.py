import os
import subprocess
import configparser
from conducto.shared import constants


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

    def get(self, section, key, default=None):
        return self.config.get(section, key, fallback=default)

    def set(self, section, key, value, write=True):
        if section not in self.config:
            self.config[section] = {}
        self.config[section][key] = value
        if write:
            self.write()

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
            return self.get("cloud", "url", "https://www.conducto.com")

    def get_token(self):
        return self.get("login", "token")

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

    ############################################################
    # helper methods
    ############################################################
    @staticmethod
    def __get_config_file():
        baseDir = constants.ConductoPaths.get_local_base_dir()
        defaultConfigFile = os.path.join(baseDir, "config")
        configFile = os.environ.get("CONDUCTO_CONFIG", defaultConfigFile)
        return os.path.expanduser(configFile)
