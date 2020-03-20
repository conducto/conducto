import os
import configparser
import stat
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
        configFile = self.__get_config_file()
        # Create config dir if doesn't exist.
        configDir = os.path.dirname(configFile)
        if not os.path.isdir(configDir):
            os.mkdir(configDir)
        with open(configFile, "w") as configFileHandle:
            self.config.write(configFileHandle)

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
