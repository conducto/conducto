import configparser
import os
import hashlib
import secrets
import subprocess
import json
import sys
import time
import typing
import tempfile
import urllib.parse
from conducto.shared import constants, log, types as t, path_utils, imagepath
from . import api_utils
from .. import api


def dirconfig_detect(dirname, auth_new=False):
    log.debug(f"auto-detecting profile from directory of {dirname}")

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
        url = config.get_profile_general(profile, "url")
        org_id = config.get_profile_general(profile, "org_id")
        if url == section["url"] and org_id == section["org_id"]:
            break
    else:
        if auth_new:
            profile = Config.get_profile_id(section["url"], section["org_id"])
        else:
            profile = None

    section["profile-id"] = profile
    section["dir-path"] = parent

    if "registered" in section and profile:
        Config().register_named_mount(profile, section["registered"], parent)

    return section


def dirconfig_select(filename):
    if "CONDUCTO_PROFILE" in os.environ:
        return

    log.debug(f"auto-detecting profile from directory of {filename}")
    dirname = os.path.dirname(os.path.abspath(filename))

    dirsettings = dirconfig_detect(dirname, auth_new=True)
    profile = dirsettings["profile-id"] if dirsettings else None

    if profile is not None:
        os.environ["CONDUCTO_PROFILE"] = profile


def dirconfig_write(dirname, url, org_id, name=None):
    dirconfig = configparser.ConfigParser()
    dirconfig["default"] = {"url": url, "org_id": org_id}
    if name is not None:
        dirconfig.set("default", "registered", name)
    dotconducto = os.path.join(dirname, ".conducto")
    if not os.path.isdir(dotconducto):
        os.mkdir(dotconducto)
    dcprofile = os.path.join(dirname, ".conducto", "profile")
    with open(dcprofile, "w") as dirfile:
        dirconfig.write(dirfile)


class Config:
    TOKEN: typing.Optional[t.Token] = None

    class Location:
        LOCAL = "local"
        AWS = "aws"

    def __init__(self):
        self.reload()

    ############################################################
    # generic methods
    ############################################################
    def reload(self):
        configFile = self._get_config_file()
        self.config = self._atomic_read_config(configFile)

        if os.environ.get("CONDUCTO_PROFILE", "") != "":
            self.default_profile = os.environ["CONDUCTO_PROFILE"]
        else:
            self.default_profile = self.get("general", "default")

    def get(self, section, key, default=None):
        return self.config.get(section, key, fallback=default)

    def set(self, section, key, value, write=True):
        if section not in self.config:
            self.config[section] = {}
        self.config[section][key] = value
        if write:
            self.write()

    def profile_sections(self):
        configdir = constants.ConductoPaths.get_local_base_dir()
        if not os.path.exists(configdir):
            # no .conducto, clearly no profiles
            return
        for fname in os.listdir(configdir):
            profdir = os.path.join(configdir, fname)
            profconf = os.path.join(configdir, fname, "config")
            if os.path.isdir(profdir) and os.path.isfile(profconf):
                yield fname

    def delete_profile(self, profile):
        dotconducto = constants.ConductoPaths.get_local_base_dir()
        dotconducto = imagepath.Path.from_localhost(dotconducto)

        cmd = [
            "docker",
            "run",
            "--rm",
            "-v",
            f"{dotconducto.to_docker_mount()}:/root/.conducto",
            "alpine",
            "rm",
            "-rf",
            f"/root/.conducto/{profile}",
        ]
        subprocess.run(cmd, check=True)

        if self.get("general", "default", None) == profile:
            self.delete("general", "default", write=True)

    def delete(self, section, key, write=True):
        del self.config[section][key]
        if not self.config[section]:
            del self.config[section]
        if write:
            self.write()

    def write(self):
        config_file = self._get_config_file()
        # Create config dir if doesn't exist.
        config_dir = os.path.dirname(config_file)
        if not os.path.isdir(config_dir):
            # local import due to import loop
            import conducto.internal.host_detection as hostdet

            if hostdet.is_wsl1():
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
                except subprocess.CalledProcessError:
                    raise RuntimeError(fallback_error)
            else:
                os.mkdir(config_dir)
        self._atomic_write_config(self.config, config_file)

    def _profile_general(self, profile) -> dict:
        profdir = constants.ConductoPaths.get_profile_base_dir(profile=profile)
        conffile = os.path.join(profdir, "config")

        profconfig = self._atomic_read_config(conffile)

        try:
            results = dict(profconfig.items("general"))
            results["default"] = self.get("general", "default", None) == profile
            return results
        except configparser.NoSectionError:
            return {}

    ############################################################
    # specific methods
    ############################################################
    def get_url(self):
        if "CONDUCTO_URL" in os.environ and os.environ["CONDUCTO_URL"]:
            return os.environ["CONDUCTO_URL"]
        elif self.default_profile and os.path.exists(
            self._get_profile_config_file(self.default_profile)
        ):
            return self._profile_general(self.default_profile)["url"]
        else:
            return "https://conducto.com"

    def get_docker_domain(self):
        url = self.get_url()
        if url == "https://conducto.com":
            return "docker.conducto.com"
        else:
            docker_url = url.replace(".conducto.", "-docker.conducto.", 1)
            netloc = urllib.parse.urlparse(docker_url).netloc
            return netloc

    def get_token(self, refresh: bool, force_refresh=False) -> typing.Optional[t.Token]:
        # First priority is the class variable
        if Config.TOKEN is not None:
            if refresh or force_refresh:
                auth = api.Auth()
                new_token = auth.get_refreshed_token(Config.TOKEN, force=force_refresh)
                if new_token is None:
                    raise PermissionError("Expired token in Config.TOKEN")
                if Config.TOKEN != new_token:
                    claims = auth.get_unverified_claims(new_token)
                    print(
                        f'Just refreshed token from Config.TOKEN with new expiration time of {time.ctime(claims["exp"])}',
                        file=sys.stderr,
                    )
                    Config.TOKEN = new_token
            return Config.TOKEN

        # If that's absent, look at the environment variable
        if "CONDUCTO_TOKEN" in os.environ:
            if refresh or force_refresh:
                auth = api.Auth()
                new_token = auth.get_refreshed_token(
                    os.environ["CONDUCTO_TOKEN"], force=force_refresh
                )
                if new_token is None:
                    raise PermissionError("Expired token in CONDUCTO_TOKEN")
                if os.environ["CONDUCTO_TOKEN"] != new_token:
                    claims = auth.get_unverified_claims(new_token)
                    print(
                        f'Just refreshed token from CONDUCTO_TOKEN with new expiration time of {time.ctime(claims["exp"])}',
                        file=sys.stderr,
                    )
                    os.environ["CONDUCTO_TOKEN"] = new_token
            return os.environ["CONDUCTO_TOKEN"]

        # If neither the environment variable nor the class variable are set then read
        # the token in from the .conducto profile config.
        if self.default_profile and os.path.exists(
            self._get_profile_config_file(self.default_profile)
        ):
            token = self._profile_general(self.default_profile).get("token", None)
            if token and (refresh or force_refresh):
                auth = api.Auth()
                new_token = auth.get_refreshed_token(
                    t.Token(token), force=force_refresh
                )
                if new_token is None:
                    raise PermissionError("Expired token in config")
                if token != new_token:
                    claims = auth.get_unverified_claims(new_token)
                    print(
                        f'Just refreshed token from config with new expiration time of {time.ctime(claims["exp"])}',
                        file=sys.stderr,
                    )
                    self.set_profile_general(self.default_profile, "token", new_token)
                return new_token
            return token
        return None

    @staticmethod
    def get_profile_config(profile):
        profdir = constants.ConductoPaths.get_profile_base_dir(profile=profile)
        conffile = os.path.join(profdir, "config")

        profconfig = Config._atomic_read_config(conffile)
        return profconfig

    @staticmethod
    def write_profile_config(profile, profconfig):
        profdir = constants.ConductoPaths.get_profile_base_dir(profile=profile)
        conffile = os.path.join(profdir, "config")
        Config._atomic_write_config(profconfig, conffile)

    def get_profile_general(self, profile, option, fallback=None):
        profconfig = self.get_profile_config(profile)
        return profconfig.get("general", option, fallback=fallback)

    def set_profile_general(self, profile, option, value):
        profconfig = self.get_profile_config(profile)
        try:
            # ensure we have the general section
            profconfig.add_section("general")
        except configparser.DuplicateSectionError:
            pass
        profconfig.set("general", option, value)
        self.write_profile_config(profile, profconfig)

    def register_named_share(self, profile, name, dirname):
        """
        Create or append to a named directory list in the profile
        configuration.
        """
        from .. import image as image_mod

        if not self.default_profile:
            auth = api.Auth()
            auth.get_token_from_shell(force=True)

        path = image_mod.Image.get_contextual_path(dirname, named_shares=False)
        dirname = path.to_docker_host()

        # TODO:  this test is good for my implementation now, but needs to go
        if "{" in dirname:
            raise RuntimeError(f"this is a bug -- {dirname}")

        profconfig = self.get_profile_config(profile)

        share_str = profconfig.get("shares", name, fallback="[]")
        current = json.loads(share_str)
        assert isinstance(current, list), "shares are stored as json lists of strings"

        if not current:
            current = [dirname]
        else:
            if dirname in current:
                return
            else:
                current.append(dirname)

        if not profconfig.has_section("shares"):
            profconfig.add_section("shares")
        profconfig.set("shares", name, json.dumps(current))

        self.write_profile_config(profile, profconfig)

    def get_named_share_mapping(self, profile):
        profconfig = self.get_profile_config(profile)

        try:
            shares = profconfig.items("shares")
        except configparser.NoSectionError:
            shares = {}

        return {k: json.loads(v) for k, v in shares}

    def get_named_share_paths(self, profile, name):
        profconfig = self.get_profile_config(profile)

        try:
            share_str = profconfig.get("shares", name, fallback="[]")
        except configparser.NoSectionError:
            share_str = "[]"
        current = json.loads(share_str)
        assert isinstance(current, list), "shares are stored as json lists of strings"
        return current

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
        if constants.ExecutionEnv.value() in constants.ExecutionEnv.cloud:
            return Config.Location.AWS
        else:
            return Config.Location.LOCAL

    def get_image_tag(self, default=None):
        return os.getenv("CONDUCTO_IMAGE_TAG") or self.get("dev", "who", default)

    def get_host_id(self):
        """
        Return the unique host_id stored in the .config. If none exist, generate one.
        """
        host_id = self.get("general", "host_id")
        if host_id is None:
            host_id = secrets.token_hex(4)
            self.set("general", "host_id", host_id)
        return host_id

    def get_default_profile(self):
        return self.default_profile

    @staticmethod
    def get_profile_id(url, org_id):
        return hashlib.md5(f"{url}|{org_id}".encode()).hexdigest()[:8]

    def write_profile(self, url, token, default=True, force_profile=None):
        # ensure that [general] section is first for readability
        if not self.config.has_section("general"):
            self.config.add_section("general")

        from .. import api

        dir_api = api.dir.Dir()
        dir_api.url = url
        userdata = dir_api.user(token)

        # search for url & org matching
        is_first = True
        if force_profile:
            profile = force_profile
        else:
            for section in self.profile_sections():
                is_first = False
                ss_url = self.config.get(section, "url", fallback=None)
                ss_org_id = self.config.get(section, "org_id", fallback=None)
                if url == ss_url and str(userdata["org_id"]) == ss_org_id:
                    # re-use this one
                    profile = section
                    break
            else:
                profile = self.get_profile_id(url, userdata["org_id"])

        profdir = constants.ConductoPaths.get_profile_base_dir(profile=profile)
        conffile = os.path.join(profdir, "config")
        profconfig = configparser.ConfigParser()
        if not profconfig.has_section("general"):
            profconfig.add_section("general")
        profconfig.set("general", "url", url)
        profconfig.set("general", "org_id", str(userdata["org_id"]))
        profconfig.set("general", "email", userdata["email"])
        profconfig.set("general", "token", token)
        os.makedirs(profdir, exist_ok=True)
        self._atomic_write_config(profconfig, conffile)

        assert default in (True, False, "first")
        if default is True or (default == "first" and is_first):
            self.set("general", "default", profile, write=False)
        if not (default is False):
            # stash this for all later config in this process
            os.environ["CONDUCTO_PROFILE"] = profile
            self.default_profile = profile
        self.write()
        return profile

    ############################################################
    # helper methods
    ############################################################
    @staticmethod
    def _get_config_file():
        base_dir = constants.ConductoPaths.get_local_base_dir()
        config_file = os.path.join(base_dir, "config")
        return os.path.expanduser(config_file)

    @staticmethod
    def _get_profile_config_file(profile):
        base_dir = constants.ConductoPaths.get_profile_base_dir(profile)
        return os.path.join(os.path.expanduser(base_dir), "config")

    @staticmethod
    def _atomic_write_config(config, filename):
        try:
            import conducto.internal.host_detection as hostdet

            is_windows = hostdet.is_windows()
        except ImportError:
            is_windows = False

        cwd = os.path.dirname(filename)
        os.makedirs(cwd, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            mode="w", delete=False, prefix=".config.", dir=cwd
        ) as temp_file:
            config.write(temp_file)
        if not is_windows:
            path_utils.outer_chown(temp_file.name)
        os.replace(temp_file.name, filename)

    @staticmethod
    def _atomic_read_config(filename):
        config = configparser.ConfigParser()
        config.read(filename)
        return config


AsyncConfig = api_utils.async_helper(Config)
