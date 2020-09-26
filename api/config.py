import configparser
import hashlib
import json
import os
import secrets
import subprocess
import time
import typing
import tempfile
import urllib.parse
from conducto.shared import constants, log, types as t, path_utils, imagepath
from . import api_utils
from .. import api


class Config:
    TOKEN: typing.Optional[t.Token] = None
    _RAN_FIX = False

    class Location:
        LOCAL = "local"
        AWS = "aws"

    def __init__(self):
        self.reload()

        # Try exactly once per run to clean up old profile names. Only run this
        # externally, not inside a container.
        if not Config._RAN_FIX:
            if constants.ExecutionEnv.value() == constants.ExecutionEnv.EXTERNAL:
                self.cleanup_profile_names()
            Config._RAN_FIX = True

    ############################################################
    # generic methods
    ############################################################
    def reload(self):
        configFile = self._get_config_file()
        self.config = self._atomic_read_config(configFile)

        if os.environ.get("CONDUCTO_PROFILE", "") != "":
            self.default_profile = os.environ["CONDUCTO_PROFILE"]
            # we use this special value to explicitly clear the default
            if self.default_profile == "__none__":
                self.default_profile = None
        else:
            self.default_profile = self.get("general", "default")

            if self.default_profile:
                profdir = constants.ConductoPaths.get_profile_base_dir(
                    expand=True, profile=self.default_profile
                )
                if not os.path.exists(profdir):
                    # This indicates that the profile dir could not be found.
                    # We could fix the general config, but I'll pass on that
                    # for now.
                    self.default_profile = None

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

    @staticmethod
    def rename_profile(old_id, new_id):
        configdir = constants.ConductoPaths.get_local_base_dir()
        old_path = os.path.join(configdir, old_id)
        new_path = os.path.join(configdir, new_id)
        os.rename(old_path, new_path)

    def delete(self, section, key, write=True):
        del self.config[section][key]
        if not self.config[section]:
            del self.config[section]
        if write:
            self.write()

    def cleanup_profile_names(self):
        """
        fix old misnamed profile sections
        """
        for section in self.profile_sections():
            cfg = self.get_profile_config(section)
            ss_url = cfg.get("general", "url", fallback=None)
            ss_org_id = cfg.get("general", "org_id", fallback=None)
            ss_email = cfg.get("general", "email", fallback=None)
            correct_profile = self.get_profile_id(ss_url, ss_org_id, ss_email)
            if section != correct_profile:
                self.rename_profile(section, correct_profile)
                if self.default_profile == section:
                    print(
                        f"# Renaming profile {section} -> {correct_profile} [default]"
                    )
                    self.default_profile = correct_profile
                    os.environ["CONDUCTO_PROFILE"] = correct_profile
                    self.set("general", "default", correct_profile, write=True)
                else:
                    print(f"# Renaming profile {section} -> {correct_profile}")

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
                    log.debug(
                        f'Just refreshed token from Config.TOKEN with new expiration time of {time.ctime(claims["exp"])}'
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
                    log.debug(
                        f'Just refreshed token from CONDUCTO_TOKEN with new expiration time of {time.ctime(claims["exp"])}'
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
                    log.debug(
                        f'Just refreshed token from config with new expiration time of {time.ctime(claims["exp"])}'
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

    def get_connect_url(self, pipeline_id, allow_short_urls=False):
        base = self.get_url().rstrip("/")
        url = f"{base}/app/p/{pipeline_id}"
        if allow_short_urls:
            if self.get_url().find("conducto.com") > 0:
                url = f"https://conduc.to/{pipeline_id}"
            elif self.get_url().find("test.conducto.io") > 0:
                url = f"https://test.conduc.to/{pipeline_id}"
        return url

    def get_location(self):
        if constants.ExecutionEnv.value() in constants.ExecutionEnv.cloud:
            return Config.Location.AWS
        else:
            return Config.Location.LOCAL

    def get_image_tag(self, default=None):
        return os.getenv("CONDUCTO_IMAGE_TAG") or self.get("dev", "who", default)

    def _generate_host_id(self, profile):
        # This generation code for the host-id is somewhat deterministic,
        # but we save the host-id in ~/.conducto/config by design to not
        # rely on any specifics of long-term determinism.  It is convenient
        # to use this to mitigate race conditions if multiple processes
        # happen to be generating a host-id at the same time for a new
        # configuration.  We know that windows and mac generate a new
        # docker id on docker restart.

        cmd = [
            "docker",
            "info",
            "--format='{{json .ID}}'",
        ]
        info = subprocess.run(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)

        docker_id = info.stdout.decode().strip('"')

        if not docker_id:
            # This should never be necessary, but having a docker error hold up
            # this host_id generation would be annoying.
            docker_id = secrets.token_hex(4)

        seed = f"{docker_id}|{profile}"
        return hashlib.md5(seed.encode()).hexdigest()[:8]

    def get_host_id(self, profile=None):
        """
        Return the unique host_id stored in the .config. If none exist, generate one.
        """
        if constants.ExecutionEnv.headless():
            return constants.HOST_ID_NO_AGENT

        if profile is None:
            profile = self.default_profile

        host_id = self.get_profile_general(profile, "host_id")
        if host_id is None:
            # for the moment, we provide a conversion path from the host_id in
            # the global config to the profile specific config
            host_id = self.get("general", "host_id")
            if host_id is None:
                host_id = self._generate_host_id(profile)
            self.set_profile_general(profile, "host_id", host_id)
        return host_id

    @staticmethod
    def get_profile_id(url, org_id, email):
        return hashlib.md5(f"{url}|{org_id}|{email}".encode()).hexdigest()[:8]

    def write_profile(self, url, token, default=True):
        # ensure that [general] section is first for readability
        if not self.config.has_section("general"):
            self.config.add_section("general")

        from .. import api

        dir_api = api.dir.Dir()
        dir_api.url = url
        userdata = dir_api.user(token)
        org_id = str(userdata["org_id"])
        email = userdata["email"]

        # compute profile id search for url & org & email matching
        profile = self.get_profile_id(url, org_id, email)
        profile_sections = list(self.profile_sections())

        profdir = constants.ConductoPaths.get_profile_base_dir(profile=profile)
        conffile = os.path.join(profdir, "config")
        profconfig = configparser.ConfigParser()
        if not profconfig.has_section("general"):
            profconfig.add_section("general")
        profconfig.set("general", "url", url)
        profconfig.set("general", "org_id", org_id)
        profconfig.set("general", "email", email)
        profconfig.set("general", "token", token)
        profconfig.set("general", "host_id", self._generate_host_id(profile))
        os.makedirs(profdir, exist_ok=True)
        self._atomic_write_config(profconfig, conffile)

        assert default in (True, False, "first")
        if default is True or (default == "first" and len(profile_sections) == 0):
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
