import configparser
import os
import sys
import hashlib
import shutil
import secrets
import subprocess
import time
from conducto.shared import constants, log


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


DOTCONDUCTO_FORMAT = None


def format_update():
    global DOTCONDUCTO_FORMAT
    if DOTCONDUCTO_FORMAT is None:
        DOTCONDUCTO_FORMAT = "checked"
        return True
    return False


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

        if format_update():
            # TODO: delete this convert chunk in June 2020
            inline_sections = list(self.legacy_profile_sections())
            basedir = constants.ConductoPaths.get_local_base_dir()
            if len(inline_sections) or os.path.exists(os.path.join(basedir, "data")):
                print(
                    "Moving profile data & configuration to ~/.conducto/<profile>",
                    file=sys.stderr,
                )
                # convert old profile with inline profiles
                self._convert2()
                # re-read
                self.config = configparser.ConfigParser()
                self.config.read(configFile)

            # TODO: delete this convert chunk in June 2020
            if self._has_pipelines_in_profile_root():
                print(
                    "Moving pipelines to ~/.conducto/<profile>/pipelines",
                    file=sys.stderr,
                )
                # move pipelines to subdir
                self._convert3()
                # re-read
                self.config = configparser.ConfigParser()
                self.config.read(configFile)

        if os.environ.get("CONDUCTO_PROFILE", "") != "":
            self.default_profile = os.environ["CONDUCTO_PROFILE"]
        else:
            self.default_profile = self.get("general", "default")

    def _convert2(self):
        inline_sections = list(self.legacy_profile_sections())
        profile_map = {pk: pk for pk in inline_sections}
        basedir = constants.ConductoPaths.get_local_base_dir()
        for profile_key in inline_sections:
            # remove the section and put it in the profile dir
            profdir = constants.ConductoPaths.get_profile_base_dir(profile=profile_key)
            if os.path.exists(profdir):
                # write the profile data to profdir
                url = self.config.get(profile_key, "url")
                org_id = self.config.get(profile_key, "org_id")
                token = self.config.get(profile_key, "token")
                newprof = self.get_profile_id(url, org_id)

                # set up with canonical profile dir
                newprofdir = constants.ConductoPaths.get_profile_base_dir(
                    profile=newprof
                )

                profile_map[profile_key] = newprof

                print(f"Moving profile {profile_key} to {newprof}", file=sys.stderr)
                if profdir != newprofdir and not os.path.exists(newprofdir):
                    shutil.move(profdir, newprofdir)

                from . import auth

                auth = auth.Auth()
                auth.url = url
                token = auth.get_refreshed_token(token)
                self.write_profile(url, token, force_profile=newprof)
                if self.config.get("general", "default", fallback=None) == profile_key:
                    self.config.set("general", "default", newprof)

                # delete the inline profile
                self.config.remove_section(profile_key)

        if os.path.exists(os.path.join(basedir, "data")):
            orig = os.path.join(basedir, "data")
            if len(inline_sections) == 1:
                newprof = profile_map[inline_sections[0]]
                dest = os.path.join(basedir, newprof, "data")
                os.makedirs(os.path.join(basedir, newprof), exist_ok=True)
                if not os.path.exists(dest):
                    # essentially:
                    # mv ~/.conducto/data to ~/.conducto/<profile>/data
                    #  (but this dir is likely owned by root due to being created from a docker mount)
                    dotconducto = constants.ConductoPaths.get_local_base_dir()
                    cmd = [
                        "docker",
                        "run",
                        "--rm",
                        "-v",
                        f"{dotconducto}:/root/.conducto",
                        "alpine",
                        "mv",
                        f"/root/.conducto/data",
                        f"/root/.conducto/{newprof}/data",
                    ]
                    subprocess.run(cmd, check=True)
            else:
                dest = os.path.join(basedir, "<profile>", "data")
                print(
                    f"WARNING:  directory {orig} is the old location for user data, but it has moved to the profile specific location.  Please remove or move this this directory to the new location {dest}.",
                    file=sys.stderr,
                )

        if len(inline_sections) > 0:
            self.write()

    def _has_pipelines_in_profile_root(self):
        for profile in self.profile_sections():
            profdir = constants.ConductoPaths.get_profile_base_dir(profile=profile)

            import re

            for candidate in os.listdir(profdir):
                if None != re.match("[a-z]{3}-[a-z]{3}", candidate):
                    return True
        return False

    def _convert3(self):
        for profile in self.profile_sections():
            profdir = constants.ConductoPaths.get_profile_base_dir(profile=profile)

            pipedir = os.path.join(profdir, "pipelines")
            os.makedirs(pipedir, exist_ok=True)

            import re

            for pipe_id in os.listdir(profdir):
                if None != re.match("[a-z]{3}-[a-z]{3}", pipe_id):
                    old_pipedir = os.path.join(profdir, pipe_id)
                    new_pipedir = os.path.join(profdir, "pipelines", pipe_id)

                    if not os.path.exists(new_pipedir):
                        shutil.move(old_pipedir, new_pipedir)

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

    def legacy_profile_sections(self):
        # TODO:  this is legacy to be deleted in June 2020
        for section in self.config.sections():
            if len(section) != 8:
                # profiles are always 8 chars, specifically exclude general
                continue
            required = ["url", "org_id", "email"]
            if all(self.config.has_option(section, rq) for rq in required):
                yield section

    def delete_profile(self, profile):
        import conducto.internal.host_detection as hostdet

        dotconducto = constants.ConductoPaths.get_local_base_dir()
        if hostdet.is_wsl():
            dotconducto = hostdet.wsl_host_docker_path(os.path.realpath(dotconducto))
        elif hostdet.is_windows():
            dotconducto = hostdet.windows_docker_path(os.path.realpath(dotconducto))

        cmd = [
            "docker",
            "run",
            "--rm",
            "-v",
            f"{dotconducto}:/root/.conducto",
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
                except subprocess.CalledProcessError:
                    raise RuntimeError(fallback_error)
            else:
                os.mkdir(config_dir)
        with open(config_file, "w") as config_fh:
            self.config.write(config_fh)

    def _profile_general(self, profile):
        profdir = constants.ConductoPaths.get_profile_base_dir(profile=profile)
        conffile = os.path.join(profdir, "config")

        profconfig = configparser.ConfigParser()
        profconfig.read(conffile)

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
            self.__get_profile_config_file(self.default_profile)
        ):
            return self._profile_general(self.default_profile)["url"]
        else:
            return "https://conducto.com"

    def get_token(self):
        if self.default_profile and os.path.exists(
            self.__get_profile_config_file(self.default_profile)
        ):
            return self._profile_general(self.default_profile).get("token", None)
        return None

    def get_profile_config(self, profile):
        profdir = constants.ConductoPaths.get_profile_base_dir(profile=profile)
        conffile = os.path.join(profdir, "config")

        profconfig = configparser.ConfigParser()
        profconfig.read(conffile)
        return profconfig

    def write_profile_config(self, profile, profconfig):
        profdir = constants.ConductoPaths.get_profile_base_dir(profile=profile)
        conffile = os.path.join(profdir, "config")

        with open(conffile, "w") as fconf:
            profconfig.write(fconf)

    def get_profile_general(self, profile, option, fallback=None):
        profconfig = self.get_profile_config(profile)
        return profconfig.get("general", option, fallback=fallback)

    def set_profile_general(self, profile, option, value):
        profconfig = self.get_profile_config(profile)
        profconfig.set("general", option, value)
        self.write_profile_config(profile, profconfig)

    def register_named_mount(self, profile, name, dirname):
        """
        Create or append to a named directory list in the profile
        configuration.
        """
        import conducto.internal.host_detection as hostdet

        sep = os.pathsep
        if hostdet.is_wsl():
            # pretend to be windows because we are going to creating this as a
            # windows path.
            sep = ";"
            dirname = hostdet.windows_drive_path(dirname).replace("/", "\\")

        profconfig = self.get_profile_config(profile)

        current = profconfig.get("mounts", name, fallback=None)
        if current == None:
            current = dirname
        else:
            current = current.split(sep)

            if dirname in current:
                return
            else:
                current.append(dirname)
                current = sep.join(current)

        if not profconfig.has_section("mounts"):
            profconfig.add_section("mounts")
        profconfig.set("mounts", name, current)

        self.write_profile_config(profile, profconfig)

    @staticmethod
    def _get_mount_sep():
        import conducto.internal.host_detection as hostdet

        sep = os.pathsep
        if constants.ExecutionEnv.value() in constants.ExecutionEnv.manager_all:
            if os.getenv("WINDOWS_HOST"):
                # outer docker host is windows
                sep = ";"
        elif hostdet.is_wsl():
            # pretend to be windows because we are going to creating this as a
            # windows path.
            sep = ";"
        return sep

    def get_named_mount_mapping(self, profile):
        profconfig = self.get_profile_config(profile)
        sep = self._get_mount_sep()

        try:
            mounts = profconfig.items("mounts")
        except configparser.NoSectionError:
            mounts = {}

        return {k: v.split(sep) for k, v in mounts}

    def get_named_mount_paths(self, profile, name):
        profconfig = self.get_profile_config(profile)
        sep = self._get_mount_sep()

        try:
            current = profconfig.get("mounts", name, fallback=None)
        except configparser.NoSectionError:
            current = None
        if current == None:
            return []
        else:
            current = current.split(sep)
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
        if os.getenv("CONDUCTO_IMAGE_TAG"):
            return os.getenv("CONDUCTO_IMAGE_TAG")
        tag = self.get("dev", "image_tag", default)
        if tag != default:
            return tag
        tag = self.get("docker", "image_tag", default)
        if tag != default:
            return tag
        return self.get("dev", "who", default)

    def get_host_id(self):
        """
        Return the unique host_id stored in the .config. If none exist, generate one.
        """
        host_id = self.get("general", "host_id")
        if host_id is None:
            host_id = secrets.token_hex(4)
            self.set("general", "host_id", host_id)
        return host_id

    @staticmethod
    def get_profile_id(url, org_id):
        return hashlib.md5(f"{url}|{org_id}".encode()).hexdigest()[:8]

    def write_profile(self, url, token, default=True, force_profile=None):
        # ensure that [general] section is first for readability
        if not self.config.has_section("general"):
            self.config.add_section("general")

        from . import dir

        dir_api = dir.Dir()
        dir_api.url = url

        i = 0
        while True:
            try:
                userdata = dir_api.user(token)
            except Exception as e:
                # There can be a race condition before the directory entry is ready.
                # Try up to three times before erroring. Assigning a profile_id
                # requires an org_id, so we need this directory data before we can
                # proceed.
                if not str(e).startswith("No user information found."):
                    raise
                if i == 2:
                    raise
            else:
                break

            i += 1
            time.sleep(1)

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
        with open(conffile, "w") as fconf:
            profconfig.write(fconf)

        assert default in (True, False, "first")
        if default is True or (default == "first" and is_first):
            self.set("general", "default", profile, write=False)
            self.default_profile = profile
        self.write()
        return profile

    ############################################################
    # helper methods
    ############################################################
    @staticmethod
    def __get_config_file():
        base_dir = constants.ConductoPaths.get_local_base_dir()
        config_file = os.path.join(base_dir, "config")
        return os.path.expanduser(config_file)

    @staticmethod
    def __get_profile_config_file(profile):
        base_dir = constants.ConductoPaths.get_profile_base_dir(profile)
        return os.path.join(os.path.expanduser(base_dir), "config")
