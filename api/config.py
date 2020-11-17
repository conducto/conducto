import sys
import configparser
import hashlib
import json
import os
import re
import secrets
import getpass
import subprocess
import time
import typing
import tempfile
import urllib.parse
import jose
from conducto.shared import constants, log, types as t, path_utils, imagepath
from http import HTTPStatus as hs
from . import api_utils
from .. import api
from jose import jwt

try:
    import contextvars as cv
except ImportError:
    cv = None


class Config:
    TOKEN: typing.Optional[t.Token] = None
    _RAN_FIX = False

    # contextvars is the async version of thread-local storage. This allows services to
    # have separate tokens for separate async tasks
    _CV_TOKEN = {} if cv is None else cv.ContextVar("_CV_TOKEN", default=None)

    class Location:
        LOCAL = "local"
        AWS = "aws"

    def __init__(self):
        self.reload()

        # Try exactly once per run to clean up old profile names. Only run this
        # externally, not inside a container.
        if not Config._RAN_FIX:
            if (
                constants.ExecutionEnv.value() == constants.ExecutionEnv.EXTERNAL
                and not constants.ExecutionEnv.images_only()
            ):
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
                path_utils.makedirs(config_dir)
        elif os.getenv("CONDUCTO_OUTER_OWNER") and os.stat(config_dir).st_uid == 0:
            path_utils.outer_chown(config_dir)
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
    def get_url(self, pretty=False):
        if "CONDUCTO_URL" in os.environ and os.environ["CONDUCTO_URL"]:
            result = os.environ["CONDUCTO_URL"]
        elif self.default_profile and os.path.exists(
            self._get_profile_config_file(self.default_profile)
        ):
            result = self._profile_general(self.default_profile)["url"]
        else:
            result = "https://www.conducto.com"

        if pretty:
            # wwwx*.conducto* -> www.conducto*
            # sandboxx*.conducto* -> sandbox.conducto*
            # testx*.conducto* -> test.conducto*
            res = urllib.parse.urlparse(result)
            m = re.search(
                r"^(wwwx|sandboxx|testx).*\.(conducto\.(?:com|io))$", res.netloc
            )
            if m:
                netloc = f"{m.group(1)[:-1]}.{m.group(2)}"
                return res._replace(netloc=netloc).geturl()

        return result

    def get_docker_domain(self):
        url = self.get_url()
        if url in ("https://conducto.com", "https://www.conducto.com"):
            return "docker.conducto.com"
        else:
            docker_url = url.replace(".conducto.", "-docker.conducto.", 1)
            netloc = urllib.parse.urlparse(docker_url).netloc
            return netloc

    def get_token(self, refresh: bool, force_refresh=False) -> typing.Optional[t.Token]:
        auth = api.Auth()

        # First look in contextvars, which is the async version of thread-local storage
        if Config._CV_TOKEN.get() is not None:
            if refresh or force_refresh:
                new_token = auth.get_refreshed_token(
                    Config._CV_TOKEN.get(), force=force_refresh
                )
                if new_token is None:
                    raise PermissionError("Expired token in Config.TOKEN")
                if Config._CV_TOKEN.get() != new_token:
                    Config._log_new_expiration_time(auth, new_token, loc="Config.TOKEN")
                    Config._CV_TOKEN.set(new_token)
            return Config._CV_TOKEN.get()

        # First priority is the class variable
        if Config.TOKEN is not None:
            if refresh or force_refresh:
                new_token = auth.get_refreshed_token(Config.TOKEN, force=force_refresh)
                if new_token is None:
                    raise PermissionError("Expired token in Config.TOKEN")
                if Config.TOKEN != new_token:
                    Config._log_new_expiration_time(auth, new_token, loc="Config.TOKEN")
                    Config.TOKEN = new_token
            return Config.TOKEN

        # If that's absent, look at the environment variable
        if "CONDUCTO_TOKEN" in os.environ:
            if refresh or force_refresh:
                new_token = auth.get_refreshed_token(
                    os.environ["CONDUCTO_TOKEN"], force=force_refresh
                )
                if new_token is None:
                    raise PermissionError("Expired token in CONDUCTO_TOKEN")
                if os.environ["CONDUCTO_TOKEN"] != new_token:
                    Config._log_new_expiration_time(
                        auth, new_token, loc="CONDUCTO_TOKEN"
                    )
                    os.environ["CONDUCTO_TOKEN"] = new_token
            return os.environ["CONDUCTO_TOKEN"]

        re_ask_cred_msg = None

        # If neither the environment variable nor the class variable are set then read
        # the token in from the .conducto profile config.
        if self.default_profile and os.path.exists(
            self._get_profile_config_file(self.default_profile)
        ):
            token = self._profile_general(self.default_profile).get("token", None)
            if not token:
                re_ask_cred_msg = "Credentials are missing in the local profile."
            if token and (refresh or force_refresh):
                try:
                    new_token = auth.get_refreshed_token(
                        t.Token(token), force=force_refresh
                    )
                except (jose.exceptions.JWTError, api.api_utils.UnauthorizedResponse):
                    re_ask_cred_msg = (
                        "Credentials stored in the local profile are expired."
                    )
                    token = None
                    new_token = None
                if new_token and token != new_token:
                    Config._log_new_expiration_time(auth, new_token, loc="config file")
                    self.set_profile_general(self.default_profile, "token", new_token)
                    token = new_token
            if token:
                return token

        # If a tty & token has not been discovered one of the prior ways or the
        # token is corrupt/missing/expired in the profile then request
        # credentials from the user.
        if sys.stdin.isatty():
            if re_ask_cred_msg:
                print(re_ask_cred_msg)

            # I want to explicitly drive the profile process here
            token = self.get_token_from_shell(force_new=True, skip_profile=True)

            # This may overwrite an existing profile; this depends on the credentials
            # entered.
            profile = self.write_profile(auth.url, token, default="first")
            # set up the default for the duration of this process
            os.environ["CONDUCTO_PROFILE"] = profile
            self.default_profile = profile

            return token
        else:
            if re_ask_cred_msg:
                raise PermissionError(re_ask_cred_msg)

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

    def write_account_profiles(self, auth_token, skip_profile=False):
        default_account_token = None

        # write/update a profile for all currently available accounts
        auth_api = api.auth.Auth()
        accounts = auth_api.list_accounts(auth_token)
        dir_api = api.dir.Dir()
        dir_api.url = self.get_url()
        accounts_list = []
        for account in accounts:
            account_token = auth_api.select_account(auth_token, account["user_id"])
            userdata = dir_api.user(account_token)
            orgdata = dir_api.org_by_user(account_token)
            if not skip_profile:
                profile = self.write_profile(
                    self.get_url(), account_token, default=False
                )
            else:
                profile = self.get_profile_id(
                    self.get_url(), userdata["org_id"], userdata["email"]
                )
            try:
                accounts_list.append(
                    {
                        "profile_id": profile,
                        "token": account_token,
                        "user_name": userdata["name"],
                        "org_name": orgdata["name"],
                        "is_default": profile == self.default_profile,
                    }
                )
            except api_utils.InvalidResponse as e:
                if e.status_code == hs.NOT_FOUND:
                    # user or org may be pending delete
                    # account effectively unavailable
                    pass
                else:
                    raise e

        # show available accounts
        if len(accounts_list) > 0:
            accounts_message = "Available accounts: "
            for a in accounts_list:
                accounts_message += (
                    f"\n  {a['profile_id']}: {a['user_name']} @ {a['org_name']}"
                    + ("*" if a["is_default"] else "")
                )
                # set the token to the account token
                if a["is_default"]:
                    default_account_token = a["token"]
            if not default_account_token:
                # if none is marked default in config, take first
                default_account_token = accounts_list[0]["token"]
            print(accounts_message)
            print(
                "NOTE: * indicates default profile.\n"
                "Change your default at anytime with the command:\n"
                "  `conducto-profile set-default <profile_id>`"
            )
        else:
            raise api.UserInputValidation(
                f"No accounts associated with this user. please visit the web app at {self.get_url()}/app/ "
                "and create or join at least one organization."
            )
        return default_account_token

    def register_named_share(self, profile, name, dirname):
        """
        Create or append to a named directory list in the profile
        configuration.
        """
        from .. import image as image_mod

        if not self.default_profile:
            self.get_token_from_shell(force=True)

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
        if constants.ExecutionEnv.headless():
            return constants.HOST_ID_NO_AGENT

        # This generation code for the host-id is somewhat deterministic,
        # but we save the host-id in ~/.conducto/config by design to not
        # rely on any specifics of long-term determinism.  It is convenient
        # to use this to mitigate race conditions if multiple processes
        # happen to be generating a host-id at the same time for a new
        # configuration.  We know that windows and mac generate a new
        # docker id on docker restart.
        host_id = os.getenv("CONDUCTO_HOST_ID")
        if host_id:
            if len(host_id) != 8:
                raise ValueError(f"Invalid CONDUCTO_HOST_ID: {host_id:r}")
            return host_id

        try:
            cmd = [
                "docker",
                "info",
                "--format='{{json .ID}}'",
            ]
            info = subprocess.run(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            docker_id = info.stdout.decode().strip('"')
        except subprocess.CalledProcessError:
            docker_id = None

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

        if api.Auth().is_anonymous(token):
            org_id = "anonymous"
            email = "anonymous"
        else:
            dir_api = api.dir.Dir()
            dir_api.url = url
            userdata = dir_api.user(token)
            org_id = str(userdata["org_id"])
            email = userdata["email"]

        # compute profile id search for url & org & email matching
        profile = self.get_profile_id(url, org_id, email)
        profile_sections = list(self.profile_sections())

        # read existing or blank profile
        profconfig = self.get_profile_config(profile)

        if not profconfig.has_section("general"):
            profconfig.add_section("general")
        profconfig.set("general", "url", url)
        profconfig.set("general", "org_id", org_id)
        profconfig.set("general", "email", email)
        profconfig.set("general", "token", token)
        try:
            profconfig.get("general", "host_id")
        except configparser.NoOptionError:
            # only generate (a new) host_id if there is not one currently
            profconfig.set("general", "host_id", self._generate_host_id(profile))

        profdir = constants.ConductoPaths.get_profile_base_dir(profile=profile)
        conffile = os.path.join(profdir, "config")
        path_utils.makedirs(profdir, exist_ok=True)
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

    def get_token_from_shell(
        self, login: dict = None, force=False, skip_profile=False, force_new=False
    ) -> typing.Optional[t.Token]:
        try:
            if not force_new:
                token = self.get_token(refresh=True)
                if token:
                    if not skip_profile:
                        # One of the reasons for a (full) write_profile here is
                        # for login with CONDUCTO_URL & CONDUCTO_TOKEN as
                        # environment variables.
                        self.write_profile(self.get_url(), token, default="first")
                    return token
        except (api_utils.UnauthorizedResponse, jose.exceptions.JWTError):
            # silently null the token and it will be prompted from the user
            # further down
            pass

        auth = api.Auth()
        auth.url = self.get_url()

        return Config._get_token_from_shell(
            self, auth, login=login, force=force, skip_profile=skip_profile
        )

    # This is equally a method of config & auth so priviledge neither as self.
    @staticmethod
    def _get_token_from_shell(
        config, auth, login: dict = None, force=False, skip_profile=False
    ):
        if not login and os.environ.get("CONDUCTO_EMAIL"):
            login = {
                "email": os.environ.get("CONDUCTO_EMAIL"),
                "password": os.environ["CONDUCTO_PASSWORD"],
            }
            print(
                f"Logging in with CONDUCTO_EMAIL={login['email']} and "
                f"CONDUCTO_PASSWORD in environment."
            )

        if login:
            auth_token = auth.get_token(login)
            account_token = config.write_account_profiles(
                auth_token, skip_profile=skip_profile
            )
        elif os.getenv("CONDUCTO_TOKEN"):
            # Is this token an auth token or account token
            account_token = os.getenv("CONDUCTO_TOKEN")
        else:
            account_token = Config._get_token_from_login(config, auth)

        if not account_token:
            # token is expired, not enough info to fetch it headlessly.
            # prompt the user to login via the UI
            raise api.UserInputValidation(
                "Failed to refresh token. Token may be expired.\n"
                f"Log in through the webapp at {auth.url}/app/ and select an account, then try again."
            )
        else:
            # try to refresh the token
            try:
                new_token = auth.get_refreshed_token(account_token, force)
            except api_utils.InvalidResponse as e:
                account_token = None
                # If cognito changed, our token is invalid, so we should
                # prompt for re-login.
                if e.status_code == hs.UNAUTHORIZED and "Invalid auth token" in str(e):
                    pass
                # Convenience case for when we're testing and user is deleted from
                # cognito. Token will still be valid but not associated with a user.
                # Re-login will straighten things out
                elif e.status_code == hs.NOT_FOUND and "User not found" in str(e):
                    pass
                else:
                    raise e
            else:
                if new_token:
                    if new_token != account_token:
                        if not skip_profile:
                            # TODO -- how do we know that this is the same profile?
                            config.set_profile_general(
                                config.default_profile, "token", new_token
                            )
                        account_token = new_token
                    # now, we continue with the refreshed token possibly
                    # writing a brand new profile (e.g. from the copied
                    # `start-agent` command from the app)

        # If no token by now, fail and let the user know that they need
        # to refresh their token in the app by logging in again.

        if not account_token:
            raise api.UserInputValidation(
                "Failed to refresh token. Token may be expired.\n"
                f"Log in through the webapp at {auth.url}/app/ and select an account, then try again."
            )
        else:
            # update profile with fresh token unless skipped
            if not skip_profile:
                config.write_profile(auth.url, account_token, default="first")
            return t.Token(account_token)

    # This is equally a method of config & auth so priviledge neither as self.
    @staticmethod
    def _prompt_for_login() -> dict:
        login = {}
        while True:
            login["email"] = input("Email: ")
            if len(login["email"]) > 0:
                break
        while True:
            login["password"] = getpass.getpass(prompt="Password: ")
            if len(login["password"]) > 0:
                break
        return login

    # This is equally a method of config & auth so priviledge neither as self.
    @staticmethod
    def _get_token_from_login(config, auth):
        NUM_TRIES = 3

        print(f"Log in to Conducto. To register, visit {auth.url}/app/")
        for i in range(NUM_TRIES):
            login = config._prompt_for_login()
            try:
                auth_token = auth.get_token(login)
                token = config.write_account_profiles(auth_token)
                # All good
                print("Login Successful...")
            except api_utils.InvalidResponse as e:
                if e.status_code == hs.CONFLICT and e.message.startswith(
                    "User registration"
                ):
                    print(e.message)
                elif e.status_code == hs.NOT_FOUND and e.message.startswith("No user"):
                    print(e.message)
                else:
                    raise
            except Exception as e:
                incorrects = ["Incorrect email or password", "User not found"]
                unfinished = ["User must confirm email", "No user information found"]
                if any(s in str(e) for s in incorrects):
                    print("Could not login. Incorrect email or password.")
                elif any(s in str(e) for s in unfinished):
                    print(
                        "Could not login. Complete registration from the link in the confirmation e-mail."
                    )
                else:
                    raise e
            else:
                return token
        raise Exception(f"Failed to login after {NUM_TRIES} attempts")

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
    def _sanity_check_token(config, filename):
        # Sanity check that we are not about to overwrite a token for a
        # different user.
        old_config = Config._atomic_read_config(filename)
        new_token = config.get("general", "token", fallback=None)
        old_token = old_config.get("general", "token", fallback=None)
        if new_token is not None and old_token is not None:
            new_claims = jwt.get_unverified_claims(new_token)
            try:
                old_claims = jwt.get_unverified_claims(old_token)
            except jose.exceptions.JWTError:
                # the old claim is garbage, lets ignore this and move on
                old_claims = new_claims

            check1 = new_claims["user_status"] == "unregistered"
            check2 = new_claims["user_type"] == "anonymous"
            assert check1 == check2, (
                f"Unexpected discrepancy: user_status={new_claims['user_status']} "
                f"user_type={new_claims['user_type']}"
            )

            # TODO:  for now tolerate claims with no token_type, but tighten this later
            if not check1 and new_claims.get("token_type", "account") != "account":
                raise Exception(
                    "Tokens written to the config must be account tokens (not auth)"
                )

            if not check1 and (
                new_claims["iss"] != old_claims["iss"]
                or new_claims["sub"] != old_claims["sub"]
            ):
                raise Exception(
                    f"Old and new token mismatch!\nnew={new_claims}\nold={old_claims}"
                )

    @staticmethod
    def _atomic_write_config(config, filename):
        Config._sanity_check_token(config, filename)
        cwd = os.path.dirname(filename)
        path_utils.makedirs(cwd, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            mode="w", delete=False, prefix=".config.", dir=cwd
        ) as temp_file:
            config.write(temp_file)
        path_utils.outer_chown(temp_file.name)
        os.replace(temp_file.name, filename)

    @staticmethod
    def _atomic_read_config(filename):
        config = configparser.ConfigParser()
        config.read(filename)
        return config

    @staticmethod
    def _log_new_expiration_time(auth, token, loc):
        claims = auth.get_unverified_claims(token)
        expiration_time = time.ctime(claims["exp"])
        log.debug(
            f"Just refreshed token from {loc} with new expiration time of {expiration_time}"
        )


AsyncConfig = api_utils.async_helper(Config)
