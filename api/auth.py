import json
import time
import typing
import getpass
import os
from .. import api
from jose import jwt
from conducto.shared import types as t, request_utils
from http import HTTPStatus as hs
from . import api_utils


class Auth:
    def __init__(self):
        self.config = api.Config()
        self.url = self.config.get_url()

    ############################################################
    # public methods
    ############################################################
    def get_token(self, login: dict) -> typing.Optional[t.Token]:
        if not login.get("email") or not login.get("password"):
            raise Exception("Login dict must specify email and password")
        data = json.dumps(login)
        headers = {"content-type": "application/json"}
        response = request_utils.post(
            self.url + "/auth/login", headers=headers, data=data
        )
        data = self._get_data(response)
        return data["AccessToken"]

    def get_refreshed_token(
        self, token: t.Token, force: bool = False
    ) -> typing.Optional[t.Token]:
        claims = self.get_unverified_claims(token)
        REFRESH_WINDOW_SECS = 120
        if time.time() + REFRESH_WINDOW_SECS < claims["exp"] and not force:
            return token
        headers = api_utils.get_auth_headers(token, refresh=False)
        response = request_utils.get(self.url + "/auth/refresh", headers=headers)
        data = self._get_data(response)
        return data["AccessToken"] if data is not None else None

    def get_id_token(self, token: t.Token = None) -> typing.Optional[t.Token]:
        headers = api_utils.get_auth_headers(token)
        response = request_utils.get(self.url + "/auth/idtoken", headers=headers)
        data = self._get_data(response)
        return data["IdToken"]

    def get_identity_claims(self, token: t.Token = None) -> dict:
        id_token = self.get_id_token(token)
        claims = self.get_unverified_claims(id_token)
        return claims

    def get_credentials(self, token: t.Token = None, force_refresh=False) -> dict:
        headers = api_utils.get_auth_headers(token, force_refresh=force_refresh)
        response = request_utils.get(self.url + "/auth/creds", headers=headers)
        data = self._get_data(response)
        return data

    def admin_disable_users(self, token: t.Token, users: list):
        headers = api_utils.get_auth_headers(token)
        body = {"user_ids": users}

        data = json.dumps(body)
        response = request_utils.post(
            self.url + "/auth/disable", headers=headers, data=data
        )
        return self._get_data(response)

    def admin_enable_users(self, token: t.Token, users: list):
        headers = api_utils.get_auth_headers(token)
        body = {"user_ids": users}

        data = json.dumps(body)
        response = request_utils.post(
            self.url + "/auth/enable", headers=headers, data=data
        )
        return self._get_data(response)

    def get_token_from_shell(
        self, login: dict = None, force=False, skip_profile=False
    ) -> typing.Optional[t.Token]:
        try:
            token = self.config.get_token(refresh=True)
        except api_utils.UnauthorizedResponse:
            # silently null the token and it will be prompted from the user
            # further down
            token = None

        # If login not specified, read from environment.
        if not login and os.environ.get("CONDUCTO_EMAIL"):
            login = {
                "email": os.environ.get("CONDUCTO_EMAIL"),
                "password": os.environ["CONDUCTO_PASSWORD"],
            }
            print(
                f"Logging in with CONDUCTO_EMAIL={login['email']} and "
                f"CONDUCTO_PASSWORD in environment."
            )

        # First try to login with specified login.
        if login:
            token = self.get_token(login)

        # Otherwise try to refresh existing token.
        elif token:
            try:
                new_token = self.get_refreshed_token(token, force)
            except api_utils.InvalidResponse as e:
                token = None
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
                    if new_token != token:
                        if os.environ.get("CONDUCTO_USE_ID_TOKEN"):
                            new_token = self.get_id_token(new_token)
                        self.config.set_profile_general(
                            self.config.default_profile, "token", new_token
                        )
                        token = new_token
                    # now, we continue with the refreshed token possibly
                    # writing a brand new profile (e.g. from the copied
                    # `start-agent` command from the app)

        # If no token by now, prompt for login.
        if not token:
            token = self._get_token_from_login()
            if self.config.default_profile:
                self.config.set_profile_general(
                    self.config.default_profile, "token", token
                )

        if os.environ.get("CONDUCTO_USE_ID_TOKEN"):
            token = self.get_id_token(token)

        if not skip_profile:
            self.config.write_profile(self.config.get_url(), token, default="first")
        return t.Token(token)

    def get_unverified_claims(self, token: t.Token = None) -> dict:
        # Returns a dict of *unverified* claims decoded from token.
        # No validation is done. Requires no knowledge of aws resources.
        if token is None:
            token = self.config.get_token(refresh=False)
        claims = jwt.get_unverified_claims(token)
        return claims

    def get_user_id(self, token: t.Token = None):
        return self.get_unverified_claims(token=token)["sub"]

    def prompt_for_login(self, i=None) -> dict:
        if not i:
            # only print this on the first iteration to avoid obscuring the
            # failure mode
            print(f"Log in to Conducto. To register, visit {self.url}/app/")
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

    def test(self, token: t.Token = None) -> bool:
        headers = api_utils.get_auth_headers(token=token)
        response = request_utils.get(self.url + "/auth/test", headers=headers)
        if response.status_code == hs.NO_CONTENT:
            return True
        elif response.status_code == hs.UNAUTHORIZED:
            return False
        else:
            text = response.read()
            try:
                data = json.loads(text)
            except json.JSONDecodeError:
                msg = text
            else:
                msg = data["message"] if "message" in data else data
            raise Exception(msg)

    ############################################################
    # helper methods
    ############################################################
    def _get_data(self, response) -> typing.Optional[dict]:
        try:
            return api_utils.get_data(response)
        except api_utils.InvalidResponse as e:
            if e.status_code == hs.NOT_FOUND:
                if "No session for user" in str(e):
                    return None
            raise

    def _get_token_from_login(self):
        NUM_TRIES = 3
        for i in range(NUM_TRIES):
            login = self.prompt_for_login(i)
            try:
                token = self.get_token(login)

                # test that the directory entry from the user is complete, it
                # makes a mockery to say successful when this is required too
                dir_api = api.dir.Dir()
                dir_api.url = self.url
                dir_api.user(token)

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


AsyncAuth = api_utils.async_helper(Auth)
