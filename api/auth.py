import json
import time
import typing
import getpass
import os
import jose, jose.jwt
from .. import api
from conducto.shared import constants, request_utils, types as t
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
        # TODO:  remove this function once get_auth is used everywhere
        if not login.get("email") or not login.get("password"):
            raise Exception("Login dict must specify email and password")
        data = json.dumps(login)
        headers = {"content-type": "application/json"}
        response = request_utils.post(
            self.url + "/auth/login", headers=headers, data=data
        )
        data = self._get_data(response)
        return data["AccessToken"]

    def get_account_token(
        self, login: dict, account_id: str = None
    ) -> typing.Optional[t.Token]:
        if not login.get("email") or not login.get("password"):
            raise Exception("Login dict must specify email and password")
        data = json.dumps(login)
        headers = {"content-type": "application/json"}
        response = request_utils.post(
            self.url + "/auth/login", headers=headers, data=data
        )
        data = self._get_data(response)

        return self.config.write_account_profiles(data["AccessToken"])

    def get_refreshed_token(
        self, token: t.Token, force: bool = False
    ) -> typing.Optional[t.Token]:
        # TODO:  remove this function once refresh_auth is used everywhere
        if constants.ExecutionEnv.images_only():
            # In images_only mode, tokens don't matter so skip the actual refresh.
            return token
        claims = self.get_unverified_claims(token)
        REFRESH_WINDOW_SECS = 120
        if time.time() + REFRESH_WINDOW_SECS < claims["exp"] and not force:
            return token
        headers = api_utils.get_auth_headers(token, refresh=False)
        response = request_utils.get(self.url + "/auth/refresh", headers=headers)
        data = self._get_data(response)
        return data["AccessToken"] if data is not None else None

    def get_auth(self, login: dict) -> dict:
        if not login.get("email") or not login.get("password"):
            raise Exception("Login dict must specify email and password")
        data = json.dumps(login)
        headers = {"content-type": "application/json"}
        response = request_utils.post(
            self.url + "/auth/login", headers=headers, data=data
        )
        return self._get_data(response)

    def refresh_auth(self, auths: dict, force: bool = False) -> dict:
        if constants.ExecutionEnv.images_only():
            # In images_only mode, tokens don't matter so skip the actual refresh.
            return auths

        acctoken = auths.get("AccessToken") or auths.get("IdToken")

        if acctoken and not force:
            claims = self.get_unverified_claims(acctoken)
            REFRESH_WINDOW_SECS = 120
            if time.time() + REFRESH_WINDOW_SECS < claims["exp"]:
                return auths

        headers = api_utils.get_auth_headers(acctoken, refresh=False)
        headers["X-Refresh-Token"] = auths["RefreshToken"]
        response = request_utils.get(self.url + "/auth/refresh", headers=headers)
        return self._get_data(response)

    def get_credentials(self, token: t.Token = None, force_refresh=False) -> dict:
        headers = api_utils.get_auth_headers(token, force_refresh=force_refresh)
        response = request_utils.get(self.url + "/auth/creds", headers=headers)
        data = self._get_data(response)
        return data

    def get_email(self, token: t.Token = None) -> dict:
        headers = api_utils.get_auth_headers(token)
        response = request_utils.get(self.url + "/auth/email", headers=headers)
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

    def admin_create_account(self, token: t.Token, user_email: str, org_id: int):
        headers = api_utils.get_auth_headers(token)
        body = {"user_email": user_email, "org_id": org_id}

        data = json.dumps(body)
        response = request_utils.post(
            self.url + "/auth/admin/account/create", headers=headers, data=data
        )
        return self._get_data(response)

    def list_accounts(self, id_token: t.Token):
        headers = api_utils.get_auth_headers(id_token)

        response = request_utils.get(self.url + "/auth/accounts", headers=headers)
        return self._get_data(response)

    def select_account(self, id_token: t.Token, account_id: str):
        headers = api_utils.get_auth_headers(id_token)

        response = request_utils.get(
            self.url + f"/auth/account/{account_id}", headers=headers
        )
        data = self._get_data(response)
        token = data["AccessToken"]

        return token

    def admin_enable_users(self, token: t.Token, users: list):
        headers = api_utils.get_auth_headers(token)
        body = {"user_ids": users}

        data = json.dumps(body)
        response = request_utils.post(
            self.url + "/auth/enable", headers=headers, data=data
        )
        return self._get_data(response)

    def admin_delete_users(self, token: t.Token, users: list):
        headers = api_utils.get_auth_headers(token)
        body = {"user_ids": users}

        data = json.dumps(body)
        response = request_utils.delete(
            self.url + "/auth/admin/account/delete", headers=headers, data=data
        )
        return self._get_data(response)

    def get_token_from_shell(
        self,
        login: dict = None,
        account_id: str = None,
        force=False,
        skip_profile=False,
        force_new=False,
    ) -> typing.Optional[t.Token]:
        account_token = None
        try:
            if not force_new:
                account_token = self.config.get_token(refresh=True)
        except (api_utils.UnauthorizedResponse, jose.exceptions.JWTError):
            # silently null the token and it will be prompted from the user
            # further down
            pass

        auth_token = None

        if account_token is None or login is not None:
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
                auth_token = self.get_token(login)
                account_token = self.config.write_account_profiles(
                    auth_token, skip_profile=skip_profile
                )
            elif os.getenv("CONDUCTO_TOKEN"):
                # Is this token an auth token or account token
                account_token = os.getenv("CONDUCTO_TOKEN")
            elif not account_id and (not self.config.default_profile or force_new):
                account_token = self._get_token_from_login()
            else:
                account_token = None

            if False and auth_token and not account_id:
                # TODO where should this go since account stuff is now resolved
                # in api.Config.write_account_profiles
                if os.environ.get("CONDUCTO_ACCOUNT"):
                    account_id = os.environ.get("CONDUCTO_ACCOUNT")
                else:
                    # if there is at least one account, select the first by default
                    # NOTE: mostly convenience for testing (account id is unpredictable)
                    print(
                        "No account id specified; Selecting first account in list by default."
                    )
                    accounts = self.list_accounts(auth_token)
                    if len(accounts) > 0:
                        user_id = accounts[0]["user_id"]
                        print(f"Defaulting to account id {user_id}")
                        account_id = user_id
                    else:
                        raise api.UserInputValidation(
                            "No accounts associated with this user.\n"
                            f"Please visit {self.url}/app and create an account, then try again."
                        )

        # if login and account_id:
        if account_id:
            # print("Getting fresh token with login data & account id")
            new_token = self.select_account(id_token=auth_token, account_id=account_id)
            if new_token and new_token != account_token:
                # print("successfully aquired new token")
                account_token = new_token
                if not skip_profile:
                    self.config.set_profile_general(
                        self.config.default_profile, "token", account_token
                    )
        elif not account_token:
            # token is expired, not enough info to fetch it headlessly.
            # prompt the user to login via the UI
            raise api.UserInputValidation(
                "Failed to refresh token. Token may be expired.\n"
                f"Log in through the webapp at {self.url}/app/ and select an account, then try again."
            )
        else:
            # try to refresh the token
            try:
                new_token = self.get_refreshed_token(account_token, force)
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
                            self.config.set_profile_general(
                                self.config.default_profile, "token", new_token
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
                f"Log in through the webapp at {self.url}/app/ and select an account, then try again."
            )
        else:
            # update profile with fresh token unless skipped
            if not skip_profile:
                self.config.write_profile(
                    self.config.get_url(), account_token, default="first"
                )
            return t.Token(account_token)

    def get_unverified_claims(self, token: t.Token = None) -> dict:
        # Returns a dict of *unverified* claims decoded from token.
        # No validation is done. Requires no knowledge of aws resources.
        if token is None:
            token = self.config.get_token(refresh=False)
        claims = jose.jwt.get_unverified_claims(token)
        return claims

    def get_user_id(self, token: t.Token = None):
        return self.get_unverified_claims(token=token)["sub"]

    def is_anonymous(self, token: t.Token = None):
        claims = self.get_unverified_claims(token)
        check1 = claims["user_status"] == "unregistered"
        check2 = claims["user_type"] == "anonymous"
        assert check1 == check2, (
            f"Unexpected discrepancy: user_status={claims['user_status']} "
            f"user_type={claims['user_type']}"
        )
        return check1

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

    def _get_token_from_login(self):
        NUM_TRIES = 3
        for i in range(NUM_TRIES):
            login = self.prompt_for_login(i)
            try:
                auth_token = self.get_token(login)
                token = self.config.write_account_profiles(auth_token)
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
