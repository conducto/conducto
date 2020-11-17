import json
import time
import typing
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
        self, login: dict, account_id: str = None, skip_profile=False
    ) -> typing.Optional[t.Token]:
        if not login.get("email") or not login.get("password"):
            raise Exception("Login dict must specify email and password")
        data = json.dumps(login)
        headers = {"content-type": "application/json"}
        response = request_utils.post(
            self.url + "/auth/login", headers=headers, data=data
        )
        data = self._get_data(response)

        return self.config.write_account_profiles(
            data["AccessToken"], skip_profile=skip_profile
        )

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

    def change_password(self, current: str, new: str, token: t.Token = None):
        data = json.dumps({"current_password": current, "new_password": new})
        headers = api_utils.get_auth_headers(token)
        response = request_utils.post(
            self.url + "/auth/change-password", headers=headers, data=data
        )
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

    def get_token_from_shell(self, *args, **kwargs) -> typing.Optional[t.Token]:
        # This function is kept in auth only for backwards compatibility
        return self.config.get_token_from_shell(*args, **kwargs)

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


AsyncAuth = api_utils.async_helper(Auth)
