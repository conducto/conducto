import json
import typing

from http import HTTPStatus as hs

from .. import api
from . import api_utils
from conducto.shared import request_utils, types


class Secrets:
    def __init__(self):
        self.config = api.Config()
        self.url = self.config.get_url()
        self.headers = {"Accept": "application/json"}

    ############################################################
    # public methods
    ############################################################
    def get_user_secrets(
        self, token: types.Token, include_org_secrets=False, obfuscate: bool = False,
    ) -> dict:
        params = None
        if obfuscate:
            params = {"obfuscate": None}
        if include_org_secrets:
            params = {"include_org_secrets": None}
        response = request_utils.get(
            f"{self.url}/secrets/user", headers=self._headers(token), params=params,
        )
        return self._get_data(response)["secrets"]

    def get_org_secrets(self, token: types.Token, obfuscate: bool = False) -> dict:
        params = None
        if obfuscate:
            params = {"obfuscate": None}
        response = request_utils.get(
            f"{self.url}/secrets/org", headers=self._headers(token), params=params,
        )
        return self._get_data(response)["secrets"]

    def put_user_secrets(
        self, token: types.Token, secrets: dict, replace: bool = False
    ):
        data = {"secrets": {k: str(v) for k, v in secrets.items()}}
        (request_utils.put if replace else request_utils.patch)(
            f"{self.url}/secrets/user",
            data=json.dumps(data),
            headers=self._headers(token),
        )

    def put_org_secrets(self, token: types.Token, secrets: dict, replace: bool = False):
        data = {"secrets": {k: str(v) for k, v in secrets.items()}}
        (request_utils.put if replace else request_utils.patch)(
            f"{self.url}/secrets/org",
            data=json.dumps(data),
            headers=self._headers(token),
        )

    def delete_user_secrets(self, token: types.Token, secret_names: list):
        data = {"secret_names": secret_names}
        request_utils.post(
            f"{self.url}/secrets/user/delete",
            data=json.dumps(data),
            headers=self._headers(token),
        )

    def delete_org_secrets(self, token: types.Token, secret_names: list):
        data = {"secret_names": secret_names}
        request_utils.post(
            f"{self.url}/secrets/org/delete",
            data=json.dumps(data),
            headers=self._headers(token),
        )

    ############################################################
    # helper methods
    ############################################################
    def _get_data(self, response) -> typing.Optional[dict]:
        if "application/json" not in response.headers["content-type"]:
            raise Exception(response.read())
        data = json.loads(response.read())
        if (
            response.status_code == hs.NOT_FOUND
            and "No session for user" in data["message"]
        ):
            return None
        if response.status_code != hs.OK:
            raise Exception(data["message"] if "message" in data else data)
        return data

    def _headers(self, token):
        return {**self.headers, **api_utils.get_auth_headers(token)}


AsyncSecrets = api_utils.async_helper(Secrets)
