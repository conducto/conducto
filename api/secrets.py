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
            f"{self.url}/secrets/user",
            headers=api_utils.get_auth_headers(token),
            params=params,
        )
        return api_utils.get_data(response)["secrets"]

    def get_org_secrets(self, token: types.Token, obfuscate: bool = False) -> dict:
        params = None
        if obfuscate:
            params = {"obfuscate": None}
        response = request_utils.get(
            f"{self.url}/secrets/org",
            headers=api_utils.get_auth_headers(token),
            params=params,
        )
        return api_utils.get_data(response)["secrets"]

    def put_user_secrets(
        self, token: types.Token, secrets: dict, replace: bool = False
    ):
        data = {"secrets": {k: str(v) for k, v in secrets.items()}}
        (request_utils.put if replace else request_utils.patch)(
            f"{self.url}/secrets/user",
            data=json.dumps(data),
            headers=api_utils.get_auth_headers(token),
        )

    def put_org_secrets(self, token: types.Token, secrets: dict, replace: bool = False):
        data = {"secrets": {k: str(v) for k, v in secrets.items()}}
        (request_utils.put if replace else request_utils.patch)(
            f"{self.url}/secrets/org",
            data=json.dumps(data),
            headers=api_utils.get_auth_headers(token),
        )

    def delete_user_secrets(self, token: types.Token, secret_names: list):
        data = {"secret_names": secret_names}
        request_utils.post(
            f"{self.url}/secrets/user/delete",
            data=json.dumps(data),
            headers=api_utils.get_auth_headers(token),
        )

    def delete_org_secrets(self, token: types.Token, secret_names: list):
        data = {"secret_names": secret_names}
        request_utils.post(
            f"{self.url}/secrets/org/delete",
            data=json.dumps(data),
            headers=api_utils.get_auth_headers(token),
        )


AsyncSecrets = api_utils.async_helper(Secrets)
