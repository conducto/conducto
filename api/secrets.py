import json
import warnings

from .. import api
from . import api_utils
from conducto.shared import request_utils, types


class SecretKey:
    def __init__(self, key, scope="user"):
        self.scope = scope
        self.key = key

    def as_dict(self):
        return {"scope": self.scope, "key": self.key}


class Secret:
    def __init__(self, key, value, scope="user", mask=True):
        self.scope = scope
        self.key = key
        self.value = value
        self.mask = mask

    def as_dict(self):
        return {
            "scope": self.scope,
            "key": self.key,
            "value": self.value,
            "mask": self.mask,
        }


class Secrets:
    def __init__(self):
        self.config = api.Config()
        self.url = self.config.get_url()

    def get(self, token: types.Token = None) -> types.List[Secret]:
        headers = api_utils.get_auth_headers(token)

        response = request_utils.get(f"{self.url}/secrets/list", headers=headers)
        return api_utils.get_data(response)["secrets"]

    def put(self, secrets: types.List[Secret], token: types.Token = None):
        headers = api_utils.get_auth_headers(token)

        data = {"secrets": [sec.as_dict() for sec in secrets]}
        response = request_utils.patch(
            f"{self.url}/secrets/list", headers=headers, data=json.dumps(data)
        )
        return api_utils.get_data(response)

    def delete(self, secrets: types.List[SecretKey], token: types.Token = None):
        headers = api_utils.get_auth_headers(token)

        data = {"secret_keys": [sec.as_dict() for sec in secrets]}
        response = request_utils.post(
            f"{self.url}/secrets/delete", headers=headers, data=json.dumps(data)
        )
        return api_utils.get_data(response)

    ############################################################
    # public methods
    ############################################################
    def get_user_secrets(
        self,
        token: types.Token = None,
        include_org_secrets=False,
        obfuscate: bool = False,
    ) -> dict:
        warnings.warn("deprecated: use co.api.Secrets.get", DeprecationWarning)

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

    def get_org_secrets(
        self, token: types.Token = None, obfuscate: bool = False
    ) -> dict:
        warnings.warn("deprecated: use co.api.Secrets.get", DeprecationWarning)

        params = None
        if obfuscate:
            params = {"obfuscate": None}
        response = request_utils.get(
            f"{self.url}/secrets/org",
            headers=api_utils.get_auth_headers(token),
            params=params,
        )
        return api_utils.get_data(response)["secrets"]

    def put_user_secrets(self, secrets: dict, token: types.Token = None):
        warnings.warn("deprecated: use co.api.Secrets.put", DeprecationWarning)

        data = {"secrets": {k: str(v) for k, v in secrets.items()}}
        request_utils.patch(
            f"{self.url}/secrets/user",
            data=json.dumps(data),
            headers=api_utils.get_auth_headers(token),
        )

    def put_org_secrets(self, secrets: dict, token: types.Token = None):
        warnings.warn("deprecated: use co.api.Secrets.put", DeprecationWarning)

        data = {"secrets": {k: str(v) for k, v in secrets.items()}}
        request_utils.patch(
            f"{self.url}/secrets/org",
            data=json.dumps(data),
            headers=api_utils.get_auth_headers(token),
        )

    def delete_user_secrets(self, secret_names: list, token: types.Token = None):
        warnings.warn("deprecated: use co.api.Secrets.delete", DeprecationWarning)

        data = {"secret_names": secret_names}
        request_utils.post(
            f"{self.url}/secrets/user/delete",
            data=json.dumps(data),
            headers=api_utils.get_auth_headers(token),
        )

    def delete_org_secrets(self, secret_names: list, token: types.Token = None):
        warnings.warn("deprecated: use co.api.Secrets.delete", DeprecationWarning)

        data = {"secret_names": secret_names}
        request_utils.post(
            f"{self.url}/secrets/org/delete",
            data=json.dumps(data),
            headers=api_utils.get_auth_headers(token),
        )


AsyncSecrets = api_utils.async_helper(Secrets)
