import typing
import json
import time
from .. import api
from conducto.shared import types as t, request_utils
from http import HTTPStatus as hs
from . import api_utils


class Dir:
    def __init__(self):
        self.config = api.Config()
        self.url = self.config.get_url()

    ############################################################
    # public methods
    ############################################################
    def org_create(
        self,
        name: str,
        icon: typing.Optional[str],
        user_name: str,
        user_email: str,
        token: t.Token = None,
    ) -> dict:
        data = json.dumps(
            {
                "name": name,
                "icon": icon,
                "user_name": user_name,
                "user_email": user_email,
            }
        )
        headers = api_utils.get_auth_headers(token)
        response = request_utils.post(
            self.url + f"/dir/org", headers=headers, data=data
        )
        return api_utils.get_data(response)

    def org(self, org_id: t.OrgId, token: t.Token = None) -> dict:
        headers = api_utils.get_auth_headers(token)
        response = request_utils.get(self.url + f"/dir/org/{org_id}", headers=headers)
        return api_utils.get_data(response)

    def org_users(self, org_id: t.OrgId, token: t.Token = None) -> typing.List[dict]:
        headers = api_utils.get_auth_headers(token)
        response = request_utils.get(
            self.url + f"/dir/org/{org_id}/users", headers=headers
        )
        return api_utils.get_data(response)

    def user(self, token: t.Token = None) -> dict:
        user_id = api.Auth().get_user_id(token)

        headers = api_utils.get_auth_headers(token)

        response = request_utils.get(self.url + f"/dir/user/{user_id}", headers=headers)

        if response.status_code == hs.NOT_FOUND:
            message = f"No user information found.  Please complete registration at {self.url}/app"
            raise api_utils.InvalidResponse(
                message,
                status_code=response.status_code,
                url=response.url,
                content_type=None,
            )
        return api_utils.get_data(response)

    def bulk_users(self, user_ids, token: t.Token = None) -> list:
        headers = api_utils.get_auth_headers(token)
        data = json.dumps({"user_ids": user_ids})
        response = request_utils.post(
            self.url + f"/dir/user/bulk", data=data, headers=headers
        )
        return api_utils.get_data(response)

    def org_by_user(self, token: t.Token = None) -> dict:
        headers = api_utils.get_auth_headers(token)

        response = request_utils.get(self.url + f"/dir/org/by_user", headers=headers)

        if response.status_code == hs.NOT_FOUND:
            message = f"No user information found.  Please complete registration at {self.url}/app"
            raise api_utils.InvalidResponse(
                message,
                status_code=response.status_code,
                url=response.url,
                content_type=None,
            )
        return api_utils.get_data(response)

    def org_delete(self, org_id: t.OrgId, token: t.Token = None):
        headers = api_utils.get_auth_headers(token)
        response = request_utils.delete(
            self.url + f"/dir/org/{org_id}", headers=headers
        )
        api_utils.get_data(response)

    def invite_recipient(self, email: str, invite_id: str):
        response = request_utils.get(
            self.url + f"/dir/invite/{invite_id}/recipient/{email}"
        )
        return api_utils.get_data(response)

    def accept_invite(self, email: str, invite_id: str, user_id: str):
        data = json.dumps({"email": email, "user_id": user_id})
        headers = {"content-type": "application/json"}
        response = request_utils.post(
            self.url + f"/dir/invite/{invite_id}/accept", data=data, headers=headers
        )
        api_utils.get_data(response)

    def teams(self, org_id: t.OrgId, token: t.Token = None):
        headers = api_utils.get_auth_headers(token)

        # get teams and users in org
        response = request_utils.get(
            self.url + f"/dir/org/{org_id}/teams", headers=headers
        )
        data = api_utils.get_data(response)
        return data

    def users(self, org_id: t.OrgId, token: t.Token = None):
        headers = api_utils.get_auth_headers(token)

        # get teams and users in org
        response = request_utils.get(
            self.url + f"/dir/org/{org_id}/users", headers=headers
        )
        return api_utils.get_data(response)

    def org_create_subscription(
        self, token: t.Token, customer_id: str, subscription_data: dict
    ) -> dict:
        headers = api_utils.get_auth_headers(token)
        data = json.dumps(subscription_data)
        response = request_utils.put(
            self.url + f"/dir/org/{customer_id}/subscription",
            data=data,
            headers=headers,
        )
        return api_utils.get_data(response)

    def create_integration_user(
        self, token: t.Token, org_id: t.OrgId, name: str, integration_id: str
    ):
        response = request_utils.post(
            f"{self.url}/dir/org/{org_id}/integration",
            headers=api_utils.get_auth_headers(token),
            data={"name": name, "integration_id": integration_id},
        )
        result = api_utils.get_data(response)
        return result["email"], result["password"]

    def delete_integration_user(
        self, token: t.Token, org_id: t.OrgId, email: str, password: str
    ):
        response = request_utils.post(
            f"{self.url}/dir/org/{org_id}/integration/delete",
            headers=api_utils.get_auth_headers(token),
            data={"email": email, "password": password},
        )
        try:
            api_utils.get_data(response)
        except api_utils.InvalidResponse as e:
            if e.status_code == hs.NOT_FOUND:
                pass
            else:
                raise


AsyncDir = api_utils.async_helper(Dir)
