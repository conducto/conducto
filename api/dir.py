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
        token: t.Token,
        name: str,
        icon: typing.Optional[str],
        user_name: str,
        user_email: str,
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

    def org(self, token: t.Token, org_id: t.OrgId) -> dict:
        headers = api_utils.get_auth_headers(token)
        response = request_utils.get(self.url + f"/dir/org/{org_id}", headers=headers)
        return api_utils.get_data(response)

    def org_users(self, token: t.Token, org_id: t.OrgId) -> typing.List[dict]:
        headers = api_utils.get_auth_headers(token)
        response = request_utils.get(
            self.url + f"/dir/org/{org_id}/users", headers=headers
        )
        return api_utils.get_data(response)

    def user(self, token: t.Token, post_login=False) -> dict:
        user_id = api.Auth().get_user_id(token)

        headers = api_utils.get_auth_headers(token)

        i = 0
        while True:
            response = request_utils.get(
                self.url + f"/dir/user/{user_id}", headers=headers
            )

            if not post_login:
                # only invoke the loop logic when requested
                break

            # There can be a race condition before the directory entry is ready
            # after a cognito bounce.  Try up to 8 times before erroring.
            # Assigning a profile_id requires an org_id, so we need this
            # directory data before we can complete a login.
            if response.status_code != 404:
                break

            if i == 7:
                raise

            i += 1
            time.sleep(1)

        if response.status_code == 404:
            raise Exception(
                f"No user information found.  Please complete registration at {self.url}/app"
            )
        return api_utils.get_data(response)

    def nuke_org(self, token: t.Token, org_id: t.OrgId):
        headers = api_utils.get_auth_headers(token)

        # get teams and users in org
        response = request_utils.get(
            self.url + f"/dir/org/{org_id}/teams", headers=headers
        )
        data = api_utils.get_data(response)
        teams = set()
        users = set()
        for team in teams:
            teams.add(team["team_id"])
            for u in team["members"]:
                users.add(u["user_id"])

        # delete teams, users, and org
        for team in teams:
            response = request_utils.delete(
                self.url + f"/dir/team/{team}", headers=headers
            )
            api_utils.get_data(response)
        for u in users:
            response = request_utils.delete(
                self.url + f"/dir/user/{u}", headers=headers
            )
            api_utils.get_data(response)
        response = request_utils.delete(
            self.url + f"/dir/org/{org_id}", headers=headers
        )
        api_utils.get_data(response)

    def invite_exists(self, email: str, invite_id: str):
        response = request_utils.get(
            self.url + f"/dir/invite/{invite_id}/exists/{email}"
        )
        if response.status_code == hs.OK:
            return True
        elif response.status_code == hs.NOT_FOUND:
            return False
        else:
            # let normal channels handle the unexpected error
            api_utils.get_data(response)

    def accept_invite(self, email: str, invite_id: str, user_id: str):
        data = json.dumps({"email": email, "user_id": user_id})
        headers = {"content-type": "application/json"}
        response = request_utils.post(
            self.url + f"/dir/invite/{invite_id}/accept", data=data, headers=headers
        )
        api_utils.get_data(response)


AsyncDir = api_utils.async_helper(Dir)
