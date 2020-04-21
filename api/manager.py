import json
from .. import api
from ..shared import types as t, request_utils
from . import api_utils


class Manager:
    def __init__(self):
        self.config = api.Config()
        self.url = self.config.get_url()

    ############################################################
    # public methods
    ############################################################
    def launch(self, token: t.Token, pipeline_id: t.PipelineId):
        # Make sure pipeline exists and this user has access to it
        api.Pipeline().get(token, pipeline_id)

        # send request for manager service to launch this
        headers = api_utils.get_auth_headers(token)
        data = json.dumps({"pipeline_id": pipeline_id})
        response = request_utils.post(
            self.url + f"/manager/run", headers=headers, data=data
        )
        api_utils.get_data(response)


AsyncManager = api_utils.async_helper(Manager)
