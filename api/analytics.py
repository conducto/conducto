from .. import api
from conducto.shared import request_utils
from . import api_utils


class Analytics:
    def __init__(self):
        self.config = api.Config()
        self.url = self.config.get_url()

    ############################################################
    # public methods
    ############################################################
    def get_pipeline_summary(self, pipeline_id):
        response = request_utils.get(f"{self.url}/analytics/data-summary/{pipeline_id}")
        return api_utils.get_data(response)

    def get_data_summary(self, token):
        credentials = api.Auth().get_credentials(token)
        identity_id = credentials["IdentityId"]
        response = request_utils.get(f"{self.url}/analytics/data-summary/{identity_id}")
        return api_utils.get_data(response)


AsyncAnalytics = api_utils.async_helper(Analytics)
