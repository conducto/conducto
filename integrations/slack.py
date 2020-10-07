import json

from .. import api
from ..api import api_utils
from conducto.shared import request_utils, types as t


class Slack:
    def __init__(self):
        self.config = api.Config()
        self.url = self.config.get_url()

    ############################################################
    # public methods
    ############################################################
    def message(
        self, recipient, text=None, blocks=None, thread_info=None, token: t.Token = None
    ) -> dict:
        """
        You must specify at least one of `text` and `blocks`. If you specify both
        text and blocks, then text is used as the fallback string to display
        in notifications.
        """
        if text is None and blocks is None:
            raise api_utils.UserInputValidation(
                "Must specify at least one of text and blocks"
            )
        headers = api_utils.get_auth_headers(token=token)
        data = {
            "recipient": recipient,
            "text": text,
            "blocks": blocks,
            "thread_info": thread_info,
        }
        url = f"{self.url}/integrations/slack/message"
        response = request_utils.post(url, headers=headers, data=data)
        return api_utils.get_data(response)


AsyncSlack = api_utils.async_helper(Slack)
