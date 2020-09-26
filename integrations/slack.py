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
    def get_channel_id(self, token, channel_name):
        """
        token: slack app token, type /conducto-token in any slack channel to get it
        channel_name: channel name or user id
        """
        url = f"{self.url}/slack/messageid"
        headers = {"content-type": "application/json"}
        body = {
            "token": token,
            "query": channel_name,
        }
        data = json.dumps(body)
        response = request_utils.post(url, headers=headers, data=data)

        data = api_utils.get_data(response)
        if not data.get("ok"):
            result = data.get("result") or data.get("error")
            raise api_utils.InvalidResponse(
                result,
                status_code=response.status_code,
                url=url,
                content_type="application/json",
            )
        return data["result"]

    def post_message(self, token, channel_id, text=None, blocks=None, thread_id=None):
        """
        token: slack app token, type /conducto-token in any slack channel to get it
        channel_id: call get_channel_id to get it
        text: optional message as text
        blocks: optional message formatted as list of slack blocks
        thread_id: any unique id for a message thread

        You must specify at least one of text and blocks. If you specify both
        text and blocks, then text is used as the fallback string to display
        in notifications.
        """
        if text is None and blocks is None:
            raise api_utils.UserInputValidation(
                "Must specify at least one of text and blocks"
            )
        url = f"{self.url}/slack/message"
        headers = {"content-type": "application/json"}
        body = {
            "token": token,
            "to": channel_id,
            "text": text if text else "",
            "blocks": json.dumps(blocks) if blocks else json.dumps([]),
            "node_id": thread_id if thread_id else "",
        }
        data = json.dumps(body)
        response = request_utils.post(url, headers=headers, data=data)

        data = api_utils.get_data(response)
        if not data.get("ok"):
            result = data.get("result") or data.get("error")
            raise api_utils.InvalidResponse(
                result,
                status_code=response.status_code,
                url=url,
                content_type="application/json",
            )


def message(
    recipient, text=None, blocks=None, thread_info=None, token: t.Token = None
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
    url = f"{api.Config().get_url()}/integrations/slack/message"
    headers = api_utils.get_auth_headers(token=token)
    data = {
        "recipient": recipient,
        "text": text,
        "blocks": blocks,
        "thread_info": thread_info,
    }
    response = request_utils.post(url, headers=headers, data=data)
    return api_utils.get_data(response)


AsyncSlack = api_utils.async_helper(Slack)
