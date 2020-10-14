import json as pyjson
import conducto as co
from conducto.integrations.slack import Slack
from conducto.shared import constants, log, agent_utils, container_utils
from . import api


def _auth_and_send_message(recipient, text=None, blocks=None):
    slack = Slack()
    token = api.Auth().get_token_from_shell()
    thread_info = slack.message(
        recipient=recipient, text=text, blocks=blocks, token=token,
    )


def text(recipient, text):
    """
    Send simple text message using Slack integration.
    """
    _auth_and_send_message(recipient, text=text)


def block(recipient, json):
    """
    Send block(s) message using Slack integration.
    """
    prepared_json = pyjson.loads(json)
    _auth_and_send_message(recipient, blocks=prepared_json)


def main():
    variables = {
        "text": text,
        "block": block,
    }
    co.main(variables=variables)