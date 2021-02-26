import json
import conducto as co
from conducto.shared import types as t, request_utils
from .api import api_utils


def create_extension(name, token: t.Token = None):
    config = co.api.Config()

    url = config.get_url()
    if not token:
        token = config.get_token(refresh=False)

    headers = api_utils.get_auth_headers(token)
    body = json.dumps({"name": name})
    response = request_utils.post(
        f"{url}/integrations/schedule/installation", headers=headers, data=body
    )
    return api_utils.get_data(response)


def delete_extension(installation_id=None, token: t.Token = None):
    config = co.api.Config()

    url = config.get_url()
    if not token:
        token = config.get_token(refresh=False)

    headers = api_utils.get_auth_headers(token)
    response = request_utils.delete(
        f"{url}/integrations/schedule/{installation_id}", headers=headers
    )
    return api_utils.get_data(response)


def list_templates(installation_id, token: t.Token = None):
    config = co.api.Config()

    url = config.get_url()
    if not token:
        token = config.get_token(refresh=False)

    headers = api_utils.get_auth_headers(token)
    response = request_utils.get(
        f"{url}/integrations/schedule/{installation_id}/templates", headers=headers
    )
    return api_utils.get_data(response)


def get_scheduled_template(installation_id, template_id, token: t.Token = None):
    config = co.api.Config()

    url = config.get_url()
    if not token:
        token = config.get_token(refresh=False)

    headers = api_utils.get_auth_headers(token)
    response = request_utils.get(
        f"{url}/integrations/schedule/{installation_id}/template/{template_id}",
        headers=headers,
    )
    return api_utils.get_data(response)


def new(installation_id, node, pipe_name, token: t.Token = None):
    config = co.api.Config()

    url = config.get_url()
    if not token:
        token = config.get_token(refresh=False)

    serialization = node.serialize()
    # TODO:  figure out if serialization goes to document db

    headers = api_utils.get_auth_headers(token)
    body = {"schedule": pipe_name, "serialization": serialization}
    response = request_utils.post(
        f"{url}/integrations/schedule/{installation_id}/pipeline",
        headers=headers,
        data=body,
    )
    return api_utils.get_data(response)


def create_template(installation_id, pipeline_id, token: t.Token = None):
    config = co.api.Config()

    url = config.get_url()
    if not token:
        token = config.get_token(refresh=False)

    headers = api_utils.get_auth_headers(token)
    response = request_utils.post(
        f"{url}/integrations/schedule/{installation_id}/template/from/{pipeline_id}",
        headers=headers,
    )
    return api_utils.get_data(response)


def launch_template(installation_id, template_id, token: t.Token = None):
    config = co.api.Config()

    url = config.get_url()
    if not token:
        token = config.get_token(refresh=False)

    headers = api_utils.get_auth_headers(token)
    response = request_utils.post(
        f"{url}/integrations/schedule/{installation_id}/template/{template_id}/launch",
        headers=headers,
    )
    return api_utils.get_data(response)


def rebuild(name1, name2, token=None):
    pass
