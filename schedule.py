import json
import conducto as co
from conducto.shared import types as t, request_utils
from .api import api_utils


def create_extension(name, token: t.Token = None):
    url = co.api.Config().get_url()

    headers = api_utils.get_auth_headers(token)
    body = json.dumps({"name": name})
    response = request_utils.post(
        f"{url}/integrations/schedule/installation", headers=headers, data=body
    )
    return api_utils.get_data(response)


def delete_extension(installation_id=None, token: t.Token = None):
    url = co.api.Config().get_url()

    headers = api_utils.get_auth_headers(token)
    response = request_utils.delete(
        f"{url}/integrations/schedule/{installation_id}", headers=headers
    )
    return api_utils.get_data(response)


def list_templates(installation_id, token: t.Token = None):
    url = co.api.Config().get_url()

    headers = api_utils.get_auth_headers(token)
    response = request_utils.get(
        f"{url}/integrations/schedule/{installation_id}/templates", headers=headers
    )
    return api_utils.get_data(response)


def get_template(installation_id, template_id, token: t.Token = None):
    url = co.api.Config().get_url()

    headers = api_utils.get_auth_headers(token)
    response = request_utils.get(
        f"{url}/integrations/schedule/{installation_id}/template/{template_id}",
        headers=headers,
    )
    return api_utils.get_data(response)


def new(installation_id, node, pipe_name, token: t.Token = None):
    url = co.api.Config().get_url()

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
    url = co.api.Config().get_url()

    headers = api_utils.get_auth_headers(token)
    response = request_utils.post(
        f"{url}/integrations/schedule/{installation_id}/template/from/{pipeline_id}",
        headers=headers,
    )
    return api_utils.get_data(response)


def delete_template(installation_id, template_id, token: t.Token = None):
    url = co.api.Config().get_url()

    headers = api_utils.get_auth_headers(token)
    response = request_utils.delete(
        f"{url}/integrations/schedule/{installation_id}/template/{template_id}",
        headers=headers,
    )
    return api_utils.get_data(response)


def launch_template(installation_id, template_id, token: t.Token = None):
    url = co.api.Config().get_url()

    headers = api_utils.get_auth_headers(token)
    response = request_utils.post(
        f"{url}/integrations/schedule/{installation_id}/template/{template_id}/launch",
        headers=headers,
    )
    return api_utils.get_data(response)


def rebuild(installation_id, template_id, token: t.Token = None):
    raise NotImplementedError("launch a stub manager and build")


def create_schedule(
    installation_id, template_id, name: str, token: t.Token = None, **kwargs
):
    url = co.api.Config().get_url()

    body = {"name": name, "template_id": template_id, **kwargs}
    body = json.dumps(body)

    headers = api_utils.get_auth_headers(token)
    response = request_utils.post(
        f"{url}/integrations/schedule/{installation_id}/schedule",
        headers=headers,
        data=body,
    )
    return api_utils.get_data(response)


def update_schedule(
    installation_id,
    schedule_id,
    name: str = None,
    simultaneous_limit: int = None,
    default_retention: int = None,
    token: t.Token = None,
):
    url = co.api.Config().get_url()

    body = {
        "name": name,
        "simultaneous_limit": simultaneous_limit,
        "default_retention": default_retention,
    }
    body = json.dumps(body)

    headers = api_utils.get_auth_headers(token)
    response = request_utils.put(
        f"{url}/integrations/schedule/{installation_id}/schedule/{schedule_id}",
        headers=headers,
        data=body,
    )
    return api_utils.get_data(response)


def list_schedules(installation_id, token: t.Token = None):
    url = co.api.Config().get_url()

    headers = api_utils.get_auth_headers(token)
    response = request_utils.get(
        f"{url}/integrations/schedule/{installation_id}/schedules", headers=headers
    )
    return api_utils.get_data(response)


def schedule_run_list(installation_id, schedule_id, token: t.Token = None):
    url = co.api.Config().get_url()

    headers = api_utils.get_auth_headers(token)
    response = request_utils.get(
        f"{url}/integrations/schedule/{installation_id}/schedule/{schedule_id}/runs",
        headers=headers,
    )
    return api_utils.get_data(response)


def delete_schedule(installation_id, schedule_id, token: t.Token = None):
    url = co.api.Config().get_url()

    headers = api_utils.get_auth_headers(token)
    response = request_utils.delete(
        f"{url}/integrations/schedule/{installation_id}/schedule/{schedule_id}",
        headers=headers,
    )
    return api_utils.get_data(response)
