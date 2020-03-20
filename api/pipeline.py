import json
from .. import api
from ..shared import constants, types as t, request_utils
from . import api_utils


class Pipeline:
    def __init__(self):
        self.config = api.Config()
        self.url = self.config.get_url()

    ############################################################
    # public methods
    ############################################################
    def create(
        self, token: t.Token, command: str, cloud: bool, **kwargs
    ) -> t.PipelineId:
        headers = api_utils.get_auth_headers(token)
        in_data = {"command": command, "cloud": cloud, **kwargs}
        if "tags" in kwargs:
            in_data["tags"] = self.sanitize_tags(in_data["tags"])
        response = request_utils.post(
            self.url + "/program/program", headers=headers, data=in_data
        )
        out_data = api_utils.get_data(response)
        return t.PipelineId(out_data["pipeline_id"])

    def archive(self, token: t.Token, pipeline_id: t.PipelineId):
        headers = api_utils.get_auth_headers(token)
        url = f"{self.url}/program/program/{pipeline_id}"
        response = request_utils.delete(url, headers=headers)
        api_utils.get_data(response)

    def get(self, token: t.Token, pipeline_id: t.PipelineId) -> dict:
        headers = api_utils.get_auth_headers(token)
        data = {"pipeline_id": pipeline_id}
        response = request_utils.get(
            self.url + f"/program/program/{pipeline_id}", headers=headers, params=data
        )
        return api_utils.get_data(response)

    def list(self, token: t.Token) -> list:
        headers = api_utils.get_auth_headers(token)
        response = request_utils.get(
            self.url + "/program/program/list", headers=headers
        )
        return api_utils.get_data(response)

    def update(self, token: t.Token, pipeline_id: t.PipelineId, params: dict, *args):
        headers = api_utils.get_auth_headers(token)
        keys = args if args else params.keys()
        if len(keys) == 0:
            raise Exception("No params to update on pipeline!")
        data = {k: params[k] for k in keys}
        if "tags" in data:
            data["tags"] = self.sanitize_tags(data["tags"])
        response = request_utils.put(
            self.url + f"/program/program/{pipeline_id}", headers=headers, data=data
        )
        api_utils.get_data(response)

    def save_serialization(
        self, token: t.Token, pipeline_id: t.PipelineId, serialization: str
    ):
        headers = api_utils.get_auth_headers(token)
        data = {"serialization": serialization}
        response = request_utils.put(
            self.url + f"/program/program/{pipeline_id}/serialization",
            headers=headers,
            data=data,
        )
        api_utils.get_data(response)

    def touch(self, token: t.Token, pipeline_id: t.PipelineId):
        headers = api_utils.get_auth_headers(token)
        response = request_utils.put(
            self.url + f"/program/program/{pipeline_id}/touch", headers=headers
        )
        api_utils.get_data(response)

    def sleep_standby(self, token: t.Token, pipeline_id: t.PipelineId):
        pipeline = self.get(token, pipeline_id)

        pl = constants.PipelineLifecycle
        if pipeline["status"] == pl.STANDBY_CLOUD:
            self.update(token, pipeline_id, {"status": pl.SLEEPING_CLOUD}, "status")
        else:
            # TODO: think about error
            pass

    def get_history(self, token: t.Token, params: dict):
        headers = api_utils.get_auth_headers(token)
        response = request_utils.get(
            self.url + "/program/program/history", headers=headers, params=params
        )
        return api_utils.get_data(response)

    def check_access(self, token: t.Token, pipeline_id: t.PipelineId):
        try:
            self.get(token, pipeline_id)
        except api_utils.InvalidResponse:
            # TODO: precisely check for the correct status_code
            return False
        else:
            return True

    @staticmethod
    def sanitize_tags(val):
        if val is None:
            return val
        elif isinstance(val, (bytes, str)):
            return [val]
        elif isinstance(val, (list, tuple, set)):
            for v in val:
                if not isinstance(v, (bytes, str)):
                    raise TypeError(f"Expected list of strings, got: {repr(v)}")
            return val
        else:
            raise TypeError(f"Cannot convert {repr(val)} to list of strings.")


AsyncPipeline = api_utils.async_helper(Pipeline)
