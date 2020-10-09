import boto3
import typing
from .. import api
from ..shared import constants, types as t, request_utils, data
from . import api_utils
import os


class Pipeline:
    def __init__(self):
        self.config = api.Config()
        self.url = self.config.get_url()

    ############################################################
    # public methods
    ############################################################
    def create(
        self, command: str, cloud: bool, token: t.Token = None, **kwargs
    ) -> t.PipelineId:
        from ..pipeline import Node

        headers = api_utils.get_auth_headers(token)
        in_data = {
            "command": command,
            "cloud": cloud,
            **kwargs,
            "host_id": self.config.get_host_id(),
        }
        # set the executable
        if "executable" not in kwargs:
            # conducto.internal has limited availability
            import conducto.internal.host_detection as hostdet

            in_data["executable"] = hostdet.host_exec()
        if "tags" in kwargs:
            in_data["tags"] = Node.sanitize_tags(in_data["tags"])
        response = request_utils.post(
            self.url + "/program/program", headers=headers, data=in_data
        )
        out_data = api_utils.get_data(response)
        return t.PipelineId(out_data["pipeline_id"])

    def archive(self, pipeline_id: t.PipelineId, token: t.Token = None):
        headers = api_utils.get_auth_headers(token)
        url = f"{self.url}/program/program/{pipeline_id}"
        response = request_utils.delete(url, headers=headers)
        api_utils.get_data(response)

    def get(self, pipeline_id: t.PipelineId, token: t.Token = None) -> dict:
        headers = api_utils.get_auth_headers(token)
        response = request_utils.get(
            self.url + f"/program/program/{pipeline_id}", headers=headers
        )
        return api_utils.get_data(response)

    def list(self, token: t.Token = None, user_only=False) -> list:
        headers = api_utils.get_auth_headers(token)
        url = self.url + "/program/program/list"
        if user_only:
            url += "?user_only=true"
        response = request_utils.get(url, headers=headers)
        return api_utils.get_data(response)

    def perms(self, pipeline_id: t.PipelineId, token: t.Token = None) -> set:
        headers = api_utils.get_auth_headers(token)
        response = request_utils.get(
            self.url + f"/program/program/{pipeline_id}/perms", headers=headers
        )
        data = api_utils.get_data(response)
        return data["perms"] if "perms" in data else []

    def update(
        self,
        pipeline_id: t.PipelineId,
        params: dict,
        *args,
        token: t.Token = None,
        **kwargs,
    ):
        headers = api_utils.get_auth_headers(token)
        keys = args if args else params.keys()
        if len(keys) == 0:
            raise Exception("No params to update on pipeline!")
        data = {k: params[k] for k in keys}
        if "tags" in data:
            from ..pipeline import Node

            data["tags"] = Node.sanitize_tags(data["tags"])
        if "extra_secret" in kwargs:
            headers["Service-Secret"] = kwargs["extra_secret"]
        response = request_utils.put(
            self.url + f"/program/program/{pipeline_id}", headers=headers, data=data
        )
        api_utils.get_data(response)

    def save_serialization(
        self, pipeline_id: t.PipelineId, serialization: str, token: t.Token = None
    ):
        pipeline = self.get(pipeline_id, token=token)
        put_serialization_s3(pipeline["program_path"], serialization, token=token)

    def touch(self, pipeline_id: t.PipelineId, token: t.Token = None, **kwargs):
        headers = api_utils.get_auth_headers(token)
        response = request_utils.put(
            self.url + f"/program/program/{pipeline_id}/touch",
            headers=headers,
            data=kwargs,
        )
        api_utils.get_data(response)

    def sleep(
        self,
        *,
        org_id: t.OrgId = None,
        user_ids: typing.List[t.UserId] = None,
        pipeline_ids: typing.List[t.PipelineId] = None,
        local: bool = False,
        cloud: bool = False,
        token: t.Token = None,
    ):
        headers = api_utils.get_auth_headers(token)
        data = {"local": local, "cloud": cloud}
        if org_id:
            data["org_id"] = org_id
        if user_ids:
            data["user_ids"] = user_ids
        if pipeline_ids:
            data["pipeline_ids"] = pipeline_ids
        response = request_utils.put(
            self.url + "/program/program/sleep", headers=headers, data=data
        )
        return api_utils.get_data(response)

    def sleep_standby(self, pipeline_id: t.PipelineId, token: t.Token = None):
        pipeline = self.get(pipeline_id, token=token)

        pl = constants.PipelineLifecycle
        if pipeline["status"] == pl.STANDBY_CLOUD:
            self.update(
                pipeline_id, {"status": pl.SLEEPING_CLOUD}, "status", token=token
            )
        else:
            # TODO: think about error
            pass

    def setcheck_runthrottle(
        self, pipeline_id: t.PipelineId, token: t.Token = None,
    ):
        headers = api_utils.get_auth_headers(token)
        response = request_utils.post(
            self.url + f"/program/program/{pipeline_id}/setcheck-runthrottle",
            headers=headers,
        )
        return api_utils.get_data(response)

    def get_history(self, params: dict, token: t.Token = None):
        headers = api_utils.get_auth_headers(token)
        response = request_utils.get(
            self.url + "/program/program/history", headers=headers, params=params
        )
        return api_utils.get_data(response)

    def agent_list(self, token: t.Token = None) -> list:
        headers = api_utils.get_auth_headers(token)
        response = request_utils.get(self.url + "/program/agent", headers=headers)
        return api_utils.get_data(response)

    def get_agent(self, *, user_id=None, host_id, token: t.Token = None) -> dict:
        headers = api_utils.get_auth_headers(token)
        response = request_utils.get(
            self.url + f"/program/agent/user/{user_id}/host/{host_id}", headers=headers
        )
        return api_utils.get_data(response)

    def admin_delete_logs(self, user_ids: list, token: t.Token = None):
        headers = api_utils.get_auth_headers(token)
        data = {"user_ids": user_ids}
        response = request_utils.post(
            self.url + "/program/admin/delete_logs", headers=headers, data=data
        )
        return api_utils.get_data(response)


def _get_s3_split(path):
    s3Prefix = "s3://"
    bucketKey = path[len(s3Prefix) :]
    bucket, key = bucketKey.split("/", 1)
    return bucket, key


def put_serialization_s3(s3path, serialization, token: t.Token = None):
    bucket, key = _get_s3_split(s3path)
    # log.log("S3 bucket={}, key={}".format(bucket, key))

    auth = api.Auth()
    token = auth.get_refreshed_token(token)
    creds = auth.get_credentials(token)

    session = boto3.Session(
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretKey"],
        aws_session_token=creds["SessionToken"],
    )

    s3 = session.client("s3")
    s3.put_object(Body=serialization.encode("utf-8"), Bucket=bucket, Key=key)

    # jmarcus: apeng's code below is elegant but doesn't use 'token' so it breaks when
    # called from the GitHub webhook. It should replace the above once we figure out how
    # to make co.data use a specific token.
    # os.environ["CONDUCTO_S3_BUCKET"] = bucket
    # data._Data._init(local=False)
    # data.superuser.puts(key.split("/", 1)[-1], serialization.encode("utf-8"))


def get_serialization_s3(s3path, token: t.Token = None):
    bucket, key = _get_s3_split(s3path)
    # log.log("S3 bucket={}, key={}".format(bucket, key))

    auth = api.Auth()
    token = auth.get_refreshed_token(token)
    creds = auth.get_credentials(token)

    session = boto3.Session(
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretKey"],
        aws_session_token=creds["SessionToken"],
    )
    s3 = session.client("s3")
    r = s3.get_object(Bucket=bucket, Key=key)
    return r["Body"].read().decode("utf-8")

    # jmarcus: apeng's code below is elegant but doesn't use 'token' so it breaks when
    # called from the GitHub webhook. It should replace the above once we figure out how
    # to make co.data use a specific token.
    # os.environ["CONDUCTO_S3_BUCKET"] = bucket
    # data._Data._init(local=False)
    # return data.superuser.gets(key.split("/", 1)[-1]).decode("utf-8")


AsyncPipeline = api_utils.async_helper(Pipeline)
