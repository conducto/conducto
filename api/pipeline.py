import boto3
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

    def archive(self, token: t.Token, pipeline_id: t.PipelineId):
        headers = api_utils.get_auth_headers(token)
        url = f"{self.url}/program/program/{pipeline_id}"
        response = request_utils.delete(url, headers=headers)
        api_utils.get_data(response)

    def get(self, token: t.Token, pipeline_id: t.PipelineId) -> dict:
        headers = api_utils.get_auth_headers(token)
        response = request_utils.get(
            self.url + f"/program/program/{pipeline_id}", headers=headers
        )
        return api_utils.get_data(response)

    def list(self, token: t.Token) -> list:
        headers = api_utils.get_auth_headers(token)
        response = request_utils.get(
            self.url + "/program/program/list", headers=headers
        )
        return api_utils.get_data(response)

    def perms(self, token: t.Token, pipeline_id: t.PipelineId) -> set:
        headers = api_utils.get_auth_headers(token)
        response = request_utils.get(
            self.url + f"/program/program/{pipeline_id}/perms", headers=headers
        )
        data = api_utils.get_data(response)
        return data["perms"] if "perms" in data else []

    def update(
        self, token: t.Token, pipeline_id: t.PipelineId, params: dict, *args, **kwargs
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
        self, token: t.Token, pipeline_id: t.PipelineId, serialization: str
    ):
        pipeline = self.get(token, pipeline_id)
        put_serialization_s3(token, pipeline["program_path"], serialization)

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


def _get_s3_split(path):
    s3Prefix = "s3://"
    bucketKey = path[len(s3Prefix) :]
    bucket, key = bucketKey.split("/", 1)
    return bucket, key


def put_serialization_s3(token, s3path, serialization):
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


def get_serialization_s3(token, s3path):
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


AsyncPipeline = api_utils.async_helper(Pipeline)
