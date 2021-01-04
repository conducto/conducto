import os
import io
import json
import re
import sys
import tarfile
import typing
import urllib.parse
import time
import functools
from conducto import api
from . import constants, types as t, path_utils
import threading


class ContextError(Exception):
    pass


client_creation_lock = threading.Lock()


class Credentials:

    _creds = None
    _s3_client = None
    local = True
    refresh_time = None

    @classmethod
    def refresh(cls):
        if cls.refresh_time is None or time.time() - cls.refresh_time >= 3000:
            with client_creation_lock:
                if not (
                    cls.refresh_time is None or time.time() - cls.refresh_time >= 3000
                ):
                    return

                import boto3
                from conducto import api

                cls._creds = api.Auth().get_credentials(force_refresh=True)
                cls._s3_client = boto3.client(
                    "s3",
                    aws_access_key_id=cls._creds["AccessKeyId"],
                    aws_secret_access_key=cls._creds["SecretKey"],
                    aws_session_token=cls._creds["SessionToken"],
                )
                cls.refresh_time = time.time()

    @classmethod
    def creds(cls):
        cls.refresh()
        return cls._creds

    @classmethod
    def s3_client(cls):
        cls.refresh()
        return cls._s3_client


class _Context:
    def __init__(self, local, uri, service=False):
        self.uri = uri
        self.local = local
        if not self.local:
            import boto3

            self.s3_client = boto3.client("s3") if service else Credentials.s3_client()
            m = re.search("^s3://(.*?)/(.*)", self.uri)
            self.bucket, self.key_root = m.group(1, 2)
        else:
            self.uri = os.path.expanduser(self.uri)

    def get_s3_key(self, name):
        # this is a slash delimited string, not a path
        return "/".join([self.key_root.rstrip("/"), name.lstrip("/")])

    def get_function(self, function_name, name):
        fxn = getattr(self.s3_client, function_name)
        return functools.partial(fxn, Bucket=self.bucket, Key=self.get_s3_key(name))

    def get_path(self, name):
        return _safe_join(self.uri, name)

    def delete_objects(self, objs):
        # TODO (apeng) ideally this will use the DeleteObjects API call, but we have no way
        # of tracking which objects are deleted in DeleteObjects calls due to a bug in AWS,
        # so we have to use the slow way of looping over DeleteObject.
        # If a user calls the delete_objects api, the backend will not know that the object
        # has been deleted, and will continue to bill users as if it was still there.

        # for pos in range(0, len(objs), 1000):
        #     self.s3_client.delete_objects(
        #         Bucket=self.bucket, Delete={"Objects": objs[pos : pos + 1000]}
        #     )

        for obj in objs:
            self.s3_client.delete_object(Bucket=self.bucket, **obj)

    def list_all_versions(self, prefix, is_exact=False):
        response = self.s3_client.list_object_versions(
            Bucket=self.bucket, Prefix=prefix
        )
        while True:
            for version in response.get("Versions", []):
                if not (is_exact and version["Key"] != prefix):
                    yield version
            if not response["IsTruncated"]:
                return
            response = self.s3_client.list_object_versions(
                Bucket=self.bucket, Prefix=prefix, KeyMarker=response["KeyMarker"]
            )

    def cleanup(self, key, skip_latest=False, is_exact=True):
        key = self.get_s3_key(key)
        # this method is invoked to clear out all but the latest version, or to delete all versions
        versions = list(self.list_all_versions(prefix=key, is_exact=is_exact))
        to_delete = [
            {"VersionId": i["VersionId"], "Key": i["Key"]}
            for i in versions
            if not (skip_latest and i["IsLatest"])
        ]
        if not to_delete:
            return
        self.delete_objects(to_delete)


class _Data:
    _pipeline_id: t.PipelineId = None
    _local: bool = None
    _s3_bucket: str = None

    @staticmethod
    def _get_uri():
        raise NotImplementedError()

    @classmethod
    def _ctx(cls):
        # Call to _init() is idempotent if things are already set. If they haven't been
        # set yet then read environment variables
        cls._init()
        return _Context(local=cls._local, uri=cls._get_uri())

    @staticmethod
    def _init(*, pipeline_id: t.PipelineId = None, local: bool = None):
        # Order of precedence for each param:
        #  - If it is specified, use it.
        #  - elif it is set on the class, use that
        #  - else use the environment variable
        #  - else default to None
        _Data._pipeline_id = (
            pipeline_id or _Data._pipeline_id or os.getenv("CONDUCTO_PIPELINE_ID")
        )
        for attr in (
            local,
            _Data._local,
            api.Config().get_location() == api.Config.Location.LOCAL,
        ):
            if attr is not None:
                _Data._local = attr
                break
        _Data._s3_bucket = _Data._s3_bucket or os.getenv("CONDUCTO_S3_BUCKET")

    @classmethod
    def get(cls, name, file, local=None):
        """
        Get object at `name`, store it to `file`.
        """
        ctx = cls._ctx()
        if not ctx.local:
            return ctx.get_function("download_file", name)(Filename=file)
        else:
            import shutil

            shutil.copy(ctx.get_path(name), file)

    @classmethod
    def gets(cls, name, *, byte_range: typing.List[int] = None) -> bytes:
        """
        Return object at `name`. Optionally restrict to the given `byte_range`.
        Byte range is on the half open interval [begin, end)
        """
        ctx = cls._ctx()
        if not ctx.local:
            kwargs = {}
            if byte_range:
                begin, end = byte_range
                kwargs["Range"] = f"bytes={begin}-{end - 1}"
            return ctx.get_function("get_object", name)(**kwargs)["Body"].read()
        else:
            with open(ctx.get_path(name), "rb") as f:
                if byte_range:
                    begin, end = byte_range
                    f.seek(begin)
                    return f.read(end - begin)
                else:
                    return f.read()

    @classmethod
    def put(cls, name, file, skip_cleanup=False):
        """
        Store object in `file` to `name`.
        """
        ctx = cls._ctx()
        if not ctx.local:
            ctx.get_function("upload_file", name)(file)
            if not skip_cleanup:
                ctx.cleanup(name, skip_latest=True)
        else:
            # Make sure to write the obj atomically. Write to a temp file then move it
            # into the final location. If anything goes wrong delete the temp file.
            import tempfile, shutil

            path = ctx.get_path(name)
            dirpath = os.path.dirname(path)
            path_utils.makedirs(os.path.dirname(path), exist_ok=True)
            fd, tmppath = tempfile.mkstemp(dir=dirpath)
            try:
                shutil.copy(file, tmppath)
            except Exception:
                os.remove(tmppath)
                raise
            else:
                shutil.move(tmppath, path)
                path_utils.outer_chown(path)

    @classmethod
    def puts(cls, name, obj: bytes):
        if not isinstance(obj, bytes):
            raise ValueError(f"Expected object of type bytes, but got {repr(obj)}")
        ctx = cls._ctx()
        if not ctx.local:
            ctx.get_function("put_object", name)(Body=obj)
            ctx.cleanup(name, skip_latest=True)
        else:
            # Make sure to write the obj atomically. Write to a temp file then move it
            # into the final location. If anything goes wrong delete the temp file.
            import tempfile, shutil

            path = ctx.get_path(name)
            dirpath = os.path.dirname(path)
            path_utils.makedirs(dirpath, exist_ok=True)
            fd, tmppath = tempfile.mkstemp(dir=dirpath)
            try:
                with open(fd, "wb") as f:
                    f.write(obj)
            except Exception:
                os.remove(tmppath)
                raise
            else:
                shutil.move(tmppath, path)
                path_utils.outer_chown(path)

    @classmethod
    def delete(cls, name, recursive=False):
        """
        Delete object at `name`.
        """
        ctx = cls._ctx()
        if not ctx.local:
            ctx.cleanup(name, is_exact=not recursive)
        else:
            import shutil

            path = ctx.get_path(name)
            if recursive and os.path.isdir(path):
                shutil.rmtree(path)
            else:
                os.remove(path)

    @classmethod
    def list(cls, prefix):
        """
        Return names of objects that start with `prefix`.
        """
        # TODO: make this more like listdir or more like glob. Right now pattern matching is inconsistent between local and cloud.
        ctx = cls._ctx()
        if not ctx.local:
            paginator = ctx.s3_client.get_paginator("list_objects")

            prefix_size = len(ctx.get_s3_key(""))

            page_iterator = paginator.paginate(
                Bucket=ctx.bucket, Prefix=ctx.get_s3_key(prefix)
            )
            res = []
            for page in page_iterator:
                if "Contents" not in page:
                    # no Contents key -> empty list
                    break
                for obj in page["Contents"]:
                    res.append(obj["Key"][prefix_size:])
            return res
        else:
            path = ctx.get_path(prefix)
            try:
                names = os.listdir(path)
            except OSError:
                return []
            return [_safe_join(prefix, name) for name in sorted(names)]

    @classmethod
    def exists(cls, name):
        """
        Test if there is an object at `name`.
        """
        ctx = cls._ctx()
        if not ctx.local:
            import botocore.exceptions

            try:
                ctx.s3_client.head_object(Bucket=ctx.bucket, Key=ctx.get_s3_key(name))
            except botocore.exceptions.ClientError:
                return False
            else:
                return True
        else:
            return os.path.exists(ctx.get_path(name))

    @classmethod
    def size(cls, name):
        """
        Return the size of the object at `name`, in bytes.
        """
        ctx = cls._ctx()
        if not ctx.local:
            result = ctx.s3_client.head_object(
                Bucket=ctx.bucket, Key=ctx.get_s3_key(name)
            )
            return result["ContentLength"]
        else:
            return os.stat(ctx.get_path(name)).st_size

    @classmethod
    def clear_cache(cls, name, checksum=None):
        """
        Clear cache at `name` with `checksum`, clears all `name` cache if no `checksum`.
        """
        data_path = f"conducto-cache/{name}"
        if checksum is None:
            for file in cls.list(data_path):
                cls.delete(file)
        else:
            cls.delete(f"{data_path}/{checksum}.tar.gz")

    @classmethod
    def cache_exists(cls, name, checksum):
        """
        Test if there is a cache at `name` with `checksum`.
        """
        data_path = f"conducto-cache/{name}/{checksum}.tar.gz"
        return cls.exists(data_path)

    @classmethod
    def save_cache(cls, name, checksum, save_dir):
        """
        Save `save_dir` to cache at `name` with `checksum`.
        """
        data_path = f"conducto-cache/{name}/{checksum}.tar.gz"
        tario = io.BytesIO()
        with tarfile.TarFile(fileobj=tario, mode="w") as cmdtar:
            cmdtar.add(save_dir, arcname=os.path.basename(os.path.normpath(save_dir)))
        cls.puts(data_path, tario.getvalue())

    @classmethod
    def restore_cache(cls, name, checksum, restore_dir):
        """
        Restore cache at `name` with `checksum` to `restore_dir`.
        """
        data_path = f"conducto-cache/{name}/{checksum}.tar.gz"
        if not cls.cache_exists(name, checksum):
            raise FileNotFoundError("Cache not found")
        byte_array = cls.gets(data_path)
        file_like = io.BytesIO(byte_array)
        tar = tarfile.open(fileobj=file_like)
        tar.extractall(path=restore_dir)

    @classmethod
    def url(cls, name, path_only=True):
        """
        Get url for object at `name`. `path_only` (default: True) will produce only the
        'path' portion of the URL, which is most suitable for viewing in the Conducto
        web app. If you need the result for viewing outside of the web app, set this to
        False.
        """
        pipeline_id = os.environ["CONDUCTO_PIPELINE_ID"]
        conducto_url = "" if path_only else os.environ["CONDUCTO_AUTO_URL"]
        qname = urllib.parse.quote(name)
        return f"{conducto_url}/gw/manager/data/{pipeline_id}/{cls.__name__}/{qname}"


class service(_Data):
    @staticmethod
    def _get_uri():
        return f"s3://{_Data._s3_bucket}/"

    @classmethod
    def _ctx(cls):
        cls._init()
        return _Context(local=False, uri=cls._get_uri(), service=True)


class superuser(_Data):
    @staticmethod
    def _get_uri():
        if _Data._local:
            return constants.ConductoPaths.get_profile_base_dir(expand=False) + "/"
        else:
            credentials = Credentials.creds()
            return f"s3://{_Data._s3_bucket}/{credentials['IdentityId']}"

    @classmethod
    def _main(cls):
        import conducto as co

        variables = {
            "delete": cls._delete_cli,
            "exists": cls._exists_cli,
            "get": cls._get_cli,
            "gets": cls._gets_cli,
            "list": cls._list_cli,
            "put": cls._put_cli,
            "puts": cls._puts_cli,
            "size": cls._size_cli,
            "url": cls._url_cli,
            "cache-exists": cls._cache_exists_cli,
            "clear-cache": cls._clear_cache_cli,
            "save-cache": cls._save_cache_cli,
            "restore-cache": cls._restore_cache_cli,
        }
        co.main(variables=variables, printer=cls._print)

    @classmethod
    def _delete_cli(cls, name, recursive=False, *, id=None, local: bool = None):
        """
        Delete object at `name`.
        """
        cls._init(pipeline_id=id, local=local)
        return cls.delete(name, recursive)

    @classmethod
    def _exists_cli(cls, name, *, id=None, local: bool = None):
        """
        Test if there is an object at `name`.
        """
        cls._init(pipeline_id=id, local=local)
        return cls.exists(name)

    @classmethod
    def _get_cli(cls, name, file, *, id=None, local: bool = None):
        """
        Get object at `name`, store it to `file`.
        """
        cls._init(pipeline_id=id, local=local)
        return cls.get(name, file)

    @classmethod
    def _gets_cli(
        cls, name, *, byte_range: typing.List[int] = None, id=None, local: bool = None
    ):
        """
        Read object stored at `name` and write it to stdout. Use `byte_range=start,end`
        to optionally specify a [start, end) range within the object to read.
        """
        cls._init(pipeline_id=id, local=local)
        obj = cls.gets(name, byte_range=byte_range)
        sys.stdout.buffer.write(obj)

    @classmethod
    def _list_cli(cls, prefix, *, id=None, local: bool = None):
        """
        Return names of objects that start with `prefix`.
        """
        cls._init(pipeline_id=id, local=local)
        return cls.list(prefix)

    @classmethod
    def _put_cli(cls, name, file, *, id=None, local: bool = None):
        """
        Store object in `file` to `name`.
        """
        cls._init(pipeline_id=id, local=local)
        return cls.put(name, file)

    @classmethod
    def _puts_cli(cls, name, *, id=None, local: bool = None):
        """
        Read object from stdin and store it to `name`.
        """
        cls._init(pipeline_id=id, local=local)
        obj = sys.stdin.buffer.read()
        return cls.puts(name, obj)

    @classmethod
    def _size_cli(cls, name, *, id=None, local: bool = None):
        """
        Return the size of the object at `name`, in bytes.
        """
        cls._init(pipeline_id=id, local=local)
        return cls.size(name)

    @classmethod
    def _url_cli(cls, name, path_only=True, *, id=None, local: bool = None):
        """
        Get url for object at `name`. `path_only` (default: True) will produce only the
        'path' portion of the URL, which is most suitable for viewing in the Conducto
        web app. If you need the result for viewing outside of the web app, set this to
        False.
        """
        cls._init(pipeline_id=id, local=local)
        return cls.url(name, path_only=path_only)

    @classmethod
    def _cache_exists_cli(cls, name, checksum, *, id=None, local: bool = None):
        """
        Test if there is a cache at `name` with `checksum`.
        """
        cls._init(pipeline_id=id, local=local)
        return cls.cache_exists(name, checksum)

    @classmethod
    def _clear_cache_cli(cls, name, checksum=None, *, id=None, local: bool = None):
        """
        Clear cache at `name` with `checksum`, clears all `name` cache if no `checksum`.
        """
        cls._init(pipeline_id=id, local=local)
        return cls.clear_cache(name, checksum)

    @classmethod
    def _save_cache_cli(cls, name, checksum, save_dir, *, id=None, local: bool = None):
        """
        Save `save_dir` to cache at `name` with `checksum`.
        """
        cls._init(pipeline_id=id, local=local)
        return cls.save_cache(name, checksum, save_dir)

    @classmethod
    def _restore_cache_cli(
        cls, name, checksum, restore_dir, *, id=None, local: bool = None
    ):
        """
        Restore cache at `name` with `checksum` to `restore_dir`.
        """
        cls._init(pipeline_id=id, local=local)
        return cls.restore_cache(name, checksum, restore_dir)

    @classmethod
    def _print(cls, val):
        if val is None:
            return
        if isinstance(val, bytes):
            val = val.decode()
        print(json.dumps(val))


class pipeline(superuser):
    @staticmethod
    def _get_uri():
        if _Data._pipeline_id:
            if _Data._local:
                return (
                    constants.ConductoPaths.get_local_path(
                        _Data._pipeline_id, expand=False
                    )
                    + "/data/"
                )
            else:
                credentials = Credentials.creds()
                return f"s3://{_Data._s3_bucket}/{credentials['IdentityId']}/pipelines/{_Data._pipeline_id}/data/"
        else:
            raise ContextError(
                "Pipeline-scoped data requires an initialized pipeline. "
                "Try using conducto.data.pipeline from a child node."
            )


class user(superuser):
    """
    See also :py:class:`pipeline` which has an identical interface.
    """

    @staticmethod
    def _get_uri():
        if _Data._local:
            return constants.ConductoPaths.get_profile_base_dir(expand=False) + "/data/"
        else:
            credentials = Credentials.creds()
            return f"s3://{_Data._s3_bucket}/{credentials['IdentityId']}/data/"


def _safe_join(*parts):
    parts = list(parts)
    parts[1:] = [p.lstrip(os.path.sep) for p in parts[1:]]
    return os.path.join(*parts)
