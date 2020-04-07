import os
import re
import io
import tarfile


class _Context:
    def __init__(self, base):
        self.uri = os.environ[f"CONDUCTO_{base}_DATA_PATH"]
        if self.uri.startswith("s3://"):
            import boto3
            from conducto.api import Auth

            self.is_s3 = True

            auth = Auth()
            token = os.environ["CONDUCTO_DATA_TOKEN"]
            token = auth.get_refreshed_token(token)
            creds = auth.get_credentials(token)

            session = boto3.Session(
                aws_access_key_id=creds["AccessKeyId"],
                aws_secret_access_key=creds["SecretKey"],
                aws_session_token=creds["SessionToken"],
            )
            self.s3 = session.resource("s3")
            m = re.search("^s3://(.*?)/(.*)", self.uri)
            self.bucket, self.key_root = m.group(1, 2)
        else:
            self.uri = os.path.expanduser(self.uri)
            self.is_s3 = False

    def get_s3_key(self, name):
        return _safe_join(self.key_root, name)

    def get_s3_obj(self, name):
        return self.s3.Object(self.bucket, self.get_s3_key(name))

    def get_path(self, name):
        return _safe_join(self.uri, name)


class _Data:
    @staticmethod
    def _ctx():
        raise NotImplementedError()

    @classmethod
    def get(cls, name, file):
        ctx = cls._ctx()
        if ctx.is_s3:
            return ctx.get_s3_obj(name).download_file(file)
        else:
            import shutil

            shutil.copy(ctx.get_path(name), file)

    @classmethod
    def gets(cls, name, *, byte_range=None) -> bytes:
        ctx = cls._ctx()
        if ctx.is_s3:
            kwargs = {}
            if byte_range:
                begin, end = byte_range
                kwargs["Range"] = f"bytes {begin}-{end}"
            return ctx.get_s3_obj(name).get(**kwargs)["Body"].read()
        else:
            with open(ctx.get_path(name), "rb") as f:
                if byte_range:
                    begin, end = byte_range
                    f.seek(begin)
                    return f.read(end - begin)
                else:
                    return f.read()

    @classmethod
    def put(cls, name, file):
        ctx = cls._ctx()
        if ctx.is_s3:
            ctx.get_s3_obj(name).upload_file(file)
        else:
            import shutil

            path = ctx.get_path(name)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            shutil.copy(file, path)

    @classmethod
    def puts(cls, name, obj: bytes):
        ctx = cls._ctx()
        if ctx.is_s3:
            ctx.get_s3_obj(name).put(Body=obj)
        else:
            path = ctx.get_path(name)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as f:
                f.write(obj)

    @classmethod
    def delete(cls, name):
        ctx = cls._ctx()
        if ctx.is_s3:
            return ctx.get_s3_obj(name).delete()
        else:
            os.remove(ctx.get_path(name))

    @classmethod
    def list(cls, prefix):
        ctx = cls._ctx()
        if ctx.is_s3:
            bkt = ctx.s3.Bucket(ctx.bucket)
            return [obj.key for obj in bkt.objects.filter(Prefix=prefix)]
        else:
            path = ctx.get_path(prefix)
            try:
                names = os.listdir(path)
            except OSError:
                return []
            return [_safe_join(prefix, name) for name in sorted(names)]

    @classmethod
    def exists(cls, name):
        ctx = cls._ctx()
        if ctx.is_s3:
            import botocore.exceptions

            try:
                ctx.s3.head_object(Bucket=ctx.bucket, Key=ctx.get_s3_obj(name))
            except botocore.exceptions.ClientError:
                return False
            else:
                return True
        else:
            return os.path.exists(ctx.get_path(name))

    @classmethod
    def size(cls, name):
        ctx = cls._ctx()
        if ctx.is_s3:
            result = ctx.s3.head_object(Bucket=ctx.bucket, Key=ctx.get_s3_obj(name))
            return result["ContentLength"]
        else:
            return os.stat(ctx.get_path(name)).st_size

    @classmethod
    def clear_cache(cls, identifier, checksum=None):
        data_path = f"conducto-cache/{identifier}"
        if checksum is None:
            for file in cls.list(data_path):
                cls.delete(file)
        else:
            cls.delete(f"{data_path}/{checksum}.tar.gz")

    @classmethod
    def cache_exists(cls, identifier, checksum):
        data_path = f"conducto-cache/{identifier}/{checksum}.tar.gz"
        return cls.exists(data_path)

    @classmethod
    def save_cache(cls, save_dir, identifier, checksum):
        data_path = f"conducto-cache/{identifier}/{checksum}.tar.gz"
        tario = io.BytesIO()
        with tarfile.TarFile(fileobj=tario, mode="w") as cmdtar:
            cmdtar.add(save_dir, arcname=os.path.basename(os.path.normpath(save_dir)))
        cls.puts(data_path, tario.getvalue())

    @classmethod
    def restore_cache(cls, restore_dir, identifier, checksum):
        data_path = f"conducto-cache/{identifier}/{checksum}.tar.gz"
        if not cls.cache_exists(identifier, checksum):
            raise FileNotFoundError("Cache not found")
        byte_array = cls.gets(data_path)
        file_like = io.BytesIO(byte_array)
        tar = tarfile.open(fileobj=file_like)
        tar.extractall(path=restore_dir)

    @classmethod
    def url(cls, name):
        # Convert CamelCase to snake_case
        # https://stackoverflow.com/questions/1175208/elegant-python-function-to-convert-camelcase-to-snake-case
        data_type = re.sub(r"(?<!^)(?=[A-Z])", "_", cls.__name__).lower()

        pipeline_id = os.environ["CONDUCTO_PIPELINE_ID"]
        conducto_url = os.environ["CONDUCTO_AUTO_URL"]
        return f"{conducto_url}/pgw/data/{pipeline_id}/{data_type}/{name}"


class TempData(_Data):
    @staticmethod
    def _ctx():
        return _Context(base="TEMP")


class PermData(_Data):
    @staticmethod
    def _ctx():
        return _Context(base="PERM")


def _safe_join(*parts):
    parts = list(parts)
    parts[1:] = [p.lstrip(os.path.sep) for p in parts[1:]]
    return os.path.join(*parts)
