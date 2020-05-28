import asyncio
import concurrent.futures
import functools
from http import HTTPStatus as hs
import json
import typing

from conducto.shared import types as t, request_utils


async def eval_in_thread(pool, cb, *args, **kwargs):
    with pool:
        return await asyncio.get_running_loop().run_in_executor(
            pool, functools.partial(cb, *args, **kwargs)
        )


def async_helper(wrapped_class):
    class inner:
        def __init__(self, pool=None):
            self.wrapped_inst = wrapped_class()
            self.pool = pool

        def __getattr__(self, attribute):
            value = getattr(self.wrapped_inst, attribute)
            if attribute in self.wrapped_inst.__dict__:
                return value
            else:
                if self.pool is None:
                    pool = concurrent.futures.ThreadPoolExecutor(1)
                else:
                    pool = self.pool

                async def fxn(*args, **kwargs):
                    return await eval_in_thread(pool, value, *args, **kwargs)

                return fxn

    return inner


def is_conducto_url(url):
    import urllib.error

    test_endpoint = f"{url}/auth/idtoken"
    # no actual auth needed, just checking for not getting a 404 and
    # name resolution.
    try:
        r = request_utils.get(test_endpoint)
        return r.status_code == 401
    except urllib.error.URLError:
        return False


class InvalidResponse(Exception):
    def __init__(self, *args, status_code=None, url=None):
        super().__init__(*args)
        self.status_code = status_code
        self.url = url

    def __str__(self):
        return (
            f"{super().__str__()}\n  status_code={self.status_code}\n  url={self.url}"
        )


def get_auth_headers(token: t.Token):
    return {
        "content-type": "application/json",
        "Authorization": "Bearer {}".format(token),
    }


def get_data(response) -> typing.Union[None, dict, list]:
    if "application/json" not in response.headers["content-type"]:
        raise InvalidResponse(
            response.read(), status_code=response.status_code, url=response.url
        )
    if response.status_code == hs.NO_CONTENT:
        return None
    data = json.loads(response.read())
    if response.status_code != hs.OK:
        raise InvalidResponse(
            data["message"] if "message" in data else data,
            status_code=response.status_code,
            url=response.url if hasattr(response, "url") else "",
        )
    return data


def get_text(response) -> str:
    if "text/plain" in response.headers["content-type"]:
        return response.read().decode("utf-8")
    else:
        # Call _get_data to parse response and throw an Exception. If it
        # doesn't throw one, raise a new one
        data = get_data(response)
        raise InvalidResponse(
            f"Got unexpected result from {response}: {data}",
            status_code=response.status_code,
            url=response.url if hasattr(response, "url") else "",
        )
