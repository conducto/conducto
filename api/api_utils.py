import asyncio
import concurrent.futures
import functools
from http import HTTPStatus as hs
import json
import typing

from conducto.shared import types as t, request_utils


class UserInputValidation(Exception):
    # Any exception derived from this should be given a message that is
    # understandable to an end-user with no further traceback.  They are
    # assumed to be expected errors with clear resolution.
    pass


class NoTracebackError(UserInputValidation):
    pass


class UserPermissionError(UserInputValidation):
    pass


class UserPathError(UserInputValidation):
    pass


class WSLMapError(UserPathError):
    pass


class WindowsMapError(UserPathError):
    pass


class InvalidResponse(Exception):
    def __init__(self, *args, status_code=None, url=None, content_type=None):
        super().__init__(*args)
        self.message = args[0] if len(args) > 0 else None
        self.status_code = status_code
        self.url = url
        self.content_type = content_type

    def __str__(self):
        return (
            f"{super().__str__()}\n"
            f"  status_code={self.status_code}\n"
            f"  url={self.url}\n"
            f"  content_type={self.content_type}"
        )


# All responses of unauthorized (401) imply a response of invalid request (400 range)
class UnauthorizedResponse(InvalidResponse):
    pass


async def eval_in_thread(pool, cb, *args, **kwargs):
    # with pool: will cause the pool to shut down after executing and is only good for one call
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


def get_auth_headers(token: t.Token = None, refresh=True, force_refresh=False):
    if token is None:
        from . import config

        token = config.Config().get_token(refresh=refresh, force_refresh=force_refresh)
        if token is None:
            raise ValueError(
                "Cannot authenticate to Conducto services because no token is available."
            )
    return {
        "content-type": "application/json",
        "Authorization": f"Bearer {token}",
    }


def get_data(
    response, content_type="application/json"
) -> typing.Union[None, dict, list, str, int, float, typing.Dict[str, dict]]:
    """
        Handle responses that have errors, and return the JSON-parsed result. May return
        None for certain status codes that have no data.
    """
    url = response.url if hasattr(response, "url") else ""
    res = response.read()
    status = response.status_code
    return _get_data(response.headers, url, res, status, content_type)


async def aiohttp_get_data(
    response, content_type="application/json"
) -> typing.Union[None, dict, list, str, int, float, typing.Dict[str, dict]]:
    """
        Handle responses that have errors, and return the JSON-parsed result. May return
        None for certain status codes that have no data.
    """
    url = response.url if hasattr(response, "url") else ""
    res = await response.read()
    status = response.status
    return _get_data(response.headers, url, res, status, content_type)


def _get_data(headers, url, res, status, content_type="application/json"):
    if content_type not in headers["content-type"]:
        raise InvalidResponse(
            res, status_code=status, url=url, content_type=headers["content-type"]
        )
    if len(res) == 0:
        if status in [hs.CREATED, hs.ACCEPTED, hs.NO_CONTENT]:
            return None
        else:
            raise InvalidResponse(
                res, status_code=status, url=url, content_type=headers["content-type"]
            )

    data = json.loads(res)

    if not 200 <= status < 299:
        message = data["message"] if "message" in data else data

        if status == hs.UNAUTHORIZED:
            raise UnauthorizedResponse(
                message,
                status_code=status,
                url=url,
                content_type=headers["content-type"],
            )
        else:
            raise InvalidResponse(
                message,
                status_code=status,
                url=url,
                content_type=headers["content-type"],
            )

    return data


def get_text(response, content_type="text/plain") -> str:
    if content_type in response.headers["content-type"]:
        return response.read().decode("utf-8")
    else:
        # Call _get_data to parse response and throw an Exception. If it
        # doesn't throw one, raise a new one
        data = get_data(response)
        raise InvalidResponse(
            f"Got unexpected result from {response}: {data}",
            status_code=response.status_code,
            url=response.url if hasattr(response, "url") else "",
            content_type=response.headers["content-type"],
        )


async def aiohttp_get_text(response, content_type="text/plain") -> str:
    if content_type in response.headers["content-type"]:
        return (await response.read()).decode("utf-8")
    else:
        # Call _get_data to parse response and throw an Exception. If it
        # doesn't throw one, raise a new one
        data = await aiohttp_get_data(response)
        raise InvalidResponse(
            f"Got unexpected result from {response}: {data}",
            status_code=response.status,
            url=response.url if hasattr(response, "url") else "",
            content_type=response.headers["content-type"],
        )
