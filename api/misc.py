import socket
import asyncio
import re
from .. import api
from ..shared import log


async def connect_to_ns(token=None):
    import websockets

    url = api.Config().get_url()
    ns_url = re.sub("^http", "ws", url) + "/ns/"
    log.debug("[run] Connecting to", ns_url)
    header = api.get_auth_headers(token)

    # we retry connection for roughly 2 minutes
    for i in range(45):
        try:
            websocket = await websockets.connect(ns_url, extra_headers=header)
            break
        except (
            websockets.ConnectionClosedError,
            websockets.InvalidStatusCode,
            socket.gaierror,
        ):
            log.debug(f"cannot connect to ns ... waiting {i}")
            await asyncio.sleep(min(3.0, (2 ** i) / 8))
    else:
        raise ConnectionError(f"Failed to connect to {ns_url}")

    log.debug("[run] ns Connected")
    return websocket


async def connect_to_pipeline(pipeline_id, token=None):
    import websockets

    gw_url = api.Config().get_url()
    gw_url = re.sub("^http", "ws", gw_url) + "/gw"

    uri = f"{gw_url}/manager/{pipeline_id}"
    log.debug("[run] Connecting to", uri)
    header = api.get_auth_headers(token)

    async def poor_mans_anext(ws):
        async for payload in ws:
            return payload

    # we retry connection for roughly 2 minutes
    for i in range(45):
        try:
            websocket = await websockets.connect(uri, extra_headers=header)
        except (
            websockets.ConnectionClosedError,
            websockets.InvalidStatusCode,
            websockets.InvalidMessage,
            socket.gaierror,
        ) as e:
            if getattr(e, "status_code", None) == 403:
                raise PermissionError(
                    "You are not permitted to connect to this pipeline."
                )
            log.debug(f"cannot connect to gw ... waiting {i}")
            await asyncio.sleep(min(3.0, (2 ** i) / 8))
            continue

        try:
            # This drops one payload if it happens in the first second.
            # This will be the "OK" packet that indicates that the server
            # is fully initialized.
            await asyncio.wait_for(poor_mans_anext(websocket), timeout=1.0)
            # TODO?: check this once the new gw is deployed everywhere?
            break
        except (websockets.ConnectionClosedError, socket.gaierror):
            await asyncio.sleep(min(3.0, (2 ** i) / 8))
            continue
    else:
        raise ConnectionError(f"Failed to connect to {uri}")
    return websocket
