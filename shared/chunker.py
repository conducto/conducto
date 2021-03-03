import json
import math
import typing
import uuid

from . import constants

MAX_SIZE = constants.GwParams.MAX_STR_CHARS // 2


async def send_json(send_str_func, msg):
    msg_str = json.dumps(msg)
    msg_str_len = len(msg_str)

    if msg_str_len > MAX_SIZE:
        total_chunks = int(math.ceil(msg_str_len / MAX_SIZE))
        msg_id = str(uuid.uuid4())
        for chunk_idx in range(total_chunks):
            start = chunk_idx * MAX_SIZE
            end = min(start + MAX_SIZE, msg_str_len)
            chunk_str = json.dumps(
                {
                    "type": "CHUNKED_MESSAGE",
                    "orig_type": msg.get("type", None),
                    "id": msg_id,
                    "idx": chunk_idx,
                    "nchunks": total_chunks,
                    "payload": msg_str[start:end],
                }
            )
            await send_str_func(chunk_str)
    else:
        await send_str_func(msg_str)


async def recv_msg(recv_func):
    receiver = ChunkReceiver()
    while True:
        raw = await recv_func()
        msg = receiver.process(raw)
        if not isinstance(msg, PartialMessage):
            return msg


class ChunkReceiver:
    def __init__(self):
        self.msgs: typing.Dict[str, PartialMessage] = {}

    def process(self, msg) -> typing.Union["PartialMessage", dict, list, str, bool]:
        """
        Return non-chunked messages untouched. Assemble chunked `msg`s piece by piece.
        If it's done, return the combined object. If unfinished, return the
        `PartialMessage`.
        """
        if msg["type"] == "CHUNKED_MESSAGE":
            if msg["id"] not in self.msgs:
                self.msgs[msg["id"]] = PartialMessage(msg)
            partial = self.msgs[msg["id"]]
            partial.add(msg)
            if partial.done():
                output = partial.combine()
                del self.msgs[msg["id"]]
                return output
            else:
                return partial
        else:
            return msg


class PartialMessage:
    def __init__(self, msg):
        self.chunks = [None] * msg["nchunks"]
        self.num_received = 0
        self.type = msg["orig_type"]

    def add(self, msg):
        idx = msg["idx"]
        if self.chunks[idx] is not None:
            raise IndexError(f"Duplicate message for index: {idx}")

        self.chunks[idx] = msg["payload"]
        self.num_received += 1

    def done(self):
        return self.num_received == len(self.chunks)

    def combine(self):
        return json.loads("".join(self.chunks))
