"""Streams (issue #43): the records a Kairo loader hands this rank, in the
stream's fixed order.

A loader listens where ``KAIRO_STREAM_<NAME>`` says (``unix:PATH`` or
``tcp:HOST:PORT``; NAME upper-cased, anything not a letter or digit an
underscore). Each rank of an attempt takes its own records: by default the
rank and the ranks' count are ``RANK`` and ``WORLD_SIZE`` (as torchrun sets
them), else 0 and 1.

``Stream.position`` is where this rank stands: ``record`` records of its own
taken in full and ``sub`` outputs of the next. A connection that breaks is
made again at that position, so nothing is taken twice or missed.

    from kairo_sdk import streams
    with streams.open("mix-v9") as train:
        for item in train:      # bytes
            ...

The protocol is in internal/loader/server.go.
"""

from __future__ import annotations

import json
import os
import re
import socket
import struct
import time
from typing import Any, Callable, Iterator

PROTOCOL_VERSION = 1
_HELLO, _READY, _CREDIT, _ITEM, _SKIP, _END, _ERROR = 1, 2, 3, 4, 5, 6, 7
_MAX_FRAME = (256 << 20) + 64


class StreamError(RuntimeError):
    """The loader said the stream failed, or it could not be reached again."""


def env_name(name: str) -> str:
    """The environment variable a stream's address is in."""
    return "KAIRO_STREAM_" + re.sub(r"[^A-Za-z0-9]", "_", name).upper()


def open(name: str, rank: int | None = None, world: int | None = None, **kwargs: Any) -> "Stream":  # noqa: A001
    """The stream ``name`` of this attempt, for this rank."""
    address = os.environ.get(env_name(name))
    if not address:
        raise StreamError(f"no stream {name} here: {env_name(name)} is not set")
    if rank is None:
        rank = int(os.environ.get("RANK", "0"))
    if world is None:
        world = int(os.environ.get("WORLD_SIZE", "1"))
    return Stream(address, rank, world, name=name, **kwargs)


class Stream:
    """One rank's records of a stream, an iterator of ``bytes``."""

    def __init__(
        self,
        address: str,
        rank: int = 0,
        world: int = 1,
        *,
        name: str = "",
        window: int = 256,
        position: dict[str, int] | None = None,
        reconnect_seconds: float = 120.0,
    ) -> None:
        self.address = address
        self.name = name or address
        self.rank = rank
        self.world = world
        self.window = max(2, window)
        self.reconnect_seconds = reconnect_seconds
        self.position: dict[str, int] = {"rank": rank, "record": 0, "sub": 0}
        self._sock: socket.socket | None = None
        self._reader: Any = None
        self._outstanding = 0
        self._ended = False
        self._connect(position)

    # -- the connection -----------------------------------------------------

    def _connect(self, at: dict[str, int] | None) -> None:
        family, target = self._target()
        sock = socket.socket(family, socket.SOCK_STREAM)
        try:
            sock.connect(target)
            hello: dict[str, Any] = {"version": PROTOCOL_VERSION, "rank": self.rank, "world": self.world}
            if at is not None:
                hello["position"] = {"record": int(at["record"]), "sub": int(at.get("sub", 0))}
            sock.sendall(_frame(_HELLO, json.dumps(hello).encode()))
            reader = sock.makefile("rb", buffering=1 << 20)
            kind, payload = _read_frame(reader)
        except BaseException:
            sock.close()
            raise
        if kind == _ERROR:
            sock.close()
            raise StreamError(f"stream {self.name}: {payload.decode(errors='replace')}")
        if kind != _READY:
            sock.close()
            raise StreamError(f"stream {self.name}: a hello answered with frame {kind}")
        ready = json.loads(payload)
        self._close_socket()
        self._sock, self._reader, self._outstanding = sock, reader, 0
        self.position = {"rank": self.rank, "record": int(ready["position"]["record"]), "sub": int(ready["position"]["sub"])}

    def _target(self) -> tuple[int, Any]:
        if self.address.startswith("unix:"):
            return socket.AF_UNIX, self.address[len("unix:") :]
        if self.address.startswith("tcp:"):
            host, _, port = self.address[len("tcp:") :].rpartition(":")
            return socket.AF_INET, (host, int(port))
        raise StreamError(f"stream {self.name}: address {self.address!r} is neither unix:PATH nor tcp:HOST:PORT")

    def _reconnect(self, cause: BaseException) -> None:
        deadline = time.monotonic() + self.reconnect_seconds
        pause = 0.5
        while True:
            try:
                self._connect(self.position)
                return
            except StreamError:
                raise
            except OSError as err:
                if time.monotonic() > deadline:
                    raise StreamError(f"stream {self.name}: the loader went ({cause}) and did not come back: {err}") from err
            time.sleep(pause)
            pause = min(pause * 2, 10.0)

    def _close_socket(self) -> None:
        if self._reader is not None:
            self._reader.close()
            self._reader = None
        if self._sock is not None:
            self._sock.close()
            self._sock = None

    def close(self) -> None:
        self._close_socket()

    def __enter__(self) -> "Stream":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- the records ---------------------------------------------------------

    def __iter__(self) -> Iterator[bytes]:
        return self

    def __next__(self) -> bytes:
        if self._ended:
            raise StopIteration
        while True:
            try:
                return self._next()
            except StopIteration:
                self._ended = True
                raise
            except (OSError, EOFError) as err:
                self._reconnect(err)

    def _next(self) -> bytes:
        while True:
            if self._outstanding < self.window // 2:
                more = self.window - self._outstanding
                assert self._sock is not None
                self._sock.sendall(_frame(_CREDIT, struct.pack("<I", more)))
                self._outstanding += more
            kind, payload = _read_frame(self._reader)
            if kind == _ITEM:
                record, sub, flags = struct.unpack_from("<QIB", payload)
                if flags & 1:
                    self.position = {"rank": self.rank, "record": record + 1, "sub": 0}
                else:
                    self.position = {"rank": self.rank, "record": record, "sub": sub + 1}
                self._outstanding -= 1
                return payload[13:]
            if kind == _SKIP:
                (record,) = struct.unpack("<Q", payload)
                self.position = {"rank": self.rank, "record": record + 1, "sub": 0}
                continue
            if kind == _END:
                raise StopIteration
            if kind == _ERROR:
                raise StreamError(f"stream {self.name}: {payload.decode(errors='replace')}")
            raise StreamError(f"stream {self.name}: a frame of type {kind}")


def dataset(name: str, decode: Callable[[bytes], Any] | None = None, **kwargs: Any) -> Any:
    """A torch IterableDataset over the stream ``name`` for this rank (each
    item ``decode(bytes)`` when given). Use it with ``num_workers=0``: one
    process takes a rank's records, and the loader already works in
    parallel. Its ``stream`` holds the position."""
    import torch.utils.data

    class StreamDataset(torch.utils.data.IterableDataset):  # type: ignore[misc]
        def __init__(self) -> None:
            self.stream: Stream | None = None

        def __iter__(self) -> Iterator[Any]:
            info = torch.utils.data.get_worker_info()
            if info is not None:
                raise StreamError(f"stream {name}: the training process takes a rank's records itself (its position goes with its checkpoints): use num_workers=0")
            if self.stream is None:
                self.stream = open(name, **kwargs)
            for item in self.stream:
                yield decode(item) if decode is not None else item

    return StreamDataset()


def _frame(kind: int, payload: bytes) -> bytes:
    return struct.pack("<IB", len(payload) + 1, kind) + payload


def _read_frame(reader: Any) -> tuple[int, bytes]:
    head = reader.read(4)
    if len(head) < 4:
        raise EOFError("the loader closed the connection")
    (size,) = struct.unpack("<I", head)
    if size < 1 or size > _MAX_FRAME:
        raise StreamError(f"a frame of {size} bytes")
    body = reader.read(size)
    if len(body) < size:
        raise EOFError("the loader closed the connection")
    return body[0], body[1:]
