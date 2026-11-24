"""The stream client against a fake loader speaking the protocol of
internal/loader/server.go."""

from __future__ import annotations

import json
import socket
import struct
import threading

import pytest

from kairo_sdk import streams


def _frame(kind: int, payload: bytes) -> bytes:
    return struct.pack("<IB", len(payload) + 1, kind) + payload


def _read(conn: socket.socket) -> tuple[int, bytes]:
    head = b""
    while len(head) < 4:
        chunk = conn.recv(4 - len(head))
        if not chunk:
            raise EOFError
        head += chunk
    (size,) = struct.unpack("<I", head)
    body = b""
    while len(body) < size:
        chunk = conn.recv(size - len(body))
        if not chunk:
            raise EOFError
        body += chunk
    return body[0], body[1:]


class FakeLoader:
    """Records 0..n-1 of rank 0, each one item; record 3 gives none, record 5
    two. It drops the first connection after `drop_after` items."""

    def __init__(self, n: int = 10, drop_after: int | None = None) -> None:
        self.n = n
        self.drop_after = drop_after
        self.hellos: list[dict] = []
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.bind(("127.0.0.1", 0))
        self.server.listen()
        self.address = "tcp:127.0.0.1:%d" % self.server.getsockname()[1]
        threading.Thread(target=self._serve, daemon=True).start()

    def _items(self, record: int, sub: int):
        for k in range(record, self.n):
            outs = [] if k == 3 else [b"r%d" % k, b"r%d-bis" % k] if k == 5 else [b"r%d" % k]
            if not outs:
                yield _frame(5, struct.pack("<Q", k)), False
                continue
            for s, data in enumerate(outs):
                if k == record and s < sub:
                    continue
                yield _frame(4, struct.pack("<QIB", k, s, 1 if s == len(outs) - 1 else 0) + data), True
        yield _frame(6, json.dumps({"reason": "the stream ended"}).encode()), False

    def _serve(self) -> None:
        connections = 0
        while True:
            try:
                conn, _ = self.server.accept()
            except OSError:
                return
            connections += 1
            kind, payload = _read(conn)
            hello = json.loads(payload)
            self.hellos.append(hello)
            at = hello.get("position") or {"record": 0, "sub": 0}
            conn.sendall(_frame(2, json.dumps({"position": {"rank": 0, **at}, "world": 1}).encode()))
            credit, sent = 0, 0
            try:
                for frame, counts in self._items(at["record"], at["sub"]):
                    while counts and credit == 0:
                        kind, payload = _read(conn)
                        credit += struct.unpack("<I", payload)[0]
                    if connections == 1 and self.drop_after is not None and sent == self.drop_after:
                        break
                    conn.sendall(frame)
                    if counts:
                        credit -= 1
                        sent += 1
            except (EOFError, OSError):
                pass
            conn.close()

    def close(self) -> None:
        self.server.close()


def test_a_rank_takes_its_records_in_order_and_knows_where_it_stands() -> None:
    loader = FakeLoader()
    try:
        with streams.Stream(loader.address, 0, 1, window=4) as s:
            got = []
            positions = []
            for item in s:
                got.append(item)
                positions.append(dict(s.position))
        assert got == [b"r0", b"r1", b"r2", b"r4", b"r5", b"r5-bis", b"r6", b"r7", b"r8", b"r9"]
        # after the first output of record 5, it stands at its second
        assert positions[4] == {"rank": 0, "record": 5, "sub": 1}
        assert positions[5] == {"rank": 0, "record": 6, "sub": 0}
        # record 3 gave nothing: after record 2 the next item was record 4's
        assert positions[2] == {"rank": 0, "record": 3, "sub": 0}
    finally:
        loader.close()


def test_a_broken_connection_is_made_again_where_the_rank_stands() -> None:
    loader = FakeLoader(drop_after=5)
    try:
        s = streams.Stream(loader.address, 0, 1, window=4)
        got = list(s)
        assert got == [b"r0", b"r1", b"r2", b"r4", b"r5", b"r5-bis", b"r6", b"r7", b"r8", b"r9"]
        assert loader.hellos[0].get("position") is None
        assert loader.hellos[1]["position"] == {"record": 5, "sub": 1}
    finally:
        loader.close()


def test_open_takes_the_address_and_rank_from_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    loader = FakeLoader(n=2)
    try:
        monkeypatch.setenv("KAIRO_STREAM_MIX_V9", loader.address)
        monkeypatch.setenv("RANK", "0")
        monkeypatch.setenv("WORLD_SIZE", "1")
        assert streams.env_name("mix-v9") == "KAIRO_STREAM_MIX_V9"
        with streams.open("mix-v9") as s:
            assert list(s) == [b"r0", b"r1"]
        monkeypatch.delenv("KAIRO_STREAM_MIX_V9")
        with pytest.raises(streams.StreamError):
            streams.open("mix-v9")
    finally:
        loader.close()
