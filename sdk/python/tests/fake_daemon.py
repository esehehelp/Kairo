"""A small programmable Kairo API for unit tests."""

from __future__ import annotations

import json
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

Response = tuple[int, Any]
Route = Response | Callable[["Request"], Response]


@dataclass
class Raw:
    """A non-JSON response body (attempt logs), with extra headers."""

    body: bytes
    headers: dict[str, str] = field(default_factory=dict)


@dataclass
class Request:
    method: str
    path: str
    headers: dict[str, str]
    body: Any


@dataclass
class FakeDaemon:
    """Answers ``(method, path)`` from ``routes``; a list value is consumed in order.

    Paths match exactly, including the query string. Unrouted requests get
    404 ``not_found`` and are recorded in ``requests`` like every other.
    """

    routes: dict[tuple[str, str], Route | list[Route]] = field(default_factory=dict)
    requests: list[Request] = field(default_factory=list)
    url: str = ""
    lock: threading.Lock = field(default_factory=threading.Lock)

    def answer(self, request: Request) -> Response:
        with self.lock:
            self.requests.append(request)
            route = self.routes.get((request.method, request.path))
            if isinstance(route, list):
                route = route.pop(0) if len(route) > 1 else route[0]
        if route is None:
            return 404, {"error": "not found", "code": "not_found"}
        if callable(route):
            return route(request)
        return route

    def paths(self, method: str | None = None) -> list[str]:
        return [r.path for r in self.requests if method is None or r.method == method]


@contextmanager
def serve(daemon: FakeDaemon | None = None) -> Iterator[FakeDaemon]:
    daemon = daemon or FakeDaemon()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args) -> None:
            pass

        def do_GET(self) -> None:  # noqa: N802
            self._serve(None)

        def do_POST(self) -> None:  # noqa: N802
            length = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(length)
            self._serve(json.loads(raw) if raw else None)

        def _serve(self, body: Any) -> None:
            status, response = daemon.answer(
                Request(self.command, self.path, dict(self.headers), body)
            )
            headers = {"Content-Type": "application/json"}
            if isinstance(response, Raw):
                encoded = response.body
                headers = {"Content-Type": "text/plain; charset=utf-8", **response.headers}
            else:
                encoded = b"" if response is None else json.dumps(response).encode()
            self.send_response(status)
            for name, value in headers.items():
                self.send_header(name, value)
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, args=(0.05,), daemon=True)
    thread.start()
    daemon.url = f"http://127.0.0.1:{server.server_port}"
    try:
        yield daemon
    finally:
        server.shutdown()
        server.server_close()


OK: Response = (202, {"ok": True})


def worker_routes(
    extra: dict[str, Route | list[Route]] | None = None,
) -> dict[tuple[str, str], Route | list[Route]]:
    """The worker API answering OK, plus ``{"METHOD /path": route}`` overrides."""
    routes: dict[tuple[str, str], Route | list[Route]] = {
        ("POST", "/api/worker/processes"): OK,
        ("POST", "/api/worker/heartbeat"): OK,
        ("GET", "/api/worker/commands"): (200, {"commands": []}),
    }
    for key, value in (extra or {}).items():
        method, _, path = key.partition(" ")
        routes[(method, path)] = value
    return routes


def suspend(command_id: str = "cmd_1", kind: str = "suspend") -> Response:
    return 200, {
        "commands": [
            {"id": command_id, "kind": kind, "origin": "scope_pause", "reason": "paused", "delivery_count": 1}
        ]
    }
