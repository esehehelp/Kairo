"""Replay sdk/conformance/*.json, the contract every Kairo client follows."""

from __future__ import annotations

import base64
import dataclasses
import json
import os
import re
import socket
import ssl
import sys
import threading
import urllib.error
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest

from kairo_sdk import (
    AttemptFenced,
    AttemptRetired,
    AttemptSession,
    KairoAPIError,
    KairoControllerClient,
    NoContinuationError,
    SuspendResult,
    classify,
    resolve_operator_settings,
)
from kairo_sdk import _http, errors
from kairo_sdk.runtime import process_identity_from_filetime, process_identity_from_proc_stat

CONFORMANCE = Path(__file__).resolve().parents[2] / "conformance"
DATA = Path(__file__).parent / "data"


def fixture(name: str) -> dict[str, Any]:
    return json.loads((CONFORMANCE / name).read_text(encoding="utf-8"))


def cases(name: str, key: str = "cases") -> list[Any]:
    loaded = fixture(name)[key]
    return [pytest.param(case, id=case.get("name") or _case_id(case)) for case in loaded]


def _case_id(case: dict[str, Any]) -> str:
    return "-".join(str(value) for value in case.values() if not isinstance(value, dict))


def _ca_der(name: str) -> bytes:
    return ssl.PEM_cert_to_DER_cert((DATA / name).read_text(encoding="ascii"))


# --------------------------------------------------------------------------
# loopback.json, token_transport.json


@pytest.mark.parametrize("case", cases("loopback.json"))
def test_loopback(case):
    assert _http.is_loopback(case["host"]) is case["loopback"]


@pytest.mark.parametrize("case", cases("token_transport.json"))
def test_token_transport(case):
    if case["allowed"]:
        _http.require_token_transport(case["url"])
    else:
        with pytest.raises(ValueError) as raised:
            _http.require_token_transport(case["url"])
        assert "pw" not in str(raised.value)  # never echo userinfo


# --------------------------------------------------------------------------
# operator_resolution.json


@pytest.fixture
def operator_dirs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Path]:
    """'{config}' and '{tmp}' for a case, with the config dir located as Go does."""
    if os.name == "nt":
        root = tmp_path / "appdata"
        monkeypatch.setenv("APPDATA", str(root))
    elif sys.platform == "darwin":
        monkeypatch.setenv("HOME", str(tmp_path / "home"))
        root = tmp_path / "home" / "Library" / "Application Support"
    else:
        root = tmp_path / "xdg"
        monkeypatch.setenv("XDG_CONFIG_HOME", str(root))
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    return {"{config}": root / "kairo", "{tmp}": scratch}


def _expand(text: str, dirs: dict[str, Path]) -> str:
    for placeholder, directory in dirs.items():
        text = text.replace(placeholder, str(directory))
    return text


@pytest.mark.parametrize("case", cases("operator_resolution.json"))
def test_operator_resolution(case, operator_dirs, monkeypatch):
    for name, content in case["files"].items():
        path = Path(_expand(name, operator_dirs))
        path.parent.mkdir(parents=True, exist_ok=True)
        if content == "<CA>":
            content = (DATA / "ca.pem").read_text(encoding="ascii")
        path.write_bytes(content.encode("utf-8"))  # byte for byte: keep "\r\n"
    for name, value in case["env"].items():
        monkeypatch.setenv(name, _expand(value, operator_dirs))
    api_url = case.get("arg", {}).get("url")
    expect = case["expect"]

    if "error" in expect:
        with pytest.raises((ValueError, RuntimeError), match=re.escape(expect["error"])):
            resolve_operator_settings(api_url=api_url)
        with pytest.raises((ValueError, RuntimeError), match=re.escape(expect["error"])):
            KairoControllerClient(api_url)
        return

    settings = resolve_operator_settings(api_url=api_url)
    expected_ca = Path(_expand(expect["ca"], operator_dirs)) if expect["ca"] else None
    assert (settings.url, settings.token, settings.ca_file) == (
        expect["url"],
        expect["token"],
        expected_ca,
    )
    client = KairoControllerClient(api_url)  # resolution never touches the network
    assert (client.api_url, client._token, client.ca_file) == (
        expect["url"],
        expect["token"],
        expected_ca,
    )
    assert expect["token"] is None or expect["token"] not in repr(settings)


# --------------------------------------------------------------------------
# worker_env.json

_CA_PLACEHOLDERS = {
    "<CA1>": lambda: base64.b64encode(_ca_der("ca.pem")).decode("ascii"),
    "<CA1+CA2>": lambda: base64.b64encode(_ca_der("ca.pem") + _ca_der("ca-other.pem")).decode(
        "ascii"
    ),
}


@pytest.mark.parametrize("case", cases("worker_env.json"))
def test_worker_environment(case, monkeypatch):
    for name, value in case["env"].items():
        monkeypatch.setenv(name, _CA_PLACEHOLDERS[value]() if value in _CA_PLACEHOLDERS else value)
    expect = case["expect"]

    if "error" in expect:
        with pytest.raises((ValueError, RuntimeError), match=re.escape(expect["error"])):
            AttemptSession.from_environment()
        return

    session = AttemptSession.from_environment()
    assert session.managed is expect["managed"]
    if not expect["managed"]:
        assert session.gang is None
        return
    gang = dataclasses.asdict(session.gang) if session.gang is not None else None
    assert {
        "managed": session.managed,
        "api_url": session.api_url,
        "execution_id": session.execution_id,
        "attempt_id": session.attempt_id,
        "ca_certificates": len(session.ca_certificates),
        "resource_ids": list(session.resource_ids),
        "resource_bindings": list(session.resource_bindings),
        "continuation_ref": session.input_continuation_ref,
        "gang": gang,
    } == expect


# --------------------------------------------------------------------------
# identity.json (both platforms' rules, on every OS)


@pytest.mark.parametrize("case", cases("identity.json", "linux"))
def test_linux_process_identity(case):
    assert process_identity_from_proc_stat(case["pid"], case["stat"]) == case["expect"]


@pytest.mark.parametrize("case", cases("identity.json", "windows"))
def test_windows_process_identity(case):
    assert process_identity_from_filetime(case["pid"], case["creation_filetime"]) == case["expect"]


# --------------------------------------------------------------------------
# classify.json


def _transport_error(kind: str) -> urllib.error.URLError:
    reason: BaseException = {
        "refused": lambda: ConnectionRefusedError(10061, "connection refused"),
        "reset": lambda: ConnectionResetError(10054, "connection reset by peer"),
        "timeout": lambda: TimeoutError("timed out"),
        "dns": lambda: socket.gaierror(11001, "getaddrinfo failed"),
        "tls_verify": lambda: ssl.SSLCertVerificationError(1, "certificate verify failed"),
        "tls_handshake": lambda: ssl.SSLError(1, "[SSL: TLSV1_ALERT_PROTOCOL_VERSION]"),
    }[kind]()
    return urllib.error.URLError(reason)


@pytest.mark.parametrize("case", cases("classify.json"))
def test_classify(case):
    role, expected = case["role"], case["class"]
    if "transport" in case:
        error = _transport_error(case["transport"])
        assert errors.transport_kind(error) == case["transport"]
        assert classify(error, role=role) == expected
        assert classify(error.reason, role=role) == expected
        assert classify(case["transport"], role=role) == expected
        return
    status, code = case["status"], case["code"]
    assert classify(status, code, role=role) == expected
    body = json.dumps({"error": "message", "code": code}) if code else ""
    error = errors.api_error(status, body.encode(), role=role)
    assert error.classification == expected
    assert classify(error) == expected
    assert (error.status, error.code) == (status, code)
    assert str(error) == f"Kairo API returned HTTP {status}: {body}"
    assert isinstance(error, RuntimeError)
    if expected == "retired":
        assert isinstance(error, AttemptRetired)
    elif expected == "fenced":
        assert isinstance(error, AttemptFenced)
    else:
        assert type(error) is KairoAPIError


# --------------------------------------------------------------------------
# worker_scenarios.json

TOKEN = "kairo_worker_scenario"


class _CallbackError(Exception):
    pass


class _ScriptedDaemon:
    """Serves the exchanges in order; anything else is recorded as a failure."""

    def __init__(self) -> None:
        self.exchanges: list[dict[str, Any]] = []
        self.failures: list[str] = []
        self.setup = True
        self.lock = threading.Lock()

    def handle(self, method: str, path: str, headers: dict[str, str], body: Any) -> tuple[int, Any]:
        with self.lock:
            if headers.get("Authorization") != f"Bearer {TOKEN}":
                self.failures.append(f"{method} {path}: Authorization {headers.get('Authorization')!r}")
            if self.setup:  # registration and the first heartbeat, before the scenario
                if method == "POST" and path in ("/api/worker/processes", "/api/worker/heartbeat"):
                    return 202, {"ok": True}
                self.failures.append(f"unexpected setup request {method} {path}")
                return 500, {"error": "unexpected", "code": "internal"}
            if not self.exchanges:
                self.failures.append(f"extra request {method} {path} {body}")
                return 500, {"error": "unexpected", "code": "internal"}
            exchange = self.exchanges.pop(0)
            request = exchange["request"]
            if (method, path) != (request["method"], request["path"]):
                self.failures.append(
                    f"expected {request['method']} {request['path']}, got {method} {path}"
                )
            elif "body" in request and not _contains(body, request["body"]):
                self.failures.append(f"{method} {path}: body {body} lacks {request['body']}")
            response = exchange["response"]
            return response["status"], response.get("body")


def _contains(actual: Any, expected: Any) -> bool:
    if isinstance(expected, dict):
        return isinstance(actual, dict) and all(
            key in actual and _contains(actual[key], value) for key, value in expected.items()
        )
    return actual == expected


@pytest.fixture
def scripted_daemon() -> Iterator[tuple[_ScriptedDaemon, str]]:
    daemon = _ScriptedDaemon()

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
            status, response = daemon.handle(self.command, self.path, dict(self.headers), body)
            encoded = b"" if response is None else json.dumps(response).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, args=(0.05,), daemon=True)
    thread.start()
    try:
        yield daemon, f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()


def _callback(spec: dict[str, Any]):
    calls: list[str] = []

    def checkpoint(command):
        calls.append(command.command_id)
        if "raises" in spec:
            raise _CallbackError(spec["raises"])
        returned = spec["returns"]
        return SuspendResult(
            continuation_ref=returned.get("continuation_ref"),
            payload=returned.get("payload", {}),
        )

    return checkpoint, calls


_ERROR_TYPES = {
    "callback": _CallbackError,
    "no_continuation": NoContinuationError,
    "retired": AttemptRetired,
    "fenced": AttemptFenced,
}


@pytest.mark.parametrize("scenario", cases("worker_scenarios.json", "scenarios"))
def test_worker_scenario(scenario, scripted_daemon):
    daemon, url = scripted_daemon
    session = AttemptSession(
        api_url=url,
        execution_id="exe_scenario",
        attempt_id="att_scenario",
        token=TOKEN,
        poll_interval_seconds=0,
        heartbeat_interval_seconds=10_000,
    )
    session.heartbeat({"unit": "step", "current": 1})  # registered, heartbeat just sent
    daemon.setup = False
    daemon.exchanges = list(scenario["exchanges"])
    checkpoint, _calls = _callback(scenario["callback"])
    expect = scenario["expect"]

    if "result" in expect:
        assert session.safe_point(checkpoint=checkpoint) is expect["result"]
        assert session.suspend_handled is expect["result"]
    else:
        with pytest.raises(BaseException) as raised:
            session.safe_point(checkpoint=checkpoint)
        expected_type = _ERROR_TYPES.get(expect["error"], KairoAPIError)
        assert isinstance(raised.value, expected_type), raised.value
        if expect["error"] in ("retired", "fenced", "transient", "permanent"):
            assert raised.value.classification == expect["error"]
        assert session.retired is (expect["error"] == "retired")
        assert session.fenced is (expect["error"] == "fenced")
        assert not session.suspend_handled

    assert daemon.failures == []
    assert daemon.exchanges == [], "the worker did not send every expected request"
