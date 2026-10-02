from __future__ import annotations

import base64
import json
import socket
import ssl
import threading
import urllib.error
import urllib.request
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from kairo_sdk import AttemptSession, KairoControllerClient, TransientControllerError
from kairo_sdk import _http

DATA = Path(__file__).parent / "data"


class _Handler(BaseHTTPRequestHandler):
    requests: list[tuple[str, str, dict[str, str]]] = []
    response_body: object = {"events": [], "commands": [], "ok": True}

    def log_message(self, *_args) -> None:
        pass

    def do_GET(self) -> None:  # noqa: N802
        self._handle()

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length") or 0)
        self.rfile.read(length)
        self._handle()

    def _handle(self) -> None:
        type(self).requests.append((self.command, self.path, dict(self.headers)))
        if self.path.startswith("/redirect"):
            self.send_response(302)
            self.send_header("Location", "/target")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        encoded = json.dumps(type(self).response_body).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)


class _QuietServer(ThreadingHTTPServer):
    def handle_error(self, _request, _client_address) -> None:
        pass  # failed handshakes are the point of several tests


@contextmanager
def serve(*, tls: bool, maximum_version: ssl.TLSVersion | None = None) -> Iterator[str]:
    _Handler.requests = []
    server = _QuietServer(("127.0.0.1", 0), _Handler)
    if tls:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(DATA / "server.pem", DATA / "server-key.pem")
        if maximum_version is not None:
            context.maximum_version = maximum_version
        server.socket = context.wrap_socket(
            server.socket, server_side=True, do_handshake_on_connect=False
        )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"{'https' if tls else 'http'}://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()


def ca_der(name: str = "ca.pem") -> bytes:
    return ssl.PEM_cert_to_DER_cert((DATA / name).read_text(encoding="ascii"))


def dead_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


@pytest.mark.parametrize(
    "trust",
    [{"ca_der": ca_der()}, {"ca_file": DATA / "ca.pem"}, {"ca_file": str(DATA / "ca.pem")}],
    ids=["der", "path", "str"],
)
def test_private_ca_is_trusted(trust):
    opener = _http.build_opener(**trust)
    with serve(tls=True) as url:
        assert _http.request_json(opener, "GET", url + "/api/x", "tok", None, 5)["ok"]
        assert _http.request_json(opener, "POST", url + "/api/y", "tok", {"a": 1}, 5)["ok"]
    assert [(method, path) for method, path, _headers in _Handler.requests] == [
        ("GET", "/api/x"),
        ("POST", "/api/y"),
    ]
    assert all(headers["Authorization"] == "Bearer tok" for *_rest, headers in _Handler.requests)


@pytest.mark.parametrize(
    "trust",
    [{"ca_der": ca_der("ca-other.pem")}, {"ca_file": DATA / "ca-other.pem"}, {}],
    ids=["other-der", "other-path", "system"],
)
def test_unrelated_ca_is_rejected(trust):
    opener = _http.build_opener(**trust)
    with serve(tls=True) as url:
        with pytest.raises(urllib.error.URLError) as raised:
            _http.request_json(opener, "GET", url + "/api/x", "tok", None, 5)
    assert isinstance(raised.value.reason, ssl.SSLCertVerificationError)
    assert _Handler.requests == []


def test_tls_below_1_3_is_refused():
    opener = _http.build_opener(ca_der=ca_der())
    with serve(tls=True, maximum_version=ssl.TLSVersion.TLSv1_2) as url:
        with pytest.raises(urllib.error.URLError) as raised:
            _http.request_json(opener, "GET", url + "/api/x", "tok", None, 5)
    assert isinstance(raised.value.reason, ssl.SSLError)
    assert _Handler.requests == []


def test_both_ca_forms_at_once_are_ambiguous():
    with pytest.raises(ValueError, match="not both"):
        _http.build_opener(ca_der=ca_der(), ca_file=DATA / "ca.pem")


@pytest.mark.parametrize("method", ["GET", "POST"])
@pytest.mark.parametrize("tls", [False, True], ids=["http", "https"])
def test_redirects_are_not_followed(method, tls):
    opener = _http.build_opener(ca_der=ca_der())
    with serve(tls=tls) as url:
        with pytest.raises(RuntimeError, match="Kairo API returned HTTP 302"):
            _http.request_json(opener, method, url + "/redirect", "tok", {} if method == "POST" else None, 5)
    assert [path for _method, path, _headers in _Handler.requests] == ["/redirect"]


@pytest.mark.parametrize("tls", [False, True], ids=["http", "https"])
def test_environment_proxies_are_ignored(monkeypatch, tls):
    proxy = f"http://127.0.0.1:{dead_port()}"
    for name in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
        monkeypatch.setenv(name, proxy)
    for name in ("NO_PROXY", "no_proxy"):
        monkeypatch.delenv(name, raising=False)
    opener = _http.build_opener(ca_der=ca_der())
    with serve(tls=tls) as url:
        # The stock opener honours the dead proxy, so the setting is live.
        stock = urllib.request.build_opener(
            urllib.request.HTTPSHandler(context=_http.ssl_context(ca_der=ca_der()))
        )
        with pytest.raises(urllib.error.URLError):
            stock.open(url + "/api/stock", timeout=5)
        assert _http.request_json(opener, "GET", url + "/api/x", "tok", None, 5)["ok"]
    assert [path for _method, path, _headers in _Handler.requests] == ["/api/x"]


def test_non_object_response_is_rejected(monkeypatch):
    monkeypatch.setattr(_Handler, "response_body", [])
    with serve(tls=False) as url:
        with pytest.raises(RuntimeError, match="must be a JSON object"):
            _http.request_json(_http.build_opener(), "GET", url + "/api/x", "tok", None, 5)


def _attempt_environment(monkeypatch, url: str, ca_name: str) -> None:
    monkeypatch.setenv("KAIRO_API_URL", url)
    monkeypatch.setenv("KAIRO_EXECUTION_ID", "ex_tls")
    monkeypatch.setenv("KAIRO_ATTEMPT_ID", "att_tls")
    monkeypatch.setenv("KAIRO_ATTEMPT_TOKEN", "tok_tls")
    monkeypatch.setenv("KAIRO_API_CA", base64.b64encode(ca_der(ca_name)).decode("ascii"))


def test_attempt_session_trusts_the_launch_ca(monkeypatch):
    with serve(tls=True) as url:
        _attempt_environment(monkeypatch, url, "ca.pem")
        session = AttemptSession.from_environment()
        session.heartbeat({"unit": "step", "current": 1})
        assert session.poll_commands() == []
    assert [(method, path) for method, path, _headers in _Handler.requests] == [
        ("POST", "/api/worker/processes"),
        ("POST", "/api/worker/heartbeat"),
        ("GET", "/api/worker/commands"),
    ]
    assert {headers["Authorization"] for *_rest, headers in _Handler.requests} == {"Bearer tok_tls"}


def test_attempt_session_rejects_a_server_outside_the_launch_ca(monkeypatch):
    with serve(tls=True) as url:
        _attempt_environment(monkeypatch, url, "ca-other.pem")
        session = AttemptSession.from_environment()
        with pytest.raises(urllib.error.URLError) as raised:
            session.heartbeat()
    assert isinstance(raised.value.reason, ssl.SSLCertVerificationError)


def test_controller_client_trusts_ca_file():
    with serve(tls=True) as url:
        client = KairoControllerClient(url, token="tok_controller", ca_file=DATA / "ca.pem")
        assert client.list_events("llm-develop") == []
    method, path, headers = _Handler.requests[0]
    assert (method, path) == ("GET", "/api/events?project=llm-develop&after=0&limit=1000")
    assert headers["Authorization"] == "Bearer tok_controller"


def test_controller_client_treats_untrusted_server_as_transient():
    with serve(tls=True) as url:
        client = KairoControllerClient(url, token="tok", ca_file=DATA / "ca-other.pem")
        with pytest.raises(TransientControllerError) as raised:
            client.list_events("llm-develop")
    assert isinstance(raised.value.__cause__, urllib.error.URLError)
    assert isinstance(raised.value.__cause__.reason, ssl.SSLCertVerificationError)
