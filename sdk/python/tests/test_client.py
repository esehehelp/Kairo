"""Operator read helpers, error classification on real failures, run_forever."""

from __future__ import annotations

import socket
import ssl
import urllib.error
from pathlib import Path

import pytest
from fake_daemon import FakeDaemon, Raw, serve

from kairo_sdk import (
    ControllerConfigurationError,
    ControllerJournal,
    KairoAPIError,
    KairoClient,
    KairoControllerClient,
    ProjectController,
    TransientControllerError,
    classify,
)
from kairo_sdk import _http

DATA = Path(__file__).parent / "data"


def event(sequence: int) -> dict:
    return {
        "sequence": sequence,
        "event_type": "attempt_started",
        "aggregate_type": "attempt",
        "aggregate_id": f"att_{sequence}",
        "coordination_epoch": 3,
        "payload": {"n": sequence},
    }


def test_whoami_and_project():
    routes = {
        ("GET", "/api/whoami"): (200, {"role": "read", "name": "dash"}),
        ("GET", "/api/projects/llm%2Fdevelop"): (200, {"project": {"name": "llm/develop"}}),
    }
    with serve(FakeDaemon(routes)) as daemon:
        client = KairoClient(daemon.url, token="kairo_read_x")
        assert client.whoami() == {"role": "read", "name": "dash"}
        assert client.project("llm/develop") == {"name": "llm/develop"}
    assert {r.headers["Authorization"] for r in daemon.requests} == {"Bearer kairo_read_x"}


def test_executions_and_attempts_pass_filters_and_limit():
    routes = {
        ("GET", "/api/executions?limit=100"): (200, {"executions": [{"id": "exe_1"}]}),
        ("GET", "/api/executions?project=p&state=running&limit=5"): (200, {"executions": []}),
        ("GET", "/api/attempts?project=p&execution_id=exe_1&state=active&limit=2"): (
            200,
            {"attempts": [{"id": "att_1"}]},
        ),
        ("GET", "/api/attempts?limit=100"): (200, {"attempts": None}),
    }
    with serve(FakeDaemon(routes)) as daemon:
        client = KairoClient(daemon.url, token="tok")
        assert client.executions() == [{"id": "exe_1"}]
        assert client.executions("p", "running", limit=5) == []
        assert client.attempts("p", "exe_1", "active", limit=2) == [{"id": "att_1"}]
        assert client.attempts() == []
        with pytest.raises(ValueError, match="limit"):
            client.executions(limit=0)


def test_events_iterate_pages_until_a_short_page():
    routes = {
        ("GET", "/api/events?after=0&limit=2"): (200, {"events": [event(1), event(2)]}),
        ("GET", "/api/events?after=2&limit=2"): (200, {"events": [event(3)]}),
    }
    with serve(FakeDaemon(routes)) as daemon:
        client = KairoClient(daemon.url, token="tok")
        events = list(client.events(page_size=2))
    assert [item.sequence for item in events] == [1, 2, 3]
    assert events[0].coordination_epoch == 3 and events[0].payload == {"n": 1}


def test_events_stop_on_an_empty_page_and_filter_by_project():
    routes = {
        ("GET", "/api/events?project=p&after=5&limit=1"): (200, {"events": [event(6)]}),
        ("GET", "/api/events?project=p&after=6&limit=1"): (200, {"events": []}),
    }
    with serve(FakeDaemon(routes)) as daemon:
        client = KairoClient(daemon.url, token="tok")
        assert [item.sequence for item in client.events("p", after=5, page_size=1)] == [6]


def test_events_refuse_a_non_advancing_cursor():
    routes = {("GET", "/api/events?after=10&limit=2"): (200, {"events": [event(9), event(10)]})}
    with serve(FakeDaemon(routes)) as daemon:
        client = KairoClient(daemon.url, token="tok")
        with pytest.raises(RuntimeError, match="did not advance"):
            list(client.events(after=10, page_size=2))


def log_slice(body: bytes, next_offset: int, size: int) -> tuple[int, Raw]:
    return 200, Raw(body, {"Kairo-Log-Next-Offset": str(next_offset), "Kairo-Log-Size": str(size)})


def test_attempt_log_returns_raw_bytes_and_offsets():
    routes = {
        ("GET", "/api/attempts/att_1/log?stream=stderr&offset=0"): log_slice(b"step 1\n\xff", 8, 8),
        ("GET", "/api/attempts/att_1/log?stream=stdout&offset=8&limit=4"): log_slice(b"", 8, 8),
        ("GET", "/api/attempts/att%2F2/log?stream=stderr&offset=0"): (
            404,
            {"error": "attempt att/2 has no stderr log", "code": "not_found"},
        ),
    }
    with serve(FakeDaemon(routes)) as daemon:
        client = KairoClient(daemon.url, token="kairo_read_x")
        assert client.attempt_log("att_1") == (b"step 1\n\xff", 8, 8)
        assert client.attempt_log("att_1", "stdout", offset=8, limit=4) == (b"", 8, 8)
        with pytest.raises(KairoAPIError) as missing:
            client.attempt_log("att/2")
        with pytest.raises(ValueError, match="stream"):
            client.attempt_log("att_1", "journal")
        with pytest.raises(ValueError, match="negative"):
            client.attempt_log("att_1", offset=-1)
    assert missing.value.status == 404 and missing.value.code == "not_found"
    assert {r.headers["Authorization"] for r in daemon.requests} == {"Bearer kairo_read_x"}


def test_attempt_log_refuses_a_response_without_offsets():
    routes = {("GET", "/api/attempts/att_1/log?stream=stderr&offset=0"): (200, Raw(b"x"))}
    with serve(FakeDaemon(routes)) as daemon:
        with pytest.raises(RuntimeError, match="Kairo-Log"):
            KairoClient(daemon.url, token="tok").attempt_log("att_1")


def test_attempt_log_transport_failure_is_transient():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    with pytest.raises(TransientControllerError):
        KairoClient(f"http://127.0.0.1:{port}", token="tok").attempt_log("att_1")


def test_operator_http_errors_are_classified_for_the_operator():
    routes = {
        ("GET", "/api/projects/missing"): (404, {"error": "not found", "code": "not_found"}),
        ("POST", "/api/executions"): (423, {"error": "gate closed", "code": "gate_closed"}),
    }
    with serve(FakeDaemon(routes)) as daemon:
        client = KairoControllerClient(daemon.url, token="tok")
        with pytest.raises(KairoAPIError) as missing:
            client.project("missing")
        with pytest.raises(KairoAPIError) as gated:
            client.submit_execution({"project": "p"})
    assert missing.value.classification == "permanent"  # an operator 404 is not "gone"
    assert gated.value.classification == "retry_later"
    assert str(missing.value).startswith("Kairo API returned HTTP 404: ")


def test_real_transport_failures_are_classified():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    opener = _http.build_opener()
    with pytest.raises(urllib.error.URLError) as refused:
        _http.request_json(opener, "GET", f"http://127.0.0.1:{port}/api/x", "tok", None, 5)
    assert classify(refused.value) == "transient"
    with pytest.raises(TransientControllerError):
        KairoClient(f"http://127.0.0.1:{port}", token="tok").whoami()


def test_tls_connection_drops_stay_transient():
    assert classify(urllib.error.URLError(ssl.SSLEOFError(8, "EOF occurred"))) == "transient"
    assert classify(ssl.SSLZeroReturnError(6, "closed")) == "transient"
    assert classify(urllib.error.URLError("unknown url type: ftp")) == "permanent"
    assert classify(ValueError("bug")) == "permanent"
    with pytest.raises(ValueError):
        classify("bogus")


def test_untrusted_tls_server_is_permanent_and_a_configuration_error():
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    class Quiet(ThreadingHTTPServer):
        def handle_error(self, *_args) -> None:
            pass

    server = Quiet(("127.0.0.1", 0), BaseHTTPRequestHandler)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(DATA / "server.pem", DATA / "server-key.pem")
    server.socket = context.wrap_socket(server.socket, server_side=True, do_handshake_on_connect=False)
    thread = threading.Thread(target=server.serve_forever, args=(0.05,), daemon=True)
    thread.start()
    url = f"https://127.0.0.1:{server.server_port}"
    try:
        opener = _http.build_opener(ca_file=DATA / "ca-other.pem")
        with pytest.raises(urllib.error.URLError) as raised:
            _http.request_json(opener, "GET", url + "/api/x", "tok", None, 5)
        assert classify(raised.value) == "permanent"
        client = KairoClient(url, token="tok", ca_file=DATA / "ca-other.pem")
        with pytest.raises(ControllerConfigurationError, match="CA file"):
            client.whoami()
    finally:
        server.shutdown()
        server.server_close()


def test_run_forever_retries_transient_api_errors_only(tmp_path: Path, monkeypatch):
    class API:
        def list_events(self, project, after=0):
            return []

        def submit_execution(self, request):
            raise AssertionError

    controller = ProjectController(API(), ControllerJournal(tmp_path / "db"))
    outcomes = [
        KairoAPIError(503, "internal", "locked"),
        KairoAPIError(423, "observe_only", "observe only"),
        KairoAPIError(403, "forbidden", "read token"),
    ]

    def run_once():
        raise outcomes.pop(0)

    sleeps: list[float] = []
    monkeypatch.setattr(controller, "run_once", run_once)
    monkeypatch.setattr("kairo_sdk.controller.time.sleep", sleeps.append)
    with pytest.raises(KairoAPIError) as raised:
        controller.run_forever(initial_backoff_seconds=0.25)
    assert raised.value.status == 403
    assert sleeps == [0.25, 0.5]


def test_operator_settings_hide_the_token():
    client = KairoClient("https://kairo.example:7474", token="kairo_admin_secret")
    assert "kairo_admin_secret" not in repr(client.settings)
    assert client.settings.url == "https://kairo.example:7474"
