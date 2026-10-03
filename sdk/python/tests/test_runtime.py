from __future__ import annotations

import base64
import json
import os
import ssl
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from kairo_sdk import AttemptSession, DistributedAdapter, SuspendResult, atomic_publish
from kairo_sdk.runtime import _current_process_identity, _parse_proc_stat_starttime

DATA = Path(__file__).parent / "data"


class _Handler(BaseHTTPRequestHandler):
    acks: list[dict] = []
    polls = 0
    gets: list[tuple[str, dict[str, str]]] = []
    posts: list[tuple[str, dict[str, str], dict]] = []
    command_kind = "suspend"
    command_id = "cmd_stable"

    def log_message(self, *_args) -> None:
        pass

    def do_GET(self) -> None:  # noqa: N802
        type(self).polls += 1
        type(self).gets.append((self.path, dict(self.headers)))
        self._send(
            {
                "commands": [
                    {
                        "id": type(self).command_id,
                        "kind": type(self).command_kind,
                        "reason": "test",
                        "delivery_count": type(self).polls,
                    }
                ]
            }
        )

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length") or 0)
        body = json.loads(self.rfile.read(length) or b"{}")
        type(self).posts.append((self.path, dict(self.headers), body))
        if self.path.endswith("/acks"):
            type(self).acks.append(body)
        self._send({"ok": True})

    def _send(self, body: dict) -> None:
        encoded = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)


@pytest.fixture
def control_server():
    _Handler.acks = []
    _Handler.polls = 0
    _Handler.gets = []
    _Handler.posts = []
    _Handler.command_kind = "suspend"
    _Handler.command_id = "cmd_stable"
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()


def managed_session(api_url: str, name: str, **overrides) -> AttemptSession:
    options = {
        "poll_interval_seconds": 0,
        "heartbeat_interval_seconds": 10_000,
        **overrides,
    }
    return AttemptSession(
        api_url=api_url,
        execution_id=f"ex_{name}",
        attempt_id=f"att_{name}",
        token=f"tok_{name}",
        **options,
    )


def test_same_command_may_execute_callback_more_than_once(tmp_path: Path, control_server):
    session = managed_session(control_server, "test")
    calls: list[str] = []

    def checkpoint(context):
        calls.append(context.command_id)
        assert context.execution_id == "ex_test"
        path = tmp_path / f"{context.command_id}.json"

        def write(temporary: Path) -> None:
            temporary.write_text('{"step":12345}', encoding="utf-8")

        atomic_publish(path, write)
        return SuspendResult(str(path), {"step": 12345})

    assert session.safe_point(checkpoint=checkpoint)
    assert session.safe_point(checkpoint=checkpoint)
    assert calls == ["cmd_stable", "cmd_stable"]
    assert json.loads((tmp_path / "cmd_stable.json").read_text()) == {"step": 12345}
    phases = [ack["phase"] for ack in _Handler.acks]
    assert phases == [
        "accepted",
        "checkpointing",
        "checkpointed",
        "accepted",
        "checkpointing",
        "checkpointed",
    ]
    assert {path for path, _headers in _Handler.gets} == {"/api/worker/commands"}
    assert {path for path, _headers, _body in _Handler.posts} == {
        "/api/worker/processes",
        "/api/worker/heartbeat",
        "/api/worker/commands/cmd_stable/acks",
    }


def test_every_request_carries_the_attempt_token_and_no_lease_headers(control_server):
    session = managed_session(control_server, "auth")
    session.heartbeat({"unit": "step", "current": 1})
    assert session.poll_commands()[0]["id"] == "cmd_stable"

    sent = [headers for _path, headers in _Handler.gets]
    sent += [headers for _path, headers, _body in _Handler.posts]
    assert len(sent) == 3
    for headers in sent:
        assert headers["Authorization"] == "Bearer tok_auth"
        assert "Kairo-Lease-Id" not in headers
        assert "Kairo-Coordination-Epoch" not in headers


def test_command_ids_are_quoted_into_one_path_segment(control_server):
    _Handler.command_id = "cmd/../odd id"
    session = managed_session(control_server, "quote")
    assert session.safe_point(checkpoint=lambda _context: "ckpt://quoted")
    ack_paths = {path for path, _headers, _body in _Handler.posts if path.endswith("/acks")}
    assert ack_paths == {"/api/worker/commands/cmd%2F..%2Fodd%20id/acks"}


def test_attempt_context_exposes_continuation(monkeypatch):
    monkeypatch.setenv("KAIRO_CONTINUATION_REF", "checkpoint://step-12345")
    session = AttemptSession(api_url=None, execution_id=None, attempt_id=None)
    assert session.input_continuation_ref == "checkpoint://step-12345"


def test_managed_session_requires_execution_attempt_and_token():
    with pytest.raises(ValueError, match="execution, attempt, and token"):
        AttemptSession(
            api_url="http://127.0.0.1:7474",
            execution_id="ex_tokenless",
            attempt_id="att_tokenless",
        )


def test_unmanaged_session_rejects_attempt_context():
    with pytest.raises(ValueError, match="unmanaged sessions"):
        AttemptSession(api_url=None, execution_id=None, attempt_id=None, token="tok")


def test_stop_file_compatibility_uses_stable_context(tmp_path: Path):
    stop = tmp_path / "STOP"
    stop.write_text("stop", encoding="utf-8")
    session = AttemptSession(
        api_url=None,
        execution_id=None,
        attempt_id=None,
        stop_paths=[stop],
    )
    seen = []
    assert session.safe_point(checkpoint=lambda context: seen.append(context.command_id))
    assert seen[0].startswith("stopfile_")
    assert not stop.exists()


def test_managed_session_rejects_stop_files(tmp_path: Path):
    stop = tmp_path / "STOP"
    stop.write_text("stop", encoding="utf-8")
    with pytest.raises(ValueError, match="cannot use stop_paths"):
        AttemptSession(
            api_url="http://127.0.0.1:7474",
            execution_id="ex_managed",
            attempt_id="att_managed",
            token="tok_managed",
            stop_paths=[stop],
        )
    assert stop.exists()


def _ca_der() -> bytes:
    return ssl.PEM_cert_to_DER_cert((DATA / "ca.pem").read_text(encoding="ascii"))


def test_environment_reads_attempt_token_and_ca(monkeypatch):
    monkeypatch.setenv("KAIRO_API_URL", "https://127.0.0.1:7474/")
    monkeypatch.setenv("KAIRO_EXECUTION_ID", "ex_environment")
    monkeypatch.setenv("KAIRO_ATTEMPT_ID", "att_environment")
    monkeypatch.setenv("KAIRO_ATTEMPT_TOKEN", "tok_environment")
    monkeypatch.setenv("KAIRO_API_CA", base64.b64encode(_ca_der()).decode("ascii"))
    monkeypatch.setenv("KAIRO_NODE_ID", "node_environment")

    session = AttemptSession.from_environment()

    assert session.managed
    assert session.api_url == "https://127.0.0.1:7474"
    assert session.execution_id == "ex_environment"
    assert session.attempt_id == "att_environment"
    assert session.node_id == "node_environment"
    assert not hasattr(session, "lease_id")
    assert not hasattr(session, "coordination_epoch")


@pytest.mark.parametrize("missing", ["KAIRO_EXECUTION_ID", "KAIRO_ATTEMPT_ID", "KAIRO_ATTEMPT_TOKEN"])
def test_managed_environment_requires_execution_attempt_and_token(monkeypatch, missing):
    monkeypatch.setenv("KAIRO_API_URL", "http://127.0.0.1:7474")
    monkeypatch.setenv("KAIRO_EXECUTION_ID", "ex_environment")
    monkeypatch.setenv("KAIRO_ATTEMPT_ID", "att_environment")
    monkeypatch.setenv("KAIRO_ATTEMPT_TOKEN", "tok_environment")
    monkeypatch.delenv(missing)
    with pytest.raises(RuntimeError, match=f"is missing {missing}"):
        AttemptSession.from_environment()


@pytest.mark.parametrize(
    "stray", ["KAIRO_EXECUTION_ID", "KAIRO_ATTEMPT_ID", "KAIRO_ATTEMPT_TOKEN", "KAIRO_API_CA"]
)
def test_unmanaged_environment_rejects_attempt_context(monkeypatch, stray):
    for name in ("KAIRO_API_URL", "KAIRO_EXECUTION_ID", "KAIRO_ATTEMPT_ID", "KAIRO_ATTEMPT_TOKEN", "KAIRO_API_CA"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv(stray, "c3RyYXk=")
    with pytest.raises(RuntimeError, match="unmanaged Kairo context"):
        AttemptSession.from_environment()


def test_environment_rejects_malformed_ca(monkeypatch):
    monkeypatch.setenv("KAIRO_API_URL", "https://127.0.0.1:7474")
    monkeypatch.setenv("KAIRO_EXECUTION_ID", "ex_environment")
    monkeypatch.setenv("KAIRO_ATTEMPT_ID", "att_environment")
    monkeypatch.setenv("KAIRO_ATTEMPT_TOKEN", "tok_environment")
    monkeypatch.setenv("KAIRO_API_CA", "-----BEGIN CERTIFICATE-----")
    with pytest.raises(RuntimeError, match="KAIRO_API_CA must be a base64 DER certificate"):
        AttemptSession.from_environment()


def test_context_manager_background_heartbeat_reports_progress(control_server):
    session = managed_session(control_server, "beat", heartbeat_interval_seconds=0.02)
    session.set_progress({"unit": "step", "current": 3, "detail": {"loss": 1.2}})
    with session:
        deadline = time.monotonic() + 1
        while not any(post[0].endswith("/heartbeat") for post in _Handler.posts) and time.monotonic() < deadline:
            time.sleep(0.01)
    path, headers, body = next(post for post in _Handler.posts if post[0].endswith("/heartbeat"))
    assert path == "/api/worker/heartbeat"
    assert headers["Authorization"] == "Bearer tok_beat"
    assert "Kairo-Lease-Id" not in headers
    assert body["progress"]["detail"] == {"loss": 1.2}


def test_current_process_identity_uses_unix_nanoseconds():
    identity = _current_process_identity()
    if os.name == "nt":
        pid_text, start_text = identity.removeprefix("pid:").split(":start:")
        assert int(pid_text) > 0
        assert 0 < time.time_ns() - int(start_text) < 60_000_000_000
    else:
        pid_text, start_text = identity.removeprefix("proc:").split(":starttime:")
        assert int(pid_text) == os.getpid()
        assert int(start_text) > 0


def test_proc_stat_parser_reads_field_22_with_spaces_and_parentheses_in_comm():
    fields = ["S", *(str(index) for index in range(4, 22)), "987654", "unused"]
    stat_text = f"42 (worker name) with parens) {' '.join(fields)}"
    assert _parse_proc_stat_starttime(stat_text) == 987654


def test_distributed_adapter_supports_single_process_suspend(tmp_path: Path, control_server):
    pytest.importorskip("torch")
    session = managed_session(control_server, "adapter")
    adapter = DistributedAdapter(session)
    adapter.start()
    checkpoint = tmp_path / "step.pt"
    assert adapter.safe_point(
        lambda _command: checkpoint,
        {"unit": "step", "current": 4, "total": 10},
    )
    adapter.close()
    assert [path for path, _headers in _Handler.gets] == ["/api/worker/commands"]
    registration = next(
        body
        for path, _headers, body in _Handler.posts
        if path == "/api/worker/processes"
    )
    assert registration["pid"] > 0
    if os.name == "nt":
        assert registration["process_identity"].startswith(
            f"pid:{registration['pid']}:start:"
        )
    else:
        assert registration["process_identity"].startswith(
            f"proc:{registration['pid']}:starttime:"
        )
    assert [ack["phase"] for ack in _Handler.acks] == [
        "accepted",
        "checkpointing",
        "checkpointed",
    ]


def test_unknown_worker_command_is_skipped_without_an_ack(control_server):
    _Handler.command_kind = "cancel"
    session = managed_session(control_server, "unknown")
    called = False

    def checkpoint(_context):
        nonlocal called
        called = True

    assert not session.safe_point(checkpoint=checkpoint)
    assert not called
    assert _Handler.acks == []
