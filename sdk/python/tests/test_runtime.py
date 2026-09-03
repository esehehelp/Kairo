from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from kairo_sdk import AttemptSession, DistributedAdapter, SuspendResult, atomic_publish
from kairo_sdk.runtime import _current_process_identity


class _Handler(BaseHTTPRequestHandler):
    acks: list[dict] = []
    polls = 0
    posts: list[tuple[str, dict[str, str], dict]] = []

    def log_message(self, *_args) -> None:
        pass

    def do_GET(self) -> None:  # noqa: N802
        type(self).polls += 1
        self._send(
            {
                "commands": [
                    {
                        "id": "cmd_stable",
                        "kind": "suspend",
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


def test_same_command_may_execute_callback_more_than_once(tmp_path: Path):
    _Handler.acks = []
    _Handler.polls = 0
    _Handler.posts = []
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        session = AttemptSession(
            api_url=f"http://127.0.0.1:{server.server_port}",
            attempt_id="att_test",
            workload_id="wrk_test",
            poll_interval_seconds=0,
            heartbeat_interval_seconds=10_000,
        )
        calls: list[str] = []

        def checkpoint(context):
            calls.append(context.command_id)
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
    finally:
        server.shutdown()
        server.server_close()


def test_attempt_context_exposes_continuation(monkeypatch):
    monkeypatch.setenv("KAIRO_CONTINUATION_REF", "checkpoint://step-12345")
    session = AttemptSession(api_url=None, attempt_id=None, workload_id=None)
    assert session.continuation_ref == "checkpoint://step-12345"


def test_stop_file_compatibility_uses_stable_context(tmp_path: Path):
    stop = tmp_path / "STOP"
    stop.write_text("stop", encoding="utf-8")
    session = AttemptSession(api_url=None, attempt_id=None, workload_id=None, stop_paths=[stop])
    seen = []
    assert session.safe_point(checkpoint=lambda context: seen.append(context.command_id))
    assert seen[0].startswith("stopfile_")
    assert not stop.exists()


def test_context_manager_background_heartbeat_uses_epoch_headers():
    _Handler.posts = []
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        session = AttemptSession(
            api_url=f"http://127.0.0.1:{server.server_port}",
            attempt_id="att_v1",
            workload_id="task_v1",
            lease_id="lease_v1",
            coordination_epoch=7,
            heartbeat_interval_seconds=0.02,
        )
        session.set_progress({"unit": "step", "current": 3, "detail": {"loss": 1.2}})
        with session:
            deadline = time.monotonic() + 1
            while not _Handler.posts and time.monotonic() < deadline:
                time.sleep(0.01)
        path, headers, body = next(post for post in _Handler.posts if post[0].endswith("/heartbeat"))
        assert path == "/v1/worker/attempts/att_v1/heartbeat"
        assert headers["Kairo-Lease-Id"] == "lease_v1"
        assert headers["Kairo-Coordination-Epoch"] == "7"
        assert body["progress"]["detail"] == {"loss": 1.2}
    finally:
        server.shutdown()
        server.server_close()


def test_current_process_identity_uses_unix_nanoseconds():
    identity = _current_process_identity()
    pid_text, start_text = identity.removeprefix("pid:").split(":start:")
    assert int(pid_text) > 0
    assert 0 < time.time_ns() - int(start_text) < 60_000_000_000


def test_distributed_adapter_supports_single_process_suspend(tmp_path: Path):
    pytest.importorskip("torch")
    _Handler.acks = []
    _Handler.polls = 0
    _Handler.posts = []
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        session = AttemptSession(
            api_url=f"http://127.0.0.1:{server.server_port}",
            attempt_id="att_adapter",
            workload_id="task_adapter",
            lease_id="lease_adapter",
            coordination_epoch=11,
            poll_interval_seconds=0,
            heartbeat_interval_seconds=10_000,
        )
        adapter = DistributedAdapter(session)
        adapter.start()
        checkpoint = tmp_path / "step.pt"
        assert adapter.safe_point(
            lambda _command: checkpoint,
            {"unit": "step", "current": 4, "total": 10},
        )
        adapter.close()
        registration = next(
            body
            for path, _headers, body in _Handler.posts
            if path.endswith("/processes")
        )
        assert registration["pid"] > 0
        assert registration["process_identity"].startswith(
            f"pid:{registration['pid']}:start:"
        )
        assert [ack["phase"] for ack in _Handler.acks] == [
            "accepted",
            "checkpointing",
            "checkpointed",
        ]
    finally:
        server.shutdown()
        server.server_close()
