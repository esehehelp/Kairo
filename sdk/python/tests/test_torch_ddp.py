from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

torch = pytest.importorskip("torch")
import torch.distributed as dist
import torch.multiprocessing as mp

from kairo_sdk import AttemptSession, DistributedAdapter


class _ControlHandler(BaseHTTPRequestHandler):
    posts: list[tuple[str, dict]] = []
    lock = threading.Lock()

    def log_message(self, *_args) -> None:
        pass

    def do_GET(self) -> None:  # noqa: N802
        self._send(
            {
                "commands": [
                    {
                        "id": "cmd_ddp",
                        "kind": "suspend",
                        "reason": "test",
                        "delivery_count": 1,
                    }
                ]
            }
        )

    def do_POST(self) -> None:  # noqa: N802
        size = int(self.headers.get("Content-Length") or 0)
        body = json.loads(self.rfile.read(size) or b"{}")
        with type(self).lock:
            type(self).posts.append((self.path, body))
        self._send({"ok": True})

    def _send(self, body: dict) -> None:
        encoded = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)


def _ddp_worker(
    rank: int, world_size: int, init_file: str, result_dir: str, api_url: str
) -> None:
    dist.init_process_group(
        "gloo",
        init_method=f"file://{init_file}",
        rank=rank,
        world_size=world_size,
    )
    try:
        session = AttemptSession(
            api_url=api_url,
            execution_id="ex_ddp",
            attempt_id="att_ddp",
            lease_id="lease_ddp",
            coordination_epoch=3,
            poll_interval_seconds=0,
            heartbeat_interval_seconds=10_000,
        )
        adapter = DistributedAdapter(session)
        adapter.start()

        def checkpoint(_command):
            marker = Path(result_dir) / f"rank-{rank}.checkpointed"
            marker.write_text("ok", encoding="utf-8")
            if rank == 0:
                return Path(result_dir) / "step_00000012.pt"
            return None

        stopped = adapter.safe_point(
            checkpoint,
            {"unit": "optimizer_step", "current": 12, "total": 20},
        )
        (Path(result_dir) / f"rank-{rank}.json").write_text(
            json.dumps({"stopped": stopped}), encoding="utf-8"
        )
        adapter.close()
    finally:
        dist.destroy_process_group()


def test_two_rank_gloo_checkpoints_before_rank_zero_ack(tmp_path: Path):
    _ControlHandler.posts = []
    server = ThreadingHTTPServer(("127.0.0.1", 0), _ControlHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    init_file = tmp_path / "gloo-init"
    try:
        mp.spawn(
            _ddp_worker,
            args=(
                2,
                str(init_file.resolve()),
                str(tmp_path.resolve()),
                f"http://127.0.0.1:{server.server_port}",
            ),
            nprocs=2,
            join=True,
        )
        assert json.loads((tmp_path / "rank-0.json").read_text())["stopped"]
        assert json.loads((tmp_path / "rank-1.json").read_text())["stopped"]
        assert (tmp_path / "rank-0.checkpointed").is_file()
        assert (tmp_path / "rank-1.checkpointed").is_file()
        registrations = [
            body
            for path, body in _ControlHandler.posts
            if path.endswith("/processes")
        ]
        assert {item["rank"] for item in registrations} == {0, 1}
        checkpointed = [
            body
            for path, body in _ControlHandler.posts
            if path.endswith("/acks") and body["phase"] == "checkpointed"
        ]
        assert len(checkpointed) == 1
        assert checkpointed[0]["payload"]["continuation_ref"].endswith(
            "step_00000012.pt"
        )
    finally:
        server.shutdown()
        server.server_close()
