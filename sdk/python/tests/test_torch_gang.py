from __future__ import annotations

import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

torch = pytest.importorskip("torch")
pytestmark = pytest.mark.skipif(
    not Path("/proc/self/stat").exists(), reason="ranks register a Linux process identity"
)
import torch.distributed as dist
import torch.multiprocessing as mp

from kairo_sdk import AttemptSession, DistributedAdapter

MEMBERS, PER_MEMBER = 2, 2


class _GangHandler(BaseHTTPRequestHandler):
    """Two members' attempts (tokens tok_m0 and tok_m1). Member 1 holds its
    command from its first poll; member 0 gets its own only at its third
    (the daemon passing it on): the gang must wait for it."""

    kind = "suspend"
    refuse = ""  # a token whose checkpointed acknowledgement is refused (409)
    polls: dict[str, int] = {}
    published: set[str] = set()
    posts: list[tuple[str, str, dict]] = []
    lock = threading.Lock()

    def log_message(self, *_args) -> None:
        pass

    def _token(self) -> str:
        return self.headers.get("Authorization", "").removeprefix("Bearer ")

    def do_GET(self) -> None:  # noqa: N802
        token = self._token()
        member = token.removeprefix("tok_")
        with type(self).lock:
            type(self).polls[token] = type(self).polls.get(token, 0) + 1
            count = type(self).polls[token]
            done = token in type(self).published
        commands = []
        if not done and (member == "m1" or count >= 3):
            commands = [{"id": f"cmd_{member}", "kind": type(self).kind, "reason": "test", "delivery_count": 1}]
        self._send({"commands": commands})

    def do_POST(self) -> None:  # noqa: N802
        size = int(self.headers.get("Content-Length") or 0)
        body = json.loads(self.rfile.read(size) or b"{}")
        token = self._token()
        checkpointed = self.path.endswith("/acks") and body.get("phase") == "checkpointed"
        with type(self).lock:
            type(self).posts.append((token, self.path, body))
            if checkpointed and token == type(self).refuse:
                self._send({"error": "refused", "code": "conflict"}, 409)
                return
            if checkpointed:
                type(self).published.add(token)
        self._send({"ok": True})

    def _send(self, body: dict, status: int = 200) -> None:
        encoded = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)


def _member_worker(rank: int, init_file: str, result_dir: str, api_url: str) -> None:
    member, local = divmod(rank, PER_MEMBER)
    os.environ.update({"KAIRO_GANG_SIZE": str(MEMBERS), "KAIRO_GANG_RANK": str(member), "LOCAL_RANK": str(local)})
    dist.init_process_group("gloo", init_method=f"file://{init_file}", rank=rank, world_size=MEMBERS * PER_MEMBER)
    try:
        session = AttemptSession(
            api_url=api_url,
            execution_id=f"ex_m{member}",
            attempt_id=f"att_m{member}",
            token=f"tok_m{member}",
            poll_interval_seconds=0,
            heartbeat_interval_seconds=10_000,
        )
        adapter = DistributedAdapter(session)
        adapter.start()
        step = 0
        stopped_at = None
        checkpointed_at: list[int] = []

        def checkpoint(command):
            checkpointed_at.append(step)
            if local == 0:
                return Path(result_dir) / f"m{member}-step{step}.pt"
            return None

        error = None
        try:
            for step in range(8):
                if adapter.safe_point(checkpoint, {"unit": "step", "current": step, "total": 8}):
                    stopped_at = step
                    break
        except RuntimeError as exc:
            error = str(exc)
        (Path(result_dir) / f"rank-{rank}.json").write_text(
            json.dumps({"stopped_at": stopped_at, "checkpointed_at": checkpointed_at, "error": error}), encoding="utf-8"
        )
        adapter.close()
    finally:
        dist.destroy_process_group()


def _run_gang(tmp_path: Path) -> list[dict]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _GangHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        mp.spawn(
            _member_worker,
            args=(str((tmp_path / "gloo-init").resolve()), str(tmp_path.resolve()), f"http://127.0.0.1:{server.server_port}"),
            nprocs=MEMBERS * PER_MEMBER,
            join=True,
        )
        return [json.loads((tmp_path / f"rank-{r}.json").read_text()) for r in range(MEMBERS * PER_MEMBER)]
    finally:
        server.shutdown()
        server.server_close()


# A member whose part of a suspend is refused for good: every rank of the
# gang ends together (the gang cannot stop as one), none waits forever.
def test_a_refused_part_of_a_suspend_ends_every_rank(tmp_path: Path):
    _GangHandler.kind, _GangHandler.refuse = "suspend", "tok_m1"
    _GangHandler.polls, _GangHandler.published, _GangHandler.posts = {}, set(), []
    try:
        results = _run_gang(tmp_path)
    finally:
        _GangHandler.refuse = ""
    assert all(r["error"] and "refused" in r["error"] for r in results), results
    assert {r["stopped_at"] for r in results} == {None}


@pytest.mark.parametrize("kind", ["suspend", "checkpoint"])
def test_a_gang_checkpoints_together_once_every_member_holds_the_command(tmp_path: Path, kind: str):
    _GangHandler.kind = kind
    _GangHandler.polls, _GangHandler.published, _GangHandler.posts = {}, set(), []
    server = ThreadingHTTPServer(("127.0.0.1", 0), _GangHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        mp.spawn(
            _member_worker,
            args=(str((tmp_path / "gloo-init").resolve()), str(tmp_path.resolve()), f"http://127.0.0.1:{server.server_port}"),
            nprocs=MEMBERS * PER_MEMBER,
            join=True,
        )
        results = [json.loads((tmp_path / f"rank-{r}.json").read_text()) for r in range(MEMBERS * PER_MEMBER)]
        # every rank checkpointed once, at the same step: when member 0 got
        # its command too (its third poll, the third safe point)
        assert {tuple(r["checkpointed_at"]) for r in results} == {(2,)}
        if kind == "suspend":
            assert {r["stopped_at"] for r in results} == {2}
        else:
            assert {r["stopped_at"] for r in results} == {None}
        # each member registered its own processes with its own attempt
        for member in range(MEMBERS):
            ranks = {body["rank"] for token, path, body in _GangHandler.posts if token == f"tok_m{member}" and path == "/api/worker/processes"}
            assert ranks == {member * PER_MEMBER + i for i in range(PER_MEMBER)}
        # each member published its own part, of one round
        parts = {token: body["payload"] for token, path, body in _GangHandler.posts if path.endswith("/acks") and body["phase"] == "checkpointed"}
        assert set(parts) == {"tok_m0", "tok_m1"}
        assert parts["tok_m0"]["continuation_ref"].endswith("m0-step2.pt")
        assert parts["tok_m1"]["continuation_ref"].endswith("m1-step2.pt")
        assert parts["tok_m0"]["gang"]["round"] == parts["tok_m1"]["gang"]["round"]
        assert [parts[f"tok_m{m}"]["gang"]["rank"] for m in range(MEMBERS)] == [0, 1]
    finally:
        server.shutdown()
        server.server_close()
