from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

torch = pytest.importorskip("torch")
import torch.distributed as dist
import torch.multiprocessing as mp

from kairo_sdk import AttemptSession, DistributedAdapter

CHECKPOINT = bytes(range(256)) * 10 + b"tail"  # 2564 bytes: several chunks and a short last one
REF = os.path.join("ckpt", "step_00000012.pt")


def _worker(rank: int, init_file: str, root: str, case: str) -> None:
    # Each rank works in its own directory and Kairo hands both the same relative
    # reference: it names rank 0's file, like a path that exists only on rank 0's node.
    home = Path(root) / f"rank{rank}"
    home.mkdir(parents=True, exist_ok=True)
    os.chdir(home)
    if rank == 0 or case == "present":
        Path("ckpt").mkdir(exist_ok=True)
        Path(REF).write_bytes(CHECKPOINT)
    if rank == 1 and case == "conflict":
        Path("dest").mkdir(exist_ok=True)
        Path("dest", "step_00000012.pt").write_bytes(b"another run")
    os.environ["KAIRO_CONTINUATION_REF"] = REF
    dist.init_process_group("gloo", init_method=f"file://{init_file}", rank=rank, world_size=2)
    result: dict = {}
    try:
        session = AttemptSession(
            api_url="http://127.0.0.1:9",
            execution_id="ex_cont",
            attempt_id="att_cont",
            token="tok_cont",
        )
        adapter = DistributedAdapter(session)
        try:
            local = adapter.local_continuation("dest", chunk_bytes=1000)
            result = {"local": local, "bytes": Path(local).read_bytes() == CHECKPOINT}
        except RuntimeError as exc:
            result = {"error": str(exc)}
        result["leftovers"] = sorted(p.name for p in Path("dest").glob("*.partial-*")) if Path("dest").exists() else []
    finally:
        dist.destroy_process_group()
    Path(root, f"rank{rank}.json").write_text(json.dumps(result), encoding="utf-8")


def _run(tmp_path: Path, case: str) -> tuple[dict, dict]:
    mp.spawn(
        _worker,
        args=(str((tmp_path / "init").resolve()), str(tmp_path.resolve()), case),
        nprocs=2,
        join=True,
    )
    return tuple(json.loads((tmp_path / f"rank{r}.json").read_text()) for r in (0, 1))


def test_missing_rank_receives_rank_zero_file(tmp_path: Path):
    rank0, rank1 = _run(tmp_path, "missing")
    assert rank0 == {"local": REF, "bytes": True, "leftovers": []}
    assert rank1 == {"local": os.path.join("dest", "step_00000012.pt"), "bytes": True, "leftovers": []}


def test_rank_with_same_file_keeps_reference(tmp_path: Path):
    rank0, rank1 = _run(tmp_path, "present")
    assert rank0["local"] == REF and rank1["local"] == REF
    assert rank0["bytes"] and rank1["bytes"]
    assert not (tmp_path / "rank1" / "dest").exists()


def test_other_file_at_destination_is_kept_and_fails_every_rank(tmp_path: Path):
    rank0, rank1 = _run(tmp_path, "conflict")
    assert "exists with other contents" in rank0["error"]
    assert "exists with other contents" in rank1["error"]
    assert (tmp_path / "rank1" / "dest" / "step_00000012.pt").read_bytes() == b"another run"
