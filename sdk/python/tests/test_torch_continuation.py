from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

torch = pytest.importorskip("torch")
import torch.distributed as dist
import torch.multiprocessing as mp

from kairo_sdk import AttemptSession, DistributedAdapter

REF = os.path.join("ckpt", "step_00000012.pt")


def _worker(rank: int, init_file: str, root: str) -> None:
    # Every rank of an attempt runs in its one container: each gets the
    # reference Kairo handed the attempt, and nothing is copied.
    os.chdir(root)
    os.environ["KAIRO_CONTINUATION_REF"] = REF
    dist.init_process_group("gloo", init_method=f"file://{init_file}", rank=rank, world_size=2)
    try:
        session = AttemptSession(
            api_url="http://127.0.0.1:9",
            execution_id="ex_cont",
            attempt_id="att_cont",
            token="tok_cont",
        )
        local = DistributedAdapter(session).local_continuation("dest", chunk_bytes=1000)
    finally:
        dist.destroy_process_group()
    Path(root, f"rank{rank}.json").write_text(json.dumps({"local": local}), encoding="utf-8")


def test_every_rank_gets_the_reference(tmp_path: Path):
    mp.spawn(_worker, args=(str((tmp_path / "init").resolve()), str(tmp_path.resolve())), nprocs=2, join=True)
    for rank in (0, 1):
        assert json.loads((tmp_path / f"rank{rank}.json").read_text()) == {"local": REF}
    assert not (tmp_path / "dest").exists()
