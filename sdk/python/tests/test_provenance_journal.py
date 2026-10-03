from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from kairo_sdk import CommandJournal, NoContinuationError, SuspendResult, provenance


# --------------------------------------------------------------------------
# CommandJournal


def test_journal_round_trip_survives_a_new_instance(tmp_path: Path):
    journal = CommandJournal(tmp_path / "j")
    assert journal.result("cmd/1") is None
    recorded = journal.record("cmd/1", SuspendResult("ckpt://1", {"step": 1}))
    assert recorded == SuspendResult("ckpt://1", {"step": 1})
    again = CommandJournal(tmp_path / "j").result("cmd/1")
    assert again == SuspendResult("ckpt://1", {"step": 1})
    assert CommandJournal(tmp_path / "j").result("cmd/2") is None
    assert [path.suffix for path in (tmp_path / "j").iterdir()] == [".json"]


def test_journal_accepts_paths_and_strings(tmp_path: Path):
    journal = CommandJournal(tmp_path)
    assert journal.record("a", tmp_path / "x.pt").continuation_ref == str(tmp_path / "x.pt")
    assert journal.record("b", "ckpt://b").continuation_ref == "ckpt://b"
    assert journal.result("b") == SuspendResult("ckpt://b")


def test_journal_refuses_a_result_without_continuation(tmp_path: Path):
    with pytest.raises(NoContinuationError):
        CommandJournal(tmp_path).record("cmd", SuspendResult(payload={"step": 1}))
    assert list(tmp_path.iterdir()) == []


def test_journal_detects_a_foreign_entry(tmp_path: Path):
    journal = CommandJournal(tmp_path)
    journal.record("cmd_a", "ckpt://a")
    entry = next(tmp_path.iterdir())
    data = json.loads(entry.read_text(encoding="utf-8"))
    data["command_id"] = "cmd_other"
    entry.write_text(json.dumps(data), encoding="utf-8")
    with pytest.raises(ValueError, match="does not belong"):
        journal.result("cmd_a")


# --------------------------------------------------------------------------
# provenance


def test_capture_outside_git_with_inputs(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path))
    data = tmp_path / "data.bin"
    data.write_bytes(b"kairo" * 1000)
    record = provenance.capture(argv=["train.py", "--lr", "1e-3"], inputs=["data.bin"], cwd=tmp_path)
    assert record["argv"] == ["train.py", "--lr", "1e-3"]
    assert record["cwd"] == str(tmp_path.resolve())
    assert record["python"]["executable"] == sys.executable
    assert record["git"] is None
    assert record["inputs"] == [
        {
            "path": str(tmp_path.resolve() / "data.bin"),
            "size": 5000,
            "sha256": hashlib.sha256(b"kairo" * 1000).hexdigest(),
        }
    ]
    assert record["kairo"] is None
    json.dumps(record)  # JSON-ready


def test_capture_defaults_to_sys_argv(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["prog", "x"])
    assert provenance.capture(cwd=tmp_path)["argv"] == ["prog", "x"]


def test_capture_tolerates_a_missing_git(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("PATH", str(tmp_path / "empty"))
    assert provenance.capture(cwd=tmp_path)["git"] is None


@pytest.mark.skipif(shutil.which("git") is None, reason="git is not installed")
def test_capture_reads_git_head_and_dirty(tmp_path: Path):
    repo = tmp_path / "repo"
    repo.mkdir()

    def git(*args: str) -> str:
        return subprocess.run(
            ["git", "-C", str(repo), "-c", "user.name=t", "-c", "user.email=t@example.com",
             "-c", "commit.gpgsign=false", *args],
            check=True, capture_output=True, text=True,
        ).stdout.strip()

    git("init", "-q")
    (repo / "a.txt").write_text("one", encoding="utf-8")
    git("add", "a.txt")
    git("commit", "-q", "-m", "one")
    head = git("rev-parse", "HEAD")
    (repo / "sub").mkdir()
    clean = provenance.capture(cwd=repo / "sub")
    assert clean["git"] == {"head": head, "dirty": False}
    (repo / "a.txt").write_text("two", encoding="utf-8")
    assert provenance.capture(cwd=repo)["git"] == {"head": head, "dirty": True}


def test_capture_records_kairo_ids_when_managed(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("KAIRO_API_URL", "https://127.0.0.1:7474")
    monkeypatch.setenv("KAIRO_EXECUTION_ID", "exe_1")
    monkeypatch.setenv("KAIRO_ATTEMPT_ID", "att_1")
    monkeypatch.setenv("KAIRO_ATTEMPT_TOKEN", "kairo_worker_secret")
    record = provenance.capture(cwd=tmp_path)
    assert record["kairo"] == {"execution_id": "exe_1", "attempt_id": "att_1", "node_id": None}
    assert "kairo_worker_secret" not in json.dumps(record)


def test_write_publishes_json_atomically(tmp_path: Path):
    record = provenance.capture(argv=["x"], cwd=tmp_path)
    destination = tmp_path / "out" / "provenance.json"
    provenance.write(destination, record)
    assert json.loads(destination.read_text(encoding="utf-8")) == record
    assert [path.name for path in destination.parent.iterdir()] == ["provenance.json"]
