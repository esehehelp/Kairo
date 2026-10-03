"""What produced an artifact: command line, code version, inputs, Kairo ids.

``capture()`` returns a JSON-ready record; ``write()`` publishes it atomically
next to the artifact it describes::

    from kairo_sdk import provenance

    record = provenance.capture(inputs=[config_path, dataset_manifest])
    provenance.write(run_dir / "provenance.json", record)
"""

from __future__ import annotations

import hashlib
import json
import os
import platform
import subprocess
import sys
from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .atomic import atomic_publish

_GIT_TIMEOUT_SECONDS = 10


def capture(
    argv: Sequence[str] | None = None,
    inputs: Iterable[str | Path] = (),
    cwd: str | Path | None = None,
) -> dict[str, Any]:
    """Describe this run.

    ``argv`` defaults to ``sys.argv``, ``cwd`` to the working directory. The
    record holds the Python executable and version, ``git`` (``head`` and
    ``dirty`` for the repository containing ``cwd``, ``None`` outside one or
    without git), the size and sha256 of each input file, and ``kairo``
    (execution, attempt and node ids) when Kairo manages the process.
    """
    directory = Path(cwd if cwd is not None else os.getcwd()).resolve()
    return {
        "captured_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "argv": list(sys.argv if argv is None else argv),
        "cwd": str(directory),
        "python": {"executable": sys.executable, "version": platform.python_version()},
        "git": _git_state(directory),
        "inputs": [_describe_input(directory, path) for path in inputs],
        "kairo": _kairo_ids(),
    }


def write(path: str | Path, record: Mapping[str, Any]) -> None:
    """Publish ``record`` as JSON at ``path`` atomically (fsynced, then renamed)."""
    encoded = (json.dumps(record, indent=2, sort_keys=True) + "\n").encode("utf-8")

    def publish(temporary: Path) -> None:
        temporary.write_bytes(encoded)

    atomic_publish(path, publish)


def _describe_input(directory: Path, path: str | Path) -> dict[str, Any]:
    resolved = Path(path)
    if not resolved.is_absolute():
        resolved = directory / resolved
    digest = hashlib.sha256()
    size = 0
    with resolved.open("rb") as handle:
        while chunk := handle.read(1 << 20):
            digest.update(chunk)
            size += len(chunk)
    return {"path": str(resolved), "size": size, "sha256": digest.hexdigest()}


def _git(directory: Path, *args: str) -> str | None:
    creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    try:
        completed = subprocess.run(
            ["git", "-C", str(directory), *args],
            capture_output=True,
            text=True,
            timeout=_GIT_TIMEOUT_SECONDS,
            creationflags=creationflags,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if completed.returncode != 0:
        return None
    return completed.stdout


def _git_state(directory: Path) -> dict[str, Any] | None:
    head = _git(directory, "rev-parse", "HEAD")
    if head is None:
        return None
    status = _git(directory, "status", "--porcelain", "--untracked-files=no")
    return {
        "head": head.strip(),
        "dirty": None if status is None else bool(status.strip()),
    }


def _kairo_ids() -> dict[str, str | None] | None:
    if not os.environ.get("KAIRO_API_URL"):
        return None
    return {
        "execution_id": os.environ.get("KAIRO_EXECUTION_ID"),
        "attempt_id": os.environ.get("KAIRO_ATTEMPT_ID"),
        "node_id": os.environ.get("KAIRO_NODE_ID"),
    }
