"""Idempotent checkpoints for at-least-once suspend commands."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .atomic import atomic_publish
from .errors import NoContinuationError
from .runtime import SuspendResult, normalize_suspend_result


class CommandJournal:
    """Remember the checkpoint published for each command id.

    Kairo may deliver the same suspend again (a lost acknowledgement, a
    restarted daemon). A checkpoint callback that looks the command up first
    publishes the same continuation instead of writing a second checkpoint::

        journal = CommandJournal(run_dir / "kairo-commands")

        def checkpoint(command):
            if (done := journal.result(command.command_id)) is not None:
                return done
            path = save_checkpoint(...)
            return journal.record(command.command_id, SuspendResult(str(path)))

    Each record is one small JSON file, written atomically and fsynced, so a
    crash leaves either the whole record or none.
    """

    def __init__(self, directory: str | Path) -> None:
        self.directory = Path(directory)

    def _path(self, command_id: str) -> Path:
        digest = hashlib.sha256(command_id.encode("utf-8")).hexdigest()[:40]
        return self.directory / f"{digest}.json"

    def result(self, command_id: str) -> SuspendResult | None:
        """The result recorded for ``command_id``, or ``None``."""
        path = self._path(command_id)
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None
        if not isinstance(raw, dict) or raw.get("command_id") != command_id:
            raise ValueError(f"command journal entry {path} does not belong to {command_id!r}")
        payload = raw.get("payload") or {}
        if not isinstance(payload, Mapping):
            raise ValueError(f"command journal entry {path} has a malformed payload")
        return SuspendResult(continuation_ref=raw.get("continuation_ref"), payload=payload)

    def record(
        self, command_id: str, result: SuspendResult | str | Path
    ) -> SuspendResult:
        """Durably record ``result`` for ``command_id`` and return it normalized."""
        normalized = normalize_suspend_result(result)
        if not normalized.continuation_ref:
            raise NoContinuationError("a journaled checkpoint needs a continuation_ref")
        document: dict[str, Any] = {
            "command_id": command_id,
            "continuation_ref": normalized.continuation_ref,
            "payload": dict(normalized.payload),
        }
        encoded = json.dumps(document, sort_keys=True, separators=(",", ":")).encode("utf-8")

        def write(temporary: Path) -> None:
            temporary.write_bytes(encoded)

        atomic_publish(self._path(command_id), write)
        return SuspendResult(
            continuation_ref=normalized.continuation_ref, payload=document["payload"]
        )
