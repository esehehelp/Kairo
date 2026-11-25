from __future__ import annotations

import hashlib
import logging
import os
import time
from typing import Any

from . import errors
from .errors import AttemptFenced, AttemptRetired, KairoAPIError, NoContinuationError
from .runtime import (
    AttemptSession,
    CommandContext,
    _current_process_identity,
    _stop_signal_context,
    normalize_suspend_result,
    stop_requested,
    merge_stream_positions,
    stream_positions,
    suspend_payload,
)

_log = logging.getLogger("kairo_sdk.torch")


class DistributedAdapter:
    """Rank-0 control-plane I/O with all-rank checkpoint coordination.

    Every rank must call :meth:`start` and :meth:`safe_point` in the same
    order. Control-plane failures are broadcast as "no command" so a temporary
    outage cannot strand peer ranks in a collective. Checkpoint failures are
    instead raised on every rank because continuing with an incomplete
    checkpoint would make a suspend acknowledgement unsafe.
    """

    def __init__(self, session: AttemptSession) -> None:
        if not session.managed:
            raise ValueError("DistributedAdapter requires a managed AttemptSession")
        self.session = session
        self._stop_signal_handled = False
        self._registered = False
        # Rank 0: a ``checkpointed`` acknowledgement whose answer did not
        # arrive, (command id, kind, payload), sent again before anything else.
        self._pending_publication: tuple[str, str, dict[str, Any]] | None = None

    @staticmethod
    def _distribution():
        import torch.distributed as dist  # optional dependency, intentionally lazy

        return dist, dist.is_available() and dist.is_initialized()

    def start(self) -> None:
        """Register every rank, then start the rank-0 background heartbeat."""
        if self._registered:
            return
        dist, distributed = self._distribution()
        rank = dist.get_rank() if distributed else 0
        world_size = dist.get_world_size() if distributed else 1
        local = {
            "rank": rank,
            "pid": os.getpid(),
            "identity": _current_process_identity(),
        }
        registrations: list[Any] = [None for _ in range(world_size)]
        if distributed:
            dist.all_gather_object(registrations, local)
        else:
            registrations[0] = local

        error: str | None = None
        if rank == 0:
            try:
                for item in registrations:
                    self.session.register_process(
                        rank=item["rank"],
                        pid=item["pid"],
                        process_identity=item["identity"],
                    )
            except BaseException as exc:
                self.session.last_control_error = exc
                error = f"Kairo process registration failed: {exc}"
        status = [error]
        if distributed:
            dist.broadcast_object_list(status, src=0)
        if status[0] is not None:
            raise RuntimeError(status[0])
        if rank == 0:
            self.session.start()
        self._registered = True

    def close(self) -> None:
        dist, distributed = self._distribution()
        rank = dist.get_rank() if distributed else 0
        if rank == 0:
            self.session.close()

    def local_continuation(
        self, dest_dir: str | os.PathLike[str], *, chunk_bytes: int = 64 << 20
    ) -> str | None:
        """The continuation Kairo handed this attempt, as a file this rank can open.

        Every rank of an attempt runs in its one container and sees the same
        files: the reference itself. A relative reference is read from each
        rank's working directory; publish absolute paths (as Kairo imports
        only those) when ranks change theirs. (``dest_dir`` and
        ``chunk_bytes`` are kept for callers written when a rank could be on
        another node.)
        Returns ``None`` when the attempt has no continuation.
        """
        del dest_dir, chunk_bytes
        return self.session.input_continuation_ref or None

    def _try_ack(
        self, command_id: str, phase: str, payload: dict[str, Any] | None = None
    ) -> str:
        """Acknowledge on rank 0: ``"ok"``, ``"gone"``, ``"terminal"`` or ``"failed"``."""
        try:
            self.session._ack(command_id, phase, payload)
        except (AttemptRetired, AttemptFenced):
            return "terminal"
        except KairoAPIError as exc:
            self.session.last_control_error = exc
            return "gone" if exc.classification == errors.GONE else "failed"
        except BaseException as exc:
            self.session.last_control_error = exc
            return "failed"
        self.session.last_control_error = None
        return "ok"

    def safe_point(self, checkpoint, progress: dict[str, Any] | None = None, streams: Any = ()) -> bool:
        """Poll on rank 0 and suspend only after every rank checkpoints.

        Returns ``True`` only when the terminal acknowledgement reached Kairo,
        or (with no command) when rank 0 received a stop signal
        (:func:`~kairo_sdk.runtime.install_stop_signals`), so every rank stops
        together. If the acknowledgement fails, training continues and the
        same command is delivered again at a later safe point; checkpoint
        callbacks must consequently be idempotent by command ID. A
        ``checkpoint`` command (``kairo task checkpoint``) is checkpointed by
        every rank the same way and returns ``False``: every rank carries on,
        also when the checkpoint fails (it is acknowledged ``rejected``).

        The semantics follow :meth:`AttemptSession.safe_point`: unknown
        command kinds are skipped, a withdrawn command (HTTP 404) is dropped,
        rank 0 returning no ``continuation_ref`` for a suspend is
        acknowledged ``rejected`` and raises
        :class:`~kairo_sdk.errors.NoContinuationError` on every rank, and a
        retired or fenced attempt raises
        :class:`~kairo_sdk.errors.AttemptRetired` /
        :class:`~kairo_sdk.errors.AttemptFenced` on every rank. Transient
        control-plane failures are reported as "no command" so a temporary
        outage cannot strand peer ranks in a collective.
        """
        self.start()
        dist, distributed = self._distribution()
        rank = dist.get_rank() if distributed else 0

        envelope: list[Any] = [None]
        if rank == 0:
            envelope[0] = self._poll_on_rank_zero(progress)
        if distributed:
            dist.broadcast_object_list(envelope, src=0)
        message = envelope[0]
        _raise_terminal(self.session, message["terminal"])
        published = message.get("published")
        if published is not None:
            # Rank 0 got the answer of an earlier publication only now.
            if published["kind"] == "suspend":
                self.session.suspend_handled = True
                return True
        raw = message["command"]
        if raw is None and message["stop"]:
            return self._checkpoint_for_stop_signal(checkpoint, dist, distributed, rank)
        if raw is None:
            return False

        context = CommandContext(
            execution_id=self.session.execution_id,
            attempt_id=self.session.attempt_id,
            **raw,
        )
        result = None
        local_error: str | None = None
        local_positions: dict[str, list[dict[str, int]]] = {}
        try:
            result = normalize_suspend_result(checkpoint(context))
            local_positions = stream_positions(tuple(streams))
        except BaseException as exc:
            local_error = f"rank {rank} checkpoint failed: {type(exc).__name__}: {exc}"

        # each rank's failure, and where each of its streams stands
        local = (local_error, local_positions)
        gathered: list[Any] = [None] * (dist.get_world_size() if distributed else 1)
        if distributed:
            dist.all_gather_object(gathered, local)
        else:
            gathered[0] = local
        failures = [error for error, _ in gathered if error is not None]
        positions: dict[str, dict[str, Any]] = {}
        if not failures:
            try:
                positions = merge_stream_positions(by_name for _, by_name in gathered)
            except ValueError as exc:
                failures = [f"stream positions: {exc}"]
        if failures:
            reason = "; ".join(failures)
            if rank == 0:
                self._try_ack(context.command_id, "rejected", {"reason": reason})
            if context.kind == "checkpoint":
                # A checkpoint command asked for a copy, not for the run to
                # end: refused, and every rank carries on.
                self.session.last_checkpoint_error = RuntimeError(reason)
                _log.warning("Kairo checkpoint %s failed; every rank carries on: %s", context.command_id, reason)
                return False
            raise RuntimeError(reason)

        if distributed:
            dist.barrier()
        outcome: dict[str, Any] = {"ok": False, "no_continuation": False, "terminal": None, "continuation_ref": None}
        if rank == 0:
            assert result is not None
            if not result.continuation_ref:
                self._try_ack(context.command_id, "rejected", {"reason": "no continuation_ref"})
                outcome["no_continuation"] = True
            else:
                payload = suspend_payload(result, positions)
                status = self._try_ack(context.command_id, "checkpointed", payload)
                outcome["ok"] = status == "ok"
                outcome["continuation_ref"] = result.continuation_ref
                if status == "terminal":
                    outcome["terminal"] = _terminal_message(self.session)
                elif status == "failed" and self._unanswered():
                    # It may have been recorded with only the answer lost, and
                    # a checkpointed command is not delivered again: the same
                    # acknowledgement goes again first at the next safe point.
                    self._pending_publication = (context.command_id, context.kind, payload)
        acknowledgement = [outcome]
        if distributed:
            dist.broadcast_object_list(acknowledgement, src=0)
        outcome = acknowledgement[0]
        _raise_terminal(self.session, outcome["terminal"])
        if outcome["no_continuation"]:
            message = f"rank 0 checkpoint returned no continuation_ref for command {context.command_id}"
            if context.kind == "checkpoint":
                self.session.last_checkpoint_error = NoContinuationError(message)
                _log.warning("Kairo checkpoint %s published nothing; every rank carries on", context.command_id)
                return False
            raise NoContinuationError(message)
        if context.kind == "checkpoint":
            return False  # published (or redelivered after a failed acknowledgement); every rank carries on
        if outcome["ok"]:
            self.session.suspend_handled = True
        return bool(outcome["ok"])

    def _unanswered(self) -> bool:
        """Whether the last failed request of the session got no answer
        (transient or retry-later), rather than a refusal."""
        error = self.session.last_control_error
        return error is not None and errors.classify(error) in (errors.TRANSIENT, errors.RETRY_LATER)

    def _checkpoint_for_stop_signal(self, checkpoint, dist, distributed: bool, rank: int) -> bool:
        """Rank 0 received a stop signal: every rank checkpoints once, then stops.

        Nothing is acknowledged (a force stop plans no continuation); the
        checkpoint lets the work be resumed by hand.
        """
        if not self._stop_signal_handled:
            local_error: str | None = None
            try:
                checkpoint(_stop_signal_context(self.session))
            except BaseException as exc:
                local_error = f"rank {rank} checkpoint failed: {type(exc).__name__}: {exc}"
            errors_by_rank: list[Any] = [None] * (dist.get_world_size() if distributed else 1)
            if distributed:
                dist.all_gather_object(errors_by_rank, local_error)
                dist.barrier()
            else:
                errors_by_rank[0] = local_error
            self._stop_signal_handled = True
            failures = [error for error in errors_by_rank if error is not None]
            if failures:
                raise RuntimeError("; ".join(failures))
        return True

    def _poll_on_rank_zero(self, progress: dict[str, Any] | None) -> dict[str, Any]:
        """Poll and accept a suspend on rank 0; the message every rank receives."""
        session = self.session
        message: dict[str, Any] = {"command": None, "stop": False, "terminal": None, "published": None}
        if progress is not None:
            session.set_progress(progress)
        if session._terminal_error is not None:
            message["terminal"] = _terminal_message(session)
            return message
        if self._pending_publication is not None:
            command_id, kind, payload = self._pending_publication
            status = self._try_ack(command_id, "checkpointed", payload)
            if status == "terminal":
                message["terminal"] = _terminal_message(session)
                return message
            if status == "failed" and self._unanswered():
                return message  # still unavailable: nothing else this time
            self._pending_publication = None
            if status == "ok":
                message["published"] = {"kind": kind, "continuation_ref": payload.get("continuation_ref")}
                return message
        now = time.monotonic()
        if now - session._last_poll >= session.poll_interval_seconds:
            session._last_poll = now
            try:
                commands = session.poll_commands()
                session.last_control_error = None
            except (AttemptRetired, AttemptFenced):
                message["terminal"] = _terminal_message(session)
                return message
            except BaseException as exc:
                # Every rank still reaches the broadcast. A temporary
                # control-plane outage must not deadlock the process group.
                session.last_control_error = exc
                commands = []
            for raw in commands:
                kind = str(raw.get("kind") or "")
                if kind not in ("suspend", "checkpoint"):
                    _log.debug(
                        "skipping Kairo command %s of unknown kind %r", raw.get("id"), raw.get("kind")
                    )
                    continue
                command_id = str(raw["id"])
                status = self._try_ack(command_id, "accepted")
                if status == "ok":
                    status = self._try_ack(command_id, "checkpointing")
                if status == "terminal":
                    message["terminal"] = _terminal_message(session)
                    return message
                if status == "gone":
                    continue
                # A transient acknowledgement failure still checkpoints: the
                # terminal acknowledgement decides, and Kairo redelivers.
                message["command"] = {
                    "command_id": command_id,
                    "kind": kind,
                    "reason": str(raw.get("reason") or ""),
                    "delivery_count": int(raw.get("delivery_count") or 0),
                }
                return message
        message["stop"] = stop_requested()
        return message


def _raise_rank_errors(errors) -> None:
    failures = [error for error in errors if error]
    if failures:
        raise RuntimeError("; ".join(failures))


def _terminal_message(session: AttemptSession) -> dict[str, Any] | None:
    error = session._terminal_error
    if error is None:
        return None
    return {
        "retired": isinstance(error, AttemptRetired),
        "status": error.status,
        "code": error.code,
        "message": error.message,
        "detail": error.detail,
    }


def _raise_terminal(session: AttemptSession, message: dict[str, Any] | None) -> None:
    if message is None:
        return
    if session._terminal_error is not None:
        raise session._terminal_error  # rank 0 re-raises what it received
    error_type = AttemptRetired if message["retired"] else AttemptFenced
    raise error_type(
        message["status"],
        message["code"],
        message["message"],
        detail=message["detail"],
        role=errors.WORKER,
    )
