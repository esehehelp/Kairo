from __future__ import annotations

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

    def safe_point(self, checkpoint, progress: dict[str, Any] | None = None) -> bool:
        """Poll on rank 0 and suspend only after every rank checkpoints.

        Returns ``True`` only when the terminal acknowledgement reached Kairo,
        or (with no command) when rank 0 received a stop signal
        (:func:`~kairo_sdk.runtime.install_stop_signals`), so every rank stops
        together. If the acknowledgement fails, training continues and the
        same command is delivered again at a later safe point; checkpoint
        callbacks must consequently be idempotent by command ID.

        The semantics follow :meth:`AttemptSession.safe_point`: unknown
        command kinds are skipped, a withdrawn command (HTTP 404) is dropped,
        rank 0 returning no ``continuation_ref`` is acknowledged ``rejected``
        and raises :class:`~kairo_sdk.errors.NoContinuationError` on every
        rank, and a retired or fenced attempt raises
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
        try:
            result = normalize_suspend_result(checkpoint(context))
        except BaseException as exc:
            local_error = f"rank {rank} checkpoint failed: {type(exc).__name__}: {exc}"

        errors_by_rank: list[Any] = [None] * (dist.get_world_size() if distributed else 1)
        if distributed:
            dist.all_gather_object(errors_by_rank, local_error)
        else:
            errors_by_rank[0] = local_error
        failures = [error for error in errors_by_rank if error is not None]
        if failures:
            if rank == 0:
                self._try_ack(
                    context.command_id,
                    "rejected",
                    {"reason": "; ".join(failures)},
                )
            raise RuntimeError("; ".join(failures))

        if distributed:
            dist.barrier()
        outcome: dict[str, Any] = {"ok": False, "no_continuation": False, "terminal": None}
        if rank == 0:
            assert result is not None
            if not result.continuation_ref:
                self._try_ack(context.command_id, "rejected", {"reason": "no continuation_ref"})
                outcome["no_continuation"] = True
            else:
                status = self._try_ack(context.command_id, "checkpointed", suspend_payload(result))
                outcome["ok"] = status == "ok"
                if status == "terminal":
                    outcome["terminal"] = _terminal_message(self.session)
        acknowledgement = [outcome]
        if distributed:
            dist.broadcast_object_list(acknowledgement, src=0)
        outcome = acknowledgement[0]
        _raise_terminal(self.session, outcome["terminal"])
        if outcome["no_continuation"]:
            raise NoContinuationError(
                f"rank 0 checkpoint returned no continuation_ref for command {context.command_id}"
            )
        if outcome["ok"]:
            self.session.suspend_handled = True
        return bool(outcome["ok"])

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
        message: dict[str, Any] = {"command": None, "stop": False, "terminal": None}
        if progress is not None:
            session.set_progress(progress)
        if session._terminal_error is not None:
            message["terminal"] = _terminal_message(session)
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
                if str(raw.get("kind") or "") != "suspend":
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
                    "kind": "suspend",
                    "reason": str(raw.get("reason") or ""),
                    "delivery_count": int(raw.get("delivery_count") or 0),
                }
                return message
        message["stop"] = stop_requested()
        return message


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
