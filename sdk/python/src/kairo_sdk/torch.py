from __future__ import annotations

import os
import time
from typing import Any

from .runtime import (
    AttemptSession,
    CommandContext,
    _current_process_identity,
    _normalize_suspend_result,
)


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
    ) -> bool:
        try:
            self.session._ack(command_id, phase, payload)
        except BaseException as exc:
            self.session.last_control_error = exc
            return False
        self.session.last_control_error = None
        return True

    def safe_point(self, checkpoint, progress: dict[str, Any] | None = None) -> bool:
        """Poll on rank 0 and suspend only after every rank checkpoints.

        Returns ``True`` only when the terminal acknowledgement reached Kairo.
        If that acknowledgement fails, training continues and the same command
        is delivered again at a later safe point; checkpoint callbacks must
        consequently be idempotent by command ID.
        """
        self.start()
        dist, distributed = self._distribution()
        rank = dist.get_rank() if distributed else 0

        envelope: list[Any] = [None]
        if rank == 0:
            if progress is not None:
                self.session.set_progress(progress)
            now = time.monotonic()
            if now - self.session._last_poll >= self.session.poll_interval_seconds:
                self.session._last_poll = now
                try:
                    response = self.session._request(
                        "GET",
                        "/v1/worker/attempts/"
                        f"{self.session.attempt_id}/commands",
                        None,
                    )
                    commands = response.get("commands", [])
                    if commands:
                        raw = commands[0]
                        envelope[0] = {
                            "command_id": str(raw["id"]),
                            "kind": str(raw["kind"]),
                            "reason": str(raw.get("reason") or ""),
                            "delivery_count": int(raw.get("delivery_count") or 0),
                        }
                    self.session.last_control_error = None
                except BaseException as exc:
                    # Every rank still reaches the broadcast below. A temporary
                    # control-plane outage must not deadlock the process group.
                    self.session.last_control_error = exc
        if distributed:
            dist.broadcast_object_list(envelope, src=0)
        raw = envelope[0]
        if raw is None:
            return False

        context = CommandContext(attempt_id=self.session.attempt_id, **raw)
        accepted = self._try_ack(context.command_id, "accepted") if rank == 0 else False
        result = None
        local_error: str | None = None
        if context.kind == "suspend":
            if rank == 0:
                self._try_ack(context.command_id, "checkpointing")
            try:
                result = _normalize_suspend_result(checkpoint(context))
            except BaseException as exc:
                local_error = f"rank {rank} checkpoint failed: {exc}"
        elif context.kind != "cancel":
            if rank == 0:
                self._try_ack(
                    context.command_id,
                    "rejected",
                    {"reason": f"unsupported command kind {context.kind!r}"},
                )
            return False

        errors: list[Any] = [None] * (dist.get_world_size() if distributed else 1)
        if distributed:
            dist.all_gather_object(errors, local_error)
        else:
            errors[0] = local_error
        failures = [error for error in errors if error is not None]
        if failures:
            if rank == 0:
                self._try_ack(
                    context.command_id,
                    "rejected",
                    {"reason": "; ".join(failures)},
                )
            raise RuntimeError("; ".join(failures))

        if context.kind == "suspend" and distributed:
            dist.barrier()
        completed = accepted
        if rank == 0 and context.kind == "suspend":
            payload = dict(result.payload)
            if result.continuation_ref is not None:
                payload["continuation_ref"] = result.continuation_ref
            completed = self._try_ack(context.command_id, "checkpointed", payload)
        completion = [completed]
        if distributed:
            dist.broadcast_object_list(completion, src=0)
        if completion[0]:
            self.session.suspend_handled = True
        return bool(completion[0])
