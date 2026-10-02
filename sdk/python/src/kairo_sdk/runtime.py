from __future__ import annotations

import base64
import hashlib
import os
import threading
import time
import urllib.parse
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from . import _http


@dataclass(frozen=True)
class CommandContext:
    command_id: str
    execution_id: str | None
    attempt_id: str | None
    kind: str
    reason: str
    delivery_count: int


@dataclass(frozen=True)
class SuspendResult:
    continuation_ref: str | None = None
    payload: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ProgressEnvelope:
    unit: str
    current: int | float
    total: int | float | None = None
    message: str | None = None
    checkpoint_age_seconds: float | None = None
    detail: Mapping[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            key: value
            for key, value in {
                "unit": self.unit,
                "current": self.current,
                "total": self.total,
                "message": self.message,
                "checkpoint_age_seconds": self.checkpoint_age_seconds,
                "detail": dict(self.detail),
            }.items()
            if value is not None
        }


class AttemptSession:
    """Managed attempt context with independent heartbeats and safe points.

    Commands are delivered at least once. A checkpoint callback can therefore
    be called repeatedly with the same command ID and must be idempotent.
    """

    def __init__(
        self,
        *,
        api_url: str | None,
        execution_id: str | None,
        attempt_id: str | None,
        token: str | None = None,
        ca_der: bytes | None = None,
        node_id: str | None = None,
        executor_id: str | None = None,
        stop_paths: Iterable[str | Path] = (),
        poll_interval_seconds: float = 1.0,
        heartbeat_interval_seconds: float = 10.0,
        timeout_seconds: float = 10.0,
    ) -> None:
        self.api_url = api_url.rstrip("/") if api_url else None
        self.stop_paths = tuple(Path(path) for path in stop_paths)
        if self.api_url is None:
            if (
                execution_id is not None
                or attempt_id is not None
                or token is not None
                or ca_der is not None
            ):
                raise ValueError(
                    "unmanaged sessions cannot provide Kairo execution, attempt, token, or CA context"
                )
        elif not execution_id or not attempt_id or not token:
            raise ValueError("managed Kairo sessions require execution, attempt, and token")
        elif self.stop_paths:
            raise ValueError("managed Kairo sessions cannot use stop_paths; pause the coordination scope")
        self.execution_id = execution_id
        self.attempt_id = attempt_id
        self._token = token
        self._opener = _http.build_opener(ca_der=ca_der) if self.api_url is not None else None
        self.node_id = node_id
        self.executor_id = executor_id
        self.poll_interval_seconds = poll_interval_seconds
        self.heartbeat_interval_seconds = heartbeat_interval_seconds
        self.timeout_seconds = timeout_seconds
        self._last_poll = float("-inf")
        self._last_heartbeat = float("-inf")
        self._progress: dict[str, Any] = {}
        self._stop = threading.Event()
        self._heartbeat_thread: threading.Thread | None = None
        self.last_control_error: BaseException | None = None
        self.suspend_handled = False
        self.process_identity = _current_process_identity()
        self._process_registered = False

    @classmethod
    def from_environment(
        cls,
        *,
        stop_paths: Iterable[str | Path] = (),
        poll_interval_seconds: float = 1.0,
        heartbeat_interval_seconds: float = 10.0,
    ) -> AttemptSession:
        api_url = os.environ.get("KAIRO_API_URL")
        execution_id = os.environ.get("KAIRO_EXECUTION_ID")
        attempt_id = os.environ.get("KAIRO_ATTEMPT_ID")
        token = os.environ.get("KAIRO_ATTEMPT_TOKEN")
        ca_text = os.environ.get("KAIRO_API_CA")
        if api_url:
            if not execution_id or not attempt_id or not token:
                raise RuntimeError(
                    "managed Kairo context requires execution ID, attempt ID, and attempt token"
                )
        elif execution_id or attempt_id or token or ca_text:
            raise RuntimeError(
                "unmanaged Kairo context cannot provide execution, attempt, token, or CA context"
            )
        try:
            ca_der = base64.b64decode(ca_text.strip(), validate=True) if ca_text else None
        except ValueError as error:  # binascii.Error, or non-ASCII text
            raise RuntimeError("KAIRO_API_CA must be a base64 DER certificate") from error
        return cls(
            api_url=api_url or None,
            execution_id=execution_id or None,
            attempt_id=attempt_id or None,
            token=token or None,
            ca_der=ca_der,
            node_id=os.environ.get("KAIRO_NODE_ID"),
            executor_id=os.environ.get("KAIRO_EXECUTOR_ID"),
            stop_paths=stop_paths,
            poll_interval_seconds=poll_interval_seconds,
            heartbeat_interval_seconds=heartbeat_interval_seconds,
        )

    @property
    def managed(self) -> bool:
        return self.api_url is not None

    @property
    def resource_ids(self) -> tuple[str, ...]:
        return _csv_environment("KAIRO_RESOURCE_IDS")

    @property
    def resource_bindings(self) -> tuple[str, ...]:
        return _csv_environment("KAIRO_RESOURCE_BINDINGS")

    @property
    def input_continuation_ref(self) -> str | None:
        """Return the project-provided continuation without interpreting it."""
        return os.environ.get("KAIRO_CONTINUATION_REF")

    def __enter__(self) -> AttemptSession:
        self.start()
        return self

    def __exit__(self, _type, _value, _traceback) -> None:
        self.close()

    def start(self) -> None:
        if not self.managed or self._heartbeat_thread is not None:
            return
        self._stop.clear()
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop, name="kairo-heartbeat", daemon=True
        )
        self._heartbeat_thread.start()

    def close(self) -> None:
        self._stop.set()
        thread, self._heartbeat_thread = self._heartbeat_thread, None
        if thread is not None:
            thread.join(timeout=max(1.0, self.timeout_seconds + 0.5))

    def _heartbeat_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.heartbeat()
                self.last_control_error = None
            except BaseException as error:
                self.last_control_error = error
            self._stop.wait(self.heartbeat_interval_seconds)

    def set_progress(self, progress: Mapping[str, Any] | ProgressEnvelope) -> None:
        self._progress = progress.as_dict() if isinstance(progress, ProgressEnvelope) else dict(progress)

    def heartbeat(self, progress: Mapping[str, Any] | ProgressEnvelope | None = None) -> None:
        if not self.managed:
            return
        if progress is not None:
            self.set_progress(progress)
        if not self._process_registered:
            self.register_process()
        self._request("POST", "/api/worker/heartbeat", {"progress": self._progress})
        self._last_heartbeat = time.monotonic()

    def register_process(self, *, rank: int = 0, pid: int | None = None, process_identity: str | None = None) -> None:
        if not self.managed:
            return
        self._request(
            "POST",
            "/api/worker/processes",
            {
                "rank": rank,
                "pid": os.getpid() if pid is None else pid,
                "process_identity": process_identity or self.process_identity,
            },
        )
        self._process_registered = True

    def safe_point(
        self,
        *,
        checkpoint: Callable[[CommandContext], SuspendResult | str | Path | None],
        progress: Mapping[str, Any] | ProgressEnvelope | None = None,
    ) -> bool:
        if progress is not None:
            self.set_progress(progress)
        # Sessions not used as context managers retain cooperative heartbeat
        # behavior for compatibility.
        now = time.monotonic()
        if self.managed and self._heartbeat_thread is None and now - self._last_heartbeat >= self.heartbeat_interval_seconds:
            self.heartbeat()
        stop_path = next((path for path in self.stop_paths if path.exists()), None)
        if stop_path is not None:
            context = CommandContext(
                command_id=_stop_file_command_id(stop_path),
                execution_id=None,
                attempt_id=None,
                kind="suspend",
                reason="stop_file",
                delivery_count=1,
            )
            checkpoint(context)
            stop_path.unlink(missing_ok=True)
            self.suspend_handled = True
            return True
        if not self.managed or now - self._last_poll < self.poll_interval_seconds:
            return False
        self._last_poll = now
        for raw in self.poll_commands():
            context = CommandContext(
                command_id=str(raw["id"]),
                execution_id=self.execution_id,
                attempt_id=self.attempt_id,
                kind=str(raw["kind"]),
                reason=str(raw.get("reason") or ""),
                delivery_count=int(raw.get("delivery_count") or 0),
            )
            if context.kind != "suspend":
                self._ack(
                    context.command_id,
                    "rejected",
                    {"reason": f"unsupported command kind {context.kind!r}"},
                )
                continue
            self._ack(context.command_id, "accepted")
            self._ack(context.command_id, "checkpointing")
            result = _normalize_suspend_result(checkpoint(context))
            payload = dict(result.payload)
            if result.continuation_ref is not None:
                payload["continuation_ref"] = result.continuation_ref
            self._ack(context.command_id, "checkpointed", payload)
            self.suspend_handled = True
            return True
        return False

    def poll_commands(self) -> list[dict[str, Any]]:
        """Fetch this attempt's pending commands without acknowledging any."""
        response = self._request("GET", "/api/worker/commands", None)
        commands = response.get("commands") or []
        if not isinstance(commands, list):
            raise RuntimeError("Kairo commands response must contain an array")
        return commands

    def _ack(self, command_id: str, phase: str, payload: Mapping[str, Any] | None = None) -> None:
        self._request(
            "POST",
            f"/api/worker/commands/{urllib.parse.quote(command_id, safe='')}/acks",
            {"phase": phase, "payload": dict(payload or {})},
        )

    def _request(self, method: str, path: str, body: Any) -> dict[str, Any]:
        if self.api_url is None or self._opener is None or self._token is None:
            raise RuntimeError("attempt is not managed by Kairo")
        return _http.request_json(
            self._opener, method, self.api_url + path, self._token, body, self.timeout_seconds
        )


def _normalize_suspend_result(value: SuspendResult | str | Path | None) -> SuspendResult:
    if value is None:
        return SuspendResult()
    if isinstance(value, SuspendResult):
        return value
    if isinstance(value, (str, Path)):
        return SuspendResult(continuation_ref=str(value))
    raise TypeError("checkpoint callback must return SuspendResult, path, string, or None")


def _stop_file_command_id(path: Path) -> str:
    stat = path.stat()
    material = f"{path.resolve()}\0{stat.st_mtime_ns}\0{stat.st_size}".encode()
    return "stopfile_" + hashlib.sha256(material).hexdigest()[:24]


def _csv_environment(name: str) -> tuple[str, ...]:
    return tuple(part for part in os.environ.get(name, "").split(",") if part)


def _current_process_identity() -> str:
    pid = os.getpid()
    if os.name != "nt":
        try:
            stat_text = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
            started = _parse_proc_stat_starttime(stat_text)
        except (OSError, ValueError) as error:
            raise RuntimeError(f"cannot establish process identity for pid {pid}") from error
        return f"proc:{pid}:starttime:{started}"
    import ctypes
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.GetCurrentProcess.argtypes = []
    kernel32.GetCurrentProcess.restype = wintypes.HANDLE
    filetime_pointer = ctypes.POINTER(wintypes.FILETIME)
    kernel32.GetProcessTimes.argtypes = [
        wintypes.HANDLE,
        filetime_pointer,
        filetime_pointer,
        filetime_pointer,
        filetime_pointer,
    ]
    kernel32.GetProcessTimes.restype = wintypes.BOOL

    created, exited, kernel, user = (wintypes.FILETIME() for _ in range(4))
    handle = kernel32.GetCurrentProcess()
    if not kernel32.GetProcessTimes(
        handle,
        ctypes.byref(created),
        ctypes.byref(exited),
        ctypes.byref(kernel),
        ctypes.byref(user),
    ):
        error_code = ctypes.get_last_error()
        raise OSError(error_code, ctypes.FormatError(error_code))
    ticks = (created.dwHighDateTime << 32) | created.dwLowDateTime
    # Match Go's windows.Filetime.Nanoseconds(), which is Unix-relative.
    unix_epoch_ticks = 116_444_736_000_000_000
    return f"pid:{pid}:start:{(ticks - unix_epoch_ticks) * 100}"


def _parse_proc_stat_starttime(stat_text: str) -> int:
    """Read Linux /proc/PID/stat field 22 without splitting the comm field."""
    closing_paren = stat_text.rfind(")")
    if closing_paren < 0:
        raise ValueError("process stat has no comm terminator")
    fields_from_state = stat_text[closing_paren + 1 :].split()
    if len(fields_from_state) < 20:
        raise ValueError("process stat has no starttime field")
    starttime = int(fields_from_state[19])
    if starttime <= 0:
        raise ValueError("process stat starttime must be positive")
    return starttime
