from __future__ import annotations

import hashlib
import json
import os
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class CommandContext:
    command_id: str
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
        attempt_id: str | None,
        workload_id: str | None,
        lease_id: str | None = None,
        coordination_epoch: int | None = None,
        node_id: str | None = None,
        executor_id: str | None = None,
        stop_paths: Iterable[str | Path] = (),
        poll_interval_seconds: float = 1.0,
        heartbeat_interval_seconds: float = 10.0,
        timeout_seconds: float = 10.0,
    ) -> None:
        if (api_url is None) != (attempt_id is None):
            raise ValueError("api_url and attempt_id must be provided together")
        if (lease_id is None) != (coordination_epoch is None):
            raise ValueError("lease_id and coordination_epoch must be provided together")
        self.api_url = api_url.rstrip("/") if api_url else None
        self.attempt_id = attempt_id
        self.workload_id = workload_id
        self.lease_id = lease_id
        self.coordination_epoch = coordination_epoch
        self.node_id = node_id
        self.executor_id = executor_id
        self.stop_paths = tuple(Path(path) for path in stop_paths)
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
        attempt_id = os.environ.get("KAIRO_ATTEMPT_ID")
        workload_id = os.environ.get("KAIRO_WORKLOAD_ID")
        lease_id = os.environ.get("KAIRO_LEASE_ID")
        epoch_text = os.environ.get("KAIRO_COORDINATION_EPOCH")
        if bool(api_url) != bool(attempt_id):
            raise RuntimeError("incomplete Kairo context: API URL and attempt ID must be set together")
        if api_url and (not lease_id or not epoch_text):
            raise RuntimeError("managed Kairo context requires lease ID and coordination epoch")
        try:
            epoch = int(epoch_text) if epoch_text else None
        except ValueError as error:
            raise RuntimeError("KAIRO_COORDINATION_EPOCH must be an integer") from error
        return cls(
            api_url=api_url,
            attempt_id=attempt_id,
            workload_id=workload_id,
            lease_id=lease_id,
            coordination_epoch=epoch,
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
    def _worker_prefix(self) -> str:
        return "/v1/worker" if self.lease_id is not None else "/v1"

    @property
    def resource_ids(self) -> tuple[str, ...]:
        return _csv_environment("KAIRO_RESOURCE_IDS")

    @property
    def resource_bindings(self) -> tuple[str, ...]:
        return _csv_environment("KAIRO_RESOURCE_BINDINGS")

    @property
    def continuation_ref(self) -> str | None:
        return os.environ.get("KAIRO_CONTINUATION_REF") or None

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
        if self.lease_id is not None and not self._process_registered:
            self.register_process()
        self._request(
            "POST",
            f"{self._worker_prefix}/attempts/{self.attempt_id}/heartbeat",
            {"progress": self._progress},
        )
        self._last_heartbeat = time.monotonic()

    def register_process(self, *, rank: int = 0, pid: int | None = None, process_identity: str | None = None) -> None:
        if not self.managed or self.lease_id is None:
            return
        self._request(
            "POST",
            f"/v1/worker/attempts/{self.attempt_id}/processes",
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
            context = CommandContext(_stop_file_command_id(stop_path), self.attempt_id, "suspend", "stop_file", 1)
            checkpoint(context)
            stop_path.unlink(missing_ok=True)
            if self.managed and self.lease_id is None:
                self.report_disposition("hold", {"reason": "stop_file"})
            self.suspend_handled = True
            return True
        if not self.managed or now - self._last_poll < self.poll_interval_seconds:
            return False
        self._last_poll = now
        response = self._request("GET", f"{self._worker_prefix}/attempts/{self.attempt_id}/commands", None)
        for raw in response.get("commands", []):
            context = CommandContext(str(raw["id"]), self.attempt_id, str(raw["kind"]), str(raw.get("reason") or ""), int(raw.get("delivery_count") or 0))
            self._ack(context.command_id, "accepted")
            if context.kind == "suspend":
                self._ack(context.command_id, "checkpointing")
                result = _normalize_suspend_result(checkpoint(context))
                payload = dict(result.payload)
                if result.continuation_ref is not None:
                    payload["continuation_ref"] = result.continuation_ref
                self._ack(context.command_id, "checkpointed", payload)
            elif context.kind == "cancel" and self.lease_id is None:
                self.report_disposition("close", {"reason": "cancelled"})
            self.suspend_handled = True
            return True
        return False

    def complete(self, payload: Mapping[str, Any] | None = None) -> None:
        # The v1 executor reports process exit; SDK completion must not release
        # a lease while ranks or child processes may still exist.
        if self.lease_id is None:
            self.report_disposition("close", payload)

    def report_disposition(self, disposition: str, payload: Mapping[str, Any] | None = None) -> None:
        if not self.managed:
            return
        if self.lease_id is not None:
            raise RuntimeError("v1 terminal state is reported by the executor after process exit")
        self._request("POST", f"/v1/attempts/{self.attempt_id}/disposition", {"disposition": disposition, "payload": dict(payload or {})})

    def _ack(self, command_id: str, phase: str, payload: Mapping[str, Any] | None = None) -> None:
        self._request("POST", f"{self._worker_prefix}/attempts/{self.attempt_id}/commands/{command_id}/acks", {"phase": phase, "payload": dict(payload or {})})

    def _request(self, method: str, path: str, body: Any) -> dict[str, Any]:
        if self.api_url is None:
            raise RuntimeError("attempt is not managed by Kairo")
        data = None
        headers = {"Accept": "application/json"}
        if self.lease_id is not None:
            headers["Kairo-Lease-ID"] = self.lease_id
            headers["Kairo-Coordination-Epoch"] = str(self.coordination_epoch)
        if body is not None:
            data = json.dumps(body, separators=(",", ":")).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(self.api_url + path, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                payload = response.read()
        except urllib.error.HTTPError as error:
            detail = error.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Kairo API returned HTTP {error.code}: {detail}") from error
        if not payload:
            return {}
        decoded = json.loads(payload)
        if not isinstance(decoded, dict):
            raise RuntimeError("Kairo API response must be a JSON object")
        return decoded


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
            started = Path(f"/proc/{pid}").stat().st_mtime_ns
        except OSError:
            started = time.time_ns()
        return f"pid:{pid}:start:{started}"
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
