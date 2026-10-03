from __future__ import annotations

import base64
import hashlib
import logging
import os
import signal
import ssl
import threading
import time
import urllib.parse
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from . import _http, errors
from .errors import AttemptFenced, AttemptRetired, KairoAPIError, NoContinuationError

_log = logging.getLogger("kairo_sdk.runtime")


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
class GangContext:
    """This attempt's place in a gang (``KAIRO_GANG_*``): one rank of ``size``."""

    id: str
    rank: int
    size: int
    master_addr: str
    master_port: int


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


# --------------------------------------------------------------------------
# Stop signals


_stop_signal = threading.Event()


def _on_stop_signal(signum, _frame) -> None:
    _stop_signal.set()
    _log.info("stop signal %s received; stopping at the next safe point", signum)


def install_stop_signals() -> None:
    """Turn SIGTERM (and SIGBREAK on Windows) into a process-wide stop request.

    Kairo's force stop sends CTRL_BREAK (Windows) or SIGTERM first and kills
    the process only after a grace period, so a worker that installs these
    handlers can leave at its next safe point instead of being killed
    mid-step. :meth:`AttemptSession.safe_point` then returns ``True`` and
    :func:`stop_requested` tells why. Call from the main thread.
    """
    signal.signal(signal.SIGTERM, _on_stop_signal)
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, _on_stop_signal)


def stop_requested() -> bool:
    """Whether a stop signal arrived since :func:`install_stop_signals`."""
    return _stop_signal.is_set()


def continuation_or(resolver: Callable[[], str | None]) -> str | None:
    """The continuation Kairo handed this attempt, else ``resolver()``.

    ``KAIRO_CONTINUATION_REF`` is set when Kairo resumes an execution from a
    published checkpoint; a first run (or an unmanaged one) falls back to
    ``resolver``, e.g. "the newest checkpoint in my run directory", which may
    return ``None`` for a fresh start.
    """
    return os.environ.get("KAIRO_CONTINUATION_REF") or resolver()


# --------------------------------------------------------------------------
# The attempt session


_shared_lock = threading.Lock()
_shared_session: AttemptSession | None = None


class AttemptSession:
    """Managed attempt context with independent heartbeats and safe points.

    Commands are delivered at least once. A checkpoint callback can therefore
    be called repeatedly with the same command ID and must be idempotent;
    :class:`CommandJournal` makes that a lookup.

    When the attempt token stops being valid (HTTP 401) the session is
    :attr:`retired`; when the attempt lost its lease or epoch it is
    :attr:`fenced`. Either raises :class:`~kairo_sdk.errors.AttemptRetired` /
    :class:`~kairo_sdk.errors.AttemptFenced` from every later call and stops
    the background heartbeat for good: the process should exit.
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
        gang: GangContext | None = None,
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
        if self.api_url is not None:
            _http.require_token_transport(self.api_url)
        self.execution_id = execution_id
        self.attempt_id = attempt_id
        self._token = token
        self.ca_certificates = split_der_certificates(ca_der) if ca_der else ()
        self._opener = _http.build_opener(ca_der=ca_der) if self.api_url is not None else None
        self.node_id = node_id
        self.executor_id = executor_id
        self.gang = gang
        self.poll_interval_seconds = poll_interval_seconds
        self.heartbeat_interval_seconds = heartbeat_interval_seconds
        self.timeout_seconds = timeout_seconds
        self._last_poll = float("-inf")
        self._last_heartbeat = float("-inf")
        self._progress: dict[str, Any] = {}
        self._stop = threading.Event()
        self._heartbeat_thread: threading.Thread | None = None
        self.last_control_error: BaseException | None = None
        self.retired = False
        self.fenced = False
        self._terminal_error: KairoAPIError | None = None
        self.suspend_handled = False
        self._stop_signal_handled = False
        self.process_identity = _current_process_identity()
        self._process_registered = False
        self._registration_lock = threading.Lock()

    @classmethod
    def from_environment(
        cls,
        *,
        stop_paths: Iterable[str | Path] = (),
        poll_interval_seconds: float = 1.0,
        heartbeat_interval_seconds: float = 10.0,
    ) -> AttemptSession:
        """Read the launch environment (``sdk/conformance/worker_env.json``).

        Managed when ``KAIRO_API_URL`` is set: ``KAIRO_EXECUTION_ID``,
        ``KAIRO_ATTEMPT_ID`` and ``KAIRO_ATTEMPT_TOKEN`` are then required.
        Unmanaged, none of those nor ``KAIRO_API_CA`` may be set.
        """
        api_url = os.environ.get("KAIRO_API_URL")
        required = ("KAIRO_EXECUTION_ID", "KAIRO_ATTEMPT_ID", "KAIRO_ATTEMPT_TOKEN")
        values = {name: os.environ.get(name) or None for name in required}
        ca_text = os.environ.get("KAIRO_API_CA")
        if api_url:
            missing = [name for name in required if values[name] is None]
            if missing:
                raise RuntimeError(
                    "managed Kairo context (KAIRO_API_URL is set) is missing "
                    + ", ".join(missing)
                )
        else:
            stray = [name for name in (*required, "KAIRO_API_CA") if os.environ.get(name)]
            if stray:
                raise RuntimeError(
                    "unmanaged Kairo context (KAIRO_API_URL is unset) cannot provide "
                    + ", ".join(stray)
                )
        ca_der = _decode_ca_environment(ca_text) if ca_text else None
        return cls(
            api_url=api_url or None,
            execution_id=values["KAIRO_EXECUTION_ID"],
            attempt_id=values["KAIRO_ATTEMPT_ID"],
            token=values["KAIRO_ATTEMPT_TOKEN"],
            ca_der=ca_der,
            node_id=os.environ.get("KAIRO_NODE_ID"),
            executor_id=os.environ.get("KAIRO_EXECUTOR_ID"),
            gang=_gang_from_environment(),
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
        """Start the background heartbeat (managed sessions only).

        The first started session also becomes the one
        :meth:`ProgressReporter.from_environment` reports through, so a
        process keeps a single heartbeat.
        """
        global _shared_session
        if not self.managed or self._heartbeat_thread is not None:
            return
        with _shared_lock:
            if _shared_session is None:
                _shared_session = self
        self._stop.clear()
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop, name="kairo-heartbeat", daemon=True
        )
        self._heartbeat_thread.start()

    def close(self) -> None:
        global _shared_session
        self._stop.set()
        thread, self._heartbeat_thread = self._heartbeat_thread, None
        if thread is not None:
            thread.join(timeout=max(1.0, self.timeout_seconds + 0.5))
        with _shared_lock:
            if _shared_session is self:
                _shared_session = None

    def _heartbeat_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.heartbeat()
                self.last_control_error = None
            except (AttemptRetired, AttemptFenced) as error:
                _log.warning(
                    "Kairo attempt %s is %s; background heartbeats stopped: %s",
                    self.attempt_id,
                    "retired" if isinstance(error, AttemptRetired) else "fenced",
                    error,
                )
                return
            except Exception as error:
                self.last_control_error = error
                classification = errors.classify(error)
                if classification in (errors.TRANSIENT, errors.RETRY_LATER):
                    _log.warning("Kairo heartbeat failed (%s), retrying: %s", classification, error)
                else:
                    # A heartbeat is the attempt's liveness: keep trying, but
                    # loudly, since no retry is expected to fix this.
                    _log.error("Kairo heartbeat failed (%s): %s", classification, error)
            self._stop.wait(self.heartbeat_interval_seconds)

    def set_progress(self, progress: Mapping[str, Any] | ProgressEnvelope) -> None:
        self._progress = progress.as_dict() if isinstance(progress, ProgressEnvelope) else dict(progress)

    def heartbeat(self, progress: Mapping[str, Any] | ProgressEnvelope | None = None) -> None:
        if not self.managed:
            return
        if progress is not None:
            self.set_progress(progress)
        if not self._process_registered:
            with self._registration_lock:  # the heartbeat thread may race a caller
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
        """Handle pending commands; ``True`` means the worker must stop now.

        A ``suspend`` is acknowledged ``accepted`` and ``checkpointing``, then
        ``checkpoint`` runs and must return the continuation Kairo resumes
        from (a ``SuspendResult``, or a path/string used as its
        ``continuation_ref``); the ``checkpointed`` acknowledgement publishes
        it and ``True`` is returned. Commands arrive at least once, so the
        callback must be idempotent by command id, for example::

            journal = CommandJournal(run_dir / "kairo-commands")

            def checkpoint(command):
                if (done := journal.result(command.command_id)) is not None:
                    return done  # redelivered: publish the same checkpoint
                path = save_checkpoint(run_dir / f"step-{step}.pt")
                return journal.record(
                    command.command_id, SuspendResult(str(path), {"step": step})
                )

            if session.safe_point(checkpoint=checkpoint):
                sys.exit(0)

        If the callback raises, the command is acknowledged ``rejected`` and
        the exception propagates; if it returns no ``continuation_ref``,
        ``rejected`` too, and :class:`~kairo_sdk.errors.NoContinuationError`
        is raised. A command that disappeared meanwhile (HTTP 404) is dropped
        and the worker carries on. Unknown command kinds are skipped.

        After command handling, a stop signal (:func:`install_stop_signals`)
        also stops the worker: ``checkpoint`` runs once, with ``reason``
        ``"stop_signal"`` and an empty ``command_id``, so the work can be
        resumed by hand, and ``True`` is returned. Nothing is acknowledged
        for it: Kairo's force stop (CTRL_BREAK / SIGTERM first, a kill after
        its grace period) plans no continuation. Keep that checkpoint within
        the grace period.

        Raises :class:`~kairo_sdk.errors.AttemptRetired` /
        :class:`~kairo_sdk.errors.AttemptFenced` when the attempt is over,
        and :class:`~kairo_sdk.errors.KairoAPIError` (see its
        ``classification``) or a transport error otherwise.
        """
        if progress is not None:
            self.set_progress(progress)
        self._raise_if_terminal()
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
        if self.managed and now - self._last_poll >= self.poll_interval_seconds:
            self._last_poll = now
            for raw in self.poll_commands():
                if self._handle_command(raw, checkpoint):
                    return True
        if stop_requested():
            if not self._stop_signal_handled:
                checkpoint(_stop_signal_context(self))
                self._stop_signal_handled = True
            return True
        return False

    def _handle_command(
        self,
        raw: Mapping[str, Any],
        checkpoint: Callable[[CommandContext], SuspendResult | str | Path | None],
    ) -> bool:
        context = CommandContext(
            command_id=str(raw["id"]),
            execution_id=self.execution_id,
            attempt_id=self.attempt_id,
            kind=str(raw.get("kind") or ""),
            reason=str(raw.get("reason") or ""),
            delivery_count=int(raw.get("delivery_count") or 0),
        )
        if context.kind != "suspend":
            _log.debug("skipping Kairo command %s of unknown kind %r", context.command_id, context.kind)
            return False
        try:
            self._ack(context.command_id, "accepted")
            self._ack(context.command_id, "checkpointing")
        except KairoAPIError as error:
            if error.classification != errors.GONE:
                raise
            _log.info("Kairo command %s was withdrawn: %s", context.command_id, error)
            return False
        try:
            result = normalize_suspend_result(checkpoint(context))
        except BaseException as error:
            self._try_reject(context.command_id, f"{type(error).__name__}: {error}")
            raise
        if not result.continuation_ref:
            self._try_reject(context.command_id, "no continuation_ref")
            raise NoContinuationError(
                f"checkpoint callback returned no continuation_ref for command {context.command_id}"
            )
        try:
            self._ack(context.command_id, "checkpointed", suspend_payload(result))
        except KairoAPIError as error:
            if error.classification != errors.GONE:
                raise
            _log.info("Kairo command %s was withdrawn: %s", context.command_id, error)
            return False
        self.suspend_handled = True
        return True

    def _try_reject(self, command_id: str, reason: str) -> None:
        try:
            self._ack(command_id, "rejected", {"reason": reason})
        except Exception as error:  # best effort: the caller raises the real failure
            self.last_control_error = error
            _log.warning("could not acknowledge Kairo command %s as rejected: %s", command_id, error)

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

    def _raise_if_terminal(self) -> None:
        if self._terminal_error is not None:
            raise self._terminal_error

    def _request(self, method: str, path: str, body: Any) -> dict[str, Any]:
        if self.api_url is None or self._opener is None or self._token is None:
            raise RuntimeError("attempt is not managed by Kairo")
        self._raise_if_terminal()
        try:
            return _http.request_json(
                self._opener,
                method,
                self.api_url + path,
                self._token,
                body,
                self.timeout_seconds,
                role=errors.WORKER,
            )
        except AttemptRetired as error:
            self.retired = True
            self._terminal_error = self.last_control_error = error
            raise
        except AttemptFenced as error:
            self.fenced = True
            self._terminal_error = self.last_control_error = error
            raise


def _process_session() -> AttemptSession | None:
    """The process's shared managed session, created and started on first use."""
    global _shared_session
    with _shared_lock:
        session = _shared_session
    if session is not None:
        return session
    created = AttemptSession.from_environment()
    if not created.managed:
        return None
    created.start()
    with _shared_lock:
        session = _shared_session
    if session is not created:
        created.close()  # another thread won the race
    return session


# --------------------------------------------------------------------------
# Progress


class ProgressReporter:
    """Report progress through the process's one Kairo heartbeat.

    ``ProgressReporter.from_environment("step", total=1000)`` does nothing
    when the process is not managed by Kairo; reporting never raises.
    """

    def __init__(
        self,
        session: AttemptSession | None,
        unit: str,
        total: int | float | None = None,
        message: str | None = None,
    ) -> None:
        self.session = session if session is not None and session.managed else None
        self._lock = threading.Lock()
        self._unit = unit
        self._current: int | float = 0
        self._total = total
        self._message = message
        self._detail: dict[str, Any] = {}
        self._publish()

    @classmethod
    def from_environment(
        cls,
        unit: str,
        total: int | float | None = None,
        message: str | None = None,
    ) -> ProgressReporter:
        """Share the process's session (started on first use), or report nowhere."""
        return cls(_process_session(), unit, total, message)

    @property
    def envelope(self) -> ProgressEnvelope:
        with self._lock:
            return ProgressEnvelope(
                unit=self._unit,
                current=self._current,
                total=self._total,
                message=self._message,
                detail=dict(self._detail),
            )

    def set(self, current: int | float) -> None:
        with self._lock:
            self._current = current
        self._publish()

    def add(self, n: int | float = 1) -> None:
        with self._lock:
            self._current += n
        self._publish()

    def set_total(self, total: int | float | None) -> None:
        with self._lock:
            self._total = total
        self._publish()

    def set_message(self, message: str | None) -> None:
        with self._lock:
            self._message = message
        self._publish()

    def detail(self, **values: Any) -> None:
        """Merge ``values`` into the free-form detail (``None`` removes a key)."""
        with self._lock:
            for key, value in values.items():
                if value is None:
                    self._detail.pop(key, None)
                else:
                    self._detail[key] = value
        self._publish()

    def phase(self, unit: str, total: int | float | None = None, message: str | None = None) -> None:
        """Switch to counting ``unit``; the same unit again keeps its count."""
        with self._lock:
            if unit != self._unit:
                self._unit = unit
                self._current = 0
            self._total = total
            self._message = message
        self._publish()

    def close(self) -> None:
        """Send a final heartbeat with the last progress."""
        if self.session is None:
            return
        try:
            self.session.heartbeat(self.envelope)
        except Exception as error:
            _log.debug("final Kairo progress heartbeat failed: %s", error)

    def __enter__(self) -> ProgressReporter:
        return self

    def __exit__(self, _type, _value, _traceback) -> None:
        self.close()

    def _publish(self) -> None:
        if self.session is None:
            return
        try:
            self.session.set_progress(self.envelope)
        except Exception as error:  # never let reporting break training
            _log.debug("Kairo progress update failed: %s", error)


# --------------------------------------------------------------------------
# Helpers


def normalize_suspend_result(value: SuspendResult | str | Path | None) -> SuspendResult:
    if value is None:
        return SuspendResult()
    if isinstance(value, SuspendResult):
        return value
    if isinstance(value, (str, Path)):
        return SuspendResult(continuation_ref=str(value))
    raise TypeError("checkpoint callback must return SuspendResult, path, string, or None")


_normalize_suspend_result = normalize_suspend_result  # compatibility alias


def suspend_payload(result: SuspendResult) -> dict[str, Any]:
    """The ``checkpointed`` acknowledgement payload for ``result``."""
    payload = dict(result.payload)
    if result.continuation_ref is not None:
        payload["continuation_ref"] = result.continuation_ref
    return payload


def split_der_certificates(data: bytes) -> tuple[bytes, ...]:
    """Split concatenated DER certificates; ``ValueError`` if ``data`` is not that."""
    certificates: list[bytes] = []
    offset = 0
    while offset < len(data):
        if data[offset] != 0x30 or offset + 2 > len(data):
            raise ValueError("not a DER certificate sequence")
        length = data[offset + 1]
        header = 2
        if length & 0x80:
            count = length & 0x7F
            if count == 0 or count > 4 or offset + 2 + count > len(data):
                raise ValueError("invalid DER length")
            length = int.from_bytes(data[offset + 2 : offset + 2 + count], "big")
            header += count
        end = offset + header + length
        if end > len(data):
            raise ValueError("truncated DER certificate")
        certificates.append(data[offset:end])
        offset = end
    if not certificates:
        raise ValueError("no certificate")
    return tuple(certificates)


def _decode_ca_environment(text: str) -> bytes:
    try:
        der = base64.b64decode(text.strip(), validate=True)
        split_der_certificates(der)
        _http.ssl_context(ca_der=der)
    except (ValueError, ssl.SSLError) as error:  # binascii.Error, non-ASCII, bad DER
        raise RuntimeError(
            "KAIRO_API_CA must be a base64 DER certificate, or several concatenated"
        ) from error
    return der


_GANG_VARIABLES = (
    "KAIRO_GANG_RANK",
    "KAIRO_GANG_SIZE",
    "KAIRO_GANG_MASTER_ADDR",
    "KAIRO_GANG_MASTER_PORT",
)


def _gang_from_environment() -> GangContext | None:
    gang_id = os.environ.get("KAIRO_GANG_ID")
    if not gang_id:
        return None
    values = {name: os.environ.get(name, "") for name in _GANG_VARIABLES}
    missing = [name for name, value in values.items() if not value]
    if missing:
        raise RuntimeError("KAIRO_GANG_ID is set but " + ", ".join(missing) + " is missing")
    numbers: dict[str, int] = {}
    for name in ("KAIRO_GANG_RANK", "KAIRO_GANG_SIZE", "KAIRO_GANG_MASTER_PORT"):
        try:
            numbers[name] = int(values[name])
        except ValueError:
            raise RuntimeError(f"{name} must be an integer, not {values[name]!r}") from None
    rank, size, port = (
        numbers["KAIRO_GANG_RANK"],
        numbers["KAIRO_GANG_SIZE"],
        numbers["KAIRO_GANG_MASTER_PORT"],
    )
    if size < 1 or not 0 <= rank < size:
        raise RuntimeError(f"KAIRO_GANG_RANK {rank} is outside KAIRO_GANG_SIZE {size}")
    if not 0 < port < 65536:
        raise RuntimeError(f"KAIRO_GANG_MASTER_PORT {port} is not a TCP port")
    return GangContext(
        id=gang_id,
        rank=rank,
        size=size,
        master_addr=values["KAIRO_GANG_MASTER_ADDR"],
        master_port=port,
    )


def _stop_signal_context(session: "AttemptSession") -> CommandContext:
    """The checkpoint request a stop signal stands for (no Kairo command)."""
    return CommandContext(
        command_id="",
        execution_id=session.execution_id,
        attempt_id=session.attempt_id,
        kind="suspend",
        reason="stop_signal",
        delivery_count=1,
    )


def _stop_file_command_id(path: Path) -> str:
    stat = path.stat()
    material = f"{path.resolve()}\0{stat.st_mtime_ns}\0{stat.st_size}".encode()
    return "stopfile_" + hashlib.sha256(material).hexdigest()[:24]


def _csv_environment(name: str) -> tuple[str, ...]:
    return tuple(part for part in os.environ.get(name, "").split(",") if part)


# --------------------------------------------------------------------------
# Process identity (sdk/conformance/identity.json)

_UNIX_EPOCH_FILETIME = 116_444_736_000_000_000


def process_identity_from_proc_stat(pid: int, stat_text: str) -> str | None:
    """Linux/WSL identity ``proc:PID:starttime:T`` from ``/proc/PID/stat`` text.

    ``None`` when the text carries no positive start time.
    """
    try:
        return f"proc:{pid}:starttime:{_parse_proc_stat_starttime(stat_text)}"
    except ValueError:
        return None


def process_identity_from_filetime(pid: int, creation_filetime: int) -> str:
    """Windows identity ``pid:PID:start:NS``, NS the creation time in Unix nanoseconds.

    Matches Go's ``windows.Filetime.Nanoseconds()``.
    """
    return f"pid:{pid}:start:{(creation_filetime - _UNIX_EPOCH_FILETIME) * 100}"


def _current_process_identity() -> str:
    pid = os.getpid()
    if os.name != "nt":
        try:
            stat_text = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
        except OSError as error:
            raise RuntimeError(f"cannot establish process identity for pid {pid}") from error
        identity = process_identity_from_proc_stat(pid, stat_text)
        if identity is None:
            raise RuntimeError(f"cannot establish process identity for pid {pid}")
        return identity
    return process_identity_from_filetime(pid, _current_creation_filetime())


def _current_creation_filetime() -> int:
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
    return (created.dwHighDateTime << 32) | created.dwLowDateTime


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
