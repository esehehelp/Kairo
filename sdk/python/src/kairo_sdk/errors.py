"""What a client does with a failed Kairo request (``sdk/conformance/classify.json``).

Every failure maps to one class:

``transient``
    Retry with backoff: 5xx, 429, a refused or reset connection, a timeout, a
    DNS failure, a TLS connection dropped mid-stream.
``permanent``
    Configuration or a bug: report, do not retry. Other 4xx, 3xx (redirects
    are never followed), TLS verification and handshake failures.
``retired``
    Worker only, HTTP 401: the attempt credential is no longer valid, which
    is expected once the attempt quiesced. Stop heartbeating and polling; the
    process should exit.
``fenced``
    Worker only, HTTP 409 ``stale_epoch``/``execution_started``: the attempt
    lost its lease or epoch. Stop all writes and exit non-zero unless already
    checkpointed.
``gone``
    Worker only, HTTP 404: the command being acknowledged no longer exists.
    Carry on.
``retry_later``
    HTTP 423: an admission gate is closed or the daemon is observe-only. Try
    again later; not an error.
"""

from __future__ import annotations

import http.client
import json
import socket
import ssl
import urllib.error
from typing import Any

TRANSIENT = "transient"
PERMANENT = "permanent"
RETIRED = "retired"
FENCED = "fenced"
GONE = "gone"
RETRY_LATER = "retry_later"

WORKER = "worker"
OPERATOR = "operator"

# Connection-level TLS failures: the peer went away mid-handshake or mid-read,
# which a daemon restart produces. Every other SSLError is a trust or protocol
# mismatch that no retry can fix.
_TRANSIENT_SSL_ERRORS = (
    ssl.SSLEOFError,
    ssl.SSLZeroReturnError,
    ssl.SSLSyscallError,
    ssl.SSLWantReadError,
    ssl.SSLWantWriteError,
)

_FENCING_CODES = frozenset({"stale_epoch", "execution_started"})


class KairoAPIError(RuntimeError):
    """The Kairo API answered with an HTTP error status.

    ``str(error)`` stays ``"Kairo API returned HTTP {status}: {body}"``.
    ``code`` and ``message`` come from the ``{"error": ..., "code": ...}``
    body when there is one; ``classification`` is one of the classes above,
    for the role (``"worker"`` or ``"operator"``) that sent the request.
    """

    def __init__(
        self,
        status: int,
        code: str = "",
        message: str = "",
        *,
        detail: str | None = None,
        role: str = OPERATOR,
    ) -> None:
        self.status = int(status)
        self.code = code or ""
        self.message = message or ""
        self.role = role
        self.classification = classify_status(self.status, self.code, role=role)
        if detail is None:
            detail = json.dumps({"error": self.message, "code": self.code})
        self.detail = detail
        super().__init__(f"Kairo API returned HTTP {self.status}: {detail}")


class AttemptRetired(KairoAPIError):
    """The attempt token is no longer valid (worker HTTP 401): stop and exit."""


class AttemptFenced(KairoAPIError):
    """The attempt lost its lease or coordination epoch: stop all writes and exit."""


class NoContinuationError(ValueError):
    """A suspend's checkpoint callback returned no ``continuation_ref``.

    The command was acknowledged ``rejected``; Kairo cannot resume an
    execution without a continuation, so nothing was published.
    """


def api_error(status: int, body: bytes | str, *, role: str = OPERATOR) -> KairoAPIError:
    """Build the exception for an HTTP error response (the matching subclass)."""
    detail = body.decode("utf-8", errors="replace") if isinstance(body, bytes) else body
    code = message = ""
    try:
        decoded: Any = json.loads(detail) if detail else None
    except ValueError:
        decoded = None
    if isinstance(decoded, dict):
        code = str(decoded.get("code") or "")
        message = str(decoded.get("error") or "")
    if not message:
        message = detail.strip()
    classification = classify_status(status, code, role=role)
    error_type = {RETIRED: AttemptRetired, FENCED: AttemptFenced}.get(
        classification, KairoAPIError
    )
    return error_type(status, code, message, detail=detail, role=role)


def classify_status(status: int, code: str = "", *, role: str = WORKER) -> str:
    """Classify an HTTP error status and wire code for a worker or an operator."""
    if role not in (WORKER, OPERATOR):
        raise ValueError(f"role must be 'worker' or 'operator', not {role!r}")
    worker = role == WORKER
    if status >= 500 or status == 429:
        return TRANSIENT
    if status == 423:
        return RETRY_LATER
    if worker and status == 401:
        return RETIRED
    if worker and status == 409 and code in _FENCING_CODES:
        return FENCED
    if worker and status == 404:
        return GONE
    return PERMANENT


def transport_kind(error: BaseException) -> str | None:
    """Name a failure below HTTP, or ``None`` when ``error`` is not one.

    ``refused``, ``reset``, ``timeout``, ``dns`` and ``tls_verify`` /
    ``tls_handshake`` are the conformance names; ``network`` covers other
    socket-level failures (unreachable, aborted, an interrupted HTTP exchange)
    and ``invalid`` a request urllib could not even attempt.
    """
    if isinstance(error, urllib.error.HTTPError):
        return None
    if isinstance(error, urllib.error.URLError):
        reason = error.reason
        if isinstance(reason, BaseException):
            return transport_kind(reason) or "network"
        return "invalid"  # e.g. "unknown url type"
    if isinstance(error, ssl.SSLCertVerificationError):
        return "tls_verify"
    if isinstance(error, _TRANSIENT_SSL_ERRORS):
        return "reset"
    if isinstance(error, ssl.SSLError):
        return "tls_handshake"
    if isinstance(error, socket.gaierror):
        return "dns"
    if isinstance(error, ConnectionRefusedError):
        return "refused"
    if isinstance(error, (ConnectionResetError, ConnectionAbortedError, BrokenPipeError)):
        return "reset"
    if isinstance(error, TimeoutError):
        return "timeout"
    if isinstance(error, (OSError, http.client.HTTPException)):
        return "network"
    return None


_TRANSPORT_CLASSES = {
    "refused": TRANSIENT,
    "reset": TRANSIENT,
    "timeout": TRANSIENT,
    "dns": TRANSIENT,
    "network": TRANSIENT,
    "tls_verify": PERMANENT,
    "tls_handshake": PERMANENT,
    "invalid": PERMANENT,
}


def classify(
    error_or_status: BaseException | int | str,
    code: str = "",
    *,
    role: str | None = None,
) -> str:
    """Classify a failed Kairo request; see the module docstring for the classes.

    Accepts a :class:`KairoAPIError` (classified for ``role``, by default the
    role that sent the request), an HTTP status with its wire ``code``
    (``role`` defaults to ``"worker"``), a transport name from the conformance
    fixtures (``"refused"``, ``"tls_verify"``, ...), or a transport exception
    (``urllib.error.URLError``, ``OSError``, ``ssl.SSLError``). Anything else
    is ``permanent``: it is a bug, and retrying will not fix it.
    """
    if isinstance(error_or_status, KairoAPIError):
        return classify_status(
            error_or_status.status,
            error_or_status.code,
            role=role or error_or_status.role,
        )
    if isinstance(error_or_status, bool):
        raise TypeError("classify expects an exception, an HTTP status or a transport name")
    if isinstance(error_or_status, int):
        return classify_status(error_or_status, code, role=role or WORKER)
    if isinstance(error_or_status, str):
        try:
            return _TRANSPORT_CLASSES[error_or_status]
        except KeyError:
            raise ValueError(f"unknown transport failure {error_or_status!r}") from None
    if isinstance(error_or_status, BaseException):
        kind = transport_kind(error_or_status)
        return _TRANSPORT_CLASSES[kind] if kind is not None else PERMANENT
    raise TypeError("classify expects an exception, an HTTP status or a transport name")
