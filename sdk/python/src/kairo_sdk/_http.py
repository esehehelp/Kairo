"""The one HTTP path every SDK client uses to reach the Kairo API.

Control traffic carries a bearer token, so it never goes through an
environment-configured proxy and never follows a redirect: either would hand
the token to a host the operator did not name. For the same reason a token
travels only over TLS, or over plain ``http://`` to a loopback host (the rule
the ``kairo`` CLI applies).
"""

from __future__ import annotations

import ipaddress
import json
import ssl
import urllib.error
import urllib.parse
import urllib.request
from email.message import Message
from pathlib import Path
from typing import Any

from . import errors


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Surface a 3xx as an ``HTTPError`` instead of following it."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def is_loopback(host: str | None) -> bool:
    """``localhost`` or a loopback IP literal; never a name that merely resolves to one.

    ``loopback.json``: 127.0.0.0/8, ::1 and ::ffff:127.x match; zoned IPv6
    literals, trailing dots and other names do not. Brackets are optional.
    """
    if not host:
        return False
    if host.startswith("[") and host.endswith("]"):
        host = host[1:-1]
    if host.lower() == "localhost":
        return True
    if "%" in host:  # a zoned IPv6 literal, which Go's net.ParseIP rejects
        return False
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return False
    if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped:
        address = address.ipv4_mapped  # Go treats ::ffff:a.b.c.d as IPv4
    return address.is_loopback


def check_api_url(url: str) -> urllib.parse.SplitResult:
    """Parse a Kairo API URL: ``http(s)://HOST[:PORT]``, never with user:password."""
    parts = urllib.parse.urlsplit(url)
    host = parts.netloc.rpartition("@")[2]  # never echo userinfo
    shown = f"{parts.scheme}://{host}{parts.path}" if "@" in parts.netloc else url
    try:
        hostname, _port = parts.hostname, parts.port
    except ValueError:
        hostname = None
    if parts.scheme.lower() not in ("http", "https") or not hostname:
        raise ValueError(f"invalid Kairo API URL {shown!r}: want https://HOST:PORT")
    if "@" in parts.netloc:
        raise ValueError(
            f"Kairo API URL {shown!r} must not carry user:password; pass the token separately"
        )
    return parts


def require_token_transport(url: str) -> None:
    """Refuse a URL a bearer token must not travel to (``token_transport.json``).

    https always; plain http only to a loopback host; any other scheme, a
    missing host, or user:password in the URL is refused.
    """
    parts = check_api_url(url)
    if parts.scheme.lower() == "https":
        return
    if not is_loopback(parts.hostname):
        raise ValueError(
            f"refusing to send a token over plain http to {parts.netloc}: use https"
        )


def ssl_context(
    *, ca_der: bytes | None = None, ca_file: str | Path | None = None
) -> ssl.SSLContext:
    """TLS 1.3 context trusting the given CA, or the system roots if none.

    ``ca_der`` may hold several certificates as the concatenation of their DER
    encodings (an old and a new CA during rotation); every one is trusted.
    """
    if ca_der is not None and ca_file is not None:
        raise ValueError("pass ca_der or ca_file, not both")
    if ca_der is not None:
        context = ssl.create_default_context(cadata=ca_der)
    elif ca_file is not None:
        context = ssl.create_default_context(cafile=str(ca_file))
    else:
        context = ssl.create_default_context()
    context.minimum_version = ssl.TLSVersion.TLSv1_3
    return context


def build_opener(
    *, ca_der: bytes | None = None, ca_file: str | Path | None = None
) -> urllib.request.OpenerDirector:
    return urllib.request.build_opener(
        urllib.request.ProxyHandler({}),
        _NoRedirect(),
        urllib.request.HTTPSHandler(
            context=ssl_context(ca_der=ca_der, ca_file=ca_file)
        ),
    )


def request_json(
    opener: urllib.request.OpenerDirector,
    method: str,
    url: str,
    token: str,
    body: Any,
    timeout: float,
    *,
    role: str = errors.OPERATOR,
) -> dict[str, Any]:
    """Send one JSON request.

    An HTTP error status raises :class:`~kairo_sdk.errors.KairoAPIError` (or
    ``AttemptRetired``/``AttemptFenced``, classified for ``role``); transport
    errors (``URLError``, ``OSError``) propagate unchanged, and
    :func:`~kairo_sdk.errors.classify` maps them.
    """
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body, separators=(",", ":")).encode("utf-8")
        headers["Content-Type"] = "application/json"
    payload, _headers = _send(opener, method, url, token, data, headers, timeout, role)
    if not payload:
        return {}
    decoded = json.loads(payload)
    if not isinstance(decoded, dict):
        raise RuntimeError("Kairo API response must be a JSON object")
    return decoded


def request_bytes(
    opener: urllib.request.OpenerDirector,
    method: str,
    url: str,
    token: str,
    timeout: float,
    *,
    role: str = errors.OPERATOR,
) -> tuple[bytes, Message]:
    """Send one bodiless request and return the raw response body and headers.

    For non-JSON responses (attempt logs); errors are raised as by
    :func:`request_json`, whose error bodies stay JSON.
    """
    return _send(opener, method, url, token, None, {}, timeout, role)


def _send(
    opener: urllib.request.OpenerDirector,
    method: str,
    url: str,
    token: str,
    data: bytes | None,
    headers: dict[str, str],
    timeout: float,
    role: str,
) -> tuple[bytes, Message]:
    require_token_transport(url)
    headers = {**headers, "Authorization": f"Bearer {token}"}
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with opener.open(request, timeout=timeout) as response:
            return response.read(), response.headers
    except urllib.error.HTTPError as error:
        try:
            detail = error.read()
        except OSError:
            detail = b""
        raise errors.api_error(error.code, detail, role=role) from error
