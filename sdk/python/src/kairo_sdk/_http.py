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
from pathlib import Path
from typing import Any


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Surface a 3xx as an ``HTTPError`` instead of following it."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def is_loopback(host: str | None) -> bool:
    """``localhost`` or a loopback IP literal; never a name that merely resolves to one."""
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


def require_token_transport(url: str) -> None:
    """Refuse a URL a bearer token must not travel to: plain http off loopback."""
    parts = urllib.parse.urlsplit(url)
    scheme = parts.scheme.lower()
    if scheme == "https":
        return
    if scheme != "http":
        raise ValueError(
            f"Kairo API URL must use https (or http to loopback), not {scheme!r}"
        )
    if not is_loopback(parts.hostname):
        host = parts.netloc.rpartition("@")[2]  # never echo userinfo
        raise ValueError(
            f"refusing to send a token over plain http to {host}: use https"
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
) -> dict[str, Any]:
    """Send one JSON request; transport errors (``URLError``) propagate."""
    require_token_transport(url)
    data = None
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
    if body is not None:
        data = json.dumps(body, separators=(",", ":")).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with opener.open(request, timeout=timeout) as response:
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
