"""The operator side of the Kairo API: settings resolution and read helpers.

An operator client (CLI, controller, dashboard) finds the daemon, its token
and its CA the way the ``kairo`` CLI does (``sdk/conformance/
operator_resolution.json``). Resolution never touches the network, and a
missing token is an error only once a request needs one.
"""

from __future__ import annotations

import http.client
import os
import ssl
import sys
import urllib.error
import urllib.parse
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from . import _http, errors

DEFAULT_API_URL = "https://127.0.0.1:7474"


class TransientControllerError(RuntimeError):
    """A transport failure that an operator loop may safely retry."""


class ControllerConfigurationError(RuntimeError):
    """A failure no retry can fix, such as a TLS handshake or verification failure."""


@dataclass(frozen=True)
class CoordinationEvent:
    sequence: int
    event_type: str
    aggregate_type: str
    aggregate_id: str
    coordination_epoch: int | None
    payload: Mapping[str, Any]

    @classmethod
    def from_api(cls, raw: Mapping[str, Any]) -> CoordinationEvent:
        payload = raw.get("payload") or {}
        if not isinstance(payload, Mapping):
            raise TypeError("coordination event payload must be an object")
        epoch = raw.get("coordination_epoch")
        return cls(
            sequence=int(raw["sequence"]),
            event_type=str(raw["event_type"]),
            aggregate_type=str(raw["aggregate_type"]),
            aggregate_id=str(raw["aggregate_id"]),
            coordination_epoch=int(epoch) if epoch is not None else None,
            payload=payload,
        )


@dataclass(frozen=True)
class OperatorSettings:
    """Where an operator client sends requests, with which token and CA.

    ``token`` is ``None`` when none was found (a request then fails);
    ``ca_file`` is ``None`` for the system roots.
    """

    url: str
    token: str | None = field(default=None, repr=False)
    ca_file: Path | None = None


def _user_config_dir() -> Path | None:
    """Mirror Go's os.UserConfigDir, where the ``kairo`` CLI keeps its files."""
    if os.name == "nt":
        appdata = os.environ.get("APPDATA")
        return Path(appdata) if appdata else None
    home = os.environ.get("HOME")
    if sys.platform == "darwin":
        return Path(home, "Library", "Application Support") if home else None
    xdg = os.environ.get("XDG_CONFIG_HOME")
    if xdg:
        # Go rejects a relative XDG_CONFIG_HOME rather than falling back.
        return Path(xdg) if os.path.isabs(xdg) else None
    return Path(home, ".config") if home else None


def _read_token_file(path: Path, source: str) -> str:
    try:
        token = path.read_text(encoding="utf-8").strip()
    except OSError as error:
        raise RuntimeError(f"{source}: cannot read Kairo token file {path}: {error}") from error
    if not token:
        raise RuntimeError(f"{source}: Kairo token file {path} is empty")
    return token


def _resolve_token(token: str | None, token_file: str | Path | None) -> str | None:
    if token is not None:
        if not token.strip():
            raise ValueError("Kairo token must not be empty")
        return token.strip()
    if token_file is not None:
        return _read_token_file(Path(token_file), "token_file")
    if environment_token := os.environ.get("KAIRO_TOKEN", "").strip():
        return environment_token
    if environment_file := os.environ.get("KAIRO_TOKEN_FILE"):
        return _read_token_file(Path(environment_file), "KAIRO_TOKEN_FILE")
    config_dir = _user_config_dir()
    if config_dir is not None and (config_dir / "kairo" / "token").is_file():
        return _read_token_file(config_dir / "kairo" / "token", "<user config dir>/kairo/token")
    return None


def _require_ca_file(path: Path, source: str) -> Path:
    if not path.is_file():
        raise RuntimeError(f"{source}: Kairo CA file {path} does not exist")
    return path


def _resolve_ca_file(ca_file: str | Path | None) -> Path | None:
    if ca_file is not None:
        return _require_ca_file(Path(ca_file), "ca_file")
    if environment_file := os.environ.get("KAIRO_CA_FILE"):
        return _require_ca_file(Path(environment_file), "KAIRO_CA_FILE")
    config_dir = _user_config_dir()
    if config_dir is not None and (config_dir / "kairo" / "ca.pem").is_file():
        return config_dir / "kairo" / "ca.pem"
    return None


def resolve_operator_settings(
    api_url: str | None = None,
    token: str | None = None,
    token_file: str | Path | None = None,
    ca_file: str | Path | None = None,
) -> OperatorSettings:
    """Resolve the API URL, token and CA the way the ``kairo`` CLI does.

    Arguments win over the environment: the URL from ``KAIRO_API`` (default
    ``https://127.0.0.1:7474``); the token from ``KAIRO_TOKEN``, else the file
    ``KAIRO_TOKEN_FILE``, else ``<user config dir>/kairo/token``; the CA from
    ``KAIRO_CA_FILE``, else ``<user config dir>/kairo/ca.pem``, else the
    system roots. Worker variables (``KAIRO_API_URL``, ``KAIRO_ATTEMPT_TOKEN``)
    are never operator settings. A named token or CA file that is missing, or
    an empty token file, is an error; no token at all is not.
    """
    url = (api_url or os.environ.get("KAIRO_API") or DEFAULT_API_URL).rstrip("/")
    _http.check_api_url(url)
    resolved_token = _resolve_token(token, token_file)
    if resolved_token is not None:
        _http.require_token_transport(url)
    return OperatorSettings(url=url, token=resolved_token, ca_file=_resolve_ca_file(ca_file))


class KairoClient:
    """Read access to the operator API with one URL, token and CA.

    Unset arguments resolve through :func:`resolve_operator_settings`.
    Transport failures raise :class:`TransientControllerError` or, for TLS
    trust and handshake failures, :class:`ControllerConfigurationError`; HTTP
    errors raise :class:`~kairo_sdk.errors.KairoAPIError`.
    """

    def __init__(
        self,
        api_url: str | None = None,
        *,
        token: str | None = None,
        token_file: str | Path | None = None,
        ca_file: str | Path | None = None,
        timeout_seconds: float = 10.0,
    ) -> None:
        self.settings = resolve_operator_settings(api_url, token, token_file, ca_file)
        self.api_url = self.settings.url
        self.ca_file = self.settings.ca_file
        self._token = self.settings.token
        self.timeout_seconds = timeout_seconds
        self._opener = _http.build_opener(ca_file=self.ca_file)

    def whoami(self) -> dict[str, Any]:
        """The caller's principal: ``role``, ``name`` and, for nodes/workers, their ids."""
        return self._request("GET", "/api/whoami")

    def project(self, name: str) -> dict[str, Any]:
        """One project's status, as ``GET /api/projects/{name}`` returns it."""
        response = self._request("GET", f"/api/projects/{urllib.parse.quote(name, safe='')}")
        return _object(response, "project")

    def executions(
        self,
        project: str | None = None,
        state: str | None = None,
        *,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Up to ``limit`` executions, optionally of one project and in one state."""
        query = _query(project=project, state=state, limit=_limit(limit))
        return _array(self._request("GET", f"/api/executions?{query}"), "executions")

    def attempts(
        self,
        project: str | None = None,
        execution_id: str | None = None,
        state: str | None = None,
        *,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Up to ``limit`` attempts, newest first, with the given filters."""
        query = _query(
            project=project, execution_id=execution_id, state=state, limit=_limit(limit)
        )
        return _array(self._request("GET", f"/api/attempts?{query}"), "attempts")

    def events(
        self,
        project: str | None = None,
        after: int = 0,
        *,
        page_size: int = 1000,
    ) -> Iterator[CoordinationEvent]:
        """Iterate coordination events with sequence greater than ``after``.

        Pages until a short page. A page whose highest sequence does not pass
        the cursor is refused (``RuntimeError``) rather than looped on.
        """
        page_size = _limit(page_size)
        cursor = int(after)
        while True:
            query = _query(project=project, after=cursor, limit=page_size)
            page = _array(self._request("GET", f"/api/events?{query}"), "events")
            if not page:
                return
            decoded = [CoordinationEvent.from_api(raw) for raw in page]
            highest = max(item.sequence for item in decoded)
            if highest <= cursor:
                raise RuntimeError("Kairo events response did not advance the cursor")
            yield from decoded
            if len(decoded) < page_size:
                return
            cursor = highest

    def _request(
        self, method: str, path: str, body: Mapping[str, Any] | None = None
    ) -> dict[str, Any]:
        if self._token is None:
            raise ControllerConfigurationError(
                "no Kairo API token: pass token or token_file, set KAIRO_TOKEN or "
                "KAIRO_TOKEN_FILE, or provide <user config dir>/kairo/token"
            )
        try:
            return _http.request_json(
                self._opener,
                method,
                self.api_url + path,
                self._token,
                body,
                self.timeout_seconds,
                role=errors.OPERATOR,
            )
        except urllib.error.URLError as error:
            raise self._transport_error(error.reason) from error
        except (OSError, http.client.HTTPException) as error:
            raise self._transport_error(error) from error

    def _transport_error(self, reason: object) -> RuntimeError:
        if isinstance(reason, BaseException) and errors.classify(reason) == errors.TRANSIENT:
            return TransientControllerError(f"Kairo API transport failed: {reason}")
        if isinstance(reason, ssl.SSLError):
            trust = f"CA file {self.ca_file}" if self.ca_file else "the system CA roots"
            return ControllerConfigurationError(
                f"TLS to the Kairo API at {self.api_url} failed, trusting {trust}: "
                f"{reason}"
            )
        return ControllerConfigurationError(f"Kairo API request to {self.api_url} failed: {reason}")


def _limit(value: int) -> int:
    value = int(value)
    if value <= 0:
        raise ValueError("limit must be positive")
    return value


def _query(**params: Any) -> str:
    return urllib.parse.urlencode({key: value for key, value in params.items() if value is not None})


def _array(response: Mapping[str, Any], key: str) -> list[dict[str, Any]]:
    value = response.get(key)
    if value is None:
        return []
    if not isinstance(value, list):
        raise TypeError(f"Kairo {key} response must contain an array")
    return value


def _object(response: Mapping[str, Any], key: str) -> dict[str, Any]:
    value = response.get(key)
    if not isinstance(value, dict):
        raise TypeError(f"Kairo {key} response must contain an object")
    return value
