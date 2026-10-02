from __future__ import annotations

import copy
import hashlib
import json
import logging
import os
import sqlite3
import ssl
import sys
import time
import urllib.error
import urllib.parse
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from . import _http

DEFAULT_API_URL = "https://127.0.0.1:7474"

_log = logging.getLogger("kairo_sdk.controller")

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


class ControllerAPI(Protocol):
    def list_events(self, project: str, after: int = 0) -> list[CoordinationEvent]: ...

    def submit_execution(self, request: Mapping[str, Any]) -> Mapping[str, Any]: ...


class TransientControllerError(RuntimeError):
    """A transport failure that the controller loop may safely retry."""


class ControllerConfigurationError(RuntimeError):
    """A failure no retry can fix, such as a TLS handshake or verification failure."""


class KairoControllerClient:
    """Small project-controller client for Kairo's coordination API.

    Unset arguments resolve the way the ``kairo`` CLI does: the API URL from
    ``KAIRO_API``; the token from ``KAIRO_TOKEN``, ``KAIRO_TOKEN_FILE`` or
    ``<user config dir>/kairo/token``; the CA from ``KAIRO_CA_FILE`` or
    ``<user config dir>/kairo/ca.pem``, falling back to the system roots.
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
        self.api_url = (api_url or os.environ.get("KAIRO_API") or DEFAULT_API_URL).rstrip("/")
        _http.require_token_transport(self.api_url)
        self.timeout_seconds = timeout_seconds
        self._token = _resolve_token(token, token_file)
        self.ca_file = _resolve_ca_file(ca_file)
        self._opener = _http.build_opener(ca_file=self.ca_file)

    def list_events(self, project: str, after: int = 0) -> list[CoordinationEvent]:
        events: list[CoordinationEvent] = []
        cursor = after
        while True:
            query = urllib.parse.urlencode(
                {"project": project, "after": cursor, "limit": 1000}
            )
            response = self._request("GET", f"/api/events?{query}")
            page = response.get("events") or []
            if not isinstance(page, list):
                raise TypeError("Kairo events response must contain an array")
            decoded = [CoordinationEvent.from_api(raw) for raw in page]
            events.extend(decoded)
            if len(decoded) < 1000:
                return events
            next_cursor = decoded[-1].sequence
            if next_cursor <= cursor:
                raise RuntimeError("Kairo events response did not advance the cursor")
            cursor = next_cursor

    def submit_execution(self, request: Mapping[str, Any]) -> Mapping[str, Any]:
        # Journaled submissions may still carry the retired schema_version;
        # the journal keeps them verbatim, but the API rejects unknown fields.
        body = {key: value for key, value in request.items() if key != "schema_version"}
        return self._request("POST", "/api/executions", body)

    def _request(
        self, method: str, path: str, body: Mapping[str, Any] | None = None
    ) -> dict[str, Any]:
        try:
            return _http.request_json(
                self._opener,
                method,
                self.api_url + path,
                self._token,
                body,
                self.timeout_seconds,
            )
        except urllib.error.URLError as error:
            raise self._transport_error(error.reason) from error
        except (TimeoutError, ConnectionError, ssl.SSLError) as error:
            raise self._transport_error(error) from error

    def _transport_error(self, reason: object) -> RuntimeError:
        if isinstance(reason, ssl.SSLError) and not isinstance(
            reason, _TRANSIENT_SSL_ERRORS
        ):
            trust = f"CA file {self.ca_file}" if self.ca_file else "the system CA roots"
            return ControllerConfigurationError(
                f"TLS to the Kairo API at {self.api_url} failed, trusting {trust}: "
                f"{reason}"
            )
        return TransientControllerError(f"Kairo API transport failed: {reason}")


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


def _read_token_file(path: Path) -> str:
    try:
        token = path.read_text(encoding="utf-8").strip()
    except OSError as error:
        raise RuntimeError(f"cannot read Kairo token file {path}: {error}") from error
    if not token:
        raise RuntimeError(f"Kairo token file {path} is empty")
    return token


def _resolve_token(token: str | None, token_file: str | Path | None) -> str:
    if token is not None:
        if not token.strip():
            raise ValueError("Kairo token must not be empty")
        return token.strip()
    if token_file is not None:
        return _read_token_file(Path(token_file))
    if environment_token := os.environ.get("KAIRO_TOKEN", "").strip():
        return environment_token
    if environment_file := os.environ.get("KAIRO_TOKEN_FILE"):
        return _read_token_file(Path(environment_file))
    config_dir = _user_config_dir()
    if config_dir is not None and (config_dir / "kairo" / "token").is_file():
        return _read_token_file(config_dir / "kairo" / "token")
    raise RuntimeError(
        "no Kairo API token: pass token or token_file, set KAIRO_TOKEN or "
        "KAIRO_TOKEN_FILE, or provide <user config dir>/kairo/token"
    )


def _resolve_ca_file(ca_file: str | Path | None) -> Path | None:
    if ca_file is not None:
        return Path(ca_file)
    if environment_file := os.environ.get("KAIRO_CA_FILE"):
        return Path(environment_file)
    config_dir = _user_config_dir()
    if config_dir is not None and (config_dir / "kairo" / "ca.pem").is_file():
        return config_dir / "kairo" / "ca.pem"
    return None


@dataclass(frozen=True)
class ControllerDecision:
    decision_key: str
    predecessor_execution_id: str
    source_command_id: str
    continuation_ref: str
    successor_client_request_id: str
    successor_execution_id: str | None
    state: str


@dataclass(frozen=True)
class PolicyDecisionFact:
    execution_id: str
    attempt_id: str
    lease_id: str
    command_id: str
    continuation_ref: str


class ControllerPolicy(Protocol):
    policy_id: str
    version: int

    def decision_key(
        self, predecessor_execution_id: str, source_command_id: str
    ) -> str: ...

    def eligible_facts(
        self,
        events: Iterable[CoordinationEvent],
        delegated_execution_ids: set[str],
    ) -> list[PolicyDecisionFact]: ...


@dataclass(frozen=True)
class ResumeAfterKairoPreemption:
    """A policy implementation that a project may explicitly delegate."""

    policy_id: str = "ResumeAfterKairoPreemption"
    version: int = 1

    def decision_key(
        self, predecessor_execution_id: str, source_command_id: str
    ) -> str:
        return (
            f"{self.policy_id}:v{self.version}:"
            f"{predecessor_execution_id}:{source_command_id}"
        )

    def eligible_facts(
        self,
        events: Iterable[CoordinationEvent],
        delegated_execution_ids: set[str],
    ) -> list[PolicyDecisionFact]:
        suspends: dict[str, tuple[int, str, str, str, int | None]] = {}
        checkpoints: dict[str, tuple[int, str, str, str, str, int | None]] = {}
        quiesced: list[tuple[int, str, str, str, int | None]] = []
        released: list[tuple[int, str, str, str, int | None]] = []
        for event in events:
            payload = event.payload
            execution_id = str(payload.get("execution_id") or "")
            if execution_id not in delegated_execution_ids:
                continue
            if event.event_type == "suspend_requested":
                if (
                    event.aggregate_type == "command"
                    and payload.get("origin") == "priority_preemption"
                    and payload.get("attempt_id")
                    and payload.get("lease_id")
                ):
                    suspends[event.aggregate_id] = (
                        event.sequence,
                        execution_id,
                        str(payload["attempt_id"]),
                        str(payload["lease_id"]),
                        event.coordination_epoch,
                    )
            elif event.event_type == "checkpoint_published":
                command_id = str(payload.get("command_id") or "")
                continuation_ref = str(payload.get("continuation_ref") or "")
                if (
                    event.aggregate_type == "attempt"
                    and command_id
                    and continuation_ref
                    and payload.get("lease_id")
                ):
                    checkpoints[command_id] = (
                        event.sequence,
                        execution_id,
                        event.aggregate_id,
                        str(payload["lease_id"]),
                        continuation_ref,
                        event.coordination_epoch,
                    )
            elif event.event_type == "attempt_quiesced" and payload.get("lease_id"):
                quiesced.append(
                    (
                        event.sequence,
                        execution_id,
                        event.aggregate_id,
                        str(payload["lease_id"]),
                        event.coordination_epoch,
                    )
                )
            elif event.event_type == "lease_released" and payload.get("attempt_id"):
                released.append(
                    (
                        event.sequence,
                        execution_id,
                        str(payload["attempt_id"]),
                        event.aggregate_id,
                        event.coordination_epoch,
                    )
                )

        facts: list[PolicyDecisionFact] = []
        for command_id, suspend in suspends.items():
            suspend_sequence, execution_id, attempt_id, lease_id, epoch = suspend
            checkpoint = checkpoints.get(command_id)
            if checkpoint is None:
                continue
            (
                checkpoint_sequence,
                checkpoint_execution,
                checkpoint_attempt,
                checkpoint_lease,
                continuation_ref,
                checkpoint_epoch,
            ) = checkpoint
            if (
                checkpoint_execution,
                checkpoint_attempt,
                checkpoint_lease,
                checkpoint_epoch,
            ) != (execution_id, attempt_id, lease_id, epoch):
                continue
            matching_quiescence = [
                item
                for item in quiesced
                if item[1:] == (execution_id, attempt_id, lease_id, epoch)
                and checkpoint_sequence < item[0]
            ]
            matching_releases = [
                item
                for item in released
                if item[1:] == (execution_id, attempt_id, lease_id, epoch)
            ]
            if len(matching_quiescence) != 1 or len(matching_releases) != 1:
                continue
            quiesced_sequence = matching_quiescence[0][0]
            released_sequence = matching_releases[0][0]
            if not (
                suspend_sequence
                < checkpoint_sequence
                < quiesced_sequence
                < released_sequence
            ):
                continue
            facts.append(
                PolicyDecisionFact(
                    execution_id=execution_id,
                    attempt_id=attempt_id,
                    lease_id=lease_id,
                    command_id=command_id,
                    continuation_ref=continuation_ref,
                )
            )
        return facts


class ControllerJournal:
    """Project-owned durable decisions and immutable policy delegations.

    Event cursors are recorded only as scan hints. Decisions, deterministic
    client request IDs, and Kairo's idempotent submission API provide the
    correctness boundary, so replaying the complete event stream is safe. A
    delegation remains valid only while its immutable policy snapshot is the
    one attached to the logical activity's current execution; server gates do
    not reinterpret or revoke that project-owned semantic choice.
    """

    def __init__(self, path: str | Path) -> None:
        self.path = str(path)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS controller_activities(
                    activity_id TEXT PRIMARY KEY,
                    project TEXT NOT NULL,
                    current_execution_id TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS controller_enrollments(
                    project TEXT NOT NULL,
                    client_request_id TEXT NOT NULL,
                    activity_id TEXT NOT NULL UNIQUE,
                    policy_id TEXT NOT NULL,
                    policy_version INTEGER NOT NULL,
                    submission_json TEXT NOT NULL,
                    submission_template_json TEXT NOT NULL,
                    execution_id TEXT,
                    state TEXT NOT NULL CHECK(state IN('planned','submitted')),
                    created_at REAL NOT NULL,
                    submitted_at REAL,
                    PRIMARY KEY(project,client_request_id)
                );
                CREATE TABLE IF NOT EXISTS policy_delegations(
                    execution_id TEXT PRIMARY KEY,
                    activity_id TEXT NOT NULL REFERENCES controller_activities(activity_id),
                    policy_id TEXT NOT NULL,
                    policy_version INTEGER NOT NULL,
                    submission_template_json TEXT NOT NULL,
                    snapshotted_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS controller_decisions(
                    decision_key TEXT PRIMARY KEY,
                    predecessor_execution_id TEXT NOT NULL,
                    source_command_id TEXT NOT NULL,
                    policy_id TEXT NOT NULL,
                    policy_version INTEGER NOT NULL,
                    continuation_ref TEXT NOT NULL,
                    successor_client_request_id TEXT NOT NULL UNIQUE,
                    successor_execution_id TEXT,
                    state TEXT NOT NULL CHECK(state IN('planned','submitted')),
                    submission_json TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    submitted_at REAL
                );
                CREATE TABLE IF NOT EXISTS event_scan_hints(
                    project TEXT PRIMARY KEY,
                    last_sequence INTEGER NOT NULL,
                    updated_at REAL NOT NULL
                );
                """
            )

    def projects(self) -> list[str]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT project FROM controller_activities UNION "
                "SELECT project FROM controller_enrollments ORDER BY project"
            ).fetchall()
        return [str(row[0]) for row in rows]

    def plan_enrollment(
        self,
        *,
        activity_id: str,
        project: str,
        policy: ResumeAfterKairoPreemption,
        submission: Mapping[str, Any],
    ) -> None:
        request = copy.deepcopy(dict(submission))
        template = _normalize_submission_template(request, project)
        client_request_id = str(request.get("client_request_id") or "")
        if not client_request_id:
            raise ValueError("controller submission requires client_request_id")
        encoded_request = _canonical_json(request)
        encoded_template = _canonical_json(template)
        stamp = time.time()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT activity_id,policy_id,policy_version,submission_json,"
                "submission_template_json FROM controller_enrollments "
                "WHERE project=? AND client_request_id=?",
                (project, client_request_id),
            ).fetchone()
            snapshot = (
                activity_id,
                policy.policy_id,
                policy.version,
                encoded_request,
                encoded_template,
            )
            if existing is not None:
                if tuple(existing) != snapshot:
                    raise ValueError(
                        "client request already has a different enrollment"
                    )
                return
            if connection.execute(
                "SELECT 1 FROM controller_activities WHERE activity_id=?",
                (activity_id,),
            ).fetchone():
                raise ValueError("activity is already bound to an execution")
            connection.execute(
                "INSERT INTO controller_enrollments VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (
                    project,
                    client_request_id,
                    *snapshot,
                    None,
                    "planned",
                    stamp,
                    None,
                ),
            )

    def pending_enrollments(self) -> list[tuple[str, str, dict[str, Any]]]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT project,client_request_id,submission_json "
                "FROM controller_enrollments WHERE state='planned' "
                "ORDER BY created_at,project,client_request_id"
            ).fetchall()
        return [
            (
                str(row["project"]),
                str(row["client_request_id"]),
                json.loads(row["submission_json"]),
            )
            for row in rows
        ]

    def mark_enrollment_submitted(
        self, project: str, client_request_id: str, execution_id: str
    ) -> None:
        stamp = time.time()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            enrollment = connection.execute(
                "SELECT * FROM controller_enrollments "
                "WHERE project=? AND client_request_id=?",
                (project, client_request_id),
            ).fetchone()
            if enrollment is None:
                raise RuntimeError("controller enrollment disappeared")
            if enrollment["state"] == "submitted":
                if enrollment["execution_id"] != execution_id:
                    raise RuntimeError(
                        "idempotent enrollment returned another execution"
                    )
                return
            existing = connection.execute(
                "SELECT project,current_execution_id FROM controller_activities "
                "WHERE activity_id=?",
                (enrollment["activity_id"],),
            ).fetchone()
            if existing is None:
                connection.execute(
                    "INSERT INTO controller_activities VALUES(?,?,?,?,?)",
                    (
                        enrollment["activity_id"],
                        project,
                        execution_id,
                        stamp,
                        stamp,
                    ),
                )
            elif (existing["project"], existing["current_execution_id"]) != (
                project,
                execution_id,
            ):
                raise RuntimeError("enrolled activity points to another execution")
            connection.execute(
                "INSERT OR IGNORE INTO policy_delegations VALUES(?,?,?,?,?,?)",
                (
                    execution_id,
                    enrollment["activity_id"],
                    enrollment["policy_id"],
                    enrollment["policy_version"],
                    enrollment["submission_template_json"],
                    stamp,
                ),
            )
            _require_delegation(
                connection,
                execution_id,
                enrollment["activity_id"],
                enrollment["policy_id"],
                enrollment["policy_version"],
                enrollment["submission_template_json"],
            )
            connection.execute(
                "UPDATE controller_enrollments SET state='submitted',execution_id=?,"
                "submitted_at=? WHERE project=? AND client_request_id=?",
                (execution_id, stamp, project, client_request_id),
            )

    def active_delegations(self, project: str) -> dict[tuple[str, int], set[str]]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT d.execution_id,d.policy_id,d.policy_version "
                "FROM policy_delegations d "
                "JOIN controller_activities a ON a.activity_id=d.activity_id "
                "WHERE a.project=? AND a.current_execution_id=d.execution_id",
                (project,),
            ).fetchall()
        delegations: dict[tuple[str, int], set[str]] = {}
        for row in rows:
            key = (str(row["policy_id"]), int(row["policy_version"]))
            delegations.setdefault(key, set()).add(str(row["execution_id"]))
        return delegations

    def plan_resume(
        self, fact: PolicyDecisionFact, policy: ControllerPolicy
    ) -> ControllerDecision | None:
        decision_key = policy.decision_key(fact.execution_id, fact.command_id)
        client_request_id = _successor_client_request_id(decision_key)
        stamp = time.time()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            delegation = connection.execute(
                "SELECT d.activity_id,d.policy_id,d.policy_version,"
                "d.submission_template_json,a.current_execution_id "
                "FROM policy_delegations d JOIN controller_activities a "
                "ON a.activity_id=d.activity_id WHERE d.execution_id=?",
                (fact.execution_id,),
            ).fetchone()
            if delegation is None:
                return None
            if (
                delegation["policy_id"] != policy.policy_id
                or delegation["policy_version"] != policy.version
                or delegation["current_execution_id"] != fact.execution_id
            ):
                return None
            request = json.loads(delegation["submission_template_json"])
            request["client_request_id"] = client_request_id
            request["input_continuation_ref"] = fact.continuation_ref
            connection.execute(
                "INSERT OR IGNORE INTO controller_decisions VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    decision_key,
                    fact.execution_id,
                    fact.command_id,
                    policy.policy_id,
                    policy.version,
                    fact.continuation_ref,
                    client_request_id,
                    None,
                    "planned",
                    _canonical_json(request),
                    stamp,
                    None,
                ),
            )
            stored = connection.execute(
                "SELECT * FROM controller_decisions WHERE decision_key=?",
                (decision_key,),
            ).fetchone()
            expected = (
                fact.execution_id,
                fact.command_id,
                policy.policy_id,
                policy.version,
                fact.continuation_ref,
                client_request_id,
                _canonical_json(request),
            )
            actual = (
                stored["predecessor_execution_id"],
                stored["source_command_id"],
                stored["policy_id"],
                stored["policy_version"],
                stored["continuation_ref"],
                stored["successor_client_request_id"],
                stored["submission_json"],
            )
            if actual != expected:
                raise RuntimeError(
                    "decision key resolved to different immutable inputs"
                )
            return _decision_from_row(stored)

    def pending(self) -> list[tuple[ControllerDecision, dict[str, Any]]]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM controller_decisions WHERE state='planned' "
                "ORDER BY created_at,decision_key"
            ).fetchall()
        return [
            (_decision_from_row(row), json.loads(row["submission_json"]))
            for row in rows
        ]

    def mark_submitted(self, decision_key: str, successor_execution_id: str) -> None:
        stamp = time.time()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            decision = connection.execute(
                "SELECT * FROM controller_decisions WHERE decision_key=?",
                (decision_key,),
            ).fetchone()
            if decision is None:
                raise RuntimeError("controller decision disappeared")
            if decision["state"] == "submitted":
                if decision["successor_execution_id"] != successor_execution_id:
                    raise RuntimeError(
                        "idempotent submission returned another execution"
                    )
                return
            delegation = connection.execute(
                "SELECT * FROM policy_delegations WHERE execution_id=?",
                (decision["predecessor_execution_id"],),
            ).fetchone()
            if delegation is None:
                raise RuntimeError("policy delegation disappeared")
            activity = connection.execute(
                "SELECT current_execution_id FROM controller_activities "
                "WHERE activity_id=?",
                (delegation["activity_id"],),
            ).fetchone()
            if activity is None or activity["current_execution_id"] not in (
                decision["predecessor_execution_id"],
                successor_execution_id,
            ):
                raise RuntimeError(
                    "logical activity advanced to an unexpected execution"
                )
            connection.execute(
                "INSERT OR IGNORE INTO policy_delegations VALUES(?,?,?,?,?,?)",
                (
                    successor_execution_id,
                    delegation["activity_id"],
                    delegation["policy_id"],
                    delegation["policy_version"],
                    delegation["submission_template_json"],
                    stamp,
                ),
            )
            _require_delegation(
                connection,
                successor_execution_id,
                delegation["activity_id"],
                delegation["policy_id"],
                delegation["policy_version"],
                delegation["submission_template_json"],
            )
            connection.execute(
                "UPDATE controller_activities SET current_execution_id=?,updated_at=? "
                "WHERE activity_id=?",
                (successor_execution_id, stamp, delegation["activity_id"]),
            )
            connection.execute(
                "UPDATE controller_decisions SET state='submitted',"
                "successor_execution_id=?,submitted_at=? WHERE decision_key=?",
                (successor_execution_id, stamp, decision_key),
            )

    def record_scan_hint(self, project: str, sequence: int) -> None:
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO event_scan_hints VALUES(?,?,?) "
                "ON CONFLICT(project) DO UPDATE SET "
                "last_sequence=MAX(last_sequence,excluded.last_sequence),"
                "updated_at=excluded.updated_at",
                (project, sequence, time.time()),
            )

    def get_decision(self, decision_key: str) -> ControllerDecision | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM controller_decisions WHERE decision_key=?",
                (decision_key,),
            ).fetchone()
        return _decision_from_row(row) if row is not None else None

    def enrolled_execution_id(self, project: str, client_request_id: str) -> str | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT execution_id FROM controller_enrollments "
                "WHERE project=? AND client_request_id=? AND state='submitted'",
                (project, client_request_id),
            ).fetchone()
        return str(row[0]) if row is not None else None


class ProjectController:
    """Reusable mechanics for project-selected execution policies."""

    def __init__(self, api: ControllerAPI, journal: ControllerJournal) -> None:
        self.api = api
        self.journal = journal
        self.policy = ResumeAfterKairoPreemption()
        self.policies: dict[tuple[str, int], ControllerPolicy] = {
            (self.policy.policy_id, self.policy.version): self.policy
        }

    def submit_with_resume_after_kairo_preemption(
        self,
        *,
        activity_id: str,
        project: str,
        submission: Mapping[str, Any],
    ) -> str:
        """Journal policy enrollment before submitting the initial execution."""
        client_request_id = str(submission.get("client_request_id") or "")
        self.journal.plan_enrollment(
            activity_id=activity_id,
            project=project,
            policy=self.policy,
            submission=submission,
        )
        self._submit_pending_enrollments()
        # The matching enrollment is now durable and submitted. Returning the
        # server execution ID is convenient without making it controller state.
        execution_id = self.journal.enrolled_execution_id(project, client_request_id)
        if execution_id is None:
            raise RuntimeError("controller enrollment was not submitted")
        return execution_id

    def run_once(self) -> list[ControllerDecision]:
        self._submit_pending_enrollments()
        submitted = self._submit_pending_decisions()
        for project in self.journal.projects():
            # Full replay is intentional in this first slice. The scan hint may
            # later avoid I/O, but losing it can never change a decision.
            events = self.api.list_events(project, after=0)
            for key, execution_ids in self.journal.active_delegations(project).items():
                policy = self.policies.get(key)
                if policy is None:
                    raise RuntimeError(
                        f"controller has no implementation for policy {key[0]}@v{key[1]}"
                    )
                for fact in policy.eligible_facts(events, execution_ids):
                    self.journal.plan_resume(fact, policy)
            if events:
                self.journal.record_scan_hint(project, events[-1].sequence)

        submitted.extend(self._submit_pending_decisions())
        return submitted

    def _submit_pending_decisions(self) -> list[ControllerDecision]:
        submitted: list[ControllerDecision] = []
        for decision, request in self.journal.pending():
            response = self.api.submit_execution(request)
            execution = response.get("execution")
            if not isinstance(execution, Mapping) or not execution.get("id"):
                raise RuntimeError("Kairo submission response has no execution ID")
            self.journal.mark_submitted(decision.decision_key, str(execution["id"]))
            completed = self.journal.get_decision(decision.decision_key)
            if completed is None:
                raise RuntimeError("submitted controller decision disappeared")
            submitted.append(completed)
        return submitted

    def _submit_pending_enrollments(self) -> None:
        for project, client_request_id, request in self.journal.pending_enrollments():
            response = self.api.submit_execution(request)
            execution = response.get("execution")
            if not isinstance(execution, Mapping) or not execution.get("id"):
                raise RuntimeError("Kairo submission response has no execution ID")
            self.journal.mark_enrollment_submitted(
                project, client_request_id, str(execution["id"])
            )

    def run_forever(
        self,
        *,
        poll_interval_seconds: float = 1.0,
        initial_backoff_seconds: float = 0.25,
        max_backoff_seconds: float = 10.0,
    ) -> None:
        if (
            poll_interval_seconds < 0
            or initial_backoff_seconds <= 0
            or max_backoff_seconds < initial_backoff_seconds
        ):
            raise ValueError("invalid controller polling or backoff intervals")
        backoff = initial_backoff_seconds
        while True:
            try:
                self.run_once()
            except TransientControllerError as error:
                _log.warning(
                    "Kairo controller pass failed transiently; retrying in %.2fs: %s",
                    backoff,
                    error,
                )
                time.sleep(backoff)
                backoff = min(max_backoff_seconds, backoff * 2)
                continue
            backoff = initial_backoff_seconds
            time.sleep(poll_interval_seconds)


def _normalize_submission_template(
    value: Mapping[str, Any], project: str
) -> dict[str, Any]:
    template = copy.deepcopy(dict(value))
    # schema_version is retired; older templates still carry it verbatim.
    if template.get("schema_version", 2) != 2:
        raise ValueError("submission template schema_version must be 2 when present")
    if template.get("project") != project:
        raise ValueError("submission template project does not match delegation")
    if not template.get("argv") or not template.get("cwd"):
        raise ValueError("submission template requires argv and cwd")
    template.pop("client_request_id", None)
    template.pop("input_continuation_ref", None)
    return template


def _canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _successor_client_request_id(decision_key: str) -> str:
    digest = hashlib.sha256(decision_key.encode("utf-8")).hexdigest()
    return f"controller-resume-{digest}"


def _decision_from_row(row: sqlite3.Row) -> ControllerDecision:
    return ControllerDecision(
        decision_key=str(row["decision_key"]),
        predecessor_execution_id=str(row["predecessor_execution_id"]),
        source_command_id=str(row["source_command_id"]),
        continuation_ref=str(row["continuation_ref"]),
        successor_client_request_id=str(row["successor_client_request_id"]),
        successor_execution_id=(
            str(row["successor_execution_id"])
            if row["successor_execution_id"] is not None
            else None
        ),
        state=str(row["state"]),
    )


def _require_delegation(
    connection: sqlite3.Connection,
    execution_id: str,
    activity_id: str,
    policy_id: str,
    policy_version: int,
    submission_template_json: str,
) -> None:
    stored = connection.execute(
        "SELECT activity_id,policy_id,policy_version,submission_template_json "
        "FROM policy_delegations WHERE execution_id=?",
        (execution_id,),
    ).fetchone()
    if stored is None or tuple(stored) != (
        activity_id,
        policy_id,
        policy_version,
        submission_template_json,
    ):
        raise RuntimeError("execution has a conflicting policy delegation")
