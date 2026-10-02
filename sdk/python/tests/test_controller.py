from __future__ import annotations

import copy
import json
import os
import shutil
import sqlite3
import sys
import threading
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest
from kairo_sdk import (
    ControllerJournal,
    KairoControllerClient,
    ProjectController,
    TransientControllerError,
)
from kairo_sdk.controller import (
    CoordinationEvent,
    PolicyDecisionFact,
    ResumeAfterKairoPreemption,
    _user_config_dir,
)

DATA = Path(__file__).parent / "data"


def event(
    sequence: int,
    event_type: str,
    aggregate_type: str,
    aggregate_id: str,
    payload: Mapping[str, Any],
    *,
    epoch: int = 3,
) -> CoordinationEvent:
    return CoordinationEvent(
        sequence, event_type, aggregate_type, aggregate_id, epoch, payload
    )


def complete_preemption_events(
    *,
    execution: str = "exe_old",
    attempt: str = "att_old",
    command: str = "cmd_suspend",
    lease: str = "lea_old",
    continuation: str = "checkpoint://step-42",
    origin: str = "priority_preemption",
) -> list[CoordinationEvent]:
    return [
        event(
            1,
            "suspend_requested",
            "command",
            command,
            {
                "execution_id": execution,
                "attempt_id": attempt,
                "lease_id": lease,
                "origin": origin,
            },
        ),
        event(
            2,
            "checkpoint_published",
            "attempt",
            attempt,
            {
                "execution_id": execution,
                "command_id": command,
                "lease_id": lease,
                "continuation_ref": continuation,
            },
        ),
        event(
            3,
            "attempt_quiesced",
            "attempt",
            attempt,
            {"execution_id": execution, "lease_id": lease},
        ),
        event(
            4,
            "lease_released",
            "lease",
            lease,
            {"execution_id": execution, "attempt_id": attempt},
        ),
    ]


class FakeAPI:
    def __init__(self, events: list[CoordinationEvent]) -> None:
        self.events = events
        self.calls: list[dict[str, Any]] = []
        self.accepted: dict[str, str] = {}
        self.accepted_bodies: dict[str, dict[str, Any]] = {}
        self.fail_after_accept_once = False
        self.fail_event_reads = False

    def list_events(self, project: str, after: int = 0) -> list[CoordinationEvent]:
        assert project == "llm-develop"
        if self.fail_event_reads:
            raise TransientControllerError("events temporarily unavailable")
        return [item for item in self.events if item.sequence > after]

    def submit_execution(self, request: Mapping[str, Any]) -> Mapping[str, Any]:
        body = copy.deepcopy(dict(request))
        self.calls.append(body)
        request_id = str(body["client_request_id"])
        accepted_body = self.accepted_bodies.setdefault(request_id, body)
        if accepted_body != body:
            raise RuntimeError("idempotency key reused with a different request")
        execution_id = self.accepted.setdefault(
            request_id,
            "exe_old" if request_id == "original-request" else "exe_successor",
        )
        if self.fail_after_accept_once:
            self.fail_after_accept_once = False
            raise TransientControllerError(
                "connection lost after server accepted request"
            )
        return {
            "execution": {"id": execution_id},
            "idempotent": len(self.calls) > 1,
        }


def template() -> dict[str, Any]:
    return {
        "schema_version": 2,
        "client_request_id": "original-request",
        "project": "llm-develop",
        "queue": "train",
        "task": "pretrain",
        "argv": ["python", "-m", "train"],
        "cwd": "D:/Dev/LLM-Develop",
        "priority": 10,
        "checkpointable": True,
        "preemptible": True,
        "executor": {"labels": {"kind": "wsl"}},
        "exclusive": [{"kind": "gpu", "count": 2}],
        "capacity": {"cpu_millis": 16000, "ram_bytes": 8_000_000_000},
    }


def delegated_controller(
    tmp_path: Path, api: FakeAPI
) -> tuple[ProjectController, ControllerJournal]:
    journal = ControllerJournal(tmp_path / "controller.db")
    controller = ProjectController(api, journal)
    execution_id = controller.submit_with_resume_after_kairo_preemption(
        activity_id="jalm-300m-v3",
        project="llm-develop",
        submission=template(),
    )
    assert execution_id == "exe_old"
    api.calls.clear()
    return controller, journal


def test_policy_must_be_explicitly_delegated(tmp_path: Path):
    api = FakeAPI(complete_preemption_events())
    controller = ProjectController(api, ControllerJournal(tmp_path / "controller.db"))

    assert controller.run_once() == []
    assert api.calls == []


@pytest.mark.parametrize(
    "events",
    [
        complete_preemption_events(origin="scope_pause"),
        complete_preemption_events()[1:],
        complete_preemption_events()[:1] + complete_preemption_events()[2:],
        complete_preemption_events()[:2] + complete_preemption_events()[3:],
        complete_preemption_events()[:3],
        [
            complete_preemption_events()[0],
            *complete_preemption_events(command="cmd_other")[1:],
        ],
        [
            complete_preemption_events()[0],
            complete_preemption_events()[1],
            *complete_preemption_events(lease="lea_other")[2:3],
            complete_preemption_events()[3],
        ],
        [
            complete_preemption_events()[0],
            complete_preemption_events(lease="lea_other")[1],
            *complete_preemption_events()[2:],
        ],
        [
            complete_preemption_events()[0],
            event(
                2,
                "checkpoint_published",
                "attempt",
                "att_other",
                {
                    "execution_id": "exe_old",
                    "command_id": "cmd_suspend",
                    "lease_id": "lea_old",
                    "continuation_ref": "checkpoint://step-42",
                },
            ),
            *complete_preemption_events()[2:],
        ],
        [
            complete_preemption_events()[0],
            event(
                2,
                "checkpoint_published",
                "attempt",
                "att_old",
                {
                    "execution_id": "exe_old",
                    "command_id": "cmd_suspend",
                    "lease_id": "lea_old",
                    "continuation_ref": "checkpoint://step-42",
                },
                epoch=4,
            ),
            *complete_preemption_events()[2:],
        ],
        [
            complete_preemption_events()[0],
            event(
                3,
                "checkpoint_published",
                "attempt",
                "att_old",
                {
                    "execution_id": "exe_old",
                    "command_id": "cmd_suspend",
                    "lease_id": "lea_old",
                    "continuation_ref": "checkpoint://step-42",
                },
            ),
            event(
                2,
                "attempt_quiesced",
                "attempt",
                "att_old",
                {"execution_id": "exe_old", "lease_id": "lea_old"},
            ),
            complete_preemption_events()[3],
        ],
    ],
)
def test_resume_requires_exact_ack_quiescence_and_release(
    tmp_path: Path, events: list[CoordinationEvent]
):
    api = FakeAPI(events)
    controller, _journal = delegated_controller(tmp_path, api)

    assert controller.run_once() == []
    assert api.calls == []


def test_complete_preemption_submits_same_spec_with_acknowledged_continuation(
    tmp_path: Path,
):
    api = FakeAPI(complete_preemption_events())
    controller, journal = delegated_controller(tmp_path, api)

    submitted = controller.run_once()

    assert len(submitted) == 1
    decision = submitted[0]
    assert decision.decision_key == (
        "ResumeAfterKairoPreemption:v1:exe_old:cmd_suspend"
    )
    assert decision.state == "submitted"
    assert decision.successor_execution_id == "exe_successor"
    assert len(api.calls) == 1
    request = api.calls[0]
    assert request["input_continuation_ref"] == "checkpoint://step-42"
    assert request["argv"] == template()["argv"]
    assert request["client_request_id"] == decision.successor_client_request_id
    assert request["client_request_id"] != template()["client_request_id"]

    # Full event replay and loss of the optional cursor cannot create another
    # successor after the durable decision advanced the logical activity.
    with sqlite3.connect(journal.path) as connection:
        connection.execute("DELETE FROM event_scan_hints")
    assert controller.run_once() == []
    assert len(api.calls) == 1


def test_crash_after_accept_retries_deterministic_idempotent_submission(tmp_path: Path):
    api = FakeAPI(complete_preemption_events())
    controller, journal = delegated_controller(tmp_path, api)
    api.fail_after_accept_once = True

    with pytest.raises(TransientControllerError, match="after server accepted"):
        controller.run_once()
    decision_key = ResumeAfterKairoPreemption().decision_key("exe_old", "cmd_suspend")
    planned = journal.get_decision(decision_key)
    assert planned is not None and planned.state == "planned"

    submitted = controller.run_once()

    assert len(submitted) == 1
    assert submitted[0].state == "submitted"
    assert len(api.calls) == 2
    assert api.calls[0]["client_request_id"] == api.calls[1]["client_request_id"]
    assert (
        len([key for key in api.accepted if key.startswith("controller-resume-")]) == 1
    )


def test_policy_snapshot_is_immutable_and_inherited_by_successor(tmp_path: Path):
    api = FakeAPI(complete_preemption_events())
    controller, journal = delegated_controller(tmp_path, api)
    controller.run_once()

    changed = template()
    changed["argv"] = ["python", "-m", "different"]
    with pytest.raises(ValueError, match="different enrollment"):
        controller.submit_with_resume_after_kairo_preemption(
            activity_id="jalm-300m-v3",
            project="llm-develop",
            submission=changed,
        )

    assert journal.active_delegations("llm-develop") == {
        ("ResumeAfterKairoPreemption", 1): {"exe_successor"}
    }


def test_initial_controller_submission_journals_enrollment_before_post(tmp_path: Path):
    api = FakeAPI([])
    api.fail_after_accept_once = True
    journal = ControllerJournal(tmp_path / "controller.db")
    controller = ProjectController(api, journal)

    with pytest.raises(TransientControllerError, match="after server accepted"):
        controller.submit_with_resume_after_kairo_preemption(
            activity_id="jalm-300m-v3",
            project="llm-develop",
            submission=template(),
        )

    # Recovery does not need the caller to reconstruct the enrollment. The
    # same project-provided client request ID reaches Kairo again.
    controller.run_once()
    assert len(api.calls) == 2
    assert api.calls[0]["client_request_id"] == "original-request"
    assert api.calls[0] == api.calls[1]
    assert len(api.accepted) == 1
    assert journal.active_delegations("llm-develop") == {
        ("ResumeAfterKairoPreemption", 1): {"exe_old"}
    }


def test_two_controllers_share_one_durable_decision(tmp_path: Path):
    api = FakeAPI(complete_preemption_events())
    journal_path = tmp_path / "controller.db"
    first, _journal = delegated_controller(tmp_path, api)
    second = ProjectController(api, ControllerJournal(journal_path))
    barrier = threading.Barrier(2)

    def run(controller: ProjectController) -> None:
        barrier.wait()
        controller.run_once()

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(run, controller) for controller in (first, second)]
        for future in futures:
            future.result()

    assert len({call["client_request_id"] for call in api.calls}) == 1
    assert all(call == api.calls[0] for call in api.calls)
    assert len(api.accepted) == 2  # initial execution plus one successor
    assert api.accepted[api.calls[0]["client_request_id"]] == "exe_successor"
    decision = first.policy.decision_key("exe_old", "cmd_suspend")
    assert first.journal.get_decision(decision).state == "submitted"


def test_replayed_decision_rejects_changed_immutable_inputs(tmp_path: Path):
    api = FakeAPI(complete_preemption_events())
    controller, journal = delegated_controller(tmp_path, api)
    api.fail_after_accept_once = True
    with pytest.raises(TransientControllerError):
        controller.run_once()

    with pytest.raises(RuntimeError, match="different immutable inputs"):
        journal.plan_resume(
            PolicyDecisionFact(
                execution_id="exe_old",
                attempt_id="att_old",
                lease_id="lea_old",
                command_id="cmd_suspend",
                continuation_ref="checkpoint://different",
            ),
            controller.policy,
        )


def test_planned_successor_is_retried_before_event_scan(tmp_path: Path):
    api = FakeAPI(complete_preemption_events())
    controller, journal = delegated_controller(tmp_path, api)
    api.fail_after_accept_once = True
    with pytest.raises(TransientControllerError):
        controller.run_once()
    api.fail_event_reads = True

    with pytest.raises(
        TransientControllerError, match="events temporarily unavailable"
    ):
        controller.run_once()

    decision_key = controller.policy.decision_key("exe_old", "cmd_suspend")
    decision = journal.get_decision(decision_key)
    assert decision is not None and decision.state == "submitted"


def test_run_forever_bounds_transient_backoff_and_leaves_invariants_fatal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    controller = ProjectController(FakeAPI([]), ControllerJournal(tmp_path / "db"))
    outcomes: list[BaseException] = [
        TransientControllerError("one"),
        TransientControllerError("two"),
        TransientControllerError("three"),
        RuntimeError("invariant mismatch"),
    ]

    def run_once():
        raise outcomes.pop(0)

    sleeps: list[float] = []
    monkeypatch.setattr(controller, "run_once", run_once)
    monkeypatch.setattr("kairo_sdk.controller.time.sleep", sleeps.append)

    with pytest.raises(RuntimeError, match="invariant mismatch"):
        controller.run_forever(
            initial_backoff_seconds=0.25,
            max_backoff_seconds=0.5,
        )

    assert sleeps == [0.25, 0.5, 0.5]


def test_templates_without_schema_version_are_accepted(tmp_path: Path):
    api = FakeAPI([])
    controller = ProjectController(api, ControllerJournal(tmp_path / "controller.db"))
    submission = template()
    del submission["schema_version"]

    assert (
        controller.submit_with_resume_after_kairo_preemption(
            activity_id="jalm-300m-v3", project="llm-develop", submission=submission
        )
        == "exe_old"
    )
    assert "schema_version" not in api.calls[0]


def test_templates_with_another_schema_version_are_rejected(tmp_path: Path):
    controller = ProjectController(FakeAPI([]), ControllerJournal(tmp_path / "db"))
    submission = {**template(), "schema_version": 3}
    with pytest.raises(ValueError, match="schema_version must be 2"):
        controller.submit_with_resume_after_kairo_preemption(
            activity_id="jalm-300m-v3", project="llm-develop", submission=submission
        )


def test_journal_keeps_schema_version_while_the_wire_drops_it(tmp_path: Path):
    # Existing journals hold schema_version in their canonical submissions;
    # replay must see them byte-identical, so only the HTTP body loses it.
    api = FakeAPI(complete_preemption_events())
    controller, journal = delegated_controller(tmp_path, api)
    controller.run_once()
    with sqlite3.connect(journal.path) as connection:
        stored = [
            json.loads(row[0])
            for row in connection.execute(
                "SELECT submission_json FROM controller_enrollments UNION ALL "
                "SELECT submission_json FROM controller_decisions"
            )
        ]
    assert len(stored) == 2
    assert all(item["schema_version"] == 2 for item in stored)


class _APIHandler(BaseHTTPRequestHandler):
    requests: list[tuple[str, str, dict[str, str], Any]] = []
    pages: list[list[dict[str, Any]]] = []

    def log_message(self, *_args) -> None:
        pass

    def do_GET(self) -> None:  # noqa: N802
        type(self).requests.append((self.command, self.path, dict(self.headers), None))
        page = type(self).pages.pop(0) if type(self).pages else []
        self._send({"events": page})

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length") or 0)
        body = json.loads(self.rfile.read(length))
        type(self).requests.append((self.command, self.path, dict(self.headers), body))
        self._send({"execution": {"id": "exe_wire"}, "idempotent": False})

    def _send(self, body: dict[str, Any]) -> None:
        encoded = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)


@pytest.fixture
def api_server():
    _APIHandler.requests = []
    _APIHandler.pages = []
    server = ThreadingHTTPServer(("127.0.0.1", 0), _APIHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()


def raw_event(sequence: int) -> dict[str, Any]:
    return {
        "sequence": sequence,
        "event_type": "attempt_started",
        "aggregate_type": "attempt",
        "aggregate_id": f"att_{sequence}",
        "coordination_epoch": None,
        "payload": {},
    }


def test_client_submits_to_api_executions_without_schema_version(api_server):
    client = KairoControllerClient(api_server, token="tok_wire")
    request = template()

    response = client.submit_execution(request)

    assert response["execution"]["id"] == "exe_wire"
    assert request["schema_version"] == 2  # the caller's mapping is untouched
    method, path, headers, body = _APIHandler.requests[0]
    assert (method, path) == ("POST", "/api/executions")
    assert headers["Authorization"] == "Bearer tok_wire"
    assert body == {key: value for key, value in template().items() if key != "schema_version"}


def test_client_pages_api_events_with_the_bearer_token(api_server):
    _APIHandler.pages = [[raw_event(sequence) for sequence in range(1, 1001)], [raw_event(1001)]]
    client = KairoControllerClient(api_server, token="tok_events")

    events = client.list_events("llm-develop", after=0)

    assert [item.sequence for item in events] == list(range(1, 1002))
    assert [path for _method, path, _headers, _body in _APIHandler.requests] == [
        "/api/events?project=llm-develop&after=0&limit=1000",
        "/api/events?project=llm-develop&after=1000&limit=1000",
    ]
    assert {headers["Authorization"] for _m, _p, headers, _b in _APIHandler.requests} == {
        "Bearer tok_events"
    }


def test_client_transport_failure_is_transient():
    with pytest.raises(TransientControllerError, match="transport failed"):
        KairoControllerClient("http://127.0.0.1:1", token="tok", timeout_seconds=2).list_events("p")


@pytest.fixture
def config_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """An empty user config dir, located the way Go's os.UserConfigDir does."""
    for name in ("KAIRO_API", "KAIRO_TOKEN", "KAIRO_TOKEN_FILE", "KAIRO_CA_FILE"):
        monkeypatch.delenv(name, raising=False)
    root = tmp_path / "config"
    if os.name == "nt":
        monkeypatch.setenv("APPDATA", str(root))
    elif sys.platform == "darwin":
        monkeypatch.setenv("HOME", str(tmp_path))
        root = tmp_path / "Library" / "Application Support"
    else:
        monkeypatch.setenv("XDG_CONFIG_HOME", str(root))
    kairo = root / "kairo"
    kairo.mkdir(parents=True)
    return kairo


def test_client_defaults_to_local_https_and_cli_token(config_dir: Path):
    (config_dir / "token").write_text("  tok_config \n", encoding="utf-8")

    client = KairoControllerClient()

    assert client.api_url == "https://127.0.0.1:7474"
    assert client._token == "tok_config"
    assert client.ca_file is None


def test_client_without_any_token_fails(config_dir: Path):
    with pytest.raises(RuntimeError, match="no Kairo API token"):
        KairoControllerClient()


def test_client_rejects_empty_token_file(config_dir: Path):
    (config_dir / "token").write_text("\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="is empty"):
        KairoControllerClient()


def test_client_token_precedence(
    config_dir: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    (config_dir / "token").write_text("tok_config", encoding="utf-8")
    environment_file = tmp_path / "environment-token"
    environment_file.write_text("tok_environment_file\n", encoding="utf-8")
    argument_file = tmp_path / "argument-token"
    argument_file.write_text("tok_argument_file\n", encoding="utf-8")

    assert KairoControllerClient()._token == "tok_config"
    monkeypatch.setenv("KAIRO_TOKEN_FILE", str(environment_file))
    assert KairoControllerClient()._token == "tok_environment_file"
    monkeypatch.setenv("KAIRO_TOKEN", "tok_environment")
    assert KairoControllerClient()._token == "tok_environment"
    assert KairoControllerClient(token_file=argument_file)._token == "tok_argument_file"
    assert (
        KairoControllerClient(token="tok_argument", token_file=argument_file)._token
        == "tok_argument"
    )


def test_client_api_url_precedence(config_dir: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("KAIRO_API", "https://kairo.example:7474/")
    assert KairoControllerClient(token="tok").api_url == "https://kairo.example:7474"
    assert (
        KairoControllerClient("http://127.0.0.1:9999", token="tok").api_url
        == "http://127.0.0.1:9999"
    )


def test_client_ca_precedence(
    config_dir: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    shutil.copyfile(DATA / "ca.pem", config_dir / "ca.pem")
    assert KairoControllerClient(token="tok").ca_file == config_dir / "ca.pem"
    monkeypatch.setenv("KAIRO_CA_FILE", str(DATA / "ca-other.pem"))
    assert KairoControllerClient(token="tok").ca_file == DATA / "ca-other.pem"
    argument = KairoControllerClient(token="tok", ca_file=str(DATA / "ca.pem"))
    assert argument.ca_file == DATA / "ca.pem"


def test_user_config_dir_matches_go(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    if os.name == "nt":
        monkeypatch.setenv("APPDATA", str(tmp_path))
        assert _user_config_dir() == tmp_path
        monkeypatch.delenv("APPDATA")
        assert _user_config_dir() is None
    elif sys.platform == "darwin":
        monkeypatch.setenv("HOME", str(tmp_path))
        assert _user_config_dir() == tmp_path / "Library" / "Application Support"
    else:
        monkeypatch.setenv("HOME", str(tmp_path / "home"))
        monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
        assert _user_config_dir() == tmp_path / "xdg"
        monkeypatch.setenv("XDG_CONFIG_HOME", "relative/config")
        assert _user_config_dir() is None
        monkeypatch.delenv("XDG_CONFIG_HOME")
        assert _user_config_dir() == tmp_path / "home" / ".config"
