from __future__ import annotations

import copy
import sqlite3
import threading
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest
from kairo_sdk import (
    ControllerJournal,
    ProjectController,
    TransientControllerError,
)
from kairo_sdk.controller import (
    CoordinationEvent,
    PolicyDecisionFact,
    ResumeAfterKairoPreemption,
)


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
