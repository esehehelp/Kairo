"""Worker-side behaviour beyond the conformance scenarios."""

from __future__ import annotations

import logging
import signal
import time
from pathlib import Path

import pytest
from fake_daemon import OK, FakeDaemon, serve, suspend, worker_routes

from kairo_sdk import (
    AttemptFenced,
    AttemptRetired,
    AttemptSession,
    CommandJournal,
    DistributedAdapter,
    GangContext,
    KairoAPIError,
    NoContinuationError,
    ProgressReporter,
    SuspendResult,
    continuation_or,
    install_stop_signals,
    runtime,
    stop_requested,
)

RETIRED = (401, {"error": "valid token required", "code": "unauthorized"})
FENCED = (409, {"error": "stale coordination epoch", "code": "stale_epoch"})
UNAVAILABLE = (503, {"error": "database is locked", "code": "internal"})
GONE = (404, {"error": "not found", "code": "not_found"})


def session_for(url: str, **overrides) -> AttemptSession:
    options = {"poll_interval_seconds": 0, "heartbeat_interval_seconds": 10_000, **overrides}
    return AttemptSession(
        api_url=url, execution_id="exe_w", attempt_id="att_w", token="tok_w", **options
    )


def acks(daemon: FakeDaemon) -> list[dict]:
    return [r.body for r in daemon.requests if r.path.endswith("/acks")]


def wait_until(condition, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError("condition not reached")
        time.sleep(0.01)


# --------------------------------------------------------------------------
# safe_point


def test_callback_error_is_rejected_with_its_type_and_message():
    with serve(FakeDaemon(worker_routes({"GET /api/worker/commands": suspend(),
                                         "POST /api/worker/commands/cmd_1/acks": OK}))) as daemon:
        session = session_for(daemon.url)

        def checkpoint(_command):
            raise OSError("disk full")

        with pytest.raises(OSError, match="disk full"):
            session.safe_point(checkpoint=checkpoint)
    assert acks(daemon)[-1] == {"phase": "rejected", "payload": {"reason": "OSError: disk full"}}


def test_callback_error_propagates_even_when_the_rejection_fails():
    routes = worker_routes({
        "GET /api/worker/commands": suspend(),
        "POST /api/worker/commands/cmd_1/acks": [OK, OK, UNAVAILABLE],
    })
    with serve(FakeDaemon(routes)) as daemon:
        session = session_for(daemon.url)
        with pytest.raises(ZeroDivisionError):
            session.safe_point(checkpoint=lambda _command: 1 / 0)
    assert [body["phase"] for body in acks(daemon)] == ["accepted", "checkpointing", "rejected"]
    assert isinstance(session.last_control_error, KairoAPIError)


@pytest.mark.parametrize("returned", [None, SuspendResult(payload={"step": 3}), ""])
def test_no_continuation_is_never_acknowledged_checkpointed(returned):
    routes = worker_routes({"GET /api/worker/commands": suspend(),
                            "POST /api/worker/commands/cmd_1/acks": OK})
    with serve(FakeDaemon(routes)) as daemon:
        session = session_for(daemon.url)
        with pytest.raises(NoContinuationError):
            session.safe_point(checkpoint=lambda _command: returned)
    phases = [body["phase"] for body in acks(daemon)]
    assert phases == ["accepted", "checkpointing", "rejected"]
    assert acks(daemon)[-1]["payload"] == {"reason": "no continuation_ref"}


def test_withdrawn_command_at_checkpointed_returns_false():
    routes = worker_routes({
        "GET /api/worker/commands": suspend(),
        "POST /api/worker/commands/cmd_1/acks": [OK, OK, GONE],
    })
    with serve(FakeDaemon(routes)) as daemon:
        session = session_for(daemon.url)
        assert session.safe_point(checkpoint=lambda _command: "ckpt://1") is False
        assert not session.suspend_handled


def test_withdrawn_command_does_not_hide_the_next_one():
    commands = (200, {"commands": [
        {"id": "cmd_gone", "kind": "suspend", "reason": "", "delivery_count": 1},
        {"id": "cmd_live", "kind": "suspend", "reason": "", "delivery_count": 1},
    ]})
    routes = worker_routes({
        "GET /api/worker/commands": commands,
        "POST /api/worker/commands/cmd_gone/acks": GONE,
        "POST /api/worker/commands/cmd_live/acks": OK,
    })
    with serve(FakeDaemon(routes)) as daemon:
        session = session_for(daemon.url)
        seen = []
        assert session.safe_point(checkpoint=lambda command: seen.append(command.command_id) or "ckpt://2")
    assert seen == ["cmd_live"]


@pytest.mark.parametrize(
    ("response", "error", "flag"),
    [(RETIRED, AttemptRetired, "retired"), (FENCED, AttemptFenced, "fenced")],
)
def test_terminal_states_raise_and_stay_without_more_requests(response, error, flag):
    with serve(FakeDaemon(worker_routes({"POST /api/worker/heartbeat": response}))) as daemon:
        session = session_for(daemon.url)
        with pytest.raises(error):
            session.heartbeat()
        assert getattr(session, flag)
        assert session.last_control_error is not None
        sent = len(daemon.requests)
        for call in (session.heartbeat, session.poll_commands,
                     lambda: session.safe_point(checkpoint=lambda _c: "x")):
            with pytest.raises(error):
                call()
        assert len(daemon.requests) == sent


def test_transient_poll_failure_is_classified():
    with serve(FakeDaemon(worker_routes({"GET /api/worker/commands": UNAVAILABLE}))) as daemon:
        session = session_for(daemon.url)
        session.heartbeat()
        with pytest.raises(KairoAPIError) as raised:
            session.safe_point(checkpoint=lambda _c: "x")
    assert raised.value.classification == "transient"
    assert raised.value.status == 503 and raised.value.code == "internal"
    assert raised.value.message == "database is locked"
    assert str(raised.value).startswith("Kairo API returned HTTP 503: ")
    assert not session.retired and not session.fenced


# --------------------------------------------------------------------------
# background heartbeat


def test_background_heartbeat_stops_for_good_when_retired(caplog):
    with serve(FakeDaemon(worker_routes({"POST /api/worker/heartbeat": RETIRED}))) as daemon:
        session = session_for(daemon.url, heartbeat_interval_seconds=0.01)
        with caplog.at_level(logging.WARNING, logger="kairo_sdk.runtime"):
            session.start()
            wait_until(lambda: session.retired)
            thread = session._heartbeat_thread
            thread.join(2)
            assert not thread.is_alive()
            count = len(daemon.paths())
            time.sleep(0.1)
            assert len(daemon.paths()) == count
        session.close()
    assert isinstance(session.last_control_error, AttemptRetired)
    assert any("retired" in record.getMessage() for record in caplog.records)


def test_background_heartbeat_logs_transient_failures_and_keeps_going(caplog):
    beats = [UNAVAILABLE, UNAVAILABLE, OK]
    with serve(FakeDaemon(worker_routes({"POST /api/worker/heartbeat": beats}))) as daemon:
        session = session_for(daemon.url, heartbeat_interval_seconds=0.01)
        with caplog.at_level(logging.WARNING, logger="kairo_sdk.runtime"):
            with session:
                wait_until(lambda: daemon.paths("POST").count("/api/worker/heartbeat") >= 4)
    warnings = [r for r in caplog.records if r.name == "kairo_sdk.runtime" and r.levelno == logging.WARNING]
    assert len(warnings) >= 2
    assert "transient" in warnings[0].getMessage()
    assert session.last_control_error is None
    assert not session.retired


# --------------------------------------------------------------------------
# stop signals


@pytest.fixture
def restore_signals():
    names = ["SIGTERM"] + (["SIGBREAK"] if hasattr(signal, "SIGBREAK") else [])
    saved = {name: signal.getsignal(getattr(signal, name)) for name in names}
    yield
    for name, handler in saved.items():
        signal.signal(getattr(signal, name), handler)


def test_stop_signal_makes_safe_point_return_true(restore_signals, tmp_path: Path):
    install_stop_signals()
    assert signal.getsignal(signal.SIGTERM) is runtime._on_stop_signal
    if hasattr(signal, "SIGBREAK"):
        assert signal.getsignal(signal.SIGBREAK) is runtime._on_stop_signal
    session = AttemptSession(api_url=None, execution_id=None, attempt_id=None)
    assert session.safe_point(checkpoint=lambda _c: None) is False
    signal.raise_signal(signal.SIGTERM)
    assert stop_requested()
    calls = []
    assert session.safe_point(checkpoint=lambda c: calls.append(c)) is True
    # A signal checkpoints once (so the work can be resumed by hand), then stops.
    assert [(c.command_id, c.reason) for c in calls] == [("", "stop_signal")]
    assert session.safe_point(checkpoint=lambda c: calls.append(c)) is True
    assert len(calls) == 1


def test_managed_safe_point_handles_commands_before_the_stop_signal():
    runtime._on_stop_signal(signal.SIGTERM, None)
    routes = worker_routes({"GET /api/worker/commands": suspend(),
                            "POST /api/worker/commands/cmd_1/acks": OK})
    with serve(FakeDaemon(routes)) as daemon:
        session = session_for(daemon.url)
        assert session.safe_point(checkpoint=lambda _c: "ckpt://signal")
    assert [body["phase"] for body in acks(daemon)][-1] == "checkpointed"
    with serve(FakeDaemon(worker_routes())) as daemon:
        session = session_for(daemon.url)
        assert session.safe_point(checkpoint=lambda _c: "unused") is True
        assert acks(daemon) == []


# --------------------------------------------------------------------------
# gang, continuation


def test_gang_context(monkeypatch):
    for name, value in {
        "KAIRO_GANG_ID": "gang_9", "KAIRO_GANG_RANK": "0", "KAIRO_GANG_SIZE": "4",
        "KAIRO_GANG_MASTER_ADDR": "10.0.0.2", "KAIRO_GANG_MASTER_PORT": "29400",
    }.items():
        monkeypatch.setenv(name, value)
    session = AttemptSession.from_environment()
    assert session.gang == GangContext("gang_9", 0, 4, "10.0.0.2", 29400)


@pytest.mark.parametrize(
    ("name", "value", "message"),
    [
        ("KAIRO_GANG_RANK", "one", "KAIRO_GANG_RANK must be an integer"),
        ("KAIRO_GANG_RANK", "4", "outside KAIRO_GANG_SIZE"),
        ("KAIRO_GANG_MASTER_PORT", "70000", "KAIRO_GANG_MASTER_PORT"),
    ],
)
def test_malformed_gang_is_an_error(monkeypatch, name, value, message):
    for key, default in {
        "KAIRO_GANG_ID": "gang_9", "KAIRO_GANG_RANK": "0", "KAIRO_GANG_SIZE": "4",
        "KAIRO_GANG_MASTER_ADDR": "10.0.0.2", "KAIRO_GANG_MASTER_PORT": "29400",
    }.items():
        monkeypatch.setenv(key, default)
    monkeypatch.setenv(name, value)
    with pytest.raises(RuntimeError, match=message):
        AttemptSession.from_environment()


def test_continuation_or(monkeypatch):
    assert continuation_or(lambda: None) is None
    assert continuation_or(lambda: "local://latest") == "local://latest"
    monkeypatch.setenv("KAIRO_CONTINUATION_REF", "ckpt://kairo")
    assert continuation_or(lambda: pytest.fail("resolver must not run")) == "ckpt://kairo"


# --------------------------------------------------------------------------
# ProgressReporter


def managed_environment(monkeypatch, url: str) -> None:
    monkeypatch.setenv("KAIRO_API_URL", url)
    monkeypatch.setenv("KAIRO_EXECUTION_ID", "exe_p")
    monkeypatch.setenv("KAIRO_ATTEMPT_ID", "att_p")
    monkeypatch.setenv("KAIRO_ATTEMPT_TOKEN", "tok_p")


def test_progress_reporter_is_a_no_op_when_unmanaged():
    with ProgressReporter.from_environment("step", total=10) as progress:
        progress.add(3)
        progress.detail(loss=0.5)
        assert progress.session is None
        assert progress.envelope.current == 3


def test_progress_reporter_tracks_phases_and_detail():
    progress = ProgressReporter(None, "shard", total=4, message="building")
    progress.set(2)
    progress.add()
    progress.detail(rows=10, skipped=None)
    progress.phase("shard", total=5)  # same unit: keeps its count
    assert progress.envelope.as_dict() == {
        "unit": "shard", "current": 3, "total": 5, "detail": {"rows": 10},
    }
    progress.phase("step", total=100, message="training")
    progress.set_total(200)
    progress.set_message("warmup")
    progress.detail(rows=None)
    assert progress.envelope.as_dict() == {
        "unit": "step", "current": 0, "total": 200, "message": "warmup", "detail": {},
    }


def test_progress_reporters_share_one_session_and_close_sends_the_last_progress(monkeypatch):
    with serve(FakeDaemon(worker_routes())) as daemon:
        managed_environment(monkeypatch, daemon.url)
        first = ProgressReporter.from_environment("step", total=10)
        second = ProgressReporter.from_environment("epoch", total=2)
        assert first.session is second.session is not None
        assert first.session._heartbeat_thread is not None
        first.set(7)
        first.close()
        last = [r.body for r in daemon.requests if r.path == "/api/worker/heartbeat"][-1]
        assert last["progress"] == {"unit": "step", "current": 7, "total": 10, "detail": {}}
        registrations = daemon.paths("POST").count("/api/worker/processes")
        assert registrations == 1


def test_progress_reporter_reuses_a_started_session(monkeypatch):
    with serve(FakeDaemon(worker_routes())) as daemon:
        managed_environment(monkeypatch, daemon.url)
        with AttemptSession.from_environment() as session:
            assert ProgressReporter.from_environment("step").session is session


def test_progress_reporting_never_raises(monkeypatch):
    with serve(FakeDaemon(worker_routes({"POST /api/worker/heartbeat": RETIRED}))) as daemon:
        session = session_for(daemon.url)
        progress = ProgressReporter(session, "step")
        progress.add(1)
        progress.close()  # the final heartbeat fails: swallowed
        progress.close()
    assert session.retired


# --------------------------------------------------------------------------
# CommandJournal with safe_point


def test_journal_makes_redelivered_suspends_publish_one_checkpoint(tmp_path: Path):
    routes = worker_routes({"GET /api/worker/commands": suspend("cmd/7"),
                            "POST /api/worker/commands/cmd%2F7/acks": OK})
    journal = CommandJournal(tmp_path / "journal")
    saved: list[int] = []

    def checkpoint(command):
        if (done := journal.result(command.command_id)) is not None:
            return done
        saved.append(len(saved))
        path = tmp_path / f"ckpt-{len(saved)}.pt"
        path.write_text("weights", encoding="utf-8")
        return journal.record(command.command_id, SuspendResult(str(path), {"step": 7}))

    with serve(FakeDaemon(routes)) as daemon:
        session = session_for(daemon.url)
        assert session.safe_point(checkpoint=checkpoint)
        assert session.safe_point(checkpoint=checkpoint)
    checkpointed = [body for body in acks(daemon) if body["phase"] == "checkpointed"]
    assert len(saved) == 1
    assert checkpointed[0]["payload"] == checkpointed[1]["payload"] == {
        "continuation_ref": str(tmp_path / "ckpt-1.pt"), "step": 7,
    }


# --------------------------------------------------------------------------
# DistributedAdapter (single process, no torch needed)


@pytest.fixture
def single_process(monkeypatch):
    monkeypatch.setattr(DistributedAdapter, "_distribution", staticmethod(lambda: (None, False)))


def test_adapter_checkpoints_a_suspend(single_process):
    routes = worker_routes({"GET /api/worker/commands": suspend(),
                            "POST /api/worker/commands/cmd_1/acks": OK})
    with serve(FakeDaemon(routes)) as daemon:
        adapter = DistributedAdapter(session_for(daemon.url, heartbeat_interval_seconds=10_000))
        assert adapter.safe_point(lambda _c: SuspendResult("ckpt://9", {"step": 9}))
        adapter.close()
    assert [body["phase"] for body in acks(daemon)] == ["accepted", "checkpointing", "checkpointed"]
    assert acks(daemon)[-1]["payload"] == {"continuation_ref": "ckpt://9", "step": 9}


def test_adapter_skips_unknown_kinds_and_drops_withdrawn_commands(single_process):
    commands = (200, {"commands": [
        {"id": "cmd_x", "kind": "drain", "reason": "", "delivery_count": 1},
        {"id": "cmd_1", "kind": "suspend", "reason": "", "delivery_count": 1},
    ]})
    routes = worker_routes({"GET /api/worker/commands": commands,
                            "POST /api/worker/commands/cmd_1/acks": GONE})
    with serve(FakeDaemon(routes)) as daemon:
        adapter = DistributedAdapter(session_for(daemon.url))
        assert adapter.safe_point(lambda _c: pytest.fail("no checkpoint")) is False
        adapter.close()
    assert [r.path for r in daemon.requests if r.path.endswith("/acks")] == [
        "/api/worker/commands/cmd_1/acks"
    ]


def test_adapter_rejects_a_missing_continuation(single_process):
    routes = worker_routes({"GET /api/worker/commands": suspend(),
                            "POST /api/worker/commands/cmd_1/acks": OK})
    with serve(FakeDaemon(routes)) as daemon:
        adapter = DistributedAdapter(session_for(daemon.url))
        with pytest.raises(NoContinuationError):
            adapter.safe_point(lambda _c: None)
        adapter.close()
    assert acks(daemon)[-1] == {"phase": "rejected", "payload": {"reason": "no continuation_ref"}}


@pytest.mark.parametrize(("response", "error"), [(RETIRED, AttemptRetired), (FENCED, AttemptFenced)])
def test_adapter_raises_terminal_states(single_process, response, error):
    routes = worker_routes({"GET /api/worker/commands": response})
    with serve(FakeDaemon(routes)) as daemon:
        adapter = DistributedAdapter(session_for(daemon.url))
        with pytest.raises(error):
            adapter.safe_point(lambda _c: "x")
        with pytest.raises(error):
            adapter.safe_point(lambda _c: "x")
        adapter.close()


def test_adapter_treats_a_transient_poll_failure_as_no_command(single_process):
    routes = worker_routes({"GET /api/worker/commands": UNAVAILABLE})
    with serve(FakeDaemon(routes)) as daemon:
        adapter = DistributedAdapter(session_for(daemon.url))
        assert adapter.safe_point(lambda _c: "x") is False
        assert isinstance(adapter.session.last_control_error, KairoAPIError)
        adapter.close()


def test_adapter_stops_every_rank_on_a_stop_signal(single_process):
    runtime._on_stop_signal(signal.SIGTERM, None)
    with serve(FakeDaemon(worker_routes())) as daemon:
        adapter = DistributedAdapter(session_for(daemon.url))
        assert adapter.safe_point(lambda _c: "x") is True
        adapter.close()


def test_adapter_checkpoints_once_for_a_stop_signal(single_process):
    runtime._on_stop_signal(signal.SIGTERM, None)
    with serve(FakeDaemon(worker_routes())) as daemon:
        adapter = DistributedAdapter(session_for(daemon.url))
        calls = []
        assert adapter.safe_point(lambda c: calls.append(c) or "ckpt://signal") is True
        assert adapter.safe_point(lambda c: calls.append(c) or "ckpt://signal") is True
        assert [(c.command_id, c.reason) for c in calls] == [("", "stop_signal")]
        assert acks(daemon) == []
