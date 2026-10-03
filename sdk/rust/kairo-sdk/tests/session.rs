//! The session's heartbeat thread against a fake daemon.

mod common;

use std::time::{Duration, Instant};

use common::{Exchange, FakeDaemon};
use kairo_sdk::{Attempt, ErrorKind, Options, ProgressEnvelope, Status};
use serde_json::json;

fn options() -> Options {
    Options { heartbeat_interval: Duration::from_secs(3600), timeout: Duration::from_secs(5), ..Options::default() }
}

#[test]
fn close_wakes_the_thread_at_once_and_sends_a_final_heartbeat() {
    let daemon = FakeDaemon::scripted(vec![
        Exchange::register(),
        Exchange::heartbeat(Some(json!({}))),
        Exchange::heartbeat(Some(json!({"unit": "steps", "current": 7, "total": 10, "detail": {}}))),
    ]);
    let session = Attempt::from_vars(daemon.vars()).unwrap().unwrap().start(options());
    daemon.wait_served(2);
    let mut progress = ProgressEnvelope::new("steps", 7.0);
    progress.total = Some(10.0);
    session.set_progress_envelope(&progress);
    let started = Instant::now();
    session.close();
    assert!(started.elapsed() < Duration::from_secs(3), "close waited {:?}", started.elapsed());
    assert_eq!(daemon.served(), 3);
    session.close(); // idempotent
    drop(session);
    daemon.finish();
}

#[test]
fn close_right_after_start_does_not_lose_the_wakeup() {
    for _ in 0..20 {
        let daemon = FakeDaemon::permissive();
        let session = Attempt::from_vars(daemon.vars()).unwrap().unwrap().start(options());
        let started = Instant::now();
        drop(session);
        assert!(started.elapsed() < Duration::from_secs(3), "drop waited {:?}", started.elapsed());
        let paths: Vec<_> = daemon.finish().into_iter().map(|r| r.path).collect();
        assert_eq!(paths[0], "/api/worker/processes");
        assert!(paths[1..].iter().all(|p| p == "/api/worker/heartbeat") && paths.len() <= 3, "{paths:?}");
    }
}

#[test]
fn a_retired_attempt_stops_heartbeating_for_good() {
    let daemon = FakeDaemon::scripted(vec![
        Exchange::register(),
        Exchange::new(
            "POST",
            "/api/worker/heartbeat",
            None,
            401,
            json!({"error": "valid token required", "code": "unauthorized"}),
        ),
    ]);
    let session = Attempt::from_vars(daemon.vars())
        .unwrap()
        .unwrap()
        .start(Options { heartbeat_interval: Duration::from_millis(20), ..options() });
    daemon.wait_served(2);
    let deadline = Instant::now() + Duration::from_secs(5);
    while session.status() == Status::Running {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(session.status(), Status::Retired);
    assert_eq!(session.last_error().map(|e| e.kind), Some(ErrorKind::Retired));
    std::thread::sleep(Duration::from_millis(100)); // several intervals: no heartbeat
    session.flush(); // no-op
    drop(session); // no final heartbeat
    daemon.finish();
}

#[test]
fn a_fenced_heartbeat_ends_the_session() {
    let daemon = FakeDaemon::scripted(vec![
        Exchange::register(),
        Exchange::new(
            "POST",
            "/api/worker/heartbeat",
            None,
            409,
            json!({"error": "stale coordination epoch", "code": "stale_epoch"}),
        ),
    ]);
    let session = Attempt::from_vars(daemon.vars()).unwrap().unwrap().start(options());
    daemon.wait_served(2);
    let deadline = Instant::now() + Duration::from_secs(5);
    while session.status() == Status::Running {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(session.status(), Status::Fenced);
    drop(session);
    daemon.finish();
}

#[test]
fn a_failed_registration_is_retried_before_any_heartbeat() {
    let daemon = FakeDaemon::scripted(vec![
        Exchange::new(
            "POST",
            "/api/worker/processes",
            None,
            503,
            json!({"error": "database is locked", "code": "internal"}),
        ),
        Exchange::register(),
        Exchange::heartbeat(Some(json!({"unit": "rows"}))),
        Exchange::heartbeat(Some(json!({"unit": "rows"}))),
    ]);
    let session = Attempt::from_vars(daemon.vars()).unwrap().unwrap().start(options());
    daemon.wait_served(1);
    session.set_progress(json!({"unit": "rows", "current": 1}));
    let deadline = Instant::now() + Duration::from_secs(5);
    while session.last_error().is_none() {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(session.last_error().map(|e| (e.kind, e.status)), Some((ErrorKind::Transient, Some(503))));
    session.flush();
    assert_eq!(daemon.served(), 3);
    assert_eq!(session.last_error(), None);
    assert_eq!(session.status(), Status::Running);
    drop(session);
    daemon.finish();
}

#[test]
fn a_registered_process_carries_rank_pid_and_identity() {
    let daemon = FakeDaemon::permissive();
    let session = Attempt::from_vars(daemon.vars()).unwrap().unwrap().start(Options { rank: 3, ..options() });
    daemon.wait_served(2);
    drop(session);
    let requests = daemon.finish();
    let body = requests[0].body.clone().unwrap();
    assert_eq!(body["rank"], 3);
    assert_eq!(body["pid"], std::process::id());
    assert_eq!(body["process_identity"], kairo_sdk::process_identity(std::process::id()).unwrap());
}
