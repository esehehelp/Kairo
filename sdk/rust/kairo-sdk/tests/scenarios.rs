//! Replays sdk/conformance/worker_scenarios.json against a fake daemon: the
//! session registers and heartbeats once, the safe point sends exactly the
//! scripted requests, and closing sends a final heartbeat unless the attempt
//! was retired or fenced.

mod common;

use std::time::Duration;

use common::{Exchange, FakeDaemon};
use kairo_sdk::{Attempt, ErrorKind, Options, SafePointError, Status, SuspendResult};
use serde_json::{json, Map, Value};

fn fixture() -> Value {
    let path = format!("{}/../../conformance/worker_scenarios.json", env!("CARGO_MANIFEST_DIR"));
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

fn kind(class: &str) -> ErrorKind {
    match class {
        "transient" => ErrorKind::Transient,
        "permanent" => ErrorKind::Permanent,
        "retired" => ErrorKind::Retired,
        "fenced" => ErrorKind::Fenced,
        "gone" => ErrorKind::Gone,
        "retry_later" => ErrorKind::RetryLater,
        other => panic!("unknown class {other}"),
    }
}

fn callback(spec: &Value) -> Result<SuspendResult, String> {
    if let Some(message) = spec["raises"].as_str() {
        return Err(message.to_string());
    }
    let returns = &spec["returns"];
    Ok(SuspendResult {
        continuation_ref: returns["continuation_ref"].as_str().map(str::to_string),
        payload: returns["payload"].as_object().cloned().unwrap_or_else(Map::new),
    })
}

fn run(scenario: &Value) {
    let name = scenario["name"].as_str().unwrap();
    let expect = &scenario["expect"];
    let error = expect["error"].as_str();
    let terminal = matches!(error, Some("retired" | "fenced"));
    let mut exchanges = vec![Exchange::register(), Exchange::heartbeat(Some(json!({})))];
    for x in scenario["exchanges"].as_array().unwrap() {
        let (req, resp) = (&x["request"], &x["response"]);
        exchanges.push(Exchange::new(
            req["method"].as_str().unwrap(),
            req["path"].as_str().unwrap(),
            req.get("body").cloned(),
            resp["status"].as_u64().unwrap() as u16,
            resp["body"].clone(),
        ));
    }
    if !terminal {
        exchanges.push(Exchange::heartbeat(None));
    }
    let daemon = FakeDaemon::scripted(exchanges);
    let attempt = Attempt::from_vars(daemon.vars()).unwrap().unwrap();
    let session = attempt.start(Options {
        heartbeat_interval: Duration::from_secs(3600),
        poll_interval: Duration::from_secs(3600),
        timeout: Duration::from_secs(5),
        ..Options::default()
    });
    // Registered and the first heartbeat just sent.
    daemon.wait_served(2);

    let mut calls = 0;
    let result = session.safe_point(|request| {
        calls += 1;
        assert!(!request.is_signal());
        assert_eq!(request.execution_id, "exe_1");
        callback(&scenario["callback"])
    });
    match (error, &result) {
        (None, Ok(stop)) => assert_eq!(*stop, expect["result"].as_bool().unwrap(), "{name}"),
        (Some("callback"), Err(SafePointError::Callback(message))) => {
            assert_eq!(message, scenario["callback"]["raises"].as_str().unwrap(), "{name}")
        }
        (Some("no_continuation"), Err(SafePointError::NoContinuation)) => {}
        (Some(class), Err(SafePointError::Kairo(e))) => assert_eq!(e.kind, kind(class), "{name}: {e}"),
        _ => panic!("{name}: expected {expect}, got {result:?}"),
    }
    // The callback runs after the accepted and checkpointing acks.
    let handled = scenario["exchanges"].as_array().unwrap().len() >= 4;
    assert_eq!(calls, usize::from(handled), "{name}: callback calls");

    let status = match error {
        Some("retired") => Status::Retired,
        Some("fenced") => Status::Fenced,
        _ => Status::Running,
    };
    assert_eq!(session.status(), status, "{name}");

    // The poll is rate-limited (or the session is over): nothing more is sent.
    let again = session.safe_point(|_| -> Result<SuspendResult, String> { panic!("{name}: callback again") });
    match &again {
        Ok(false) => assert!(!terminal),
        Err(SafePointError::Kairo(e)) => assert!(terminal && e.is_terminal(), "{name}: {e}"),
        other => panic!("{name}: second safe point {other:?}"),
    }
    drop(session);
    daemon.finish();
}

#[test]
fn worker_scenarios() {
    let v = fixture();
    let scenarios = v["scenarios"].as_array().unwrap();
    assert!(!scenarios.is_empty());
    for scenario in scenarios {
        run(scenario);
    }
}
