//! A process-wide stop request (its own test binary: the flag is global).

mod common;

use std::time::Duration;

use common::{Exchange, FakeDaemon};
use kairo_sdk::{Attempt, Options, SafePointError, SuspendResult};
use serde_json::json;

#[test]
fn a_stop_request_checkpoints_once_without_polling() {
    let daemon = FakeDaemon::scripted(vec![
        Exchange::register(),
        Exchange::heartbeat(Some(json!({}))),
        Exchange::heartbeat(None), // the final one
    ]);
    let session = Attempt::from_vars(daemon.vars())
        .unwrap()
        .unwrap()
        .start(Options { heartbeat_interval: Duration::from_secs(3600), ..Options::default() });
    daemon.wait_served(2);
    assert!(!kairo_sdk::stop_requested());
    kairo_sdk::request_stop();
    assert!(kairo_sdk::stop_requested());

    let failed = session.safe_point(|request| {
        assert!(request.is_signal());
        Err::<SuspendResult, _>("disk full")
    });
    assert!(matches!(failed, Err(SafePointError::Callback("disk full"))), "{failed:?}");

    let mut calls = 0;
    let mut checkpoint = |request: &kairo_sdk::SuspendRequest| {
        calls += 1;
        assert!(request.is_signal());
        assert_eq!(request.attempt_id, "att_1");
        Ok::<_, String>(SuspendResult::new("ckpt://signal"))
    };
    assert!(session.safe_point(&mut checkpoint).unwrap());
    assert!(session.safe_point(&mut checkpoint).unwrap());
    assert_eq!(calls, 1);
    drop(session);
    daemon.finish();
}
