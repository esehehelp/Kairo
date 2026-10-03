//! Reporters of one process share one session: one registration, one
//! heartbeat thread (its own test binary: the session is process-wide and
//! comes from the environment).

mod common;

use common::{FakeDaemon, TOKEN};
use kairo_sdk::Progress;
use serde_json::{json, Value};

fn heartbeats(daemon: &FakeDaemon) -> Vec<Value> {
    daemon
        .requests()
        .into_iter()
        .filter(|r| r.path == "/api/worker/heartbeat")
        .map(|r| r.body.unwrap()["progress"].clone())
        .collect()
}

#[test]
fn reporters_share_one_registration_and_one_thread() {
    let daemon = FakeDaemon::permissive();
    for (k, _) in std::env::vars().filter(|(k, _)| k.starts_with("KAIRO_")) {
        std::env::remove_var(k);
    }
    std::env::set_var("KAIRO_API_URL", &daemon.url);
    std::env::set_var("KAIRO_EXECUTION_ID", "exe_1");
    std::env::set_var("KAIRO_ATTEMPT_ID", "att_1");
    std::env::set_var("KAIRO_ATTEMPT_TOKEN", TOKEN);
    assert_eq!(kairo_sdk::live_heartbeat_threads(), 0);

    let first = kairo_sdk::phase("rows", Some(10), "first");
    assert!(first.is_managed());
    assert_eq!(kairo_sdk::live_heartbeat_threads(), 1);
    // The first heartbeat already carries the first reporter.
    daemon.wait_served(2);
    assert_eq!(heartbeats(&daemon)[0]["unit"], "rows");

    let second = kairo_sdk::reporter("bytes");
    assert!(second.is_managed());
    assert_eq!(kairo_sdk::live_heartbeat_threads(), 1);
    first.add(3);
    second.add(5);
    second.detail("files", 2);
    second.set_message("copying");

    drop(second); // the shown reporter: a final heartbeat with its values
    assert_eq!(
        heartbeats(&daemon).last().unwrap(),
        &json!({"unit": "bytes", "current": 5, "message": "copying", "detail": {"files": 2}})
    );
    first.set_total(12);
    drop(first);
    assert_eq!(
        heartbeats(&daemon).last().unwrap(),
        &json!({"unit": "rows", "current": 3, "total": 12, "message": "first"})
    );

    // The compat shim of kairo-progress rides the same session.
    let p = Progress::start("spans", None, "span-synth build");
    p.add(4);
    p.set(9);
    drop(p);
    assert_eq!(
        heartbeats(&daemon).last().unwrap(),
        &json!({"unit": "spans", "current": 9, "message": "span-synth build"})
    );

    assert_eq!(kairo_sdk::live_heartbeat_threads(), 1);
    let session = kairo_sdk::report::session().unwrap();
    assert_eq!(session.status(), kairo_sdk::Status::Running);
    let registrations = daemon.requests().iter().filter(|r| r.path == "/api/worker/processes").count();
    assert_eq!(registrations, 1);
}
