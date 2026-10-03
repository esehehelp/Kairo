//! Outside Kairo the reporters are no-ops (its own test binary: the session
//! is process-wide and comes from the environment).

#[test]
fn outside_kairo_reporting_is_a_no_op() {
    for (k, _) in std::env::vars().filter(|(k, _)| k.starts_with("KAIRO_")) {
        std::env::remove_var(k);
    }
    assert!(kairo_sdk::Attempt::from_env().unwrap().is_none());
    let r = kairo_sdk::phase("rows", None, "test");
    r.add(3);
    r.set_total(10);
    r.detail("k", "v");
    assert!(!r.is_managed());
    assert_eq!((r.current(), r.total()), (3, Some(10)));
    let p = kairo_sdk::Progress::start("bytes", Some(1), "compat");
    p.add(1);
    drop(p);
    drop(r);
    assert!(kairo_sdk::report::session().is_none());
    assert_eq!(kairo_sdk::live_heartbeat_threads(), 0);
}

#[test]
fn a_stray_kairo_variable_is_refused() {
    let err = kairo_sdk::Attempt::from_vars(|k| (k == "KAIRO_ATTEMPT_TOKEN").then(|| "t".to_string())).unwrap_err();
    assert!(err.message().contains("KAIRO_ATTEMPT_TOKEN"), "{err}");
}
