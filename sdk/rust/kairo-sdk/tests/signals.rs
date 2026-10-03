//! Stop handlers (feature "signals"; its own test binary: the flag is global).

#![cfg(feature = "signals")]

#[test]
fn a_stop_signal_requests_a_stop() {
    kairo_sdk::install_stop_handlers().unwrap();
    assert!(!kairo_sdk::stop_requested());
    #[cfg(unix)]
    {
        // SAFETY: raising a signal whose handler only stores an atomic.
        assert_eq!(unsafe { libc::raise(libc::SIGTERM) }, 0);
        assert!(kairo_sdk::stop_requested());
    }
}
