//! A process-wide stop request that [`crate::Session::safe_point`] honours.

use std::sync::atomic::{AtomicBool, Ordering};

static STOP: AtomicBool = AtomicBool::new(false);

/// Ask every safe point of this process to checkpoint and stop.
pub fn request_stop() {
    STOP.store(true, Ordering::SeqCst);
}

/// Whether a stop was requested (by [`request_stop`] or a stop signal).
pub fn stop_requested() -> bool {
    STOP.load(Ordering::SeqCst)
}

/// Make SIGTERM request a stop instead of terminating the process; the next safe point then checkpoints and returns
/// `true`.
#[cfg(feature = "signals")]
pub fn install_stop_handlers() -> std::io::Result<()> {
    os::install()
}

#[cfg(all(feature = "signals", unix))]
mod os {
    extern "C" fn on_signal(_: libc::c_int) {
        super::request_stop();
    }

    pub(super) fn install() -> std::io::Result<()> {
        // SAFETY: a zeroed sigaction with an async-signal-safe handler that
        // only stores an atomic.
        unsafe {
            let mut action: libc::sigaction = std::mem::zeroed();
            action.sa_sigaction = on_signal as extern "C" fn(libc::c_int) as libc::sighandler_t;
            action.sa_flags = libc::SA_RESTART;
            libc::sigemptyset(&mut action.sa_mask);
            if libc::sigaction(libc::SIGTERM, &action, std::ptr::null_mut()) != 0 {
                return Err(std::io::Error::last_os_error());
            }
        }
        Ok(())
    }
}

/// Elsewhere (an attempt runs in a Linux container) there is no stop signal
/// to catch: [`request_stop`] still works.
#[cfg(all(feature = "signals", not(unix)))]
mod os {
    pub(super) fn install() -> std::io::Result<()> {
        Ok(())
    }
}
