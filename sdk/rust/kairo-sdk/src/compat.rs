//! The public API of neo-ime's `kairo-progress` crate, on top of
//! [`crate::report`]: a dependency renamed to `kairo_progress` keeps
//! `kairo_progress::Progress` working unchanged.

use crate::report::{phase, Reporter};

/// Progress of a Kairo-managed job for the Kairo monitor; a no-op outside
/// Kairo. Dropping it sends one last heartbeat with the final count.
pub struct Progress {
    reporter: Reporter,
}

impl Progress {
    /// Start reporting; `total` may be set later with [`Progress::set_total`].
    pub fn start(unit: &str, total: Option<u64>, message: &str) -> Progress {
        Progress { reporter: phase(unit, total, message) }
    }

    pub fn set(&self, current: u64) {
        self.reporter.set(current);
    }

    pub fn add(&self, n: u64) {
        self.reporter.add(n);
    }

    pub fn set_total(&self, total: u64) {
        self.reporter.set_total(total);
    }
}
