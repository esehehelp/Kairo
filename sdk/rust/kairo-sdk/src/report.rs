//! Soft-fail progress reporting for jobs that only want the Kairo monitor to
//! show how far they are: one process-wide [`Session`] (one registration,
//! one heartbeat thread) however many reporters the process starts.
//!
//! Outside Kairo every call is a no-op. An incomplete or malformed Kairo
//! environment disables reporting with one warning, and a failed report
//! never fails the job: a warning on stderr when a failure first appears or
//! changes kind. Nothing here panics or returns an error.
//!
//! The heartbeat carries the most recently started live reporter; when it
//! is dropped the previous one shows again.
//!
//! ```no_run
//! let p = kairo_sdk::phase("bytes", Some(1 << 30), "span-synth build");
//! p.add(4096);
//! // dropped: one last heartbeat with the final count
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

use serde_json::{Map, Value};

use crate::env::Attempt;
use crate::session::{Options, Session};

const UNKNOWN: u64 = u64::MAX;

/// `None` outside Kairo or when the Kairo environment is unusable.
static GLOBAL: OnceLock<Option<Arc<Session>>> = OnceLock::new();
/// The live reporters, oldest first.
static BOARD: Mutex<Board> = Mutex::new(Board { entries: Vec::new(), last: None });
static NEXT_ID: AtomicU64 = AtomicU64::new(0);

struct Board {
    entries: Vec<Arc<Entry>>,
    /// What the heartbeats carry once every reporter is gone.
    last: Option<Value>,
}

struct Entry {
    id: u64,
    unit: String,
    current: AtomicU64,
    total: AtomicU64,
    message: Mutex<String>,
    detail: Mutex<Map<String, Value>>,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Entry {
    fn snapshot(&self) -> Value {
        let mut v = Map::new();
        v.insert("unit".into(), self.unit.clone().into());
        v.insert("current".into(), self.current.load(Ordering::Relaxed).into());
        let total = self.total.load(Ordering::Relaxed);
        if total != UNKNOWN {
            v.insert("total".into(), total.into());
        }
        let message = lock(&self.message);
        if !message.is_empty() {
            v.insert("message".into(), message.clone().into());
        }
        let detail = lock(&self.detail);
        if !detail.is_empty() {
            v.insert("detail".into(), Value::Object(detail.clone()));
        }
        Value::Object(v)
    }
}

fn board_progress() -> Value {
    let board = lock(&BOARD);
    match board.entries.last() {
        Some(entry) => Arc::clone(entry),
        None => return board.last.clone().unwrap_or_else(|| Value::Object(Map::new())),
    }
    .snapshot()
}

/// The process-wide session of the reporters, started on first use; `None`
/// outside Kairo. Jobs that report progress this way can call
/// [`Session::safe_point`] on it.
pub fn session() -> Option<Arc<Session>> {
    GLOBAL
        .get_or_init(|| match Attempt::from_env() {
            Ok(Some(attempt)) => {
                let session = attempt.start(Options { log_failures: true, ..Options::default() });
                session.set_progress_source(Arc::new(board_progress));
                Some(Arc::new(session))
            }
            Ok(None) => None,
            Err(e) => {
                eprintln!("[kairo] {e}; progress is not reported");
                None
            }
        })
        .clone()
}

/// A progress reporter counting `unit`s, total unknown.
pub fn reporter(unit: &str) -> Reporter {
    Reporter::new(unit, None, "")
}

/// A progress reporter for one phase of the job: `total` units (if known)
/// and a message for the monitor.
pub fn phase(unit: &str, total: Option<u64>, message: &str) -> Reporter {
    Reporter::new(unit, total, message)
}

/// A handle on one progress counter; cheap to update from any thread.
/// Dropping it sends a final heartbeat with its last values.
pub struct Reporter {
    entry: Arc<Entry>,
    session: Option<Arc<Session>>,
}

impl Reporter {
    fn new(unit: &str, total: Option<u64>, message: &str) -> Reporter {
        let entry = Arc::new(Entry {
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
            unit: unit.to_string(),
            current: AtomicU64::new(0),
            total: AtomicU64::new(total.unwrap_or(UNKNOWN)),
            message: Mutex::new(message.to_string()),
            detail: Mutex::new(Map::new()),
        });
        // On the board before the session may start: its first heartbeat
        // already carries this reporter.
        let unmanaged = matches!(GLOBAL.get(), Some(None));
        if unmanaged {
            return Reporter { entry, session: None };
        }
        lock(&BOARD).entries.push(Arc::clone(&entry));
        let session = session();
        if session.is_none() {
            lock(&BOARD).entries.retain(|e| e.id != entry.id);
        }
        Reporter { entry, session }
    }

    pub fn set(&self, current: u64) {
        self.entry.current.store(current, Ordering::Relaxed);
    }

    pub fn add(&self, n: u64) {
        self.entry.current.fetch_add(n, Ordering::Relaxed);
    }

    pub fn set_total(&self, total: u64) {
        self.entry.total.store(total, Ordering::Relaxed);
    }

    pub fn set_message(&self, message: &str) {
        *lock(&self.entry.message) = message.to_string();
    }

    /// Set one key of the progress `detail` object.
    pub fn detail(&self, key: &str, value: impl Into<Value>) {
        lock(&self.entry.detail).insert(key.to_string(), value.into());
    }

    pub fn current(&self) -> u64 {
        self.entry.current.load(Ordering::Relaxed)
    }

    pub fn total(&self) -> Option<u64> {
        Some(self.entry.total.load(Ordering::Relaxed)).filter(|&t| t != UNKNOWN)
    }

    /// Whether this reporter reaches a Kairo daemon.
    pub fn is_managed(&self) -> bool {
        self.session.is_some()
    }
}

impl Drop for Reporter {
    fn drop(&mut self) {
        let Some(session) = &self.session else {
            return;
        };
        let shown = lock(&BOARD).entries.last().is_some_and(|e| e.id == self.entry.id);
        if shown {
            // The final values, while this reporter is still the one shown.
            session.flush();
        }
        let mut board = lock(&BOARD);
        board.entries.retain(|e| e.id != self.entry.id);
        if board.entries.is_empty() {
            board.last = Some(self.entry.snapshot());
        }
    }
}
