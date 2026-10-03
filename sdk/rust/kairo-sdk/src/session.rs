//! A managed attempt session: one registration, one background heartbeat
//! thread, and safe points where the worker honours suspend commands.

use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use serde_json::{Map, Value};

use crate::client::{Command, Error, ErrorKind, Phase, Transport, WorkerClient};
use crate::env::Attempt;

/// Heartbeat threads currently running in this process.
static LIVE_THREADS: AtomicUsize = AtomicUsize::new(0);

/// The number of session heartbeat threads running in this process.
#[doc(hidden)]
pub fn live_heartbeat_threads() -> usize {
    LIVE_THREADS.load(Ordering::SeqCst)
}

/// How a [`Session`] talks to the daemon.
#[derive(Clone, Debug)]
pub struct Options {
    /// Time between background heartbeats (10 s).
    pub heartbeat_interval: Duration,
    /// Minimum time between two command polls of [`Session::safe_point`] (1 s).
    pub poll_interval: Duration,
    /// Bound of every request (10 s).
    pub timeout: Duration,
    /// The rank this process registers as within the attempt (0).
    pub rank: u32,
    /// Warn on stderr when a heartbeat failure appears or changes (off).
    pub log_failures: bool,
}

impl Default for Options {
    fn default() -> Options {
        Options {
            heartbeat_interval: Duration::from_secs(10),
            poll_interval: Duration::from_secs(1),
            timeout: Duration::from_secs(10),
            rank: 0,
            log_failures: false,
        }
    }
}

/// Whether a session still talks to the daemon.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    Running,
    /// The attempt token was refused (the attempt quiesced): heartbeats
    /// stopped for good and the process should exit.
    Retired,
    /// The attempt lost its lease or epoch: heartbeats stopped for good; exit
    /// non-zero unless already checkpointed.
    Fenced,
}

/// The progress the Kairo monitor shows, sent with every heartbeat.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ProgressEnvelope {
    pub unit: String,
    pub current: f64,
    pub total: Option<f64>,
    pub message: Option<String>,
    pub checkpoint_age_seconds: Option<f64>,
    pub detail: Map<String, Value>,
}

impl ProgressEnvelope {
    pub fn new(unit: impl Into<String>, current: f64) -> ProgressEnvelope {
        ProgressEnvelope { unit: unit.into(), current, ..ProgressEnvelope::default() }
    }

    /// The JSON object sent as `progress`: unset fields are left out and whole
    /// numbers are sent as integers.
    pub fn to_value(&self) -> Value {
        let mut v = Map::new();
        v.insert("unit".into(), self.unit.clone().into());
        v.insert("current".into(), number(self.current));
        if let Some(total) = self.total {
            v.insert("total".into(), number(total));
        }
        if let Some(message) = &self.message {
            v.insert("message".into(), message.clone().into());
        }
        if let Some(age) = self.checkpoint_age_seconds {
            v.insert("checkpoint_age_seconds".into(), number(age));
        }
        v.insert("detail".into(), Value::Object(self.detail.clone()));
        Value::Object(v)
    }
}

fn number(x: f64) -> Value {
    if x.fract() == 0.0 && x.abs() < 9.007_199_254_740_992e15 {
        Value::from(x as i64)
    } else {
        serde_json::Number::from_f64(x).map_or(Value::Null, Value::Number)
    }
}

/// The suspend a safe point is handling, for the checkpoint callback.
/// Commands are delivered at least once: the callback may see the same
/// command id again and must be idempotent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SuspendRequest {
    /// The command id; empty for a stop requested by a signal.
    pub command_id: String,
    /// Why the daemon suspends the attempt (`scope_pause`,
    /// `priority_preemption`, ...), or `signal`.
    pub origin: String,
    pub reason: String,
    pub delivery_count: u64,
    pub execution_id: String,
    pub attempt_id: String,
}

impl SuspendRequest {
    /// Whether this suspend comes from a stop signal rather than a command.
    pub fn is_signal(&self) -> bool {
        self.command_id.is_empty() && self.origin == "signal"
    }
}

/// What the checkpoint callback published: the continuation the next
/// attempt resumes from, and any extra payload for the `checkpointed` ack.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SuspendResult {
    pub continuation_ref: Option<String>,
    pub payload: Map<String, Value>,
}

impl SuspendResult {
    pub fn new(continuation_ref: impl Into<String>) -> SuspendResult {
        SuspendResult { continuation_ref: Some(continuation_ref.into()), payload: Map::new() }
    }

    pub fn with_payload(mut self, payload: Map<String, Value>) -> SuspendResult {
        self.payload = payload;
        self
    }
}

/// Why a safe point failed.
#[derive(Debug)]
pub enum SafePointError<E> {
    /// A worker request failed; [`ErrorKind::Retired`] and
    /// [`ErrorKind::Fenced`] also end the session's heartbeats.
    Kairo(Error),
    /// The checkpoint callback failed; the suspend was acknowledged `rejected`.
    Callback(E),
    /// The callback returned no continuation_ref; the suspend was
    /// acknowledged `rejected`.
    NoContinuation,
}

impl<E: fmt::Display> fmt::Display for SafePointError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SafePointError::Kairo(e) => write!(f, "{e}"),
            SafePointError::Callback(e) => write!(f, "checkpoint failed: {e}"),
            SafePointError::NoContinuation => f.write_str("the checkpoint callback returned no continuation_ref"),
        }
    }
}

impl<E: fmt::Debug + fmt::Display> std::error::Error for SafePointError<E> {}

pub(crate) type ProgressSource = Arc<dyn Fn() -> Value + Send + Sync>;

enum Progress {
    Fixed(Value),
    Source(ProgressSource),
}

struct State {
    stop: bool,
    finished: bool,
    status: Status,
    terminal: Option<Error>,
    last_error: Option<Error>,
    progress: Progress,
    registered: bool,
    flush_requested: u64,
    flush_done: u64,
}

struct Inner {
    client: WorkerClient,
    options: Options,
    execution_id: String,
    attempt_id: String,
    state: Mutex<State>,
    /// Wakes the heartbeat thread: stop, flush or a terminal status.
    wake: Condvar,
    /// Signals flushed heartbeats and the thread's exit.
    done: Condvar,
}

struct SafePointState {
    last_poll: Option<Instant>,
    signal_handled: bool,
}

/// A managed attempt: this process registered once, a background thread
/// heartbeating the latest progress, and [`Session::safe_point`]. Dropping
/// (or [`Session::close`]) sends a final heartbeat, waiting a bounded time.
pub struct Session {
    inner: Arc<Inner>,
    thread: Mutex<Option<JoinHandle<()>>>,
    safe_point: Mutex<SafePointState>,
}

impl fmt::Debug for Session {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Session")
            .field("execution_id", &self.inner.execution_id)
            .field("attempt_id", &self.inner.attempt_id)
            .field("status", &self.status())
            .finish_non_exhaustive()
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Session {
    pub(crate) fn start(attempt: &Attempt, options: Options) -> Session {
        let inner = Arc::new(Inner {
            client: attempt.client_with_timeout(options.timeout),
            options,
            execution_id: attempt.execution_id().to_string(),
            attempt_id: attempt.attempt_id().to_string(),
            state: Mutex::new(State {
                stop: false,
                finished: false,
                status: Status::Running,
                terminal: None,
                last_error: None,
                progress: Progress::Fixed(Value::Object(Map::new())),
                registered: false,
                flush_requested: 0,
                flush_done: 0,
            }),
            wake: Condvar::new(),
            done: Condvar::new(),
        });
        let worker = Arc::clone(&inner);
        // Counted from here so that the thread is visible as soon as start returns.
        LIVE_THREADS.fetch_add(1, Ordering::SeqCst);
        let thread = match std::thread::Builder::new().name("kairo-heartbeat".into()).spawn(move || worker.run()) {
            Ok(thread) => Some(thread),
            Err(e) => {
                LIVE_THREADS.fetch_sub(1, Ordering::SeqCst);
                let mut s = lock(&inner.state);
                s.finished = true;
                s.last_error =
                    Some(Error::new(ErrorKind::Permanent, format!("cannot start the heartbeat thread: {e}")));
                None
            }
        };
        Session {
            inner,
            thread: Mutex::new(thread),
            safe_point: Mutex::new(SafePointState { last_poll: None, signal_handled: false }),
        }
    }

    pub fn execution_id(&self) -> &str {
        &self.inner.execution_id
    }

    pub fn attempt_id(&self) -> &str {
        &self.inner.attempt_id
    }

    /// The session's worker API client.
    pub fn client(&self) -> &WorkerClient {
        &self.inner.client
    }

    pub fn status(&self) -> Status {
        lock(&self.inner.state).status
    }

    /// The failure of the latest heartbeat or registration, `None` after a
    /// success.
    pub fn last_error(&self) -> Option<Error> {
        lock(&self.inner.state).last_error.clone()
    }

    /// The progress the next heartbeat sends.
    pub fn set_progress(&self, progress: Value) {
        lock(&self.inner.state).progress = Progress::Fixed(progress);
    }

    pub fn set_progress_envelope(&self, progress: &ProgressEnvelope) {
        self.set_progress(progress.to_value());
    }

    /// Progress computed at each heartbeat.
    pub(crate) fn set_progress_source(&self, source: ProgressSource) {
        lock(&self.inner.state).progress = Progress::Source(source);
    }

    /// Send a heartbeat now and wait (at most the request timeout plus half a
    /// second) until it has been sent.
    pub fn flush(&self) {
        let deadline = Instant::now() + self.inner.options.timeout + Duration::from_millis(500);
        let mut s = lock(&self.inner.state);
        if s.finished || s.stop || s.status != Status::Running {
            return;
        }
        s.flush_requested += 1;
        let target = s.flush_requested;
        self.inner.wake.notify_all();
        while s.flush_done < target && !s.finished {
            let Some(left) = deadline.checked_duration_since(Instant::now()).filter(|d| !d.is_zero()) else {
                return;
            };
            s = self.inner.done.wait_timeout(s, left).unwrap_or_else(PoisonError::into_inner).0;
        }
    }

    /// Stop heartbeating after one final heartbeat, waiting for it at most
    /// the request timeout plus half a second (one second at least).
    /// Idempotent; also done on drop.
    pub fn close(&self) {
        let Some(thread) = lock(&self.thread).take() else {
            return;
        };
        let wait = (self.inner.options.timeout + Duration::from_millis(500)).max(Duration::from_secs(1));
        let deadline = Instant::now() + wait;
        let mut s = lock(&self.inner.state);
        // Set under the lock the thread waits with: the wakeup cannot be lost.
        s.stop = true;
        self.inner.wake.notify_all();
        while !s.finished {
            let Some(left) = deadline.checked_duration_since(Instant::now()).filter(|d| !d.is_zero()) else {
                // Still sending; the detached thread finishes on its own.
                return;
            };
            s = self.inner.done.wait_timeout(s, left).unwrap_or_else(PoisonError::into_inner).0;
        }
        drop(s);
        let _ = thread.join();
    }

    /// One safe point. Returns `Ok(true)` when the worker must stop now (a
    /// suspend was checkpointed and its continuation published, or a stop
    /// was requested by [`crate::request_stop`] or a signal and the callback
    /// ran), `Ok(false)` to carry on.
    ///
    /// Polls the commands at most once per `poll_interval`. The first
    /// `suspend` is acknowledged `accepted` and `checkpointing`, the callback
    /// runs, and `checkpointed` publishes its continuation_ref with its
    /// payload. A failing callback, or one returning no continuation_ref, is
    /// acknowledged `rejected`. Other command kinds are skipped without an
    /// ack. A command withdrawn while acknowledged (404) means carry on.
    pub fn safe_point<E: fmt::Display>(
        &self,
        callback: impl FnOnce(&SuspendRequest) -> Result<SuspendResult, E>,
    ) -> Result<bool, SafePointError<E>> {
        let mut sp = lock(&self.safe_point);
        if let Some(e) = lock(&self.inner.state).terminal.clone() {
            return Err(SafePointError::Kairo(e));
        }
        if crate::signals::stop_requested() {
            if !sp.signal_handled {
                let request = SuspendRequest {
                    command_id: String::new(),
                    origin: "signal".into(),
                    reason: "stop requested".into(),
                    delivery_count: 1,
                    execution_id: self.inner.execution_id.clone(),
                    attempt_id: self.inner.attempt_id.clone(),
                };
                callback(&request).map_err(SafePointError::Callback)?;
                sp.signal_handled = true;
            }
            return Ok(true);
        }
        let now = Instant::now();
        if sp.last_poll.is_some_and(|last| now.duration_since(last) < self.inner.options.poll_interval) {
            return Ok(false);
        }
        sp.last_poll = Some(now);
        let commands = self.inner.client.poll_commands().map_err(|e| SafePointError::Kairo(self.observe(e)))?;
        match commands.into_iter().find(|c| c.kind == "suspend") {
            Some(command) => self.suspend(command, callback),
            None => Ok(false),
        }
    }

    fn suspend<E: fmt::Display>(
        &self,
        command: Command,
        callback: impl FnOnce(&SuspendRequest) -> Result<SuspendResult, E>,
    ) -> Result<bool, SafePointError<E>> {
        let client = &self.inner.client;
        // Ok(false): the command was withdrawn (404), carry on.
        let ack = |phase: Phase, payload: Option<&Value>| match client.ack(&command.id, phase, payload) {
            Ok(()) => Ok(true),
            Err(e) if e.kind == ErrorKind::Gone => Ok(false),
            Err(e) => Err(SafePointError::Kairo(self.observe(e))),
        };
        if !ack(Phase::Accepted, None)? || !ack(Phase::Checkpointing, None)? {
            return Ok(false);
        }
        let request = SuspendRequest {
            command_id: command.id.clone(),
            origin: command.origin.clone(),
            reason: command.reason.clone(),
            delivery_count: command.delivery_count,
            execution_id: self.inner.execution_id.clone(),
            attempt_id: self.inner.attempt_id.clone(),
        };
        let reject = |reason: String| {
            if let Err(e) = client.ack(&command.id, Phase::Rejected, Some(&serde_json::json!({ "reason": reason }))) {
                self.observe(e);
            }
        };
        let result = match callback(&request) {
            Ok(result) => result,
            Err(e) => {
                reject(e.to_string());
                return Err(SafePointError::Callback(e));
            }
        };
        let Some(continuation_ref) = result.continuation_ref.filter(|r| !r.is_empty()) else {
            reject("no continuation_ref".into());
            return Err(SafePointError::NoContinuation);
        };
        let mut payload = result.payload;
        payload.insert("continuation_ref".into(), continuation_ref.into());
        ack(Phase::Checkpointed, Some(&Value::Object(payload)))
    }

    /// Record a failed request: a retired or fenced attempt ends the
    /// heartbeats for good.
    fn observe(&self, e: Error) -> Error {
        self.inner.observe_terminal(&e);
        e
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.close();
    }
}

impl Inner {
    fn observe_terminal(&self, e: &Error) {
        let status = match e.kind {
            ErrorKind::Retired => Status::Retired,
            ErrorKind::Fenced => Status::Fenced,
            _ => return,
        };
        let mut s = lock(&self.state);
        if s.status == Status::Running {
            s.status = status;
            s.terminal = Some(e.clone());
        }
        self.wake.notify_all();
    }

    fn run(self: Arc<Inner>) {
        let mut warnings = Warnings::default();
        loop {
            let (stopping, flush_target) = {
                let s = lock(&self.state);
                if s.status != Status::Running {
                    break;
                }
                (s.stop, s.flush_requested)
            };
            let result = self.beat();
            if let Err(e) = &result {
                self.observe_terminal(e);
            }
            if self.options.log_failures {
                if let Some(message) = warnings.observe(&result) {
                    eprintln!("[kairo] heartbeat failed (the job continues): {message}");
                }
            }
            let mut s = lock(&self.state);
            s.last_error = result.err();
            s.flush_done = s.flush_done.max(flush_target);
            self.done.notify_all();
            if stopping || s.status != Status::Running {
                break;
            }
            let deadline = Instant::now().checked_add(self.options.heartbeat_interval);
            while !s.stop && s.flush_requested == s.flush_done && s.status == Status::Running {
                s = match deadline {
                    None => self.wake.wait(s).unwrap_or_else(PoisonError::into_inner),
                    Some(deadline) => {
                        let Some(left) = deadline.checked_duration_since(Instant::now()).filter(|d| !d.is_zero())
                        else {
                            break;
                        };
                        self.wake.wait_timeout(s, left).unwrap_or_else(PoisonError::into_inner).0
                    }
                };
            }
        }
        let mut s = lock(&self.state);
        s.finished = true;
        self.done.notify_all();
        drop(s);
        LIVE_THREADS.fetch_sub(1, Ordering::SeqCst);
    }

    /// Register this process if it is not yet, then heartbeat.
    fn beat(&self) -> Result<(), Error> {
        if !lock(&self.state).registered {
            let pid = std::process::id();
            let identity = crate::identity::process_identity(pid).map_err(|e| {
                Error::new(ErrorKind::Permanent, format!("cannot establish the process identity of pid {pid}: {e}"))
            })?;
            self.client.register_process(self.options.rank, pid, &identity)?;
            lock(&self.state).registered = true;
        }
        let progress = match &lock(&self.state).progress {
            Progress::Fixed(v) => Ok(v.clone()),
            Progress::Source(source) => Err(Arc::clone(source)),
        };
        let progress = progress.unwrap_or_else(|source| source());
        self.client.heartbeat(&progress)
    }
}

/// One warning per run of the same failure: a new kind of failure (or a
/// failure after a success) is warned about, a repeat is not.
#[derive(Default)]
pub(crate) struct Warnings {
    last: Option<FailureKey>,
}

/// What makes two failures the same kind for [`Warnings`].
type FailureKey = (ErrorKind, Option<u16>, Option<String>, Option<Transport>);

impl Warnings {
    pub(crate) fn observe<'a>(&mut self, result: &'a Result<(), Error>) -> Option<&'a str> {
        match result {
            Ok(()) => {
                self.last = None;
                None
            }
            Err(e) => {
                let key = (e.kind, e.status, e.code.clone(), e.transport);
                if self.last.as_ref() == Some(&key) {
                    return None;
                }
                self.last = Some(key);
                Some(&e.message)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_failure_is_warned_about_when_it_appears_or_changes() {
        let refused = || -> Result<(), Error> {
            Err(Error {
                kind: ErrorKind::Transient,
                status: None,
                code: None,
                transport: Some(Transport::Refused),
                message: "refused".into(),
            })
        };
        let status = |code: u16| -> Result<(), Error> {
            Err(Error {
                kind: crate::client::classify_status(code, ""),
                status: Some(code),
                code: None,
                transport: None,
                message: format!("HTTP {code}"),
            })
        };
        let mut w = Warnings::default();
        assert_eq!(w.observe(&refused()), Some("refused"));
        assert_eq!(w.observe(&refused()), None);
        assert_eq!(w.observe(&status(500)), Some("HTTP 500"));
        assert_eq!(w.observe(&status(500)), None);
        assert_eq!(w.observe(&status(503)), Some("HTTP 503"));
        assert_eq!(w.observe(&Ok(())), None);
        assert_eq!(w.observe(&status(503)), Some("HTTP 503"));
    }

    #[test]
    fn an_envelope_leaves_unset_fields_out() {
        let mut p = ProgressEnvelope::new("steps", 3.0);
        assert_eq!(p.to_value(), serde_json::json!({"unit": "steps", "current": 3, "detail": {}}));
        p.total = Some(10.0);
        p.message = Some("train".into());
        p.checkpoint_age_seconds = Some(1.5);
        p.detail.insert("loss".into(), 0.25.into());
        assert_eq!(
            p.to_value(),
            serde_json::json!({
                "unit": "steps", "current": 3, "total": 10, "message": "train",
                "checkpoint_age_seconds": 1.5, "detail": {"loss": 0.25}
            })
        );
    }
}
