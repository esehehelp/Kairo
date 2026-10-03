//! Worker SDK for Kairo-managed jobs.
//!
//! Kairo launches an attempt with its context in the environment
//! (`KAIRO_API_URL`, `KAIRO_EXECUTION_ID`, `KAIRO_ATTEMPT_ID`, the bearer
//! token `KAIRO_ATTEMPT_TOKEN` and, when the daemon serves HTTPS with its own
//! CA, `KAIRO_API_CA`). [`Attempt::from_env`] reads it strictly;
//! [`Attempt::client`] talks to the worker API; [`Attempt::start`] runs a
//! [`Session`]: the process registered once, a background heartbeat thread
//! and [`Session::safe_point`] for suspend commands. [`report`] is the
//! soft-fail progress reporter for jobs that only want the monitor to show
//! progress, and [`Progress`] keeps the API of neo-ime's `kairo-progress`.
//!
//! The token goes only to the attempt's URL: no proxy, no redirects, TLS 1.3
//! only, and never over plain `http://` unless the URL names a loopback host.
//!
//! ```no_run
//! use kairo_sdk::{Attempt, Options, SuspendResult};
//!
//! let attempt = Attempt::from_env()?.expect("run me under Kairo");
//! let session = attempt.start(Options::default());
//! for step in 0..1000u64 {
//!     // ... train one step ...
//!     session.set_progress(serde_json::json!({"unit": "steps", "current": step, "total": 1000}));
//!     let stop = session.safe_point(|_request| {
//!         // ... save a checkpoint ...
//!         Ok::<_, std::io::Error>(SuspendResult::new(format!("ckpt://step-{step}")))
//!     })?;
//!     if stop {
//!         break;
//!     }
//! }
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

mod client;
pub mod compat;
mod env;
mod identity;
pub mod report;
mod session;
mod signals;

pub use client::{classify_status, classify_transport, Command, Error, ErrorKind, Phase, Transport, WorkerClient};
pub use compat::Progress;
pub use env::{check_token_transport, is_loopback, Attempt, EnvError, Gang};
pub use identity::{identity_from_filetime, identity_from_stat, process_identity};
pub use report::{phase, reporter, Reporter};
#[doc(hidden)]
pub use session::live_heartbeat_threads;
pub use session::{Options, ProgressEnvelope, SafePointError, Session, Status, SuspendRequest, SuspendResult};
#[cfg(feature = "signals")]
pub use signals::install_stop_handlers;
pub use signals::{request_stop, stop_requested};
