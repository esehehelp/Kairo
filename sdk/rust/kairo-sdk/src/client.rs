//! The worker API: heartbeats, process registration, commands and their
//! acknowledgements, with failures classified the way every Kairo client
//! classifies them.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;
use serde_json::Value;

/// What a client does with a failed request.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ErrorKind {
    /// Retry with backoff: the daemon or the network is briefly unavailable.
    Transient,
    /// Configuration or bug: report it, do not retry.
    Permanent,
    /// The worker credential is no longer valid, expected once the attempt
    /// quiesced: stop heartbeating and polling, the process should exit.
    Retired,
    /// The attempt lost its lease or epoch: stop all writes and exit non-zero
    /// unless already checkpointed.
    Fenced,
    /// The acknowledged command no longer exists: continue.
    Gone,
    /// An admission gate or an observe-only daemon: try again later.
    RetryLater,
}

/// A failure below HTTP.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Transport {
    Refused,
    Reset,
    Timeout,
    Dns,
    /// The server's certificate did not verify against the trusted roots.
    TlsVerify,
    /// Any other TLS failure, e.g. no TLS 1.3.
    TlsHandshake,
    /// Another I/O or protocol failure.
    Other,
}

/// The class of an HTTP error response to a worker request.
pub fn classify_status(status: u16, code: &str) -> ErrorKind {
    match (status, code) {
        (401, _) => ErrorKind::Retired,
        (409, "stale_epoch" | "execution_started") => ErrorKind::Fenced,
        (404, _) => ErrorKind::Gone,
        (423, "gate_closed" | "observe_only") => ErrorKind::RetryLater,
        (429, _) | (500..=599, _) => ErrorKind::Transient,
        _ => ErrorKind::Permanent,
    }
}

/// The class of a failure below HTTP: TLS failures are permanent (a wrong CA
/// or protocol does not fix itself), the rest transient.
pub fn classify_transport(transport: Transport) -> ErrorKind {
    match transport {
        Transport::TlsVerify | Transport::TlsHandshake => ErrorKind::Permanent,
        Transport::Refused | Transport::Reset | Transport::Timeout | Transport::Dns | Transport::Other => {
            ErrorKind::Transient
        }
    }
}

/// A failed worker request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Error {
    pub kind: ErrorKind,
    /// The HTTP status of an error response.
    pub status: Option<u16>,
    /// The `code` of an error response's `{"error", "code"}` body.
    pub code: Option<String>,
    /// The failure below HTTP, when there was no response.
    pub transport: Option<Transport>,
    pub message: String,
}

impl Error {
    pub(crate) fn new(kind: ErrorKind, message: impl Into<String>) -> Error {
        Error { kind, status: None, code: None, transport: None, message: message.into() }
    }

    /// Whether this failure ends the attempt's control traffic for good.
    pub fn is_terminal(&self) -> bool {
        matches!(self.kind, ErrorKind::Retired | ErrorKind::Fenced)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Error {}

/// A command the daemon delivered to this attempt. Commands are delivered at
/// least once: the same id may come again.
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct Command {
    pub id: String,
    pub kind: String,
    #[serde(default)]
    pub origin: String,
    #[serde(default)]
    pub reason: String,
    #[serde(default)]
    pub delivery_count: u64,
    #[serde(default)]
    pub execution_id: String,
    #[serde(default)]
    pub attempt_id: String,
    #[serde(default)]
    pub payload: Option<Value>,
}

/// The phase a command acknowledgement reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Phase {
    Accepted,
    Checkpointing,
    Checkpointed,
    Rejected,
}

impl Phase {
    pub fn as_str(self) -> &'static str {
        match self {
            Phase::Accepted => "accepted",
            Phase::Checkpointing => "checkpointing",
            Phase::Checkpointed => "checkpointed",
            Phase::Rejected => "rejected",
        }
    }
}

/// A blocking client of the worker API (`/api/worker/...`), authenticated
/// with the attempt token. Requests go only to the attempt's URL: TLS 1.3
/// only, `KAIRO_API_CA` as the only roots when given, no proxy, no
/// redirects. Cheap to share between threads.
#[derive(Clone)]
pub struct WorkerClient {
    agent: ureq::Agent,
    base: String,
    authorization: String,
}

impl fmt::Debug for WorkerClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkerClient").field("base", &self.base).finish_non_exhaustive()
    }
}

impl WorkerClient {
    pub(crate) fn new(api_url: &str, token: &str, ca: &[Vec<u8>], timeout: Duration) -> WorkerClient {
        let mut tls = ureq::tls::TlsConfig::builder()
            .provider(ureq::tls::TlsProvider::Rustls)
            .unversioned_rustls_crypto_provider(tls13_provider());
        if !ca.is_empty() {
            let certs: Vec<_> = ca.iter().map(|der| ureq::tls::Certificate::from_der(der).to_owned()).collect();
            tls = tls.root_certs(ureq::tls::RootCerts::new_with_certs(&certs));
        }
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .timeout_global(Some(timeout))
            .proxy(None)
            .max_redirects(0)
            .http_status_as_error(false)
            .tls_config(tls.build())
            .build()
            .into();
        WorkerClient {
            agent,
            base: format!("{}/api/worker", api_url.trim_end_matches('/')),
            authorization: format!("Bearer {token}"),
        }
    }

    /// `POST /api/worker/heartbeat {"progress": progress}`.
    pub fn heartbeat(&self, progress: &Value) -> Result<(), Error> {
        self.request("POST", "/heartbeat", Some(&serde_json::json!({ "progress": progress }))).map(drop)
    }

    /// `POST /api/worker/processes`: bind `rank` of the attempt to this
    /// process (`pid` and its [`crate::process_identity`]).
    pub fn register_process(&self, rank: u32, pid: u32, identity: &str) -> Result<(), Error> {
        let body = serde_json::json!({ "rank": rank, "pid": pid, "process_identity": identity });
        self.request("POST", "/processes", Some(&body)).map(drop)
    }

    /// `GET /api/worker/commands`: the pending commands, none acknowledged.
    pub fn poll_commands(&self) -> Result<Vec<Command>, Error> {
        #[derive(Deserialize)]
        struct Commands {
            #[serde(default)]
            commands: Option<Vec<Command>>,
        }
        let body = self.request("GET", "/commands", None)?;
        let parsed: Commands = serde_json::from_value(body).map_err(|e| {
            Error::new(ErrorKind::Permanent, format!("GET /api/worker/commands: malformed commands response: {e}"))
        })?;
        Ok(parsed.commands.unwrap_or_default())
    }

    /// `POST /api/worker/commands/{id}/acks {"phase", "payload"}`; the id is
    /// percent-encoded as one path segment and the payload defaults to `{}`.
    pub fn ack(&self, command_id: &str, phase: Phase, payload: Option<&Value>) -> Result<(), Error> {
        let empty = Value::Object(Default::default());
        let body = serde_json::json!({ "phase": phase.as_str(), "payload": payload.unwrap_or(&empty) });
        self.request("POST", &format!("/commands/{}/acks", encode_segment(command_id)), Some(&body)).map(drop)
    }

    fn request(&self, method: &str, path: &str, body: Option<&Value>) -> Result<Value, Error> {
        let url = format!("{}{path}", self.base);
        let what = format!("{method} /api/worker{path}");
        let sent = match body {
            Some(body) => self
                .agent
                .post(&url)
                .header("Accept", "application/json")
                .header("Authorization", &self.authorization)
                .content_type("application/json")
                .send(body.to_string()),
            None => self
                .agent
                .get(&url)
                .header("Accept", "application/json")
                .header("Authorization", &self.authorization)
                .call(),
        };
        let mut response = sent.map_err(|e| transport_error(&what, e))?;
        let status = response.status().as_u16();
        let text = response.body_mut().read_to_string().map_err(|e| transport_error(&what, e))?;
        if (200..300).contains(&status) {
            if text.trim().is_empty() {
                return Ok(Value::Object(Default::default()));
            }
            return serde_json::from_str(&text)
                .map_err(|e| Error::new(ErrorKind::Permanent, format!("{what}: response is not JSON: {e}")));
        }
        // Errors are {"error", "code"}; a 3xx is reported, never followed.
        let parsed = serde_json::from_str::<Value>(&text).ok();
        let code = parsed.as_ref().and_then(|v| v["code"].as_str()).unwrap_or("").to_string();
        let detail = match &parsed {
            Some(v) => v["error"].as_str().unwrap_or("").to_string(),
            None => text.chars().take(200).collect(),
        };
        let mut message = format!("{what}: HTTP {status}");
        for part in [&code, &detail] {
            if !part.trim().is_empty() {
                message.push_str(": ");
                message.push_str(part.trim());
            }
        }
        Err(Error {
            kind: classify_status(status, &code),
            status: Some(status),
            code: (!code.is_empty()).then_some(code),
            transport: None,
            message,
        })
    }
}

fn transport_error(what: &str, e: ureq::Error) -> Error {
    let transport = transport_of(&e);
    Error {
        kind: match &e {
            // A request ureq refuses to send is a bug or a misconfiguration.
            ureq::Error::BadUri(_) | ureq::Error::Http(_) | ureq::Error::RequireHttpsOnly(_) => ErrorKind::Permanent,
            _ => classify_transport(transport),
        },
        status: None,
        code: None,
        transport: Some(transport),
        message: format!("{what}: {e}"),
    }
}

fn transport_of(e: &ureq::Error) -> Transport {
    match e {
        ureq::Error::Timeout(_) => Transport::Timeout,
        ureq::Error::HostNotFound => Transport::Dns,
        ureq::Error::ConnectionFailed => Transport::Refused,
        ureq::Error::Rustls(e) => tls_transport(e),
        ureq::Error::Tls(_) | ureq::Error::Pem(_) => Transport::TlsHandshake,
        ureq::Error::Io(io) => {
            if let Some(tls) = io.get_ref().and_then(|inner| inner.downcast_ref::<rustls::Error>()) {
                return tls_transport(tls);
            }
            use std::io::ErrorKind as K;
            match io.kind() {
                K::ConnectionRefused => Transport::Refused,
                K::ConnectionReset | K::ConnectionAborted | K::BrokenPipe | K::UnexpectedEof => Transport::Reset,
                K::TimedOut | K::WouldBlock => Transport::Timeout,
                _ => Transport::Other,
            }
        }
        _ => Transport::Other,
    }
}

fn tls_transport(e: &rustls::Error) -> Transport {
    use rustls::AlertDescription as A;
    match e {
        rustls::Error::InvalidCertificate(_) | rustls::Error::NoCertificatesPresented => Transport::TlsVerify,
        rustls::Error::AlertReceived(A::BadCertificate | A::UnknownCA | A::CertificateUnknown) => Transport::TlsVerify,
        _ => Transport::TlsHandshake,
    }
}

/// Percent-encode everything but the URL-unreserved characters, so that the
/// result is exactly one path segment.
pub(crate) fn encode_segment(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// The ring provider with its TLS 1.3 cipher suites only: rustls offers a
/// protocol version only when a suite supports it, so the client hello
/// carries TLS 1.3 alone (ureq asks rustls for every version).
fn tls13_provider() -> Arc<rustls::crypto::CryptoProvider> {
    let mut provider = rustls::crypto::ring::default_provider();
    provider.cipher_suites.retain(|s| s.version() == &rustls::version::TLS13);
    Arc::new(provider)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_command_id_is_one_path_segment() {
        assert_eq!(encode_segment("cmd/1"), "cmd%2F1");
        assert_eq!(encode_segment("cmd_1-a.b~c"), "cmd_1-a.b~c");
        assert_eq!(encode_segment("a b?c#d%e"), "a%20b%3Fc%23d%25e");
        assert_eq!(encode_segment("é"), "%C3%A9");
    }

    #[test]
    fn the_crypto_provider_offers_tls13_only() {
        let provider = tls13_provider();
        assert!(!provider.cipher_suites.is_empty());
        assert!(provider.cipher_suites.iter().all(|s| s.version() == &rustls::version::TLS13));
    }

    #[test]
    fn io_failures_map_to_transports() {
        let io = |kind: std::io::ErrorKind| transport_of(&ureq::Error::Io(kind.into()));
        assert_eq!(io(std::io::ErrorKind::ConnectionRefused), Transport::Refused);
        assert_eq!(io(std::io::ErrorKind::ConnectionReset), Transport::Reset);
        assert_eq!(io(std::io::ErrorKind::TimedOut), Transport::Timeout);
        assert_eq!(transport_of(&ureq::Error::HostNotFound), Transport::Dns);
        let tls = std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            rustls::Error::InvalidCertificate(rustls::CertificateError::UnknownIssuer),
        );
        assert_eq!(transport_of(&ureq::Error::Io(tls)), Transport::TlsVerify);
    }
}
