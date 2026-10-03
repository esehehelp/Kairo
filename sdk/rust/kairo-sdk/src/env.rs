//! The attempt a worker runs as, read from its launch environment.

use std::fmt;
use std::net::{IpAddr, Ipv6Addr};
use std::time::Duration;

use crate::client::WorkerClient;
use crate::session::{Options, Session};

/// The variables that may be set only when the attempt is managed.
const MANAGED_ONLY: [&str; 4] = ["KAIRO_EXECUTION_ID", "KAIRO_ATTEMPT_ID", "KAIRO_ATTEMPT_TOKEN", "KAIRO_API_CA"];

/// A Kairo environment that is incomplete, contradictory or malformed. The
/// message names the offending variable or rule.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnvError(String);

impl EnvError {
    pub fn message(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for EnvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for EnvError {}

/// The gang an attempt belongs to: attempts started together, one per rank.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Gang {
    pub id: String,
    pub rank: u32,
    pub size: u32,
    pub master_addr: String,
    pub master_port: u16,
}

/// A Kairo-managed attempt: where the daemon is, which attempt this process
/// runs as, and the attempt's bearer token (never shown by `Debug`).
#[derive(Clone)]
pub struct Attempt {
    api_url: String,
    execution_id: String,
    attempt_id: String,
    token: String,
    ca: Vec<Vec<u8>>,
    continuation_ref: Option<String>,
    resource_ids: Vec<String>,
    resource_bindings: Vec<String>,
    gang: Option<Gang>,
}

impl fmt::Debug for Attempt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Attempt")
            .field("api_url", &self.api_url)
            .field("execution_id", &self.execution_id)
            .field("attempt_id", &self.attempt_id)
            .field("token", &"<redacted>")
            .field("ca_certificates", &self.ca.len())
            .field("continuation_ref", &self.continuation_ref)
            .field("resource_ids", &self.resource_ids)
            .field("resource_bindings", &self.resource_bindings)
            .field("gang", &self.gang)
            .finish()
    }
}

impl Attempt {
    /// The attempt of this process: `Ok(None)` when it is not managed by
    /// Kairo (no `KAIRO_API_URL`), an error when the Kairo environment is
    /// partial, stray, malformed or names a plain-http remote daemon.
    pub fn from_env() -> Result<Option<Attempt>, EnvError> {
        Attempt::from_vars(|name| std::env::var(name).ok())
    }

    /// [`Attempt::from_env`] over any variable lookup. An empty value counts
    /// as unset.
    pub fn from_vars(var: impl Fn(&str) -> Option<String>) -> Result<Option<Attempt>, EnvError> {
        let var = |name: &str| var(name).filter(|v| !v.is_empty());
        let Some(api_url) = var("KAIRO_API_URL") else {
            if let Some(stray) = MANAGED_ONLY.iter().find(|name| var(name).is_some()) {
                return Err(EnvError(format!(
                    "{stray} is set but KAIRO_API_URL is not: an unmanaged process cannot carry Kairo attempt context"
                )));
            }
            return Ok(None);
        };
        let required =
            |name: &str| var(name).ok_or_else(|| EnvError(format!("KAIRO_API_URL is set but {name} is missing")));
        let execution_id = required("KAIRO_EXECUTION_ID")?;
        let attempt_id = required("KAIRO_ATTEMPT_ID")?;
        let token = required("KAIRO_ATTEMPT_TOKEN")?;
        let api_url = api_url.trim();
        check_token_transport(api_url).map_err(|e| EnvError(format!("KAIRO_API_URL: {e}")))?;
        let ca = match var("KAIRO_API_CA") {
            Some(b64) => parse_ca(&b64)?,
            None => Vec::new(),
        };
        let list = |name: &str| -> Vec<String> {
            var(name).map(|v| v.split(',').filter(|s| !s.is_empty()).map(str::to_string).collect()).unwrap_or_default()
        };
        let gang = match var("KAIRO_GANG_ID") {
            None => None,
            Some(id) => {
                let field = |name: &str| {
                    var(name).ok_or_else(|| EnvError(format!("KAIRO_GANG_ID is set but {name} is missing")))
                };
                fn number<T: std::str::FromStr>(name: &str, value: String) -> Result<T, EnvError> {
                    value.trim().parse().map_err(|_| EnvError(format!("{name} {value:?} is not a valid number")))
                }
                let rank = number("KAIRO_GANG_RANK", field("KAIRO_GANG_RANK")?)?;
                let size = number("KAIRO_GANG_SIZE", field("KAIRO_GANG_SIZE")?)?;
                let master_addr = field("KAIRO_GANG_MASTER_ADDR")?;
                let master_port = number("KAIRO_GANG_MASTER_PORT", field("KAIRO_GANG_MASTER_PORT")?)?;
                Some(Gang { id, rank, size, master_addr, master_port })
            }
        };
        Ok(Some(Attempt {
            api_url: api_url.trim_end_matches('/').to_string(),
            execution_id,
            attempt_id,
            token: token.trim().to_string(),
            ca,
            continuation_ref: var("KAIRO_CONTINUATION_REF"),
            resource_ids: list("KAIRO_RESOURCE_IDS"),
            resource_bindings: list("KAIRO_RESOURCE_BINDINGS"),
            gang,
        }))
    }

    /// The daemon's base URL, without a trailing slash.
    pub fn api_url(&self) -> &str {
        &self.api_url
    }

    pub fn execution_id(&self) -> &str {
        &self.execution_id
    }

    pub fn attempt_id(&self) -> &str {
        &self.attempt_id
    }

    /// The continuation the attempt resumes from (`KAIRO_CONTINUATION_REF`),
    /// uninterpreted.
    pub fn continuation_ref(&self) -> Option<&str> {
        self.continuation_ref.as_deref()
    }

    pub fn resource_ids(&self) -> &[String] {
        &self.resource_ids
    }

    pub fn resource_bindings(&self) -> &[String] {
        &self.resource_bindings
    }

    pub fn gang(&self) -> Option<&Gang> {
        self.gang.as_ref()
    }

    /// The DER certificates of `KAIRO_API_CA`: the only trust anchors for the
    /// daemon when present, otherwise the webpki roots are used.
    pub fn ca_certificates(&self) -> &[Vec<u8>] {
        &self.ca
    }

    /// A worker API client with the default 10 s request timeout.
    pub fn client(&self) -> WorkerClient {
        self.client_with_timeout(Duration::from_secs(10))
    }

    /// A worker API client whose every request is bounded by `timeout`.
    pub fn client_with_timeout(&self, timeout: Duration) -> WorkerClient {
        WorkerClient::new(&self.api_url, &self.token, &self.ca, timeout)
    }

    /// Register this process and start heartbeating in the background.
    pub fn start(&self, options: Options) -> Session {
        Session::start(self, options)
    }
}

/// May a client send a bearer token to `url`? `https://` always, `http://`
/// only to a loopback host; any other scheme, a missing host or a
/// `user:password@` in the URL is refused. The error never echoes userinfo.
pub fn check_token_transport(url: &str) -> Result<(), String> {
    let (scheme, rest) = url.split_once("://").ok_or_else(|| "not an http(s):// URL".to_string())?;
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    if authority.contains('@') {
        return Err("the URL must not carry user:password".into());
    }
    let host = if let Some(v6) = authority.strip_prefix('[') {
        v6.split_once(']').map(|(h, _)| h).ok_or_else(|| "unterminated IPv6 host".to_string())?
    } else {
        authority.rsplit_once(':').map_or(authority, |(h, _)| h)
    };
    if host.is_empty() {
        return Err("the URL has no host".into());
    }
    match scheme.to_ascii_lowercase().as_str() {
        "https" => Ok(()),
        "http" if is_loopback(host) => Ok(()),
        "http" => Err(format!(
            "plain http to {authority} which is not loopback; the attempt token goes only over https \
             (or http to localhost / a loopback IP)"
        )),
        _ => Err(format!("scheme {scheme:?} is not http(s)")),
    }
}

/// `localhost` (any case), or an IP literal in 127.0.0.0/8, `::1` or
/// `::ffff:127.x.y.z`. `host` has no IPv6 brackets. Names that merely
/// resolve to a loopback address and zoned IPv6 literals are not loopback.
pub fn is_loopback(host: &str) -> bool {
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    match host.parse::<IpAddr>() {
        Ok(IpAddr::V4(ip)) => ip.is_loopback(),
        Ok(IpAddr::V6(ip)) => is_loopback_v6(ip),
        Err(_) => false,
    }
}

fn is_loopback_v6(ip: Ipv6Addr) -> bool {
    ip.is_loopback() || ip.to_ipv4_mapped().is_some_and(|v4| v4.is_loopback())
}

/// `KAIRO_API_CA`: base64 (standard alphabet, surrounding whitespace
/// ignored) of one or more concatenated DER certificates.
fn parse_ca(b64: &str) -> Result<Vec<Vec<u8>>, EnvError> {
    use base64::Engine as _;
    let der = base64::engine::general_purpose::STANDARD
        .decode(b64.trim())
        .map_err(|e| EnvError(format!("KAIRO_API_CA is not base64: {e}")))?;
    let mut certs = Vec::new();
    let mut rest = der.as_slice();
    while !rest.is_empty() {
        let n = certs.len() + 1;
        let len = der_sequence_len(rest)
            .ok_or_else(|| EnvError(format!("KAIRO_API_CA: certificate {n} is not a complete DER SEQUENCE")))?;
        let (cert, tail) = rest.split_at(len);
        rustls::RootCertStore::empty()
            .add(rustls::pki_types::CertificateDer::from(cert))
            .map_err(|e| EnvError(format!("KAIRO_API_CA: certificate {n} is not an X.509 certificate: {e}")))?;
        certs.push(cert.to_vec());
        rest = tail;
    }
    if certs.is_empty() {
        return Err(EnvError("KAIRO_API_CA holds no certificate".into()));
    }
    Ok(certs)
}

/// The length (header included) of the DER SEQUENCE at the start of `b`, if
/// it is one and `b` holds all of it. Lengths are definite and minimal,
/// short form or long form of one to three bytes.
fn der_sequence_len(b: &[u8]) -> Option<usize> {
    if *b.first()? != 0x30 {
        return None;
    }
    let first = *b.get(1)?;
    let (header, len) = match first {
        0x00..=0x7f => (2, first as usize),
        0x81..=0x83 => {
            let k = (first & 0x7f) as usize;
            let bytes = b.get(2..2 + k)?;
            if bytes[0] == 0 {
                return None;
            }
            let len = bytes.iter().fold(0usize, |acc, &x| (acc << 8) | x as usize);
            if len < 0x80 {
                return None;
            }
            (2 + k, len)
        }
        _ => return None,
    };
    let total = header + len;
    (total <= b.len()).then_some(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    const CA: &[u8] = include_bytes!("../tests/data/ca.der");
    const CA2: &[u8] = include_bytes!("../tests/data/ca2.der");
    const SERVER_KEY: &[u8] = include_bytes!("../tests/data/server.key.der");

    fn b64(der: &[u8]) -> String {
        use base64::Engine as _;
        base64::engine::general_purpose::STANDARD.encode(der)
    }

    #[test]
    fn every_certificate_of_a_bundle_is_kept_in_order() {
        assert_eq!(parse_ca(&b64(&[CA2, CA].concat())).unwrap(), [CA2.to_vec(), CA.to_vec()]);
        assert_eq!(parse_ca(&format!("  {}\n", b64(CA))).unwrap(), [CA.to_vec()]);
    }

    #[test]
    fn a_malformed_ca_is_refused() {
        let truncated = &CA[..CA.len() - 1];
        let trailing = [CA, &[0x30]].concat();
        let not_a_certificate = [0x30, 0x03, 0x02, 0x01, 0x00];
        for ca in [
            "-----BEGIN CERTIFICATE-----".to_string(),
            "MIIBAA==".to_string(),
            b64(b"garbage, not DER at all"),
            b64(truncated),
            b64(&trailing),
            b64(&not_a_certificate),
            b64(&[CA, &not_a_certificate].concat()),
            b64(SERVER_KEY),
            " ".to_string(),
        ] {
            let err = parse_ca(&ca).err().unwrap_or_else(|| panic!("accepted {ca:?}"));
            assert!(err.message().contains("KAIRO_API_CA"), "{err}");
        }
    }

    #[test]
    fn der_sequence_lengths_are_definite_and_minimal() {
        assert_eq!(der_sequence_len(&[0x30, 0x01, 0xff, 0xee]), Some(3));
        assert_eq!(der_sequence_len(&[[0x30, 0x81, 0x80].as_slice(), &[0; 0x80]].concat()), Some(0x83));
        assert_eq!(der_sequence_len(&[[0x30, 0x82, 0x01, 0x00].as_slice(), &[0; 0x100]].concat()), Some(0x104));
        assert_eq!(
            der_sequence_len(&[[0x30, 0x83, 0x01, 0x00, 0x00].as_slice(), &[0; 0x10000]].concat()),
            Some(0x10005)
        );
        assert_eq!(der_sequence_len(&[0x30, 0x81, 0x01, 0x00]), None, "non-minimal long form");
        assert_eq!(der_sequence_len(&[0x30, 0x82, 0x00, 0x80]), None, "leading zero length byte");
        assert_eq!(der_sequence_len(&[0x30, 0x80, 0x00, 0x00]), None, "indefinite length");
        assert_eq!(der_sequence_len(&[0x30, 0x84, 0, 0, 0, 1, 0]), None, "four length bytes");
        assert_eq!(der_sequence_len(&[0x31, 0x01, 0x00]), None, "not a SEQUENCE");
        assert_eq!(der_sequence_len(&[0x30, 0x02, 0x00]), None, "truncated");
        assert_eq!(der_sequence_len(&[0x30]), None);
        assert_eq!(der_sequence_len(&[]), None);
    }

    #[test]
    fn the_token_never_shows_in_debug() {
        let attempt = Attempt::from_vars(|k| {
            match k {
                "KAIRO_API_URL" => Some("https://127.0.0.1:7474"),
                "KAIRO_EXECUTION_ID" => Some("exe"),
                "KAIRO_ATTEMPT_ID" => Some("att"),
                "KAIRO_ATTEMPT_TOKEN" => Some("tok_secret"),
                _ => None,
            }
            .map(str::to_string)
        })
        .unwrap()
        .unwrap();
        assert!(!format!("{attempt:?}").contains("tok_secret"));
        assert!(!format!("{:?}", attempt.client()).contains("tok_secret"));
    }

    #[test]
    fn a_url_error_never_echoes_userinfo() {
        let err = check_token_transport("http://user:pw@127.0.0.1:7474").unwrap_err();
        assert!(!err.contains("pw"), "{err}");
    }
}
