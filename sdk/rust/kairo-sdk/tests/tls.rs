//! The worker client over TLS against an in-process rustls server.
//!
//! A throwaway test PKI (EC P-256, valid until 2126): two CAs and a server
//! certificate for 127.0.0.1 / localhost signed by the first.

use std::io::{Read, Write};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use kairo_sdk::{Attempt, Error, ErrorKind, Transport};

const CA: &[u8] = include_bytes!("data/ca.der");
const CA2: &[u8] = include_bytes!("data/ca2.der");
const SERVER: &[u8] = include_bytes!("data/server.der");
const SERVER_KEY: &[u8] = include_bytes!("data/server.key.der");

type Seen = Option<(String, Option<rustls::ProtocolVersion>)>;

/// A one-request HTTPS server on 127.0.0.1; the thread yields the request
/// and the protocol version, or `None` when the handshake fails.
fn serve_once(versions: &[&'static rustls::SupportedProtocolVersion]) -> (String, JoinHandle<Seen>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("https://127.0.0.1:{}/", listener.local_addr().unwrap().port());
    let config = rustls::ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_protocol_versions(versions)
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![rustls::pki_types::CertificateDer::from(SERVER.to_vec())],
            rustls::pki_types::PrivateKeyDer::Pkcs8(SERVER_KEY.to_vec().into()),
        )
        .unwrap();
    let server = std::thread::spawn(move || {
        let (tcp, _) = listener.accept().ok()?;
        tcp.set_read_timeout(Some(Duration::from_secs(10))).ok()?;
        let mut tls = rustls::StreamOwned::new(rustls::ServerConnection::new(Arc::new(config)).ok()?, tcp);
        let mut request = Vec::new();
        let mut buf = [0u8; 4096];
        loop {
            let n = tls.read(&mut buf).ok()?;
            if n == 0 {
                return None;
            }
            request.extend_from_slice(&buf[..n]);
            let text = String::from_utf8_lossy(&request);
            if let Some(end) = text.find("\r\n\r\n") {
                let length = text[..end]
                    .lines()
                    .filter_map(|l| l.split_once(':'))
                    .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
                    .and_then(|(_, v)| v.trim().parse::<usize>().ok())
                    .unwrap_or(0);
                if request.len() >= end + 4 + length {
                    break;
                }
            }
        }
        let version = tls.conn.protocol_version();
        tls.write_all(b"HTTP/1.1 202 Accepted\r\nContent-Length: 11\r\nConnection: close\r\n\r\n{\"ok\":true}").ok()?;
        tls.conn.send_close_notify();
        tls.flush().ok()?;
        Some((String::from_utf8_lossy(&request).into_owned(), version))
    });
    (url, server)
}

fn b64(der: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(der)
}

fn heartbeat(url: &str, ca: Option<&[u8]>) -> Result<(), Error> {
    let ca = ca.map(b64);
    let attempt = Attempt::from_vars(|k| match k {
        "KAIRO_API_URL" => Some(url.to_string()),
        "KAIRO_EXECUTION_ID" => Some("exe_1".into()),
        "KAIRO_ATTEMPT_ID" => Some("att_1".into()),
        "KAIRO_ATTEMPT_TOKEN" => Some("tok_secret".into()),
        "KAIRO_API_CA" => ca.clone(),
        _ => None,
    })
    .unwrap()
    .unwrap();
    attempt.client_with_timeout(Duration::from_secs(5)).heartbeat(&serde_json::json!({"unit": "rows", "current": 1}))
}

#[test]
fn a_tls13_server_signed_by_the_passed_ca_is_trusted() {
    let (url, server) = serve_once(&[&rustls::version::TLS13]);
    heartbeat(&url, Some(CA)).unwrap();
    let (request, version) = server.join().unwrap().expect("the server saw the request");
    assert!(request.starts_with("POST /api/worker/heartbeat HTTP/1.1\r\n"), "{request}");
    assert!(request.to_ascii_lowercase().contains("\r\nauthorization: bearer tok_secret\r\n"), "{request}");
    assert!(request.ends_with(r#"{"progress":{"current":1,"unit":"rows"}}"#), "{request}");
    assert_eq!(version, Some(rustls::ProtocolVersion::TLSv1_3));
}

#[test]
fn every_ca_of_a_rotation_bundle_is_trusted() {
    let (url, server) = serve_once(&[&rustls::version::TLS13]);
    heartbeat(&url, Some(&[CA2, CA].concat())).unwrap();
    assert!(server.join().unwrap().is_some());
}

#[test]
fn a_server_signed_by_another_ca_is_refused_for_good() {
    let (url, server) = serve_once(&[&rustls::version::TLS13]);
    let err = heartbeat(&url, Some(CA2)).unwrap_err();
    assert_eq!((err.kind, err.transport), (ErrorKind::Permanent, Some(Transport::TlsVerify)), "{err:?}");
    assert!(server.join().unwrap().is_none());
}

#[test]
fn the_webpki_roots_do_not_trust_a_private_ca() {
    let (url, server) = serve_once(&[&rustls::version::TLS13]);
    let err = heartbeat(&url, None).unwrap_err();
    assert_eq!((err.kind, err.transport), (ErrorKind::Permanent, Some(Transport::TlsVerify)), "{err:?}");
    assert!(server.join().unwrap().is_none());
}

#[test]
fn a_tls12_only_server_is_refused_for_good() {
    let (url, server) = serve_once(&[&rustls::version::TLS12]);
    let err = heartbeat(&url, Some(CA)).unwrap_err();
    assert_eq!((err.kind, err.transport), (ErrorKind::Permanent, Some(Transport::TlsHandshake)), "{err:?}");
    assert!(server.join().unwrap().is_none());
}

#[test]
fn a_refused_connection_is_transient() {
    let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let err = heartbeat(&format!("https://127.0.0.1:{port}"), Some(CA)).unwrap_err();
    assert_eq!(err.kind, ErrorKind::Transient, "{err:?}");
}
