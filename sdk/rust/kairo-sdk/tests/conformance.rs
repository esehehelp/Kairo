//! Replays the cross-language contract in sdk/conformance.

use std::collections::HashMap;

use kairo_sdk::{
    check_token_transport, classify_status, classify_transport, identity_from_filetime, identity_from_stat,
    is_loopback, Attempt, ErrorKind, Transport,
};
use serde_json::Value;

const CA1: &[u8] = include_bytes!("data/ca.der");
const CA2: &[u8] = include_bytes!("data/ca2.der");

pub fn fixture(name: &str) -> Value {
    let path = format!("{}/../../conformance/{name}", env!("CARGO_MANIFEST_DIR"));
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}"));
    serde_json::from_str(&text).unwrap()
}

fn cases<'a>(v: &'a Value, key: &str) -> &'a Vec<Value> {
    v[key].as_array().unwrap_or_else(|| panic!("no {key} array"))
}

fn b64(der: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(der)
}

#[test]
fn loopback() {
    for case in cases(&fixture("loopback.json"), "cases") {
        let host = case["host"].as_str().unwrap();
        assert_eq!(is_loopback(host), case["loopback"].as_bool().unwrap(), "{host:?}");
    }
}

#[test]
fn token_transport() {
    for case in cases(&fixture("token_transport.json"), "cases") {
        let url = case["url"].as_str().unwrap();
        let result = check_token_transport(url);
        assert_eq!(result.is_ok(), case["allowed"].as_bool().unwrap(), "{url:?}: {result:?}");
    }
}

#[test]
fn worker_env() {
    for case in cases(&fixture("worker_env.json"), "cases") {
        let name = case["name"].as_str().unwrap();
        let env: HashMap<String, String> = case["env"]
            .as_object()
            .unwrap()
            .iter()
            .map(|(k, v)| {
                let v = match v.as_str().unwrap() {
                    "<CA1>" => b64(CA1),
                    "<CA1+CA2>" => b64(&[CA1, CA2].concat()),
                    other => other.to_string(),
                };
                (k.clone(), v)
            })
            .collect();
        let result = Attempt::from_vars(|k| env.get(k).cloned());
        let expect = &case["expect"];
        if let Some(error) = expect["error"].as_str() {
            let err = result.err().unwrap_or_else(|| panic!("{name}: accepted"));
            assert!(err.message().contains(error), "{name}: {err} does not name {error}");
            continue;
        }
        let attempt = result.unwrap_or_else(|e| panic!("{name}: {e}"));
        if !expect["managed"].as_bool().unwrap() {
            assert!(attempt.is_none(), "{name}");
            continue;
        }
        let a = attempt.unwrap_or_else(|| panic!("{name}: unmanaged"));
        let strings = |v: &Value| -> Vec<String> {
            v.as_array().unwrap().iter().map(|s| s.as_str().unwrap().to_string()).collect()
        };
        assert_eq!(a.api_url(), expect["api_url"].as_str().unwrap(), "{name}");
        assert_eq!(a.execution_id(), expect["execution_id"].as_str().unwrap(), "{name}");
        assert_eq!(a.attempt_id(), expect["attempt_id"].as_str().unwrap(), "{name}");
        assert_eq!(a.ca_certificates().len() as u64, expect["ca_certificates"].as_u64().unwrap(), "{name}");
        assert_eq!(a.resource_ids(), strings(&expect["resource_ids"]), "{name}");
        assert_eq!(a.resource_bindings(), strings(&expect["resource_bindings"]), "{name}");
        assert_eq!(a.continuation_ref(), expect["continuation_ref"].as_str(), "{name}");
        match &expect["gang"] {
            Value::Null => assert!(a.gang().is_none(), "{name}"),
            g => {
                let gang = a.gang().unwrap_or_else(|| panic!("{name}: no gang"));
                assert_eq!(gang.id, g["id"].as_str().unwrap(), "{name}");
                assert_eq!(gang.rank as u64, g["rank"].as_u64().unwrap(), "{name}");
                assert_eq!(gang.size as u64, g["size"].as_u64().unwrap(), "{name}");
                assert_eq!(gang.master_addr, g["master_addr"].as_str().unwrap(), "{name}");
                assert_eq!(gang.master_port as u64, g["master_port"].as_u64().unwrap(), "{name}");
            }
        }
    }
}

#[test]
fn identity() {
    let v = fixture("identity.json");
    for case in cases(&v, "linux") {
        let pid = case["pid"].as_u64().unwrap() as u32;
        let got = identity_from_stat(pid, case["stat"].as_str().unwrap()).ok();
        assert_eq!(got.as_deref(), case["expect"].as_str(), "{case}");
    }
    for case in cases(&v, "windows") {
        let pid = case["pid"].as_u64().unwrap() as u32;
        let got = identity_from_filetime(pid, case["creation_filetime"].as_u64().unwrap());
        assert_eq!(got, case["expect"].as_str().unwrap(), "{case}");
    }
}

pub fn kind(class: &str) -> ErrorKind {
    match class {
        "transient" => ErrorKind::Transient,
        "permanent" => ErrorKind::Permanent,
        "retired" => ErrorKind::Retired,
        "fenced" => ErrorKind::Fenced,
        "gone" => ErrorKind::Gone,
        "retry_later" => ErrorKind::RetryLater,
        other => panic!("unknown class {other}"),
    }
}

#[test]
fn classify_worker() {
    let mut n = 0;
    for case in cases(&fixture("classify.json"), "cases").iter().filter(|c| c["role"] == "worker") {
        let got = match case["transport"].as_str() {
            Some(t) => classify_transport(match t {
                "refused" => Transport::Refused,
                "reset" => Transport::Reset,
                "timeout" => Transport::Timeout,
                "dns" => Transport::Dns,
                "tls_verify" => Transport::TlsVerify,
                "tls_handshake" => Transport::TlsHandshake,
                other => panic!("unknown transport {other}"),
            }),
            None => classify_status(case["status"].as_u64().unwrap() as u16, case["code"].as_str().unwrap()),
        };
        assert_eq!(got, kind(case["class"].as_str().unwrap()), "{case}");
        n += 1;
    }
    assert!(n > 0);
}
