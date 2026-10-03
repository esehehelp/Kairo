//! A tiny in-process Kairo daemon over plain http on 127.0.0.1 (allowed by
//! the loopback rule): scripted (serves exchanges in order and records any
//! deviation or extra request) or permissive (answers everything).

#![allow(dead_code)]

use std::collections::{HashMap, VecDeque};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

pub const TOKEN: &str = "kairo_worker_t";

#[derive(Clone, Debug)]
pub struct Exchange {
    pub method: String,
    pub path: String,
    /// Must be contained in the request body (objects compared key by key).
    pub body: Option<Value>,
    pub status: u16,
    pub response: Value,
}

impl Exchange {
    pub fn new(method: &str, path: &str, body: Option<Value>, status: u16, response: Value) -> Exchange {
        Exchange { method: method.into(), path: path.into(), body, status, response }
    }

    pub fn register() -> Exchange {
        let body = json!({"rank": 0, "pid": std::process::id()});
        Exchange::new("POST", "/api/worker/processes", Some(body), 202, json!({"ok": true}))
    }

    pub fn heartbeat(progress: Option<Value>) -> Exchange {
        let body = progress.map(|p| json!({ "progress": p }));
        Exchange::new("POST", "/api/worker/heartbeat", body, 202, json!({"ok": true}))
    }
}

#[derive(Clone, Debug)]
pub struct Recorded {
    pub method: String,
    pub path: String,
    pub authorization: Option<String>,
    pub body: Option<Value>,
}

struct Shared {
    script: Option<Mutex<VecDeque<Exchange>>>,
    requests: Mutex<Vec<Recorded>>,
    failures: Mutex<Vec<String>>,
    stop: AtomicBool,
}

pub struct FakeDaemon {
    pub url: String,
    addr: std::net::SocketAddr,
    shared: Arc<Shared>,
    thread: Option<JoinHandle<()>>,
}

impl FakeDaemon {
    pub fn scripted(exchanges: Vec<Exchange>) -> FakeDaemon {
        FakeDaemon::start(Some(exchanges))
    }

    pub fn permissive() -> FakeDaemon {
        FakeDaemon::start(None)
    }

    fn start(script: Option<Vec<Exchange>>) -> FakeDaemon {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let shared = Arc::new(Shared {
            script: script.map(|s| Mutex::new(s.into())),
            requests: Mutex::new(Vec::new()),
            failures: Mutex::new(Vec::new()),
            stop: AtomicBool::new(false),
        });
        let server = Arc::clone(&shared);
        let thread = std::thread::spawn(move || {
            for stream in listener.incoming() {
                if server.stop.load(Ordering::SeqCst) {
                    return;
                }
                let Ok(stream) = stream else { continue };
                let server = Arc::clone(&server);
                std::thread::spawn(move || serve(&server, stream));
            }
        });
        FakeDaemon { url: format!("http://127.0.0.1:{}", addr.port()), addr, shared, thread: Some(thread) }
    }

    /// The attempt variables of a managed worker of this daemon.
    pub fn vars(&self) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<&str, String> = [
            ("KAIRO_API_URL", self.url.clone()),
            ("KAIRO_EXECUTION_ID", "exe_1".to_string()),
            ("KAIRO_ATTEMPT_ID", "att_1".to_string()),
            ("KAIRO_ATTEMPT_TOKEN", TOKEN.to_string()),
        ]
        .into_iter()
        .collect();
        move |k| map.get(k).cloned()
    }

    pub fn requests(&self) -> Vec<Recorded> {
        self.shared.requests.lock().unwrap().clone()
    }

    pub fn served(&self) -> usize {
        self.shared.requests.lock().unwrap().len()
    }

    /// Wait until at least `n` requests were served.
    pub fn wait_served(&self, n: usize) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while self.served() < n {
            assert!(Instant::now() < deadline, "only {} of {n} requests arrived: {:?}", self.served(), self.requests());
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// Stop serving; a scripted daemon asserts that every exchange happened
    /// as scripted and nothing more was sent.
    pub fn finish(mut self) -> Vec<Recorded> {
        // Late requests (a stray heartbeat) would show up within this pause.
        std::thread::sleep(Duration::from_millis(100));
        self.stop();
        let failures = self.shared.failures.lock().unwrap().clone();
        assert!(failures.is_empty(), "{failures:#?}");
        if let Some(script) = &self.shared.script {
            let left = script.lock().unwrap();
            assert!(left.is_empty(), "exchanges never requested: {left:#?}");
        }
        self.requests()
    }

    fn stop(&mut self) {
        if let Some(t) = self.thread.take() {
            self.shared.stop.store(true, Ordering::SeqCst);
            // Wake the accept loop (only while it listens: connecting to a
            // closed port takes seconds on Windows).
            let _ = TcpStream::connect(self.addr);
            let _ = t.join();
        }
    }
}

impl Drop for FakeDaemon {
    fn drop(&mut self) {
        self.stop();
    }
}

/// `expected` is contained in `actual`: objects key by key (extra keys
/// allowed), everything else equal.
pub fn contains(actual: &Value, expected: &Value) -> bool {
    match (actual, expected) {
        (Value::Object(a), Value::Object(e)) => e.iter().all(|(k, v)| a.get(k).is_some_and(|av| contains(av, v))),
        _ => actual == expected,
    }
}

fn serve(shared: &Shared, stream: TcpStream) {
    stream.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut line = String::new();
    if reader.read_line(&mut line).unwrap_or(0) == 0 {
        return; // the shutdown poke
    }
    let mut parts = line.split_whitespace();
    let (method, path) = (parts.next().unwrap_or("").to_string(), parts.next().unwrap_or("").to_string());
    let mut headers = HashMap::new();
    loop {
        let mut h = String::new();
        if reader.read_line(&mut h).unwrap_or(0) == 0 || h == "\r\n" {
            break;
        }
        if let Some((k, v)) = h.split_once(':') {
            headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
        }
    }
    let length = headers.get("content-length").and_then(|v| v.parse().ok()).unwrap_or(0);
    let mut body = vec![0; length];
    reader.read_exact(&mut body).unwrap();
    let body = (!body.is_empty()).then(|| serde_json::from_slice::<Value>(&body).unwrap());
    let recorded = Recorded { method, path, authorization: headers.get("authorization").cloned(), body: body.clone() };
    let (status, response) = respond(shared, &recorded);
    shared.requests.lock().unwrap().push(recorded);
    let text = response.to_string();
    let reason = if status < 300 { "OK" } else { "Error" };
    let mut stream = stream;
    let _ = write!(
        stream,
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{text}",
        text.len()
    );
    let _ = stream.flush();
}

fn respond(shared: &Shared, r: &Recorded) -> (u16, Value) {
    let fail = |why: String| {
        shared.failures.lock().unwrap().push(why);
        (500, json!({"error": "unexpected request", "code": "internal"}))
    };
    if r.authorization.as_deref() != Some(&format!("Bearer {TOKEN}")) {
        return fail(format!("{} {}: Authorization {:?}", r.method, r.path, r.authorization));
    }
    let Some(script) = &shared.script else {
        return match (r.method.as_str(), r.path.as_str()) {
            ("GET", "/api/worker/commands") => (200, json!({"commands": []})),
            _ => (202, json!({"ok": true})),
        };
    };
    let Some(next) = script.lock().unwrap().pop_front() else {
        return fail(format!("extra request {} {} {:?}", r.method, r.path, r.body));
    };
    if next.method != r.method || next.path != r.path {
        return fail(format!("expected {} {}, got {} {}", next.method, next.path, r.method, r.path));
    }
    if let Some(expected) = &next.body {
        if !r.body.as_ref().is_some_and(|actual| contains(actual, expected)) {
            return fail(format!("{} {}: body {:?} does not contain {expected}", r.method, r.path, r.body));
        }
    }
    (next.status, next.response)
}
