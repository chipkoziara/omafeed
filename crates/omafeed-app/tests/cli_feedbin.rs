//! The `sync` and `feedbin` commands against a local server that speaks Feedbin's API, so the
//! command line's own path — arguments, the account file, the JSON and text it prints — is
//! checked without the internet.
//!
//! The four things two-way sync promises are the four tests here: a change made in Feedbin
//! arrives, a local change reaches Feedbin, a field changed on both sides keeps the local value,
//! and a sync with nothing to do writes nothing.
use std::{
    collections::HashMap,
    io::{BufRead, BufReader, Read, Write},
    net::TcpListener,
    path::PathBuf,
    process::Command,
    sync::{Arc, Mutex},
};

use serde_json::{Value, json};

/// The fake account behind the server, which a test changes between syncs.
#[derive(Clone, Default)]
struct Account {
    subscriptions: Vec<Value>,
    entries: Vec<Value>,
    unread: Vec<i64>,
    starred: Vec<i64>,
    /// Bodies of the requests that changed something, in order.
    writes: Vec<String>,
}

impl Account {
    fn star(&mut self, id: i64) {
        if !self.starred.contains(&id) {
            self.starred.push(id);
        }
    }
}

/// Serve `account` until the test ends; returns its base URL.
fn serve(account: Arc<Mutex<Account>>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}/", listener.local_addr().unwrap());
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let account = account.clone();
            std::thread::spawn(move || {
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut line = String::new();
                let mut length = 0;
                let mut request = String::new();
                let mut first = true;
                loop {
                    line.clear();
                    if reader.read_line(&mut line).unwrap_or(0) == 0 {
                        return;
                    }
                    if let Some(value) = line.to_lowercase().strip_prefix("content-length:") {
                        length = value.trim().parse().unwrap_or(0);
                    }
                    request.push_str(&line);
                    if line == "\r\n" && !first {
                        break;
                    }
                    first = false;
                }
                let mut body = vec![0; length];
                let _ = reader.read_exact(&mut body);
                let body = String::from_utf8_lossy(&body).to_string();
                let mut parts = request.lines().next().unwrap_or_default().split(' ');
                let method = parts.next().unwrap_or_default().to_owned();
                let target = parts.next().unwrap_or_default().to_owned();
                let (status, reply) = answer(&account, &method, &target, &body);
                let mut stream = stream;
                let _ = write!(
                    stream,
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{reply}",
                    reply.len()
                );
            });
        }
    });
    base
}

fn answer(
    account: &Arc<Mutex<Account>>,
    method: &str,
    target: &str,
    body: &str,
) -> (&'static str, String) {
    let path = target.split('?').next().unwrap_or_default();
    let query: HashMap<String, String> = target
        .split('?')
        .nth(1)
        .unwrap_or_default()
        .split('&')
        .filter_map(|pair| {
            let (k, v) = pair.split_once('=')?;
            Some((k.to_owned(), percent(v)))
        })
        .collect();
    let request: Value = serde_json::from_str(body).unwrap_or(json!({}));
    let ids = |key: &str| -> Vec<i64> {
        request[key]
            .as_array()
            .map(|a| a.iter().filter_map(Value::as_i64).collect())
            .unwrap_or_default()
    };
    let mut account = account.lock().unwrap();
    if method != "GET" {
        account.writes.push(format!("{method} {path} {body}"));
    }
    match (method, path) {
        ("GET", "/v2/subscriptions.json") => (
            "200 OK",
            serde_json::to_string(&account.subscriptions).unwrap(),
        ),
        ("GET", "/v2/taggings.json") => ("200 OK", "[]".into()),
        ("GET", "/v2/entries.json") => {
            let since = query.get("since").cloned().unwrap_or_default();
            let entries: Vec<&Value> = account
                .entries
                .iter()
                .filter(|e| {
                    e["created_at"]
                        .as_str()
                        .is_some_and(|created| created > since.as_str())
                })
                .collect();
            ("200 OK", serde_json::to_string(&entries).unwrap())
        }
        ("GET", "/v2/unread_entries.json") => {
            ("200 OK", serde_json::to_string(&account.unread).unwrap())
        }
        ("GET", "/v2/starred_entries.json") => {
            ("200 OK", serde_json::to_string(&account.starred).unwrap())
        }
        ("POST", "/v2/unread_entries.json") => {
            let sent = ids("unread_entries");
            for id in &sent {
                if !account.unread.contains(id) {
                    account.unread.push(*id);
                }
            }
            ("200 OK", serde_json::to_string(&sent).unwrap())
        }
        ("DELETE", "/v2/unread_entries.json") => {
            let sent = ids("unread_entries");
            account.unread.retain(|id| !sent.contains(id));
            ("200 OK", serde_json::to_string(&sent).unwrap())
        }
        ("POST", "/v2/starred_entries.json") => {
            let sent = ids("starred_entries");
            for id in &sent {
                account.star(*id);
            }
            ("200 OK", serde_json::to_string(&sent).unwrap())
        }
        ("DELETE", "/v2/starred_entries.json") => {
            let sent = ids("starred_entries");
            account.starred.retain(|id| !sent.contains(id));
            ("200 OK", serde_json::to_string(&sent).unwrap())
        }
        _ => ("404 Not Found", "no such route".into()),
    }
}

/// `%XX` escapes, so a `since` and a date compare as they should.
fn percent(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let Ok(byte) = u8::from_str_radix(&value[i + 1..i + 3], 16)
        {
            out.push(byte);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).to_string()
}

/// A library with a Feedbin account configured, and the fake account it syncs with.
struct Cli {
    home: tempfile::TempDir,
    account: Arc<Mutex<Account>>,
    base: String,
}

impl Cli {
    fn new() -> Self {
        let account = Arc::new(Mutex::new(Account {
            subscriptions: vec![json!({
                "id": 11,
                "feed_id": 21,
                "title": "Example",
                "feed_url": "https://example.org/feed",
                "site_url": "https://example.org/",
                "created_at": "2026-01-01T00:00:00.000000Z"
            })],
            entries: vec![json!({
                "id": 1001,
                "feed_id": 21,
                "title": "First",
                "url": "https://example.org/one",
                "author": "Someone",
                "content": "<p>One</p>",
                "published": "2026-01-02T03:04:05.000000Z",
                "created_at": "2026-01-02T03:04:05.000000Z"
            })],
            unread: vec![1001],
            ..Default::default()
        }));
        let base = serve(account.clone());
        let home = tempfile::tempdir().unwrap();
        // OMAFEED_HOME puts the config directory under it, where the account file lives.
        std::fs::create_dir_all(home.path().join("config")).unwrap();
        std::fs::write(
            home.path().join("config/feedbin.toml"),
            "email = \"me@example.com\"\npassword = \"secret\"\n",
        )
        .unwrap();
        Self {
            home,
            account,
            base,
        }
    }

    /// Run a command that must succeed, returning its stdout.
    fn ok(&self, args: &[&str]) -> String {
        let (code, stdout, stderr) = self.output(args);
        assert_eq!(code, 0, "{args:?}: {stderr}");
        stdout
    }

    /// Run a command and read the JSON it printed.
    fn json(&self, args: &[&str]) -> Value {
        serde_json::from_str(self.ok(args).trim()).unwrap_or_else(|e| panic!("{args:?}: {e}"))
    }

    fn output(&self, args: &[&str]) -> (i32, String, String) {
        let out = Command::new(env!("CARGO_BIN_EXE_omafeed"))
            .args(args)
            .env("OMAFEED_HOME", self.home.path())
            .env("OMAFEED_FEEDBIN_API", &self.base)
            .env("TZ", "UTC")
            .env("NO_PROXY", "*")
            .env("no_proxy", "*")
            .output()
            .unwrap();
        (
            out.status.code().unwrap(),
            String::from_utf8(out.stdout).unwrap(),
            String::from_utf8(out.stderr).unwrap(),
        )
    }

    fn data(&self) -> PathBuf {
        self.home.path().join("data")
    }
}

#[test]
fn a_change_made_in_feedbin_arrives_and_a_local_star_reaches_it() {
    let cli = Cli::new();
    let first = cli.json(&["sync", "--json"]);
    assert_eq!(first["added"], 2, "one feed and one article: {first}");
    assert_eq!(first["pushed"], 0, "nothing to push yet: {first}");

    // Feedbin starred it; the next sync brings that here.
    cli.account.lock().unwrap().star(1001);
    cli.ok(&["sync"]);
    let starred = cli.json(&["articles", "--scope", "starred", "--json"]);
    assert_eq!(starred["count"], 1, "the star came down: {starred}");

    // A star here reaches Feedbin.
    cli.ok(&["unstar", "1"]);
    cli.ok(&["sync"]);
    assert!(
        cli.account.lock().unwrap().starred.is_empty(),
        "the unstar was pushed"
    );
    cli.ok(&["star", "1"]);
    let pushed = cli.json(&["sync", "--json"]);
    assert_eq!(pushed["pushed"], 1, "one request sent: {pushed}");
    assert_eq!(cli.account.lock().unwrap().starred, vec![1001]);
    assert!(
        cli.account
            .lock()
            .unwrap()
            .writes
            .iter()
            .any(|w| w.starts_with("POST /v2/starred_entries.json")),
    );
}

#[test]
fn a_field_changed_on_both_sides_keeps_the_local_value() {
    let cli = Cli::new();
    cli.ok(&["sync"]);
    // Feedbin unread it while this library marked it read: both sides moved.
    cli.account.lock().unwrap().unread = vec![1001];
    cli.ok(&["read", "1"]);
    cli.ok(&["sync"]);

    let account = cli.account.lock().unwrap();
    assert!(account.unread.is_empty(), "the local read reached Feedbin");
    drop(account);
    let article = cli.json(&["article", "1", "--json"]);
    assert!(article["read"].as_bool().unwrap(), "still read here");
}

#[test]
fn a_sync_with_nothing_to_do_writes_nothing() {
    let cli = Cli::new();
    cli.ok(&["sync"]);
    let settled = cli.account.lock().unwrap().writes.len();
    let idle = cli.json(&["sync", "--json"]);
    assert_eq!(idle["writes"], 0, "no rows written: {idle}");
    assert_eq!(idle["pushed"], 0, "nothing sent: {idle}");
    assert_eq!(
        cli.account.lock().unwrap().writes.len(),
        settled,
        "no write requests"
    );
}

#[test]
fn status_says_what_the_account_holds_and_a_logout_forgets_it() {
    let cli = Cli::new();
    cli.ok(&["sync"]);
    let status = cli.json(&["feedbin", "status", "--json"]);
    assert_eq!(status["email"], "me@example.com");
    assert_eq!(status["linked"], 1, "{status}");
    assert_eq!(status["pending"], 0, "{status}");
    assert!(
        status["cursor"].is_string(),
        "the cursor was kept: {status}"
    );

    cli.ok(&["feedbin", "logout"]);
    assert!(!cli.home.path().join("config/feedbin.toml").exists());
    // Without an account, refresh fetches feeds itself and sync says so.
    let (code, _, stderr) = cli.output(&["sync"]);
    assert_eq!(code, 1);
    assert!(stderr.contains("No Feedbin account"), "{stderr}");
    assert!(cli.data().join("omafeed.db").exists(), "the library stays");
}
