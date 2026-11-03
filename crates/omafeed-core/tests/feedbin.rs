//! Two-way sync against a local server that speaks Feedbin's API, so every case here — a
//! change pushed, a change pulled, a conflict, an idle sync — runs without the internet.
use omafeed_core::{
    Db,
    feedbin::{Api, Credentials, sync},
    model::{Query, Scope, StatePatch},
};
use serde_json::json;
use std::sync::{Arc, Mutex};

mod fake {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// A subscription as the fake account holds it.
    #[derive(Clone)]
    pub struct Sub {
        pub id: i64,
        pub feed_id: i64,
        pub title: String,
        pub feed_url: String,
        pub site_url: String,
    }
    /// A tagging as the fake account holds it.
    #[derive(Clone)]
    pub struct Tagging {
        pub id: i64,
        pub feed_id: i64,
        pub name: String,
    }
    /// An entry as the fake account holds it.
    #[derive(Clone)]
    pub struct Entry {
        pub id: i64,
        pub feed_id: i64,
        pub title: String,
        pub url: String,
        pub content: String,
        pub created_at: String,
    }

    /// The account behind the server, which a test may change between syncs.
    #[derive(Clone, Default)]
    pub struct Account {
        pub subscriptions: Vec<Sub>,
        pub taggings: Vec<Tagging>,
        pub entries: Vec<Entry>,
        pub unread: Vec<i64>,
        pub starred: Vec<i64>,
        pub next_id: i64,
        /// `METHOD /path` of every request, in order.
        pub requests: Vec<String>,
        /// Bodies of every write, in order.
        pub bodies: Vec<String>,
    }

    impl Account {
        pub fn requests(&self) -> Vec<String> {
            self.requests.clone()
        }
        pub fn writes(&self) -> Vec<String> {
            self.requests
                .iter()
                .zip(&self.bodies)
                .filter(|(r, _)| !r.starts_with("GET "))
                .map(|(r, b)| format!("{r} {b}"))
                .collect()
        }
    }

    /// Serve `account` until the test ends; returns its base URL.
    pub async fn serve(account: Arc<Mutex<Account>>) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}/", listener.local_addr().unwrap());
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let account = account.clone();
                tokio::spawn(async move {
                    let mut raw = Vec::new();
                    let mut chunk = [0; 4096];
                    // Headers first, then as many body bytes as they announce.
                    loop {
                        let n = socket.read(&mut chunk).await.unwrap_or(0);
                        if n == 0 {
                            return;
                        }
                        raw.extend_from_slice(&chunk[..n]);
                        let Some(end) = raw.windows(4).position(|w| w == b"\r\n\r\n") else {
                            continue;
                        };
                        let head = String::from_utf8_lossy(&raw[..end]).to_string();
                        let length: usize = head
                            .lines()
                            .find_map(|l| {
                                l.to_lowercase()
                                    .strip_prefix("content-length:")
                                    .and_then(|v| v.trim().parse().ok())
                            })
                            .unwrap_or(0);
                        if raw.len() >= end + 4 + length {
                            let body = String::from_utf8_lossy(&raw[end + 4..end + 4 + length])
                                .to_string();
                            let (status, reply) = answer(&account, &head, &body);
                            let response = format!(
                                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{reply}",
                                reply.len()
                            );
                            let _ = socket.write_all(response.as_bytes()).await;
                            let _ = socket.flush().await;
                            return;
                        }
                    }
                });
            }
        });
        base
    }

    /// `%XX` escapes as a request sends them, so a `since` and a date compare as they should.
    fn decode(value: &str) -> String {
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

    fn answer(account: &Arc<Mutex<Account>>, head: &str, body: &str) -> (&'static str, String) {
        let mut line = head.lines().next().unwrap_or_default().split(' ');
        let method = line.next().unwrap_or_default().to_owned();
        let target = line.next().unwrap_or_default().to_owned();
        let path = target.split('?').next().unwrap_or_default().to_owned();
        let mut account = account.lock().unwrap();
        account.requests.push(format!("{method} {path}"));
        account.bodies.push(body.to_owned());
        let request: serde_json::Value = serde_json::from_str(body).unwrap_or(json!({}));
        let ids = |key: &str| -> Vec<i64> {
            request[key]
                .as_array()
                .map(|a| a.iter().filter_map(|v| v.as_i64()).collect())
                .unwrap_or_default()
        };
        match (method.as_str(), path.as_str()) {
            ("GET", "/v2/authentication.json") => ("200 OK", "{}".into()),
            ("GET", "/v2/subscriptions.json") => (
                "200 OK",
                serde_json::to_string(
                    &account
                        .subscriptions
                        .iter()
                        .map(|s| {
                            json!({
                                "id": s.id, "feed_id": s.feed_id, "title": s.title,
                                "feed_url": s.feed_url, "site_url": s.site_url,
                                "created_at": "2026-01-01T00:00:00.000000Z"
                            })
                        })
                        .collect::<Vec<_>>(),
                )
                .unwrap(),
            ),
            ("POST", "/v2/subscriptions.json") => {
                let url = request["feed_url"].as_str().unwrap_or_default().to_owned();
                if let Some(existing) = account.subscriptions.iter().find(|s| s.feed_url == url) {
                    return (
                        "302 Found",
                        json!({
                            "id": existing.id, "feed_id": existing.feed_id,
                            "title": existing.title, "feed_url": existing.feed_url,
                            "site_url": existing.site_url
                        })
                        .to_string(),
                    );
                }
                if !url.starts_with("http") {
                    return ("404 Not Found", "not found".into());
                }
                account.next_id += 1;
                let sub = Sub {
                    id: 900 + account.next_id,
                    feed_id: 500 + account.next_id,
                    title: "Remote title".into(),
                    feed_url: url,
                    site_url: "https://example.org/".into(),
                };
                account.subscriptions.push(sub.clone());
                (
                    "201 Created",
                    json!({
                        "id": sub.id, "feed_id": sub.feed_id, "title": sub.title,
                        "feed_url": sub.feed_url, "site_url": sub.site_url
                    })
                    .to_string(),
                )
            }
            ("PATCH", p) if p.starts_with("/v2/subscriptions/") => {
                let id: i64 = p
                    .trim_start_matches("/v2/subscriptions/")
                    .trim_end_matches(".json")
                    .parse()
                    .unwrap_or_default();
                let title = request["title"].as_str().unwrap_or_default().to_owned();
                for s in &mut account.subscriptions {
                    if s.id == id {
                        s.title = title.clone();
                    }
                }
                ("200 OK", "{}".into())
            }
            ("DELETE", p) if p.starts_with("/v2/subscriptions/") => {
                let id: i64 = p
                    .trim_start_matches("/v2/subscriptions/")
                    .trim_end_matches(".json")
                    .parse()
                    .unwrap_or_default();
                account.subscriptions.retain(|s| s.id != id);
                ("204 No Content", String::new())
            }
            ("GET", "/v2/taggings.json") => (
                "200 OK",
                serde_json::to_string(
                    &account
                        .taggings
                        .iter()
                        .map(|t| json!({ "id": t.id, "feed_id": t.feed_id, "name": t.name }))
                        .collect::<Vec<_>>(),
                )
                .unwrap(),
            ),
            ("POST", "/v2/taggings.json") => {
                let feed_id = request["feed_id"].as_i64().unwrap_or_default();
                let name = request["name"].as_str().unwrap_or_default().to_owned();
                let existing = account
                    .taggings
                    .iter()
                    .find(|t| t.feed_id == feed_id && t.name == name)
                    .cloned();
                let tagging = existing.unwrap_or_else(|| {
                    account.next_id += 1;
                    let t = Tagging {
                        id: 700 + account.next_id,
                        feed_id,
                        name,
                    };
                    account.taggings.push(t.clone());
                    t
                });
                (
                    "201 Created",
                    json!({ "id": tagging.id, "feed_id": tagging.feed_id, "name": tagging.name })
                        .to_string(),
                )
            }
            ("DELETE", p) if p.starts_with("/v2/taggings/") => {
                let id: i64 = p
                    .trim_start_matches("/v2/taggings/")
                    .trim_end_matches(".json")
                    .parse()
                    .unwrap_or_default();
                account.taggings.retain(|t| t.id != id);
                ("204 No Content", String::new())
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
                    if !account.starred.contains(id) {
                        account.starred.push(*id);
                    }
                }
                ("200 OK", serde_json::to_string(&sent).unwrap())
            }
            ("DELETE", "/v2/starred_entries.json") => {
                let sent = ids("starred_entries");
                account.starred.retain(|id| !sent.contains(id));
                ("200 OK", serde_json::to_string(&sent).unwrap())
            }
            ("GET", "/v2/entries.json") => {
                let query: std::collections::HashMap<_, _> = target
                    .split('?')
                    .nth(1)
                    .unwrap_or_default()
                    .split('&')
                    .filter_map(|pair| {
                        let (k, v) = pair.split_once('=')?;
                        Some((k.to_owned(), decode(v)))
                    })
                    .collect();
                let page: usize = query.get("page").and_then(|p| p.parse().ok()).unwrap_or(1);
                let per_page: usize = query
                    .get("per_page")
                    .and_then(|p| p.parse().ok())
                    .unwrap_or(100);
                let mut entries: Vec<_> = account
                    .entries
                    .iter()
                    .filter(|e| query.get("since").is_none_or(|since| &e.created_at > since))
                    .cloned()
                    .collect();
                entries.sort_by(|a, b| b.created_at.cmp(&a.created_at));
                let page_entries: Vec<_> = entries
                    .into_iter()
                    .skip((page - 1) * per_page)
                    .take(per_page)
                    .map(|e| {
                        json!({
                            "id": e.id, "feed_id": e.feed_id, "title": e.title,
                            "url": e.url, "author": "Someone", "content": e.content,
                            "published": e.created_at, "created_at": e.created_at
                        })
                    })
                    .collect();
                ("200 OK", serde_json::to_string(&page_entries).unwrap())
            }
            _ => ("404 Not Found", "no such route".into()),
        }
    }
}

fn credentials() -> Credentials {
    Credentials {
        email: "me@example.com".into(),
        password: "secret".into(),
    }
}

fn account() -> Arc<Mutex<fake::Account>> {
    Arc::new(Mutex::new(fake::Account::default()))
}

fn all() -> Query {
    Query {
        scope: Scope::All,
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_first_sync_brings_subscriptions_folders_entries_and_state_down() {
    let account = account();
    {
        let mut a = account.lock().unwrap();
        a.subscriptions.push(fake::Sub {
            id: 11,
            feed_id: 21,
            title: "Example".into(),
            feed_url: "https://example.org/feed".into(),
            site_url: "https://example.org/".into(),
        });
        a.taggings.push(fake::Tagging {
            id: 31,
            feed_id: 21,
            name: "Work/Blogs".into(),
        });
        a.entries.push(fake::Entry {
            id: 1001,
            feed_id: 21,
            title: "First".into(),
            url: "https://example.org/one".into(),
            content: "<p>One</p>".into(),
            created_at: "2026-01-02T03:04:05.000000Z".into(),
        });
        a.entries.push(fake::Entry {
            id: 1002,
            feed_id: 21,
            title: "Second".into(),
            url: "https://example.org/two".into(),
            content: "<p>Two</p>".into(),
            created_at: "2026-01-02T03:04:06.000000Z".into(),
        });
        a.unread = vec![1002];
        a.starred = vec![1001];
    }
    let base = fake::serve(account.clone()).await;
    let db = Db::open(":memory:").unwrap();
    db.set_feedbin(true);
    let api = Api::new(&base, credentials()).unwrap();

    let outcome = sync(&db, &api).await.unwrap();
    assert_eq!(outcome.added, 3, "one feed and two articles");

    let lib = db.call(|s| s.library()).await.unwrap();
    assert_eq!(lib.feeds.len(), 1);
    assert_eq!(lib.feeds[0].title, "Example");
    assert_eq!(lib.folders.len(), 2, "Work and Blogs");
    let path: Vec<String> = lib
        .folder_path(lib.feeds[0].folder)
        .iter()
        .map(|f| f.name.clone())
        .collect();
    assert_eq!(path, ["Work", "Blogs"]);

    let articles = db.call(|s| s.articles(&all())).await.unwrap();
    assert_eq!(articles.len(), 2);
    let first = articles.iter().find(|a| a.title == "First").unwrap();
    let second = articles.iter().find(|a| a.title == "Second").unwrap();
    assert!(first.starred && first.read, "starred and read");
    assert!(!second.starred && !second.read, "unread, not starred");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_local_star_is_pushed_and_a_remote_read_change_is_pulled() {
    let account = account();
    {
        let mut a = account.lock().unwrap();
        a.subscriptions.push(fake::Sub {
            id: 11,
            feed_id: 21,
            title: "Example".into(),
            feed_url: "https://example.org/feed".into(),
            site_url: String::new(),
        });
        a.entries.push(fake::Entry {
            id: 1001,
            feed_id: 21,
            title: "First".into(),
            url: "https://example.org/one".into(),
            content: "<p>One</p>".into(),
            created_at: "2026-01-02T03:04:05.000000Z".into(),
        });
        a.unread = vec![1001];
    }
    let base = fake::serve(account.clone()).await;
    let db = Db::open(":memory:").unwrap();
    db.set_feedbin(true);
    let api = Api::new(&base, credentials()).unwrap();
    sync(&db, &api).await.unwrap();

    // A star here reaches Feedbin.
    let article = db.call(|s| s.articles(&all())).await.unwrap()[0].id;
    db.call(move |s| s.set_state(article, StatePatch::starred(true)))
        .await
        .unwrap();
    sync(&db, &api).await.unwrap();
    assert_eq!(account.lock().unwrap().starred, vec![1001]);
    assert!(
        account
            .lock()
            .unwrap()
            .writes()
            .iter()
            .any(|r| r.contains("POST /v2/starred_entries.json")),
        "the star was pushed"
    );

    // Reading it on another device reaches this library.
    account.lock().unwrap().unread = vec![];
    sync(&db, &api).await.unwrap();
    let read = db.call(move |s| s.article(article)).await.unwrap();
    assert!(read.read, "Feedbin marked it read");
    assert!(read.starred, "the star survives the pull");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_field_changed_on_both_sides_keeps_the_local_value() {
    let account = account();
    {
        let mut a = account.lock().unwrap();
        a.subscriptions.push(fake::Sub {
            id: 11,
            feed_id: 21,
            title: "Example".into(),
            feed_url: "https://example.org/feed".into(),
            site_url: String::new(),
        });
        a.entries.push(fake::Entry {
            id: 1001,
            feed_id: 21,
            title: "First".into(),
            url: "https://example.org/one".into(),
            content: "<p>One</p>".into(),
            created_at: "2026-01-02T03:04:05.000000Z".into(),
        });
        a.unread = vec![1001];
    }
    let base = fake::serve(account.clone()).await;
    let db = Db::open(":memory:").unwrap();
    db.set_feedbin(true);
    let api = Api::new(&base, credentials()).unwrap();
    sync(&db, &api).await.unwrap();
    let article = db.call(|s| s.articles(&all())).await.unwrap()[0].id;

    // This library marks it read while Feedbin still has it unread: both sides changed since
    // the last sync, and the local value wins because the push goes first.
    db.call(move |s| s.set_state(article, StatePatch::read(true)))
        .await
        .unwrap();
    sync(&db, &api).await.unwrap();
    assert!(account.lock().unwrap().unread.is_empty(), "pushed as read");
    assert!(
        db.call(move |s| s.article(article)).await.unwrap().read,
        "still read here"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_pending_change_is_never_overwritten_by_a_pull() {
    let account = account();
    {
        let mut a = account.lock().unwrap();
        a.subscriptions.push(fake::Sub {
            id: 11,
            feed_id: 21,
            title: "Example".into(),
            feed_url: "https://example.org/feed".into(),
            site_url: String::new(),
        });
        a.entries.push(fake::Entry {
            id: 1001,
            feed_id: 21,
            title: "First".into(),
            url: "https://example.org/one".into(),
            content: "<p>One</p>".into(),
            created_at: "2026-01-02T03:04:05.000000Z".into(),
        });
        // Feedbin says it is read.
        a.unread = vec![];
    }
    let base = fake::serve(account.clone()).await;
    let db = Db::open(":memory:").unwrap();
    db.set_feedbin(true);
    let api = Api::new(&base, credentials()).unwrap();
    sync(&db, &api).await.unwrap();
    let article = db.call(|s| s.articles(&all())).await.unwrap()[0].id;
    assert!(db.call(move |s| s.article(article)).await.unwrap().read);

    // Marked unread here and not yet pushed: the pull leaves it alone.
    db.call(move |s| s.set_state(article, StatePatch::read(false)))
        .await
        .unwrap();
    db.call(move |s| s.feedbin_apply_state(&[], &[]))
        .await
        .unwrap();
    assert!(!db.call(move |s| s.article(article)).await.unwrap().read);
    // Once pushed, the same pull applies.
    db.call(move |s| s.feedbin_pushed(true, false, &[article]))
        .await
        .unwrap();
    db.call(move |s| s.feedbin_apply_state(&[], &[]))
        .await
        .unwrap();
    assert!(db.call(move |s| s.article(article)).await.unwrap().read);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_sync_with_nothing_to_do_writes_nothing() {
    let account = account();
    {
        let mut a = account.lock().unwrap();
        a.subscriptions.push(fake::Sub {
            id: 11,
            feed_id: 21,
            title: "Example".into(),
            feed_url: "https://example.org/feed".into(),
            site_url: String::new(),
        });
        a.taggings.push(fake::Tagging {
            id: 31,
            feed_id: 21,
            name: "Work".into(),
        });
        a.entries.push(fake::Entry {
            id: 1001,
            feed_id: 21,
            title: "First".into(),
            url: "https://example.org/one".into(),
            content: "<p>One</p>".into(),
            created_at: "2026-01-02T03:04:05.000000Z".into(),
        });
        a.unread = vec![1001];
    }
    let base = fake::serve(account.clone()).await;
    let db = Db::open(":memory:").unwrap();
    db.set_feedbin(true);
    let api = Api::new(&base, credentials()).unwrap();
    sync(&db, &api).await.unwrap();

    let settled = account.lock().unwrap().requests().len();
    let outcome = sync(&db, &api).await.unwrap();
    assert_eq!(outcome.writes, 0, "nothing changed, so nothing was written");
    assert_eq!(outcome.pushed, 0, "nothing was sent");
    assert_eq!(outcome.added + outcome.updated + outcome.removed, 0);
    let (before, after) = {
        let a = account.lock().unwrap();
        let all = a.writes();
        let before = a
            .requests
            .iter()
            .take(settled)
            .filter(|r| !r.starts_with("GET "))
            .count();
        (before, all)
    };
    assert_eq!(after.len(), before, "no write requests: {after:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_feed_deleted_here_is_removed_from_feedbin_once() {
    let account = account();
    {
        let mut a = account.lock().unwrap();
        a.subscriptions.push(fake::Sub {
            id: 11,
            feed_id: 21,
            title: "Example".into(),
            feed_url: "https://example.org/feed".into(),
            site_url: String::new(),
        });
        a.entries.push(fake::Entry {
            id: 1001,
            feed_id: 21,
            title: "First".into(),
            url: "https://example.org/one".into(),
            content: "<p>One</p>".into(),
            created_at: "2026-01-02T03:04:05.000000Z".into(),
        });
        a.unread = vec![1001];
    }
    let base = fake::serve(account.clone()).await;
    let db = Db::open(":memory:").unwrap();
    db.set_feedbin(true);
    let api = Api::new(&base, credentials()).unwrap();
    sync(&db, &api).await.unwrap();

    let feed = db.call(|s| s.library()).await.unwrap().feeds[0].id;
    db.call(move |s| s.delete_feed(feed)).await.unwrap();
    sync(&db, &api).await.unwrap();
    assert!(account.lock().unwrap().subscriptions.is_empty(), "removed");
    let removals = account
        .lock()
        .unwrap()
        .writes()
        .into_iter()
        .filter(|r| r.starts_with("DELETE /v2/subscriptions/"))
        .count();
    assert_eq!(removals, 1);
    // The tombstone is gone, so a third sync sends nothing again.
    sync(&db, &api).await.unwrap();
    let removals = account
        .lock()
        .unwrap()
        .writes()
        .into_iter()
        .filter(|r| r.starts_with("DELETE /v2/subscriptions/"))
        .count();
    assert_eq!(removals, 1, "removed once, not twice");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_local_subscription_is_created_remotely_and_its_folder_becomes_a_tag() {
    let account = account();
    let base = fake::serve(account.clone()).await;
    let db = Db::open(":memory:").unwrap();
    db.set_feedbin(true);
    let api = Api::new(&base, credentials()).unwrap();
    db.call(|s| {
        let folder = s.add_folder("Read later", None)?;
        s.add_feed("Local", "https://example.org/local", Some(folder))?;
        Ok(())
    })
    .await
    .unwrap();

    sync(&db, &api).await.unwrap();
    let remote = account.lock().unwrap();
    assert_eq!(remote.subscriptions.len(), 1, "created on Feedbin");
    assert_eq!(
        remote.subscriptions[0].feed_url,
        "https://example.org/local"
    );
    assert_eq!(remote.taggings.len(), 1);
    assert_eq!(remote.taggings[0].name, "Read later");
    assert_eq!(remote.taggings[0].feed_id, remote.subscriptions[0].feed_id);
}
