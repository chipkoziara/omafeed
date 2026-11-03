//! Feedbin sync bookkeeping: what the remote side calls each row, which read and star changes
//! it has not been told about, and what it still has to be told.
//!
//! Feedbin owns the subscriptions and the entries; this library owns read and star changes that
//! have not been pushed yet. [`Store::feedbin_pull`], [`Store::feedbin_entries`] and
//! [`Store::feedbin_apply_state`] take what Feedbin says, and [`Store::feedbin_pending_state`]
//! and [`Store::feedbin_deletions`] hand back what it is owed.
//!
//! Every write here is conditional on a value actually differing, so a sync with nothing to do
//! writes no rows.
use super::Store;
use crate::model::{ArticleId, FeedId, FolderId, Library};
use anyhow::Result;
use rusqlite::{OptionalExtension, params};
use std::collections::HashMap;

/// A tag Feedbin has on a feed: a flat name, whose ID deletes that one tagging.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Tag {
    /// The tagging's ID.
    pub id: i64,
    /// Its name, which for a feed in `Work/Blogs` is exactly that.
    pub name: String,
}

/// A subscription as Feedbin describes it.
#[derive(Clone, Debug)]
pub struct Subscription {
    /// The subscription's ID, which renames and removes it.
    pub id: i64,
    /// The feed's ID, which its entries carry.
    pub feed_id: i64,
    /// The title Feedbin has for it.
    pub title: String,
    /// The feed's address.
    pub feed_url: String,
    /// The website the feed names, if any.
    pub site_url: String,
    /// Its tag, if it is in a folder.
    pub tag: Option<Tag>,
}

/// A local feed as a sync sees it: its own state, and what Feedbin last said about it.
#[derive(Clone, Debug)]
pub struct LocalFeed {
    /// Its row ID.
    pub id: FeedId,
    /// Its address, which matches a subscription's.
    pub url: String,
    /// The name the user sees.
    pub title: String,
    /// Its folder as a `/`-joined path, or `None` at the top level.
    pub folder: Option<String>,
    /// Feedbin's ID for the feed.
    pub remote_id: Option<i64>,
    /// Feedbin's ID for the subscription.
    pub subscription: Option<i64>,
    /// Feedbin's ID for the tag it carries.
    pub tagging: Option<i64>,
    /// The title Feedbin had when this row last synced.
    pub remote_title: Option<String>,
    /// The tag Feedbin had when this row last synced.
    pub remote_folder: Option<String>,
}

/// An entry to store, its HTML already sanitized and its text already extracted.
#[derive(Clone, Debug)]
pub struct RemoteEntry {
    /// Feedbin's ID for it.
    pub id: i64,
    /// Feedbin's ID for its feed.
    pub feed_id: i64,
    /// What makes it the same article on the next pull: its link, else its Feedbin ID.
    pub identity: String,
    /// Plain text.
    pub title: String,
    /// The original on the web, or empty.
    pub url: String,
    /// Authors, comma-separated, or empty.
    pub author: String,
    /// When Feedbin says it was published, in Unix seconds.
    pub published: i64,
    /// Sanitized HTML.
    pub html: String,
    /// Plain text of `html`.
    pub text: String,
}

/// What a pull changed, in rows.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Pulled {
    /// Rows added.
    pub added: usize,
    /// Rows whose stored copy changed.
    pub updated: usize,
    /// Feeds removed because Feedbin no longer has them.
    pub removed: usize,
    /// Subscriptions that cannot be stored, such as one whose address is not HTTP(S).
    pub skipped: usize,
    /// Whether an entry's text changed, which is what makes search's word list stale.
    pub text_changed: bool,
}

impl Pulled {
    /// Every row this pull wrote.
    pub fn writes(&self) -> usize {
        self.added + self.updated + self.removed
    }
}

/// A read or star change Feedbin has not been told about yet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Pending {
    /// The article's row ID.
    pub article: ArticleId,
    /// Feedbin's ID for it.
    pub entry: i64,
    /// The read value to send, if that is what changed.
    pub read: Option<bool>,
    /// The star value to send, if that is what changed.
    pub starred: Option<bool>,
}

/// A row that is gone locally and still exists remotely.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Deletion {
    /// A subscription, by its ID.
    Subscription(i64),
}

/// What `feedbin status` reports about the account and what is owed to it.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct Status {
    /// Subscriptions Feedbin knows.
    pub linked: usize,
    /// Local feeds it does not know yet.
    pub unlinked: usize,
    /// Read and star changes waiting to be pushed.
    pub pending: usize,
    /// Deletions waiting to be pushed.
    pub deletions: usize,
    /// Where the last pull stopped reading entries, as Feedbin writes dates.
    pub cursor: Option<String>,
}

/// A folder path from a folder, such as `Work/Blogs`; `None` at the top level.
pub fn folder_path(library: &Library, id: Option<FolderId>) -> Option<String> {
    let parts: Vec<&str> = library
        .folder_path(id)
        .iter()
        .map(|f| f.name.as_str())
        .collect();
    (!parts.is_empty()).then(|| parts.join("/"))
}

impl Store {
    /// Every local feed, with the paths of their folders and what Feedbin last said.
    pub fn feedbin_feeds(&self) -> Result<Vec<LocalFeed>> {
        self.snapshot(|s| s.read_feedbin_feeds())
    }

    fn read_feedbin_feeds(&self) -> Result<Vec<LocalFeed>> {
        let library = self.library()?;
        let rows = self
            .conn
            .prepare(
                "SELECT id, url, title, folder, feedbin_id, feedbin_subscription, feedbin_title,
                        feedbin_folder, feedbin_tagging
                 FROM feeds ORDER BY id",
            )?
            .query_map([], |r| {
                Ok((
                    LocalFeed {
                        id: FeedId(r.get(0)?),
                        url: r.get(1)?,
                        title: r.get(2)?,
                        folder: None,
                        remote_id: r.get(4)?,
                        subscription: r.get(5)?,
                        tagging: r.get(8)?,
                        remote_title: r.get(6)?,
                        remote_folder: r.get(7)?,
                    },
                    r.get::<_, Option<FolderId>>(3)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows
            .into_iter()
            .map(|(mut feed, folder)| {
                feed.folder = folder_path(&library, folder);
                feed
            })
            .collect())
    }

    /// Record Feedbin's IDs and last known title and tag for a feed, leaving the local title
    /// and folder as they are.
    pub fn feedbin_adopt(&self, id: FeedId, sub: &Subscription) -> Result<bool> {
        let changed = self.conn.execute(
            "UPDATE feeds SET feedbin_id = ?1, feedbin_subscription = ?2, feedbin_title = ?3,
                    feedbin_folder = ?4, feedbin_tagging = ?5
             WHERE id = ?6
               AND (feedbin_id IS NOT ?1 OR feedbin_subscription IS NOT ?2
                    OR feedbin_title IS NOT ?3 OR feedbin_folder IS NOT ?4
                    OR feedbin_tagging IS NOT ?5)",
            params![
                sub.feed_id,
                sub.id,
                sub.title,
                sub.tag.as_ref().map(|t| &t.name),
                sub.tag.as_ref().map(|t| t.id),
                id
            ],
        )?;
        Ok(changed > 0)
    }

    /// Record that Feedbin has the feed's new title.
    pub fn feedbin_title_pushed(&self, id: FeedId, title: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE feeds SET feedbin_title = ?1 WHERE id = ?2 AND feedbin_title IS NOT ?1",
            params![title, id],
        )?;
        Ok(())
    }

    /// Record that Feedbin has the feed's new tag, or none.
    pub fn feedbin_tag_pushed(&self, id: FeedId, tag: Option<&Tag>) -> Result<()> {
        self.conn.execute(
            "UPDATE feeds SET feedbin_folder = ?1, feedbin_tagging = ?2
             WHERE id = ?3 AND (feedbin_folder IS NOT ?1 OR feedbin_tagging IS NOT ?2)",
            params![tag.map(|t| &t.name), tag.map(|t| t.id), id],
        )?;
        Ok(())
    }

    /// The folder at `path`, such as `Work/Blogs`, creating the folders on the way; `None` for
    /// a path with no names in it.
    pub fn feedbin_folder(&self, path: &str) -> Result<Option<FolderId>> {
        let mut parent = None;
        for name in path.split('/').filter(|n| !n.trim().is_empty()) {
            let name = Self::name(name)?;
            let existing: Option<FolderId> = self
                .conn
                .query_row(
                    "SELECT id FROM folders WHERE parent IS ?1 AND name = ?2",
                    params![parent, name],
                    |r| r.get(0),
                )
                .optional()?;
            parent = Some(match existing {
                Some(id) => id,
                None => {
                    self.conn.execute(
                        "INSERT INTO folders(name, parent) VALUES(?1, ?2)",
                        params![name, parent],
                    )?;
                    FolderId(self.conn.last_insert_rowid())
                }
            });
        }
        Ok(parent)
    }

    /// Take Feedbin's subscriptions as the account's: add the ones the library lacks, keep the
    /// local title and folder where the user changed them since the last sync, and remove the
    /// feeds Feedbin no longer has.
    pub fn feedbin_pull(&self, subs: &[Subscription]) -> Result<Pulled> {
        self.immediate(|s| s.pull_now(subs))
    }

    fn pull_now(&self, subs: &[Subscription]) -> Result<Pulled> {
        let mut pulled = Pulled::default();
        let mut local = self.read_feedbin_feeds()?;
        let mut seen: Vec<FeedId> = Vec::new();
        for sub in subs {
            let index = local
                .iter()
                .position(|f| f.subscription == Some(sub.id))
                .or_else(|| local.iter().position(|f| f.url == sub.feed_url));
            let Some(index) = index else {
                let Ok(url) = crate::util::validate_url(&sub.feed_url) else {
                    pulled.skipped += 1;
                    continue;
                };
                let folder = match &sub.tag {
                    Some(tag) => self.feedbin_folder(&tag.name)?,
                    None => None,
                };
                let site = crate::util::validate_url(&sub.site_url).unwrap_or_default();
                self.conn.execute(
                    "INSERT INTO feeds(title, url, folder, site_url, feedbin_id,
                                        feedbin_subscription, feedbin_title, feedbin_folder,
                                        feedbin_tagging, revision)
                     VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                    params![
                        stored_title(&sub.title),
                        url,
                        folder,
                        site,
                        sub.feed_id,
                        sub.id,
                        sub.title,
                        sub.tag.as_ref().map(|t| &t.name),
                        sub.tag.as_ref().map(|t| t.id),
                        Self::next_revision(&self.conn)?
                    ],
                )?;
                pulled.added += 1;
                continue;
            };
            let feed = local.remove(index);
            seen.push(feed.id);
            if self.feedbin_adopt(feed.id, sub)? {
                pulled.updated += 1;
            }
            // A local title or folder that differs from what Feedbin had is the user's, and the
            // push phase sends it; otherwise take Feedbin's copy.
            let title = if feed.title != feed.remote_title.clone().unwrap_or_default()
                && !feed.title.trim().is_empty()
            {
                feed.title.clone()
            } else {
                sub.title.clone()
            };
            pulled.updated += usize::from(
                self.conn.execute(
                    "UPDATE feeds SET title = ?1 WHERE id = ?2 AND title <> ?1",
                    params![title, feed.id],
                )? > 0,
            );
            if feed.folder == feed.remote_folder {
                let folder = match &sub.tag {
                    Some(tag) => self.feedbin_folder(&tag.name)?,
                    None => None,
                };
                let changed = match folder {
                    Some(id) => self.conn.execute(
                        "UPDATE feeds SET folder = ?1 WHERE id = ?2 AND folder IS NOT ?1",
                        params![id, feed.id],
                    )?,
                    None => self.conn.execute(
                        "UPDATE feeds SET folder = NULL WHERE id = ?1 AND folder IS NOT NULL",
                        [feed.id],
                    )?,
                };
                pulled.updated += usize::from(changed > 0);
            }
        }
        let remote: Vec<i64> = subs.iter().map(|s| s.id).collect();
        for feed in &local {
            let Some(subscription) = feed.subscription else {
                continue;
            };
            if remote.contains(&subscription) {
                continue;
            }
            self.conn
                .execute("DELETE FROM feeds WHERE id = ?1", [feed.id])?;
            pulled.removed += 1;
        }
        if pulled.removed > 0 {
            self.articles_changed();
        }
        Ok(pulled)
    }

    /// Take Feedbin's entries: add the ones the library lacks, match the ones it has by
    /// Feedbin's ID or by link, and change a row only where its stored copy differs.
    pub fn feedbin_entries(&self, entries: &[RemoteEntry], now: i64) -> Result<Pulled> {
        self.immediate(|s| s.entries_now(entries, now))
    }

    fn entries_now(&self, entries: &[RemoteEntry], now: i64) -> Result<Pulled> {
        let mut pulled = Pulled::default();
        let mut feeds: HashMap<i64, FeedId> = HashMap::new();
        for row in self
            .conn
            .prepare("SELECT feedbin_id, id FROM feeds WHERE feedbin_id IS NOT NULL")?
            .query_map([], |r| Ok((r.get::<_, i64>(0)?, FeedId(r.get(1)?))))?
        {
            let (remote, id) = row?;
            feeds.insert(remote, id);
        }
        for entry in entries {
            let Some(feed) = feeds.get(&entry.feed_id).copied() else {
                continue;
            };
            let title = stored_title(&entry.title);
            let existing: Option<i64> = self
                .conn
                .query_row(
                    "SELECT id FROM articles WHERE feedbin_id = ?1",
                    [entry.id],
                    |r| r.get(0),
                )
                .optional()?
                .or(self
                    .conn
                    .query_row(
                        "SELECT id FROM articles WHERE feed_id = ?1 AND identity = ?2",
                        params![feed, entry.identity],
                        |r| r.get(0),
                    )
                    .optional()?);
            let Some(article) = existing else {
                self.conn.execute(
                    "INSERT INTO articles(feed_id, identity, title, url, author, published,
                                          first_seen, html, text, feedbin_id)
                     VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                    params![
                        feed,
                        entry.identity,
                        title,
                        entry.url,
                        entry.author,
                        entry.published,
                        now,
                        entry.html,
                        entry.text,
                        entry.id
                    ],
                )?;
                pulled.added += 1;
                pulled.text_changed = true;
                continue;
            };
            let stored: (String, String, String, i64, String, Option<i64>, FeedId) =
                self.conn.query_row(
                    "SELECT title, url, author, published, html, feedbin_id, feed_id
                     FROM articles WHERE id = ?1",
                    [article],
                    |r| {
                        Ok((
                            r.get(0)?,
                            r.get(1)?,
                            r.get(2)?,
                            r.get(3)?,
                            r.get(4)?,
                            r.get(5)?,
                            r.get(6)?,
                        ))
                    },
                )?;
            let same = stored.0 == title
                && stored.1 == entry.url
                && stored.2 == entry.author
                && stored.3 == entry.published
                && stored.4 == entry.html
                && stored.5 == Some(entry.id)
                && stored.6 == feed;
            if same {
                continue;
            }
            self.conn.execute(
                "UPDATE articles SET feed_id = ?1, title = ?2, url = ?3, author = ?4,
                        published = ?5, html = ?6, text = ?7, feedbin_id = ?8
                 WHERE id = ?9",
                params![
                    feed,
                    title,
                    entry.url,
                    entry.author,
                    entry.published,
                    entry.html,
                    entry.text,
                    entry.id,
                    article
                ],
            )?;
            pulled.updated += 1;
            pulled.text_changed |= stored.4 != entry.html;
        }
        if pulled.text_changed {
            self.articles_changed();
        }
        Ok(pulled)
    }

    /// Read and star changes Feedbin has not been told about, at most `limit` of them.
    pub fn feedbin_pending_state(&self, limit: usize) -> Result<Vec<Pending>> {
        let rows = self
            .conn
            .prepare(
                "SELECT a.id, a.feedbin_id, s.read, s.starred, s.read_pending, s.starred_pending
                 FROM article_state s JOIN articles a ON a.id = s.article_id
                 WHERE a.feedbin_id IS NOT NULL AND (s.read_pending = 1 OR s.starred_pending = 1)
                 ORDER BY a.id LIMIT ?1",
            )?
            .query_map([limit as i64], |r| {
                Ok((
                    Pending {
                        article: ArticleId(r.get(0)?),
                        entry: r.get(1)?,
                        read: None,
                        starred: None,
                    },
                    r.get::<_, bool>(2)?,
                    r.get::<_, bool>(3)?,
                    r.get::<_, bool>(4)?,
                    r.get::<_, bool>(5)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows
            .into_iter()
            .map(|(mut p, read, starred, read_pending, starred_pending)| {
                p.read = read_pending.then_some(read);
                p.starred = starred_pending.then_some(starred);
                p
            })
            .collect())
    }

    /// Forget the pending mark of a field Feedbin now has. A change made while the push was in
    /// flight has a different value, so its mark stays for the next sync.
    pub fn feedbin_pushed(&self, read: bool, value: bool, articles: &[ArticleId]) -> Result<()> {
        let (field, pending) = if read {
            ("read", "read_pending")
        } else {
            ("starred", "starred_pending")
        };
        self.conn.execute(
            &format!(
                "UPDATE article_state SET {pending} = 0
                 WHERE {field} = ?1 AND article_id IN (SELECT value FROM json_each(?2))"
            ),
            params![value, serde_json::to_string(articles)?],
        )?;
        Ok(())
    }

    /// Take Feedbin's read and star state for every article it knows, leaving alone the fields
    /// with a local change that has not been pushed.
    pub fn feedbin_apply_state(&self, unread: &[i64], starred: &[i64]) -> Result<()> {
        self.immediate(|s| {
            for (ids, field, pending, present) in [
                (unread, "read", "read_pending", false),
                (starred, "starred", "starred_pending", true),
            ] {
                let ids = serde_json::to_string(ids)?;
                let absent = if present { 0 } else { 1 };
                // In Feedbin's set, so the value is `present`; the subquery is uncorrelated, so
                // SQLite reads the array once.
                s.conn.execute(
                    &format!(
                        "UPDATE article_state SET {field} = {present}
                         WHERE {pending} = 0 AND {field} <> {present}
                           AND article_id IN (SELECT id FROM articles
                                              WHERE feedbin_id IN (SELECT value FROM json_each(?1)))"
                    ),
                    [&ids],
                )?;
                // Not in it, so the value is the other one. An article Feedbin has never heard
                // of keeps what it has.
                s.conn.execute(
                    &format!(
                        "UPDATE article_state SET {field} = {absent}
                         WHERE {pending} = 0 AND {field} <> {absent}
                           AND article_id IN (SELECT id FROM articles
                                              WHERE feedbin_id IS NOT NULL
                                                AND feedbin_id NOT IN (SELECT value FROM json_each(?1)))"
                    ),
                    [&ids],
                )?;
            }
            Ok(())
        })
    }

    /// Record a feed whose subscription Feedbin still has, so the next sync removes it there.
    pub fn feedbin_queue_deletion(&self, id: FeedId) -> Result<()> {
        self.conn.execute(
            "INSERT OR IGNORE INTO feedbin_deleted(kind, remote_id, name)
             SELECT 'subscription', feedbin_subscription, NULL FROM feeds
             WHERE id = ?1 AND feedbin_subscription IS NOT NULL",
            [id],
        )?;
        Ok(())
    }

    /// Subscriptions Feedbin still has to be told about.
    pub fn feedbin_deletions(&self) -> Result<Vec<Deletion>> {
        Ok(self
            .conn
            .prepare(
                "SELECT remote_id FROM feedbin_deleted WHERE kind = 'subscription'
                 ORDER BY remote_id",
            )?
            .query_map([], |r| Ok(Deletion::Subscription(r.get(0)?)))?
            .collect::<Result<Vec<_>, _>>()?)
    }

    /// Forget a deletion Feedbin has done.
    pub fn feedbin_deletion_done(&self, deletion: Deletion) -> Result<()> {
        match deletion {
            Deletion::Subscription(id) => self.conn.execute(
                "DELETE FROM feedbin_deleted WHERE kind = 'subscription' AND remote_id = ?1",
                [id],
            )?,
        };
        Ok(())
    }

    /// Where the last pull stopped reading entries, for Feedbin's `since`.
    pub fn feedbin_cursor(&self) -> Result<Option<String>> {
        Ok(self
            .conn
            .query_row("SELECT cursor FROM feedbin_meta WHERE id = 1", [], |r| {
                r.get(0)
            })
            .optional()?
            .flatten())
    }

    /// Remember where this pull stopped reading entries.
    pub fn set_feedbin_cursor(&self, cursor: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE feedbin_meta SET cursor = ?1 WHERE id = 1 AND cursor IS NOT ?1",
            [cursor],
        )?;
        Ok(())
    }

    /// What the account and the queue of unsent changes look like.
    pub fn feedbin_status(&self) -> Result<Status> {
        let (linked, unlinked) = self.conn.query_row(
            "SELECT COALESCE(sum(feedbin_subscription IS NOT NULL), 0),
                    COALESCE(sum(feedbin_subscription IS NULL), 0)
             FROM feeds",
            [],
            |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)),
        )?;
        let pending = self.conn.query_row(
            "SELECT count(*) FROM article_state s JOIN articles a ON a.id = s.article_id
             WHERE a.feedbin_id IS NOT NULL AND (s.read_pending = 1 OR s.starred_pending = 1)",
            [],
            |r| r.get::<_, i64>(0),
        )?;
        let deletions = self
            .conn
            .query_row("SELECT count(*) FROM feedbin_deleted", [], |r| {
                r.get::<_, i64>(0)
            })?;
        Ok(Status {
            linked: usize::try_from(linked).unwrap_or_default(),
            unlinked: usize::try_from(unlinked).unwrap_or_default(),
            pending: usize::try_from(pending).unwrap_or_default(),
            deletions: usize::try_from(deletions).unwrap_or_default(),
            cursor: self.feedbin_cursor()?,
        })
    }
}

/// A title as it is stored; Feedbin allows entries and subscriptions without one.
fn stored_title(title: &str) -> String {
    let title = title.trim();
    if title.is_empty() {
        "Untitled article".into()
    } else {
        title.into()
    }
}
