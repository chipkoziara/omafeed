//! One sync: tell Feedbin what it is owed, then take what it has.
//!
//! The order is what makes the conflict rule work. Pushing first means a read or star change
//! made here reaches Feedbin before the pull reads Feedbin's state back, so a pull can only
//! undo a local value that was never pushed. And a field with an unsent change keeps its local
//! value when the pull arrives, because [`Store::feedbin_apply_state`](crate::Store::feedbin_apply_state) skips it.
use super::api::{self, Api};
use crate::{
    Db, content,
    store::feedbin::{Deletion, RemoteEntry, Subscription},
};
use anyhow::Result;

/// How many pages of entries the first sync reads; later syncs read only what is new.
const FIRST_PAGES: u32 = 5;
/// Entries per page.
const PER_PAGE: u32 = 100;
/// The most IDs Feedbin accepts in one read or star request.
const BATCH: usize = 1000;

/// What one sync did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Outcome {
    /// Subscriptions added to the library, counting the ones the pull removed as negative
    /// nowhere: `removed` is its own count.
    pub added: usize,
    /// Rows whose stored copy changed.
    pub updated: usize,
    /// Feeds removed because Feedbin no longer has them.
    pub removed: usize,
    /// Changes sent to Feedbin.
    pub pushed: usize,
    /// Subscriptions Feedbin has that this library cannot store, such as a non-HTTP address.
    pub skipped: usize,
    /// Entries Feedbin did not accept a read or star change for, usually a feed it no longer
    /// serves.
    pub rejected: usize,
    /// Rows this sync wrote. A sync with nothing to do writes none.
    pub writes: u64,
}

impl Outcome {
    fn absorb(&mut self, pulled: crate::store::feedbin::Pulled) {
        self.added += pulled.added;
        self.updated += pulled.updated;
        self.removed += pulled.removed;
        self.skipped += pulled.skipped;
    }
}

/// Push this library's changes, then pull the account's.
pub async fn sync(db: &Db, api: &Api) -> Result<Outcome> {
    let before = db.call(|s| Ok(s.write_count())).await?;
    let mut outcome = Outcome::default();
    let remote = api.subscriptions().await?;
    push_subscriptions(db, api, &remote, &mut outcome).await?;
    push_deletions(db, api, &mut outcome).await?;
    // A subscription this sync created is not in the list read before it.
    let remote = if outcome.pushed > 0 {
        api.subscriptions().await?
    } else {
        remote
    };
    // Feeds first: an entry is stored only for a feed that is already here.
    let pulled = db.call(move |s| s.feedbin_pull(&remote)).await?;
    outcome.absorb(pulled);
    push_state(db, api, &mut outcome).await?;
    pull_entries(db, api, &mut outcome).await?;
    pull_state(db, api).await?;
    outcome.writes = db.call(|s| Ok(s.write_count())).await? - before;
    Ok(outcome)
}

/// Create the subscriptions Feedbin lacks and send it the titles and tags it differs on.
async fn push_subscriptions(
    db: &Db,
    api: &Api,
    remote: &[Subscription],
    outcome: &mut Outcome,
) -> Result<()> {
    for feed in db.call(|s| s.feedbin_feeds()).await? {
        let known = feed
            .subscription
            .and_then(|id| remote.iter().find(|s| s.id == id).cloned());
        let sub = match known {
            Some(sub) => sub,
            None => {
                let url = feed.url.clone();
                let created = api.subscribe(&url).await?;
                let id = feed.id;
                let adopting = created.clone();
                db.call(move |s| {
                    s.feedbin_adopt(id, &adopting)?;
                    Ok(())
                })
                .await?;
                outcome.pushed += 1;
                created
            }
        };
        // A title that differs from the one Feedbin last confirmed is this library's to send.
        let unpushed = feed.title.trim() != feed.remote_title.clone().unwrap_or_default().trim()
            && !feed.title.trim().is_empty();
        if unpushed {
            api.rename(sub.id, &feed.title).await?;
            let (id, title) = (feed.id, feed.title.clone());
            db.call(move |s| s.feedbin_title_pushed(id, &title)).await?;
            outcome.pushed += 1;
        }
        // The folder, which is one flat tag on Feedbin.
        if feed.folder != feed.remote_folder {
            let tag = match &feed.folder {
                Some(path) if !path.trim().is_empty() => Some(api.tag(sub.feed_id, path).await?),
                _ => None,
            };
            if let Some(old) = feed.tagging {
                api.untag(old).await?;
            }
            let id = feed.id;
            db.call(move |s| s.feedbin_tag_pushed(id, tag.as_ref()))
                .await?;
            outcome.pushed += 1;
        }
    }
    Ok(())
}

/// Remove the subscriptions deleted here since the last sync.
async fn push_deletions(db: &Db, api: &Api, outcome: &mut Outcome) -> Result<()> {
    for deletion in db.call(|s| s.feedbin_deletions()).await? {
        let Deletion::Subscription(id) = deletion;
        api.unsubscribe(id).await?;
        db.call(move |s| s.feedbin_deletion_done(deletion)).await?;
        outcome.pushed += 1;
    }
    Ok(())
}

/// Send Feedbin the read and star changes it has not been told about.
async fn push_state(db: &Db, api: &Api, outcome: &mut Outcome) -> Result<()> {
    loop {
        let pending = db.call(|s| s.feedbin_pending_state(BATCH)).await?;
        if pending.is_empty() {
            return Ok(());
        }
        for read in [true, false] {
            for value in [true, false] {
                let sent: Vec<_> = pending
                    .iter()
                    .filter(|p| {
                        if read {
                            p.read == Some(value)
                        } else {
                            p.starred == Some(value)
                        }
                    })
                    .map(|p| (p.article, p.entry))
                    .collect();
                if sent.is_empty() {
                    continue;
                }
                let ids: Vec<i64> = sent.iter().map(|(_, entry)| *entry).collect();
                let accepted = if read {
                    api.set_read(&ids, value).await?
                } else {
                    api.set_starred(&ids, value).await?
                };
                // An entry Feedbin did not accept is one it no longer serves; keeping its mark
                // would retry it on every sync forever.
                outcome.rejected += ids.len() - accepted.len();
                let articles: Vec<_> = sent.iter().map(|(article, _)| *article).collect();
                db.call(move |s| s.feedbin_pushed(read, value, &articles))
                    .await?;
                outcome.pushed += 1;
            }
        }
    }
}

/// Read the entries Feedbin has published since the last sync.
async fn pull_entries(db: &Db, api: &Api, outcome: &mut Outcome) -> Result<()> {
    let cursor = db.call(|s| s.feedbin_cursor()).await?;
    let first = cursor.is_none();
    let mut page = 1;
    let mut newest: Option<String> = None;
    loop {
        let entries = api.entries(cursor.as_deref(), page, PER_PAGE).await?;
        if entries.is_empty() {
            break;
        }
        let full = entries.len() == PER_PAGE as usize;
        for entry in &entries {
            if let Some(created) = &entry.created_at
                && newest.as_deref().is_none_or(|n| created.as_str() > n)
            {
                newest = Some(created.clone());
            }
        }
        let now = chrono::Utc::now().timestamp();
        let remote: Vec<RemoteEntry> = entries.iter().map(|e| remote_entry(e, now)).collect();
        let pulled = db.call(move |s| s.feedbin_entries(&remote, now)).await?;
        outcome.absorb(pulled);
        if !full || (first && page >= FIRST_PAGES) {
            break;
        }
        page += 1;
    }
    // Feedbin's `since` is exclusive, so the newest entry this sync read is the next one's
    // starting point. Its `created_at` is a microsecond timestamp, so two entries never share
    // it in practice.
    if let Some(newest) = newest {
        db.call(move |s| s.set_feedbin_cursor(&newest)).await?;
    }
    Ok(())
}

/// Take Feedbin's read and star state.
async fn pull_state(db: &Db, api: &Api) -> Result<()> {
    let unread = api.unread().await?;
    let starred = api.starred().await?;
    db.call(move |s| s.feedbin_apply_state(&unread, &starred))
        .await?;
    Ok(())
}

/// One entry as it is stored, sanitized and with its text extracted.
fn remote_entry(entry: &api::Entry, now: i64) -> RemoteEntry {
    let url = entry.url.clone().unwrap_or_default();
    let body = entry
        .content
        .clone()
        .or_else(|| entry.summary.clone())
        .unwrap_or_default();
    let base = if url.is_empty() { "" } else { &url };
    let html = content::sanitize(&body, base);
    RemoteEntry {
        id: entry.id,
        feed_id: entry.feed_id,
        identity: if url.is_empty() {
            format!("feedbin:{}", entry.id)
        } else {
            format!("url:{url}")
        },
        title: entry
            .title
            .clone()
            .map(|t| content::one_line(&t))
            .unwrap_or_default(),
        url,
        author: entry
            .author
            .clone()
            .map(|a| content::one_line(&a))
            .unwrap_or_default(),
        published: entry
            .published
            .as_deref()
            .and_then(parse_date)
            .unwrap_or(now),
        text: content::plain(&html),
        html,
    }
}

/// An RFC 3339 date as Unix seconds.
fn parse_date(date: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(date)
        .ok()
        .map(|d| d.timestamp())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn feedbin_dates_are_read_or_ignored() {
        assert_eq!(
            parse_date("2013-02-03T01:00:19.000000Z"),
            Some(1_359_853_219)
        );
        assert_eq!(parse_date("2013-02-03T01:00:19Z"), Some(1_359_853_219));
        assert_eq!(parse_date("not a date"), None);
    }
}
