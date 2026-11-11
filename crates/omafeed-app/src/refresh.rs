//! Refreshing, whichever way the library is configured: one Feedbin sync when an account is
//! set up, one direct pass over the feeds otherwise.
//!
//! A sync reports itself through the same [`Progress`] events as a refresh, as a single entry
//! named after the service, so the window's status bar and `omafeed refresh` need no special
//! case.
use anyhow::Result;
use omafeed_core::{
    Db, Paths,
    feedbin::{self, Api, Outcome},
    ingest::{Progress, Refresher, Which},
};

/// Whether a sync is due, given when the last one finished.
///
/// A sync covers every subscription at once, so it has no per-feed interval to go by:
/// [`Which::Due`] means the refresh interval has elapsed, as with a feed's own. A refresh
/// the user asked for always syncs.
fn due(which: &Which, minutes: u32, last_sync: Option<i64>, now: i64) -> bool {
    match which {
        Which::Due => last_sync.is_none_or(|last| now - last >= i64::from(minutes) * 60),
        Which::All | Which::Only(_) => true,
    }
}

/// Refresh the library: sync with Feedbin if an account is configured, and report whether it
/// was a sync. `last_sync` is when the account was last synced, so the window's scheduled
/// checks in between are skipped instead of syncing again.
pub async fn refresh(
    db: Db,
    paths: Paths,
    refresher: Refresher,
    which: Which,
    minutes: u32,
    last_sync: Option<i64>,
    progress: async_channel::Sender<Progress>,
) -> Result<Option<Outcome>> {
    match feedbin::account(&paths)? {
        Some(api) => {
            let now = chrono::Utc::now().timestamp();
            if !due(&which, minutes, last_sync, now) {
                return Ok(None);
            }
            Ok(Some(sync(&db, &api, feedbin::FIRST_PAGES, progress).await?))
        }
        None => {
            refresher.refresh(db, which, minutes, progress).await?;
            Ok(None)
        }
    }
}

/// Push what Feedbin is owed, then pull what it has, reporting the sync's start and end.
pub async fn sync(
    db: &Db,
    api: &Api,
    pages: u32,
    progress: async_channel::Sender<Progress>,
) -> Result<Outcome> {
    db.set_feedbin(true);
    let _ = progress.send(Progress::SyncStarted).await;
    let result = feedbin::sync(db, api, pages).await;
    let error = result.as_ref().err().map(|e| format!("{e:#}"));
    let writes = result.as_ref().map_or(0, |outcome| outcome.writes);
    let _ = progress.send(Progress::Synced { error, writes }).await;
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINUTES: u32 = 30;
    const NOW: i64 = 1_700_000_000;

    #[test]
    fn a_scheduled_check_syncs_only_after_the_interval() {
        assert!(due(&Which::Due, MINUTES, None, NOW));
        let just_now = Some(NOW - 60);
        assert!(!due(&Which::Due, MINUTES, just_now, NOW));
        let at_the_edge = Some(NOW - i64::from(MINUTES) * 60);
        assert!(due(&Which::Due, MINUTES, at_the_edge, NOW));
    }

    #[test]
    fn a_refresh_the_user_asked_for_always_syncs() {
        let just_now = Some(NOW);
        assert!(due(&Which::All, MINUTES, just_now, NOW));
        assert!(due(&Which::Only(Vec::new()), MINUTES, just_now, NOW));
    }
}
