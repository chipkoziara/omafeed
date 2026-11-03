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

/// Refresh the library: sync with Feedbin if an account is configured, and report whether it
/// was a sync.
pub async fn refresh(
    db: Db,
    paths: Paths,
    refresher: Refresher,
    which: Which,
    minutes: u32,
    progress: async_channel::Sender<Progress>,
) -> Result<Option<Outcome>> {
    match feedbin::account(&paths)? {
        Some(api) => Ok(Some(sync(&db, &api, progress).await?)),
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
    progress: async_channel::Sender<Progress>,
) -> Result<Outcome> {
    db.set_feedbin(true);
    let _ = progress.send(Progress::SyncStarted).await;
    let result = feedbin::sync(db, api).await;
    let error = result.as_ref().err().map(|e| format!("{e:#}"));
    let _ = progress.send(Progress::Synced { error }).await;
    result
}
