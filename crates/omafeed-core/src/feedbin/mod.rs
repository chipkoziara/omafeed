//! Feedbin: the account's credentials, its API, and one sync.
//!
//! With an account configured, Feedbin owns the subscriptions and the entries, and this library
//! owns the read and star changes Feedbin has not been told about yet. A sync pushes those,
//! then pulls the account's state back. Without an account, nothing here runs and the reader
//! fetches feeds itself, exactly as before.
pub mod api;
pub mod credentials;
pub mod sync;

pub use api::Api;
pub use credentials::Credentials;
pub use sync::{Outcome, sync};

use crate::Paths;
use anyhow::Result;

/// The configured account, or `None` when there is none.
pub fn account(paths: &Paths) -> Result<Option<Api>> {
    match Credentials::load(paths)? {
        Some(credentials) => Ok(Some(Api::new(api::API, credentials)?)),
        None => Ok(None),
    }
}
