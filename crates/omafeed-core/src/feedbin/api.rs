//! Feedbin's HTTP API: as much of it as a sync uses, and no more.
//!
//! Every request carries the account's credentials as HTTP basic auth. Responses that mean
//! something to the user beyond "it failed" — credentials refused, asked to slow down, a
//! subscription that would be a duplicate — become [`Error::Feedbin`];
//! everything else carries the status and what was being done.
use super::credentials::Credentials;
use crate::{
    Error,
    http::{self, Body},
    store::feedbin::{Subscription, Tag},
};
use anyhow::{Context, Result, bail};
use reqwest::{Client, Method, Response, StatusCode};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::json;
use url::Url;

/// Feedbin's API address.
pub const API: &str = "https://api.feedbin.com/";

/// The API's address: [`API`], or `OMAFEED_FEEDBIN_API` when it is set. The override exists so
/// a test can answer every request from its own server, and so a Feedbin-compatible proxy can
/// be used instead.
pub fn base() -> String {
    std::env::var("OMAFEED_FEEDBIN_API").unwrap_or_else(|_| API.to_owned())
}

/// An entry as Feedbin sends it; any field but the IDs can be null.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct Entry {
    /// Feedbin's ID for it.
    pub id: i64,
    /// Feedbin's ID for its feed.
    pub feed_id: i64,
    /// Its title.
    pub title: Option<String>,
    /// Its link.
    pub url: Option<String>,
    /// Its authors.
    pub author: Option<String>,
    /// Its HTML.
    pub content: Option<String>,
    /// A summary, when there is no content.
    pub summary: Option<String>,
    /// When it was published, as an RFC 3339 date.
    pub published: Option<String>,
    /// When Feedbin stored it, as an RFC 3339 date.
    pub created_at: Option<String>,
}

/// A tag on one feed.
#[derive(Clone, Debug, Deserialize)]
pub struct Tagging {
    /// The tagging's ID, which removes it.
    pub id: i64,
    /// The feed it is on.
    pub feed_id: i64,
    /// Its name.
    pub name: String,
}

#[derive(Deserialize)]
struct RawSubscription {
    id: i64,
    feed_id: i64,
    title: Option<String>,
    feed_url: String,
    site_url: Option<String>,
}

/// A Feedbin account's API.
pub struct Api {
    http: Client,
    base: Url,
    credentials: Credentials,
}

impl Api {
    /// An API for `base`, which is [`API`] outside tests.
    pub fn new(base: &str, credentials: Credentials) -> Result<Self> {
        Ok(Self {
            http: http::client()?,
            base: Url::parse(base).with_context(|| format!("Feedbin address {base}"))?,
            credentials,
        })
    }

    fn url(&self, path: &str) -> Result<Url> {
        self.base
            .join(path)
            .with_context(|| format!("Feedbin address {path}"))
    }

    fn request(&self, method: Method, path: &str) -> Result<reqwest::RequestBuilder> {
        Ok(self
            .http
            .request(method, self.url(path)?)
            .basic_auth(&self.credentials.email, Some(&self.credentials.password)))
    }

    /// Send a request, turning the answers the user can act on into [`Error::Feedbin`].
    async fn send(&self, request: reqwest::RequestBuilder, what: &str) -> Result<Response> {
        let response = request
            .send()
            .await
            .with_context(|| format!("{what}: could not reach Feedbin"))?;
        let status = response.status();
        match status {
            StatusCode::UNAUTHORIZED => {
                return Err(Error::Feedbin(
                    "Feedbin did not accept the email and password; check them with `omafeed feedbin login`".into(),
                )
                .into());
            }
            StatusCode::TOO_MANY_REQUESTS => {
                return Err(Error::Feedbin(
                    "asked this client to slow down; try again shortly".into(),
                )
                .into());
            }
            s if s.is_server_error() => {
                bail!("{what}: Feedbin answered {s}");
            }
            _ => {}
        }
        Ok(response)
    }

    /// Send a request whose body is JSON.
    async fn send_json(
        &self,
        method: Method,
        path: &str,
        body: &serde_json::Value,
        what: &str,
    ) -> Result<Response> {
        let request = self
            .request(method, path)?
            .header(
                reqwest::header::CONTENT_TYPE,
                "application/json; charset=utf-8",
            )
            .body(serde_json::to_vec(body)?);
        self.send(request, what).await
    }

    /// Send a request and read its JSON answer.
    async fn json<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, String)],
        what: &str,
    ) -> Result<T> {
        let request = self.request(method, path)?.query(query);
        let response = self.send(request, what).await?;
        let status = response.status();
        if !status.is_success() {
            return Err(Error::Feedbin(format!("{what}: Feedbin answered {status}")).into());
        }
        let body = http::read_limited(response, http::BODY_LIMIT, Body::Whole).await?;
        serde_json::from_slice(&body).with_context(|| format!("{what}: unexpected answer"))
    }

    /// Check that the credentials work, without reading anything.
    pub async fn authentication(&self) -> Result<()> {
        let request = self.request(Method::GET, "v2/authentication.json")?;
        let response = self.send(request, "Check the account").await?;
        let status = response.status();
        if !status.is_success() {
            return Err(
                Error::Feedbin(format!("Check the account: Feedbin answered {status}")).into(),
            );
        }
        Ok(())
    }

    /// Every subscription, each with the first tag Feedbin lists on it.
    pub async fn subscriptions(&self) -> Result<Vec<Subscription>> {
        let taggings = self.taggings().await?;
        let raw: Vec<RawSubscription> = self
            .json(
                Method::GET,
                "v2/subscriptions.json",
                &[],
                "List subscriptions",
            )
            .await?;
        Ok(raw
            .into_iter()
            .map(|s| Subscription {
                id: s.id,
                feed_id: s.feed_id,
                title: s.title.unwrap_or_default(),
                feed_url: s.feed_url,
                site_url: s.site_url.unwrap_or_default(),
                tag: taggings
                    .iter()
                    .find(|t| t.feed_id == s.feed_id)
                    .map(|t| Tag {
                        id: t.id,
                        name: t.name.clone(),
                    }),
            })
            .collect())
    }

    /// Every tag on every feed.
    pub async fn taggings(&self) -> Result<Vec<Tagging>> {
        self.json(Method::GET, "v2/taggings.json", &[], "List tags")
            .await
    }

    /// One page of entries, newest first, after `since` when it is given.
    pub async fn entries(
        &self,
        since: Option<&str>,
        page: u32,
        per_page: u32,
    ) -> Result<Vec<Entry>> {
        let mut query = vec![
            ("page", page.to_string()),
            ("per_page", per_page.to_string()),
        ];
        if let Some(since) = since {
            query.push(("since", since.to_owned()));
        }
        self.json(Method::GET, "v2/entries.json", &query, "Read entries")
            .await
    }

    /// The IDs of every entry Feedbin considers unread.
    pub async fn unread(&self) -> Result<Vec<i64>> {
        self.json(
            Method::GET,
            "v2/unread_entries.json",
            &[],
            "Read unread entries",
        )
        .await
    }

    /// The IDs of every starred entry.
    pub async fn starred(&self) -> Result<Vec<i64>> {
        self.json(
            Method::GET,
            "v2/starred_entries.json",
            &[],
            "Read starred entries",
        )
        .await
    }

    /// Mark entries read or unread, returning the IDs Feedbin accepted.
    pub async fn set_read(&self, ids: &[i64], read: bool) -> Result<Vec<i64>> {
        let what = if read {
            "Mark entries read"
        } else {
            "Mark entries unread"
        };
        let method = if read { Method::DELETE } else { Method::POST };
        let response = self
            .send_json(
                method,
                "v2/unread_entries.json",
                &json!({ "unread_entries": ids }),
                what,
            )
            .await?;
        let status = response.status();
        if !status.is_success() {
            return Err(Error::Feedbin(format!("{what}: Feedbin answered {status}")).into());
        }
        let body = http::read_limited(response, http::BODY_LIMIT, Body::Whole).await?;
        Ok(serde_json::from_slice(&body).unwrap_or_default())
    }

    /// Star or unstar entries, returning the IDs Feedbin accepted.
    pub async fn set_starred(&self, ids: &[i64], starred: bool) -> Result<Vec<i64>> {
        let what = if starred {
            "Star entries"
        } else {
            "Unstar entries"
        };
        let method = if starred {
            Method::POST
        } else {
            Method::DELETE
        };
        let response = self
            .send_json(
                method,
                "v2/starred_entries.json",
                &json!({ "starred_entries": ids }),
                what,
            )
            .await?;
        let status = response.status();
        if !status.is_success() {
            return Err(Error::Feedbin(format!("{what}: Feedbin answered {status}")).into());
        }
        let body = http::read_limited(response, http::BODY_LIMIT, Body::Whole).await?;
        Ok(serde_json::from_slice(&body).unwrap_or_default())
    }

    /// Subscribe to a feed URL, or report the subscription that already exists.
    pub async fn subscribe(&self, feed_url: &str) -> Result<Subscription> {
        let response = self
            .send_json(
                Method::POST,
                "v2/subscriptions.json",
                &json!({ "feed_url": feed_url }),
                "Subscribe",
            )
            .await?;
        match response.status() {
            StatusCode::CREATED | StatusCode::FOUND | StatusCode::OK => {}
            StatusCode::MULTIPLE_CHOICES => {
                return Err(Error::Feedbin(format!(
                    "{feed_url} offers more than one feed; choose one on Feedbin's website"
                ))
                .into());
            }
            StatusCode::NOT_FOUND => {
                return Err(Error::Feedbin(format!("no feed found at {feed_url}")).into());
            }
            status => {
                return Err(Error::Feedbin(format!("Subscribe: Feedbin answered {status}")).into());
            }
        }
        let body = http::read_limited(response, http::BODY_LIMIT, Body::Whole).await?;
        let raw: RawSubscription = serde_json::from_slice(&body)
            .with_context(|| format!("Subscribe to {feed_url}: unexpected answer"))?;
        let taggings = self.taggings().await.unwrap_or_default();
        Ok(Subscription {
            id: raw.id,
            feed_id: raw.feed_id,
            title: raw.title.unwrap_or_default(),
            feed_url: raw.feed_url,
            site_url: raw.site_url.unwrap_or_default(),
            tag: taggings
                .iter()
                .find(|t| t.feed_id == raw.feed_id)
                .map(|t| Tag {
                    id: t.id,
                    name: t.name.clone(),
                }),
        })
    }

    /// Remove a subscription. One that is already gone counts as removed.
    pub async fn unsubscribe(&self, id: i64) -> Result<()> {
        let request = self.request(Method::DELETE, &format!("v2/subscriptions/{id}.json"))?;
        let response = self.send(request, "Remove subscription").await?;
        match response.status() {
            s if s.is_success() => Ok(()),
            StatusCode::NOT_FOUND | StatusCode::FORBIDDEN => Ok(()),
            status => Err(Error::Feedbin(format!(
                "Remove subscription: Feedbin answered {status}"
            ))
            .into()),
        }
    }

    /// Set a subscription's title.
    pub async fn rename(&self, id: i64, title: &str) -> Result<()> {
        let response = self
            .send_json(
                Method::PATCH,
                &format!("v2/subscriptions/{id}.json"),
                &json!({ "title": title }),
                "Rename subscription",
            )
            .await?;
        let status = response.status();
        if !status.is_success() {
            return Err(
                Error::Feedbin(format!("Rename subscription: Feedbin answered {status}")).into(),
            );
        }
        Ok(())
    }

    /// Put a feed in a tag.
    pub async fn tag(&self, feed_id: i64, name: &str) -> Result<Tag> {
        #[derive(Deserialize)]
        struct RawTagging {
            id: i64,
            name: String,
        }
        let response = self
            .send_json(
                Method::POST,
                "v2/taggings.json",
                &json!({ "feed_id": feed_id, "name": name }),
                "Tag a feed",
            )
            .await?;
        match response.status() {
            StatusCode::CREATED | StatusCode::FOUND | StatusCode::OK => {}
            status => {
                return Err(
                    Error::Feedbin(format!("Tag a feed: Feedbin answered {status}")).into(),
                );
            }
        }
        let body = http::read_limited(response, http::BODY_LIMIT, Body::Whole).await?;
        let raw: RawTagging =
            serde_json::from_slice(&body).context("Tag a feed: unexpected answer")?;
        Ok(Tag {
            id: raw.id,
            name: raw.name,
        })
    }

    /// Remove a tag from the feed it is on. One that is already gone counts as removed.
    pub async fn untag(&self, id: i64) -> Result<()> {
        let request = self.request(Method::DELETE, &format!("v2/taggings/{id}.json"))?;
        let response = self.send(request, "Remove a tag").await?;
        match response.status() {
            s if s.is_success() => Ok(()),
            StatusCode::NOT_FOUND | StatusCode::FORBIDDEN => Ok(()),
            status => {
                Err(Error::Feedbin(format!("Remove a tag: Feedbin answered {status}")).into())
            }
        }
    }
}
