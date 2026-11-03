//! The Feedbin account's email and password, kept out of `settings.toml`.
//!
//! `settings.toml` is rewritten whole whenever the window saves its size, and its values are
//! read leniently: a bad value is replaced by a default. Neither suits a secret. This is a file
//! of its own, created readable only by its owner, written atomically, and never rewritten by
//! anything but `feedbin login` and `feedbin logout`.
use crate::Paths;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// What Feedbin authenticates: the account's email and its password or API token.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Credentials {
    /// The account's email address.
    pub email: String,
    /// Its password, or an application password.
    pub password: String,
}

impl Credentials {
    fn file(paths: &Paths) -> PathBuf {
        paths.config.join("feedbin.toml")
    }

    /// The account's credentials, or `None` when no account is configured.
    pub fn load(paths: &Paths) -> Result<Option<Self>> {
        let path = Self::file(paths);
        let text = match std::fs::read_to_string(&path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e).with_context(|| format!("Read {}", path.display())),
        };
        let credentials: Self = toml::from_str(&text)
            .with_context(|| format!("Read {}: expected `email` and `password`", path.display()))?;
        if credentials.email.trim().is_empty() || credentials.password.is_empty() {
            anyhow::bail!(
                "{} names an empty email or password; run `omafeed feedbin login`",
                path.display()
            );
        }
        Ok(Some(credentials))
    }

    /// Store the credentials for the next command and the window, readable only by their owner.
    pub fn save(&self, paths: &Paths) -> Result<()> {
        let path = Self::file(paths);
        let temp = paths.config.join("feedbin.toml.tmp");
        std::fs::write(&temp, toml::to_string_pretty(self)?)?;
        let mut permissions = std::fs::metadata(&temp)?.permissions();
        std::os::unix::fs::PermissionsExt::set_mode(&mut permissions, 0o600);
        std::fs::set_permissions(&temp, permissions)?;
        std::fs::rename(&temp, &path).with_context(|| format!("Write {}", path.display()))?;
        Ok(())
    }

    /// Forget the account, leaving the library as it is.
    pub fn remove(paths: &Paths) -> Result<()> {
        match std::fs::remove_file(Self::file(paths)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e).context("Remove feedbin.toml"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn paths(dir: &std::path::Path) -> Paths {
        Paths {
            data: dir.into(),
            config: dir.into(),
            cache: dir.into(),
        }
    }

    #[test]
    fn credentials_round_trip_and_are_readable_only_by_their_owner() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(Credentials::load(&paths(dir.path())).unwrap(), None);
        let credentials = Credentials {
            email: "me@example.com".into(),
            password: "secret".into(),
        };
        credentials.save(&paths(dir.path())).unwrap();
        assert_eq!(
            Credentials::load(&paths(dir.path())).unwrap(),
            Some(credentials)
        );
        let mode = std::fs::metadata(dir.path().join("feedbin.toml"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
        Credentials::remove(&paths(dir.path())).unwrap();
        assert_eq!(Credentials::load(&paths(dir.path())).unwrap(), None);
        Credentials::remove(&paths(dir.path())).unwrap();
    }

    #[test]
    fn an_empty_password_is_reported_rather_than_used() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("feedbin.toml"),
            "email = \"me@example.com\"\npassword = \"\"\n",
        )
        .unwrap();
        let error = Credentials::load(&paths(dir.path())).unwrap_err();
        assert!(format!("{error:#}").contains("feedbin login"), "{error:#}");
    }
}
