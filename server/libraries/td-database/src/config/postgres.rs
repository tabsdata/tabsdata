//
// Copyright 2026 Tabsdata Inc.
//

use serde::{Deserialize, Deserializer, Serialize};
use sqlx::Database;
use std::ops::Deref;
use url::Url;

#[derive(Debug, Clone, Serialize)]
pub struct PostgresUrl(Url);

impl Deref for PostgresUrl {
    type Target = Url;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryFrom<Url> for PostgresUrl {
    type Error = String;

    fn try_from(url: Url) -> Result<Self, Self::Error> {
        let scheme = url.scheme();
        if sqlx::Postgres::URL_SCHEMES.contains(&scheme) {
            Ok(PostgresUrl(url))
        } else {
            Err(format!("expected postgres URL, found scheme '{scheme}'"))
        }
    }
}

impl<'de> Deserialize<'de> for PostgresUrl {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Url::deserialize(deserializer)?
            .try_into()
            .map_err(serde::de::Error::custom)
    }
}

/// Postgres backend, addressed by URL — external servers and the managed
/// in-process server alike (the latter is booted out of band from
/// [`crate::server`] and reached on its localhost URL). The URL carries
/// host, port, and database name only; credentials are explicit fields on
/// [`super::DatabaseConfig`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresConfig {
    pub(crate) url: PostgresUrl,
}
