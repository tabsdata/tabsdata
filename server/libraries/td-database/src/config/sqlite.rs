//
// Copyright 2026 Tabs Data Inc.
//

use serde::{Deserialize, Deserializer, Serialize};
use sqlx::Database;
use std::ops::Deref;
use url::Url;

#[derive(Debug, Clone, Serialize)]
pub struct SqliteUrl(Url);

impl Deref for SqliteUrl {
    type Target = Url;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryFrom<Url> for SqliteUrl {
    type Error = String;

    fn try_from(url: Url) -> Result<Self, Self::Error> {
        let scheme = url.scheme();
        if sqlx::Sqlite::URL_SCHEMES.contains(&scheme) || scheme.eq_ignore_ascii_case("file") {
            Ok(SqliteUrl(url))
        } else {
            Err(format!("expected sqlite URL, found scheme '{scheme}'"))
        }
    }
}

impl<'de> Deserialize<'de> for SqliteUrl {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Url::deserialize(deserializer)?
            .try_into()
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqliteConfig {
    pub(crate) url: SqliteUrl,
}
