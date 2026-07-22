//
// Copyright 2026 Tabsdata Inc.
//

use serde::{Deserialize, Deserializer, Serialize};
use sqlx::Database;
use std::ops::Deref;
use url::Url;

#[derive(Debug, Clone, Serialize)]
pub struct MySqlUrl(Url);

impl Deref for MySqlUrl {
    type Target = Url;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryFrom<Url> for MySqlUrl {
    type Error = String;

    fn try_from(url: Url) -> Result<Self, Self::Error> {
        let scheme = url.scheme();
        if sqlx::MySql::URL_SCHEMES.contains(&scheme) {
            Ok(MySqlUrl(url))
        } else {
            Err(format!("expected mysql URL, found scheme '{scheme}'"))
        }
    }
}

impl<'de> Deserialize<'de> for MySqlUrl {
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
pub struct MySqlConfig {
    pub(crate) url: MySqlUrl,
}
