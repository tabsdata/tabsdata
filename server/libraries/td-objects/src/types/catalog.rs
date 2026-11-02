//
// Copyright 2026 Tabsdata Inc.
//

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// `catalog.*` in the apiserver configuration. `refresh_interval` (`24h`,
/// `off`) re-explores every connection whose metadata is older, below the
/// interactive probes; off by default. `maintenance_samples` lets a
/// `maintain_catalog` holder sample a few rows of a connection; off by
/// default.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CatalogConfig {
    #[serde(with = "refresh_interval")]
    pub refresh_interval: Option<Duration>,
    pub maintenance_samples: bool,
}

mod refresh_interval {
    use serde::{Deserialize, Deserializer, Serializer, de::Error};
    use std::time::Duration;

    const OFF: &str = "off";

    pub fn serialize<S: Serializer>(
        value: &Option<Duration>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            None => serializer.serialize_str(OFF),
            Some(interval) => {
                serializer.serialize_str(&humantime::format_duration(*interval).to_string())
            }
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Duration>, D::Error> {
        let text = Option::<String>::deserialize(deserializer)?;
        match text.as_deref().map(str::trim) {
            None | Some("") | Some(OFF) => Ok(None),
            Some(text) => {
                let interval = humantime::parse_duration(text).map_err(D::Error::custom)?;
                if interval.is_zero() {
                    return Err(D::Error::custom(
                        "catalog.refresh_interval must be positive or 'off'",
                    ));
                }
                Ok(Some(interval))
            }
        }
    }
}
