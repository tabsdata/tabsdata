//
// Copyright 2026 Tabsdata Inc.
//

//! [`PoolConfig`] — the sqlx connection-pool settings shared across all
//! backends (sizes, timeouts, statement log level).

use serde::{Deserialize, Serialize};
use serde_with::DurationSeconds;
use serde_with::serde_as;
use std::time::Duration;
use tracing::log::LevelFilter;

fn min_connections() -> u32 {
    1
}

fn max_connections() -> u32 {
    10
}

fn acquire_timeout() -> Duration {
    Duration::from_secs(30)
}

fn max_lifetime() -> Duration {
    Duration::from_secs(60 * 60)
}

fn idle_timeout() -> Duration {
    Duration::from_secs(60)
}

fn test_before_acquire() -> bool {
    true
}

fn log_level() -> LevelFilter {
    LevelFilter::Off
}

mod log_level_serde {
    use serde::{Deserializer, Serializer};
    use std::str::FromStr;
    use tracing::log::LevelFilter;

    pub(crate) fn serialize<S>(level: &LevelFilter, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&level.to_string())
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<LevelFilter, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value = LevelFilter;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a LevelFilter - off, error, warn, info, debug, trace")
            }
            fn visit_bool<E: serde::de::Error>(self, _v: bool) -> Result<LevelFilter, E> {
                // If off is not inbetween "", serde thinks it is a bool in some cases.
                Ok(LevelFilter::Off)
            }
            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<LevelFilter, E> {
                LevelFilter::from_str(v).map_err(serde::de::Error::custom)
            }
            fn visit_string<E: serde::de::Error>(self, v: String) -> Result<LevelFilter, E> {
                LevelFilter::from_str(&v).map_err(serde::de::Error::custom)
            }
        }

        deserializer.deserialize_any(V)
    }
}

/// Connection pool settings shared across all database backends.
#[serde_as]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PoolConfig {
    /// The minimum number of database connections, defaults to `1`.
    #[serde(default = "min_connections")]
    pub min_connections: u32,
    /// The maximum number of database connections, defaults to `10`.
    #[serde(default = "max_connections")]
    pub max_connections: u32,
    /// The maximum time to wait for a database connection to be acquired, defaults to `30 seconds`.
    #[serde(default = "acquire_timeout")]
    #[serde_as(as = "DurationSeconds<u64>")]
    pub acquire_timeout: Duration,
    /// The maximum lifetime of a database connection, defaults to `60 minutes`.
    #[serde(default = "max_lifetime")]
    #[serde_as(as = "DurationSeconds<u64>")]
    pub max_lifetime: Duration,
    /// The maximum time a database connection can be idle, defaults to `60 seconds`.
    #[serde(default = "idle_timeout")]
    #[serde_as(as = "DurationSeconds<u64>")]
    pub idle_timeout: Duration,
    /// Whether to test the connection before acquiring it, defaults to `true`.
    #[serde(default = "test_before_acquire")]
    pub test_before_acquire: bool,
    /// Log level to be used by the configured connection pool.
    #[serde(default = "log_level", with = "log_level_serde")]
    pub log_level: LevelFilter,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            min_connections: min_connections(),
            max_connections: max_connections(),
            acquire_timeout: acquire_timeout(),
            max_lifetime: max_lifetime(),
            idle_timeout: idle_timeout(),
            test_before_acquire: test_before_acquire(),
            log_level: log_level(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_config_all_defaults() {
        let cfg: PoolConfig = serde_yaml::from_str("{}").unwrap();
        assert_eq!(cfg, PoolConfig::default());
    }

    #[test]
    fn pool_config_partial_override() {
        let yaml = "
idle_timeout: 120
test_before_acquire: false
";
        let cfg: PoolConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.idle_timeout, Duration::from_secs(120));
        assert!(!cfg.test_before_acquire);
        // rest defaults
        assert_eq!(cfg.min_connections, 1);
        assert_eq!(cfg.max_connections, 10);
    }
}
