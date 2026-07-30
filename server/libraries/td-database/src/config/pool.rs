//
// Copyright 2026 Tabsdata Inc.
//

//! [`PoolConfig`] — the full sqlx `PoolOptions` surface, shared across all
//! backends. Times are bare integer seconds.

use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use serde_with::{DisplayFromStr, DurationSeconds};
use std::time::Duration;
use tracing::log::LevelFilter;

fn max_lifetime() -> Option<Duration> {
    Some(Duration::from_secs(60 * 60))
}

fn idle_timeout() -> Option<Duration> {
    Some(Duration::from_secs(60))
}

/// Connection pool settings shared across all database backends.
#[serde_as]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct PoolConfig {
    /// The minimum number of database connections, defaults to `1`.
    pub min_connections: u32,
    /// The maximum number of database connections, defaults to `10`.
    pub max_connections: u32,
    /// The maximum time to wait for a database connection to be acquired, defaults to `30 seconds`.
    #[serde_as(as = "DurationSeconds<u64>")]
    pub acquire_timeout: Duration,
    /// The maximum lifetime of a database connection, defaults to `60 minutes`; `~` disables it.
    #[serde(default = "max_lifetime")]
    #[serde_as(as = "Option<DurationSeconds<u64>>")]
    pub max_lifetime: Option<Duration>,
    /// The maximum time a database connection can be idle, defaults to `60 seconds`; `~` disables it.
    #[serde(default = "idle_timeout")]
    #[serde_as(as = "Option<DurationSeconds<u64>>")]
    pub idle_timeout: Option<Duration>,
    /// Whether to test the connection before acquiring it, defaults to `true`.
    pub test_before_acquire: bool,
    /// Level every connection acquire is logged at, defaults to `off`.
    #[serde_as(as = "DisplayFromStr")]
    pub acquire_time_level: LevelFilter,
    /// Level acquires slower than the threshold are logged at, defaults to `off`.
    #[serde_as(as = "DisplayFromStr")]
    pub acquire_slow_level: LevelFilter,
    /// Slow-acquire threshold in seconds, defaults to `2`.
    #[serde_as(as = "DurationSeconds<u64>")]
    pub acquire_slow_threshold: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            min_connections: 1,
            max_connections: 10,
            acquire_timeout: Duration::from_secs(30),
            max_lifetime: max_lifetime(),
            idle_timeout: idle_timeout(),
            test_before_acquire: true,
            acquire_time_level: LevelFilter::Off,
            acquire_slow_level: LevelFilter::Off,
            acquire_slow_threshold: Duration::from_secs(2),
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
        assert_eq!(cfg.idle_timeout, Some(Duration::from_secs(120)));
        assert!(!cfg.test_before_acquire);
        // rest defaults
        assert_eq!(cfg.min_connections, 1);
        assert_eq!(cfg.max_connections, 10);
        assert_eq!(cfg.acquire_slow_threshold, Duration::from_secs(2));
    }

    #[test]
    fn pool_config_null_disables_expiries() {
        let yaml = "
max_lifetime: ~
idle_timeout: ~
";
        let cfg: PoolConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.max_lifetime, None);
        assert_eq!(cfg.idle_timeout, None);
    }

    #[test]
    fn pool_config_acquire_log_levels() {
        let yaml = "
acquire_time_level: debug
acquire_slow_level: off
acquire_slow_threshold: 10
";
        let cfg: PoolConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.acquire_time_level, LevelFilter::Debug);
        assert_eq!(cfg.acquire_slow_level, LevelFilter::Off);
        assert_eq!(cfg.acquire_slow_threshold, Duration::from_secs(10));
    }
}
