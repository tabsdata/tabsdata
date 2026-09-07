//
// Copyright 2026 Tabsdata Inc.
//

//! [`PoolsConfig`] — one full [`PoolConfig`] per pool kind (`regular`, the
//! apiserver's `exclusive`, plus the privileged `admin`), the config-side
//! sibling of the runtime [`ComponentPools`](crate::pool::ComponentPools).
//! Times are bare integer seconds.

use crate::pool::PoolKind;
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

fn acquire_timeout() -> Duration {
    Duration::from_secs(30)
}

fn test_before_acquire() -> bool {
    true
}

fn level_off() -> LevelFilter {
    LevelFilter::Off
}

fn acquire_slow_threshold() -> Duration {
    Duration::from_secs(2)
}

/// The per-kind pool configurations of one entry's `pool:` block. Serving
/// pools are strict — an absent block fails when that pool opens, nothing is
/// inherited; `admin` (the privileged single connection) falls back to code
/// defaults.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PoolsConfig {
    /// The pool every non-serialized request runs on: reads and ordinary
    /// writes alike.
    pub regular: Option<PoolConfig>,
    /// The serialized-write pool, where the component declares one. Its
    /// connections hold their project's lock for a whole transaction, so its
    /// size bounds how many can queue and its `acquire_timeout` bounds the
    /// wait.
    pub exclusive: Option<PoolConfig>,
    /// The privileged admin connection.
    pub admin: Option<PoolConfig>,
}

impl PoolsConfig {
    pub fn get(&self, kind: PoolKind) -> &Option<PoolConfig> {
        match kind {
            PoolKind::Regular => &self.regular,
            PoolKind::Exclusive => &self.exclusive,
        }
    }
}

/// One pool's settings — the full sqlx `PoolOptions` surface, shared across
/// all backends. Connection counts are required; the remaining knobs default.
#[serde_as]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolConfig {
    /// The minimum number of database connections.
    pub min_connections: u32,
    /// The maximum number of database connections.
    pub max_connections: u32,
    /// The maximum time to wait for a database connection to be acquired, defaults to `30 seconds`.
    #[serde(default = "acquire_timeout")]
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
    #[serde(default = "test_before_acquire")]
    pub test_before_acquire: bool,
    /// Level every connection acquire is logged at, defaults to `off`.
    #[serde(default = "level_off")]
    #[serde_as(as = "DisplayFromStr")]
    pub acquire_time_level: LevelFilter,
    /// Level acquires slower than the threshold are logged at, defaults to `off`.
    #[serde(default = "level_off")]
    #[serde_as(as = "DisplayFromStr")]
    pub acquire_slow_level: LevelFilter,
    /// Slow-acquire threshold in seconds, defaults to `2`.
    #[serde(default = "acquire_slow_threshold")]
    #[serde_as(as = "DurationSeconds<u64>")]
    pub acquire_slow_threshold: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            min_connections: 1,
            max_connections: 10,
            acquire_timeout: acquire_timeout(),
            max_lifetime: max_lifetime(),
            idle_timeout: idle_timeout(),
            test_before_acquire: test_before_acquire(),
            acquire_time_level: level_off(),
            acquire_slow_level: level_off(),
            acquire_slow_threshold: acquire_slow_threshold(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pools_config_absent_blocks() {
        let cfg: PoolsConfig = serde_yaml::from_str("{}").unwrap();
        assert_eq!(cfg, PoolsConfig::default());
        assert!(cfg.regular.is_none() && cfg.exclusive.is_none() && cfg.admin.is_none());
    }

    #[test]
    fn pools_config_full_shape() {
        let yaml = "
regular:
  min_connections: 5
  max_connections: 50
exclusive:
  min_connections: 1
  max_connections: 10
admin:
  min_connections: 1
  max_connections: 1
";
        let cfg: PoolsConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.regular.unwrap().max_connections, 50);
        assert_eq!(cfg.exclusive.unwrap().max_connections, 10);
        assert_eq!(cfg.admin.unwrap().max_connections, 1);
    }

    /// The pre-split spelling (`ro:`/`rw:`) is not silently ignored: one
    /// regular pool serves both, so a config still naming them fails to load.
    #[test]
    fn pools_config_rejects_access_mode_keys() {
        let err = serde_yaml::from_str::<PoolsConfig>(
            "ro:\n  min_connections: 1\n  max_connections: 2\n",
        )
        .unwrap_err();
        assert!(err.to_string().contains("unknown field"), "{err}");
    }

    /// The pre-split flat shape (sizes directly under `pool:`) must fail
    /// loudly, not silently size nothing.
    #[test]
    fn pools_config_rejects_flat_keys() {
        let err = serde_yaml::from_str::<PoolsConfig>("max_connections: 50").unwrap_err();
        assert!(err.to_string().contains("unknown field"), "{err}");
    }

    /// Opening a block means stating both connection counts.
    #[test]
    fn pool_config_requires_connections() {
        let err = serde_yaml::from_str::<PoolConfig>("acquire_timeout: 10").unwrap_err();
        assert!(err.to_string().contains("missing field"), "{err}");
    }

    #[test]
    fn pool_config_partial_override() {
        let yaml = "
min_connections: 2
max_connections: 20
idle_timeout: 120
test_before_acquire: false
";
        let cfg: PoolConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.idle_timeout, Some(Duration::from_secs(120)));
        assert!(!cfg.test_before_acquire);
        // rest defaults
        assert_eq!(cfg.acquire_timeout, Duration::from_secs(30));
        assert_eq!(cfg.acquire_slow_threshold, Duration::from_secs(2));
    }

    #[test]
    fn pool_config_null_disables_expiries() {
        let yaml = "
min_connections: 1
max_connections: 10
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
min_connections: 1
max_connections: 10
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
