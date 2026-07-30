//
// Copyright 2026 Tabsdata Inc.
//

//! [`StatementLogConfig`] — the sqlx statement-logging knobs shared by every
//! backend's connect options. Levels are [`LevelFilter`]s serialized through
//! their `Display`/`FromStr` (case-insensitive names, `off`..`trace`).

use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use serde_with::{DisplayFromStr, DurationSeconds};
use std::time::Duration;
use tracing::log::LevelFilter;

/// Statement logging for one connection (sqlx `ConnectOptions`), flattened into
/// each backend's config block. Times are bare integer seconds.
#[serde_as]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct StatementLogConfig {
    /// Level every executed statement is logged at, defaults to `off`.
    #[serde_as(as = "DisplayFromStr")]
    pub log_statements: LevelFilter,
    /// Level statements slower than the threshold are logged at, defaults to `warn`.
    #[serde_as(as = "DisplayFromStr")]
    pub log_slow_statements: LevelFilter,
    /// Slow-statement threshold in seconds, defaults to `5`.
    #[serde_as(as = "DurationSeconds<u64>")]
    pub log_slow_statements_threshold: Duration,
}

impl Default for StatementLogConfig {
    fn default() -> Self {
        Self {
            log_statements: LevelFilter::Off,
            log_slow_statements: LevelFilter::Warn,
            log_slow_statements_threshold: Duration::from_secs(5),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statement_log_defaults() {
        let cfg: StatementLogConfig = serde_yaml::from_str("{}").unwrap();
        assert_eq!(cfg, StatementLogConfig::default());
        assert_eq!(cfg.log_statements, LevelFilter::Off);
        assert_eq!(cfg.log_slow_statements, LevelFilter::Warn);
        assert_eq!(cfg.log_slow_statements_threshold, Duration::from_secs(5));
    }

    #[test]
    fn statement_log_overrides() {
        let yaml = "
log_statements: debug
log_slow_statements: error
log_slow_statements_threshold: 1
";
        let cfg: StatementLogConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.log_statements, LevelFilter::Debug);
        assert_eq!(cfg.log_slow_statements, LevelFilter::Error);
        assert_eq!(cfg.log_slow_statements_threshold, Duration::from_secs(1));
    }

    #[test]
    fn statement_log_off_and_bad_values() {
        let cfg: StatementLogConfig = serde_yaml::from_str("log_statements: off").unwrap();
        assert_eq!(cfg.log_statements, LevelFilter::Off);
        assert!(serde_yaml::from_str::<StatementLogConfig>("log_statements: true").is_err());
        assert!(serde_yaml::from_str::<StatementLogConfig>("log_statements: verbose").is_err());
    }

    #[test]
    fn statement_log_round_trip() {
        let original = StatementLogConfig {
            log_statements: LevelFilter::Info,
            ..Default::default()
        };
        let restored: StatementLogConfig =
            serde_yaml::from_str(&serde_yaml::to_string(&original).unwrap()).unwrap();
        assert_eq!(original, restored);
    }
}
