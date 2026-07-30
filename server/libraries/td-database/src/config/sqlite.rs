//
// Copyright 2026 Tabsdata Inc.
//

//! [`SqliteConfig`] — the full sqlx `SqliteConnectOptions` surface. SQLite is
//! a file with no credentials, so this backend has none. `read_only` is not
//! config: the connect path builds a read-only and a read-write pool itself.
//! Closure-based sqlx options (`collation`, `thread_name`) and the
//! feature-gated `with_regexp` are intentionally not exposed.

use super::logging::StatementLogConfig;
use serde::{Deserialize, Serialize};
use sqlx::sqlite::{SqliteAutoVacuum, SqliteJournalMode, SqliteLockingMode, SqliteSynchronous};
use std::path::PathBuf;
use std::time::Duration;

/// Mirror of [`SqliteJournalMode`] with a serde surface (sqlx's enum has none).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SqliteJournalModeConfig {
    Delete,
    Truncate,
    Persist,
    Memory,
    #[default]
    Wal,
    Off,
}

impl From<SqliteJournalModeConfig> for SqliteJournalMode {
    fn from(mode: SqliteJournalModeConfig) -> Self {
        match mode {
            SqliteJournalModeConfig::Delete => SqliteJournalMode::Delete,
            SqliteJournalModeConfig::Truncate => SqliteJournalMode::Truncate,
            SqliteJournalModeConfig::Persist => SqliteJournalMode::Persist,
            SqliteJournalModeConfig::Memory => SqliteJournalMode::Memory,
            SqliteJournalModeConfig::Wal => SqliteJournalMode::Wal,
            SqliteJournalModeConfig::Off => SqliteJournalMode::Off,
        }
    }
}

/// Mirror of [`SqliteSynchronous`] with a serde surface (sqlx's enum has none).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SqliteSynchronousConfig {
    Off,
    Normal,
    Full,
    Extra,
}

impl From<SqliteSynchronousConfig> for SqliteSynchronous {
    fn from(mode: SqliteSynchronousConfig) -> Self {
        match mode {
            SqliteSynchronousConfig::Off => SqliteSynchronous::Off,
            SqliteSynchronousConfig::Normal => SqliteSynchronous::Normal,
            SqliteSynchronousConfig::Full => SqliteSynchronous::Full,
            SqliteSynchronousConfig::Extra => SqliteSynchronous::Extra,
        }
    }
}

/// Mirror of [`SqliteLockingMode`] with a serde surface (sqlx's enum has none).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SqliteLockingModeConfig {
    Normal,
    Exclusive,
}

impl From<SqliteLockingModeConfig> for SqliteLockingMode {
    fn from(mode: SqliteLockingModeConfig) -> Self {
        match mode {
            SqliteLockingModeConfig::Normal => SqliteLockingMode::Normal,
            SqliteLockingModeConfig::Exclusive => SqliteLockingMode::Exclusive,
        }
    }
}

/// Mirror of [`SqliteAutoVacuum`] with a serde surface (sqlx's enum has none).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SqliteAutoVacuumConfig {
    None,
    Full,
    Incremental,
}

impl From<SqliteAutoVacuumConfig> for SqliteAutoVacuum {
    fn from(mode: SqliteAutoVacuumConfig) -> Self {
        match mode {
            SqliteAutoVacuumConfig::None => SqliteAutoVacuum::None,
            SqliteAutoVacuumConfig::Full => SqliteAutoVacuum::Full,
            SqliteAutoVacuumConfig::Incremental => SqliteAutoVacuum::Incremental,
        }
    }
}

/// One `PRAGMA key = value` applied on connect, in list order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PragmaConfig {
    pub key: String,
    pub value: String,
}

/// A SQLite extension loaded on connect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqliteExtensionConfig {
    pub name: String,
    #[serde(default)]
    pub entrypoint: Option<String>,
}

/// SQLite connect options. Unset optional fields leave sqlx's own defaults in
/// place.
#[serde_with::serde_as]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct SqliteConfig {
    /// Database file path; unset it only with `in_memory`.
    pub path: Option<PathBuf>,
    /// Use an in-memory database.
    pub in_memory: bool,
    /// Create the database file when missing, defaults to `true`. Only the
    /// read-write pool creates; the read-only pool never does.
    pub create_if_missing: bool,
    /// Enforce foreign keys, defaults to `true`.
    pub foreign_keys: bool,
    /// Journal mode, defaults to `wal`.
    pub journal_mode: SqliteJournalModeConfig,
    /// Busy handler timeout in seconds, defaults to `10`.
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    pub busy_timeout: Duration,
    /// Shared-cache mode.
    pub shared_cache: bool,
    /// Locking mode.
    pub locking_mode: Option<SqliteLockingModeConfig>,
    /// Synchronous mode.
    pub synchronous: Option<SqliteSynchronousConfig>,
    /// Auto-vacuum mode.
    pub auto_vacuum: Option<SqliteAutoVacuumConfig>,
    /// Page size in bytes.
    pub page_size: Option<u32>,
    /// Prepared-statement cache size per connection.
    pub statement_cache_capacity: Option<usize>,
    /// Additional pragmas applied on connect, in order.
    pub pragma: Vec<PragmaConfig>,
    /// Open the database as immutable.
    pub immutable: bool,
    /// Serialized threading mode.
    pub serialized: bool,
    /// Worker command channel size.
    pub command_buffer_size: Option<usize>,
    /// Row fetch channel size.
    pub row_buffer_size: Option<usize>,
    /// VFS name.
    pub vfs: Option<String>,
    /// Extensions loaded on connect.
    pub extensions: Vec<SqliteExtensionConfig>,
    /// Run `PRAGMA optimize` when a connection closes.
    pub optimize_on_close: bool,
    /// `analysis_limit` for `optimize_on_close`.
    pub analysis_limit: Option<u32>,
    /// Statement logging.
    #[serde(flatten)]
    pub log: StatementLogConfig,
}

impl Default for SqliteConfig {
    fn default() -> Self {
        Self {
            path: None,
            in_memory: false,
            create_if_missing: true,
            foreign_keys: true,
            journal_mode: SqliteJournalModeConfig::default(),
            busy_timeout: Duration::from_secs(10),
            shared_cache: false,
            locking_mode: None,
            synchronous: None,
            auto_vacuum: None,
            page_size: None,
            statement_cache_capacity: None,
            pragma: Vec::new(),
            immutable: false,
            serialized: false,
            command_buffer_size: None,
            row_buffer_size: None,
            vfs: None,
            extensions: Vec::new(),
            optimize_on_close: false,
            analysis_limit: None,
            log: StatementLogConfig::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlite_config_minimal_defaults() {
        let cfg: SqliteConfig = serde_yaml::from_str("path: /tmp/test.db").unwrap();
        assert_eq!(cfg.path.as_deref(), Some("/tmp/test.db".as_ref()));
        assert!(cfg.create_if_missing);
        assert!(cfg.foreign_keys);
        assert_eq!(cfg.journal_mode, SqliteJournalModeConfig::Wal);
        assert_eq!(cfg.busy_timeout, Duration::from_secs(10));
        assert!(!cfg.in_memory);
    }

    #[test]
    fn sqlite_config_full_surface() {
        let yaml = "
path: /tmp/full.db
create_if_missing: false
foreign_keys: false
journal_mode: memory
busy_timeout: 3
shared_cache: true
locking_mode: exclusive
synchronous: extra
auto_vacuum: incremental
page_size: 8192
statement_cache_capacity: 64
pragma:
  - key: cache_size
    value: '-2000'
  - key: temp_store
    value: memory
immutable: true
serialized: true
command_buffer_size: 128
row_buffer_size: 512
vfs: unix-dotfile
extensions:
  - name: vector0
  - name: crypto
    entrypoint: sqlite3_crypto_init
optimize_on_close: true
analysis_limit: 400
";
        let cfg: SqliteConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(!cfg.create_if_missing);
        assert!(!cfg.foreign_keys);
        assert_eq!(cfg.journal_mode, SqliteJournalModeConfig::Memory);
        assert_eq!(cfg.busy_timeout, Duration::from_secs(3));
        assert!(cfg.shared_cache);
        assert_eq!(cfg.locking_mode, Some(SqliteLockingModeConfig::Exclusive));
        assert_eq!(cfg.synchronous, Some(SqliteSynchronousConfig::Extra));
        assert_eq!(cfg.auto_vacuum, Some(SqliteAutoVacuumConfig::Incremental));
        assert_eq!(cfg.page_size, Some(8192));
        assert_eq!(cfg.statement_cache_capacity, Some(64));
        assert_eq!(cfg.pragma.len(), 2);
        assert_eq!(cfg.pragma[0].key, "cache_size");
        assert_eq!(cfg.pragma[0].value, "-2000");
        assert_eq!(cfg.pragma[1].key, "temp_store");
        assert!(cfg.immutable);
        assert!(cfg.serialized);
        assert_eq!(cfg.command_buffer_size, Some(128));
        assert_eq!(cfg.row_buffer_size, Some(512));
        assert_eq!(cfg.vfs.as_deref(), Some("unix-dotfile"));
        assert_eq!(cfg.extensions.len(), 2);
        assert_eq!(cfg.extensions[0].name, "vector0");
        assert!(cfg.extensions[0].entrypoint.is_none());
        assert_eq!(
            cfg.extensions[1].entrypoint.as_deref(),
            Some("sqlite3_crypto_init")
        );
        assert!(cfg.optimize_on_close);
        assert_eq!(cfg.analysis_limit, Some(400));
    }

    #[test]
    fn sqlite_off_modes_parse_as_strings() {
        let cfg: SqliteConfig =
            serde_yaml::from_str("journal_mode: off\nsynchronous: off").unwrap();
        assert_eq!(cfg.journal_mode, SqliteJournalModeConfig::Off);
        assert_eq!(cfg.synchronous, Some(SqliteSynchronousConfig::Off));
    }

    #[test]
    fn sqlite_bad_journal_mode_errors() {
        assert!(serde_yaml::from_str::<SqliteConfig>("journal_mode: rollback").is_err());
    }
}
