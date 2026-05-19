//
// Copyright 2026 Tabs Data Inc.
//

//! Database connection configuration.
//!
//! [`BackendConfig`] picks one of the supported backends (sqlite, postgres,
//! embedded postgres, mysql); each has its own per-backend config struct in
//! a submodule. [`DatabaseConfig`] bundles that with [`PoolConfig`] for the
//! sqlx pool settings.

#[cfg(all(postgres, feature = "embedded-postgres"))]
pub mod embedded;
#[cfg(mysql)]
pub mod mysql;
#[cfg(postgres)]
pub mod postgres;
#[cfg(sqlite)]
pub mod sqlite;

#[cfg(all(postgres, feature = "embedded-postgres"))]
pub use embedded::EmbeddedConfig;
#[cfg(mysql)]
pub use mysql::{MySqlConfig, MySqlUrl};
#[cfg(postgres)]
pub use postgres::{PostgresConfig, PostgresUrl};
#[cfg(sqlite)]
pub use sqlite::{SqliteConfig, SqliteUrl};

use crate::dynamic::DynamicPool;
#[cfg(all(postgres, feature = "embedded-postgres"))]
use crate::embedded::EmbeddedPostgres;
use crate::error::DbError;
use serde::{Deserialize, Serialize};
use serde_with::DurationSeconds;
use serde_with::serde_as;
use sqlx::any::AnyConnectOptions;
use sqlx::pool::PoolOptions;
use sqlx::{ConnectOptions, Database};
#[cfg(sqlite)]
use std::fs;
#[cfg(all(postgres, feature = "embedded-postgres"))]
use std::path::PathBuf;
use std::time::Duration;
use tracing::debug;
use tracing::log::LevelFilter;
#[cfg(sqlite)]
use tracing::warn;
use url::Url;

const SLOW_QUERIES_THRESHOLD: u64 = 5000;

// ── BackendConfig enum ────────────────────────────────────────────────

/// Database connection configuration. Each variant pairs a URL (or, for
/// embedded postgres, the boot knobs) with the backend-specific config.
///
/// The backend is determined by which config key is present (`sqlite`,
/// `postgres`, or `mysql`). When none is present, the backend is inferred
/// from the URL scheme.
///
/// ```yaml
/// url: file:///tmp/app.db
/// sqlite:
///   journal_mode: wal
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum BackendConfig {
    #[cfg(sqlite)]
    Sqlite(SqliteConfig),
    #[cfg(postgres)]
    Postgres(PostgresConfig),
    /// In-process Postgres booted by [`crate::embedded::start`] at connect
    /// time. Mutually exclusive with [`Self::Postgres`] — pick external
    /// (URL set) or embedded (boot knobs set), not both.
    #[cfg(all(postgres, feature = "embedded-postgres"))]
    EmbeddedPostgres(EmbeddedConfig),
    #[cfg(mysql)]
    MySql(MySqlConfig),
}

impl BackendConfig {
    /// The configured URL. `None` for [`Self::EmbeddedPostgres`], since the
    /// URL isn't known until the server boots. Query the live URL via
    /// [`crate::pool::TdPool::url`] instead.
    pub fn url(&self) -> Option<&Url> {
        match self {
            #[cfg(sqlite)]
            Self::Sqlite(c) => Some(&c.url),
            #[cfg(postgres)]
            Self::Postgres(c) => Some(&c.url),
            #[cfg(all(postgres, feature = "embedded-postgres"))]
            Self::EmbeddedPostgres(_) => None,
            #[cfg(mysql)]
            Self::MySql(c) => Some(&c.url),
        }
    }

    /// Build the embedded-postgres variant from boot knobs.
    #[cfg(all(postgres, feature = "embedded-postgres"))]
    pub fn embedded_postgres(cfg: EmbeddedConfig) -> Self {
        Self::EmbeddedPostgres(cfg)
    }

    /// Persistent embedded postgres rooted at `data_dir`, optionally on a
    /// fixed `port`. The apiserver's no-URL fallback uses this so callers
    /// don't need to import [`EmbeddedConfig`].
    #[cfg(all(postgres, feature = "embedded-postgres"))]
    pub fn embedded_postgres_persistent(data_dir: Option<PathBuf>, port: Option<u16>) -> Self {
        Self::embedded_postgres(EmbeddedConfig {
            data_dir,
            port,
            persistent: true,
        })
    }

    /// Infer the backend variant from the URL scheme.
    pub fn from_url(url: Url) -> Result<Self, DbError> {
        #[cfg(sqlite)]
        if let Ok(sqlite_url) = SqliteUrl::try_from(url.clone()) {
            return Ok(Self::Sqlite(SqliteConfig { url: sqlite_url }));
        }
        #[cfg(postgres)]
        if let Ok(pg_url) = PostgresUrl::try_from(url.clone()) {
            return Ok(Self::Postgres(PostgresConfig { url: pg_url }));
        }
        #[cfg(mysql)]
        if let Ok(my_url) = MySqlUrl::try_from(url.clone()) {
            return Ok(Self::MySql(MySqlConfig { url: my_url }));
        }
        Err(DbError::UnsupportedDatabase(url.scheme().to_string()))
    }

    /// Replace the URL, preserving backend-specific config when the backend
    /// stays the same, or resetting to defaults when the backend changes.
    /// Setting an explicit URL on the embedded-postgres variant switches it
    /// to external postgres.
    pub fn with_url(self, url: Url) -> Result<Self, DbError> {
        let new = Self::from_url(url)?;
        Ok(match (self, new) {
            #[cfg(sqlite)]
            (Self::Sqlite(_), Self::Sqlite(new)) => {
                debug!("URL changed, preserving sqlite backend");
                Self::Sqlite(new)
            }
            #[cfg(postgres)]
            (Self::Postgres(_), Self::Postgres(new)) => {
                debug!("URL changed, preserving postgres backend");
                Self::Postgres(new)
            }
            #[cfg(mysql)]
            (Self::MySql(_), Self::MySql(new)) => {
                debug!("URL changed, preserving mysql backend");
                Self::MySql(new)
            }
            #[allow(unreachable_patterns)]
            (old, new) => {
                debug!("Backend changed from {old:?} to {new:?}, config reset to defaults");
                new
            }
        })
    }
}

// ── Pool config ─────────────────────────────────────────────────────────

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

// ── DatabaseConfig ─────────────────────────────────────────────────────

/// Result of [`DatabaseConfig::connect`]. Bundles the sqlx pool with the
/// URL it actually connected to and the embedded postgres handle when one
/// was booted; `TdPool` parks the handle under `Arc`.
pub struct ConnectOutput {
    pub pool: DynamicPool,
    pub url: Url,
    #[cfg(feature = "embedded-postgres")]
    pub embedded: Option<EmbeddedPostgres>,
}

/// Configuration for a database connection pool.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DatabaseConfig {
    /// The database connection configuration.
    #[serde(default, flatten)]
    pub conn: Option<BackendConfig>,
    /// Connection pool settings.
    #[serde(default, flatten)]
    pub pool: PoolConfig,
}

impl DatabaseConfig {
    pub fn url(&self) -> Option<&Url> {
        self.conn.as_ref().and_then(|c| c.url())
    }

    pub fn new<P: Into<Url>>(db_url: P) -> Result<Self, DbError> {
        let config = Self {
            conn: Some(BackendConfig::from_url(db_url.into())?),
            ..Default::default()
        };
        Ok(config)
    }

    /// Connect using this configuration. For the embedded-postgres variant,
    /// boots the in-process server first and returns the live handle in
    /// `ConnectOutput.embedded`.
    pub async fn connect(&self) -> Result<ConnectOutput, DbError> {
        if self.pool.max_connections == u32::MIN {
            Err(DbError::InvalidMaxConnections(self.pool.max_connections))?
        }
        if self.pool.min_connections > self.pool.max_connections {
            Err(DbError::InvalidConnections(
                self.pool.max_connections,
                self.pool.min_connections,
            ))?
        }

        let conn_config = self.conn.as_ref().ok_or(DbError::MissingDatabaseLocation)?;

        match conn_config {
            #[cfg(sqlite)]
            BackendConfig::Sqlite(c) => {
                Self::try_create_missing_dir(&c.url);
                let conn_options = self.conn_options(&c.url)?;
                let (rw, ro) = (
                    // important to create rw first so create_if_missing kicks in
                    self.connect_sqlite(&conn_options, false).await?,
                    self.connect_sqlite(&conn_options, true).await?,
                );
                Ok(ConnectOutput {
                    pool: DynamicPool::Sqlite { ro, rw },
                    url: (*c.url).clone(),
                    #[cfg(feature = "embedded-postgres")]
                    embedded: None,
                })
            }
            #[cfg(postgres)]
            BackendConfig::Postgres(c) => {
                let url = (*c.url).clone();
                let conn_options = self.conn_options(&url)?;
                let (rw, ro) = tokio::try_join!(
                    self.connect_postgres(&conn_options, false),
                    self.connect_postgres(&conn_options, true),
                )?;
                Ok(ConnectOutput {
                    pool: DynamicPool::Postgres { ro, rw },
                    url,
                    #[cfg(feature = "embedded-postgres")]
                    embedded: None,
                })
            }
            #[cfg(all(postgres, feature = "embedded-postgres"))]
            BackendConfig::EmbeddedPostgres(ecfg) => {
                let (pg, url) = crate::embedded::start(ecfg).await?;
                tracing::info!("Embedded postgres listening at {}", url);
                let conn_options = self.conn_options(&url)?;
                let (rw, ro) = tokio::try_join!(
                    self.connect_postgres(&conn_options, false),
                    self.connect_postgres(&conn_options, true),
                )?;
                Ok(ConnectOutput {
                    pool: DynamicPool::Postgres { ro, rw },
                    url,
                    embedded: Some(pg),
                })
            }
            #[cfg(mysql)]
            BackendConfig::MySql(c) => {
                let conn_options = self.conn_options(&c.url)?;
                let (rw, ro) = tokio::try_join!(
                    self.connect_mysql(&conn_options, false),
                    self.connect_mysql(&conn_options, true),
                )?;
                Ok(ConnectOutput {
                    pool: DynamicPool::MySql { ro, rw },
                    url: (*c.url).clone(),
                    #[cfg(feature = "embedded-postgres")]
                    embedded: None,
                })
            }
        }
    }

    fn conn_options(&self, url: &Url) -> Result<AnyConnectOptions, DbError> {
        let conn_options = AnyConnectOptions::from_url(url)
            .map_err(DbError::FailedToConnectToDatabase)?
            .log_statements(self.pool.log_level)
            .log_slow_statements(
                LevelFilter::Warn,
                Duration::from_millis(SLOW_QUERIES_THRESHOLD),
            );
        Ok(conn_options)
    }

    fn pool_options<DB: Database>(&self, read_only: bool) -> PoolOptions<DB> {
        PoolOptions::new()
            .min_connections(if read_only {
                self.pool.min_connections
            } else {
                1
            })
            .max_connections(if read_only {
                self.pool.max_connections
            } else {
                1
            })
            .acquire_timeout(self.pool.acquire_timeout)
            .max_lifetime(self.pool.max_lifetime)
            .idle_timeout(self.pool.idle_timeout)
            .test_before_acquire(self.pool.test_before_acquire)
    }

    #[cfg(sqlite)]
    fn try_create_missing_dir(url: &Url) {
        if let Ok(path) = url.to_file_path()
            && let Some(parent) = path.parent()
            && !parent.exists()
        {
            if let Err(err) = fs::create_dir_all(parent) {
                warn!("Failed to create database directory: {parent:?}. Error: {err}");
            } else {
                debug!("Created database directory: {parent:?}");
            }
        }
    }

    #[cfg(sqlite)]
    async fn connect_sqlite(
        &self,
        conn_options: &AnyConnectOptions,
        read_only: bool,
    ) -> Result<sqlx::SqlitePool, sqlx::Error> {
        let conn_options = sqlx::sqlite::SqliteConnectOptions::try_from(conn_options)?
            .create_if_missing(!read_only)
            .busy_timeout(Duration::from_secs(10))
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .read_only(read_only);
        debug!("Database(read_only:{read_only}). Connection options: {conn_options:#?}");

        let pool_options = self.pool_options(read_only);
        debug!("Database(read_only:{read_only}). Pool options: {pool_options:#?}");

        let pool = pool_options.connect_with(conn_options).await?;
        Ok(pool)
    }

    #[cfg(postgres)]
    async fn connect_postgres(
        &self,
        conn_options: &AnyConnectOptions,
        read_only: bool,
    ) -> Result<sqlx::PgPool, sqlx::Error> {
        let conn_options = sqlx::postgres::PgConnectOptions::try_from(conn_options)?;
        debug!("Database(read_only:{read_only}). Connection options: {conn_options:#?}");

        let pool_options = self
            .pool_options(read_only)
            .after_connect(move |conn, _meta| {
                Box::pin(async move {
                    if read_only {
                        sqlx::query("SET default_transaction_read_only = on")
                            .execute(&mut *conn)
                            .await?;
                    }

                    Ok(())
                })
            });
        debug!("Database(read_only:{read_only}). Pool options: {pool_options:#?}");

        let pool = pool_options.connect_with(conn_options).await?;
        Ok(pool)
    }

    #[cfg(mysql)]
    async fn connect_mysql(
        &self,
        conn_options: &AnyConnectOptions,
        read_only: bool,
    ) -> Result<sqlx::MySqlPool, sqlx::Error> {
        let conn_options = sqlx::mysql::MySqlConnectOptions::try_from(conn_options)?;
        debug!("Database(read_only:{read_only}). Connection options: {conn_options:#?}");

        let pool_options = self
            .pool_options(read_only)
            .after_connect(move |conn, _meta| {
                Box::pin(async move {
                    if read_only {
                        sqlx::query("SET SESSION TRANSACTION READ ONLY")
                            .execute(&mut *conn)
                            .await?;
                    }

                    Ok(())
                })
            });
        debug!("Database(read_only:{read_only}). Pool options: {pool_options:#?}");

        let pool = pool_options.connect_with(conn_options).await?;
        Ok(pool)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── BackendConfig round-trip ──────────────────────────────────────

    #[cfg(sqlite)]
    #[test]
    fn conn_sqlite_explicit() {
        let yaml = "url: file:///tmp/test.db";
        let conn: BackendConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(conn, BackendConfig::Sqlite(_)));
        assert_eq!(conn.url().unwrap().as_str(), "file:///tmp/test.db");
    }

    #[cfg(sqlite)]
    #[test]
    fn conn_sqlite_round_trip() {
        let conn = BackendConfig::from_url(Url::parse("file:///tmp/test.db").unwrap()).unwrap();
        let yaml = serde_yaml::to_string(&conn).unwrap();
        let conn2: BackendConfig = serde_yaml::from_str(&yaml).unwrap();
        assert!(matches!(conn2, BackendConfig::Sqlite(_)));
        assert_eq!(conn.url(), conn2.url());
    }

    #[cfg(postgres)]
    #[test]
    fn conn_postgres_round_trip() {
        let conn =
            BackendConfig::from_url(Url::parse("postgres://user:pass@localhost/mydb").unwrap())
                .unwrap();
        let yaml = serde_yaml::to_string(&conn).unwrap();
        let conn2: BackendConfig = serde_yaml::from_str(&yaml).unwrap();
        assert!(matches!(conn2, BackendConfig::Postgres(_)));
        assert_eq!(conn.url(), conn2.url());
    }

    #[cfg(mysql)]
    #[test]
    fn conn_mysql_round_trip() {
        let conn = BackendConfig::from_url(Url::parse("mysql://user:pass@localhost/mydb").unwrap())
            .unwrap();
        let yaml = serde_yaml::to_string(&conn).unwrap();
        let conn2: BackendConfig = serde_yaml::from_str(&yaml).unwrap();
        assert!(matches!(conn2, BackendConfig::MySql(_)));
        assert_eq!(conn.url(), conn2.url());
    }

    #[cfg(sqlite)]
    #[test]
    fn conn_infer_sqlite_from_url() {
        let yaml = "url: file:///tmp/inferred.db";
        let conn: BackendConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(conn, BackendConfig::Sqlite(_)));
        assert_eq!(conn.url().unwrap().as_str(), "file:///tmp/inferred.db");
    }

    #[cfg(postgres)]
    #[test]
    fn conn_infer_postgres_from_url() {
        let yaml = "url: postgres://user:pass@localhost/mydb";
        let conn: BackendConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(conn, BackendConfig::Postgres(_)));
    }

    #[cfg(mysql)]
    #[test]
    fn conn_infer_mysql_from_url() {
        let yaml = "url: mysql://user:pass@localhost/mydb";
        let conn: BackendConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(conn, BackendConfig::MySql(_)));
    }

    #[test]
    fn conn_unknown_scheme_errors() {
        let yaml = "url: oracle://localhost/mydb";
        let result = serde_yaml::from_str::<BackendConfig>(yaml);
        assert!(result.is_err());
    }

    /// `sqlite: {}` has no `url`, so deserialization fails. Empty bodies are
    /// rejected by serde when no untagged variant accepts them.
    #[cfg(all(sqlite, not(postgres)))]
    #[test]
    fn conn_missing_url_errors() {
        let yaml = "sqlite: {}";
        let result = serde_yaml::from_str::<BackendConfig>(yaml);
        assert!(result.is_err());
    }

    // ── DatabaseConfig ────────────────────────────────────────────────

    #[cfg(sqlite)]
    #[test]
    fn database_config_conn_inferred() {
        let yaml = "
url: file:///tmp/test.db
min_connections: 2
max_connections: 20
";
        let cfg: DatabaseConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(cfg.conn, Some(BackendConfig::Sqlite(_))));
        assert_eq!(
            cfg.conn.as_ref().unwrap().url().unwrap().as_str(),
            "file:///tmp/test.db"
        );
        assert_eq!(cfg.pool.min_connections, 2);
        assert_eq!(cfg.pool.max_connections, 20);
        // defaults kick in for unset fields
        assert_eq!(cfg.pool.acquire_timeout, Duration::from_secs(30));
        assert!(cfg.pool.test_before_acquire);
    }

    #[cfg(all(postgres, not(sqlite)))]
    #[test]
    fn database_config_conn_inferred() {
        let yaml = "
url: postgres://user:pass@localhost/mydb
min_connections: 2
max_connections: 20
";
        let cfg: DatabaseConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(cfg.conn, Some(BackendConfig::Postgres(_))));
        assert_eq!(cfg.pool.min_connections, 2);
        assert_eq!(cfg.pool.max_connections, 20);
        assert_eq!(cfg.pool.acquire_timeout, Duration::from_secs(30));
        assert!(cfg.pool.test_before_acquire);
    }

    /// An empty body matches [`BackendConfig::EmbeddedPostgres`] when
    /// `embedded-postgres` is enabled (all three of its fields default).
    /// Otherwise no variant matches and `conn` parses as `None`.
    #[test]
    fn database_config_no_conn() {
        let yaml = "
max_connections: 5
";
        let cfg: DatabaseConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.pool.max_connections, 5);
        assert_eq!(cfg.pool.min_connections, 1);
    }

    #[test]
    fn database_config_log_level() {
        let yaml = "log_level: off";
        let cfg: DatabaseConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.pool.log_level, LevelFilter::Off);

        let yaml = r#"log_level: "off""#;
        let cfg: DatabaseConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.pool.log_level, LevelFilter::Off);

        let yaml = "log_level: trace";
        let cfg: DatabaseConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.pool.log_level, LevelFilter::Trace);

        let yaml = "log_level: dEbuG";
        let cfg: DatabaseConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.pool.log_level, LevelFilter::Debug);
    }

    #[test]
    fn database_config_empty_yaml() {
        let cfg: DatabaseConfig = serde_yaml::from_str("{}").unwrap();
        assert_eq!(cfg.pool.min_connections, 1);
        assert_eq!(cfg.pool.max_connections, 10);
    }

    #[test]
    fn database_config_round_trip() {
        #[cfg(sqlite)]
        let url = Url::parse("file:///tmp/rt.db").unwrap();
        #[cfg(all(postgres, not(sqlite)))]
        let url = Url::parse("postgres://user:pass@localhost/rt").unwrap();

        let original = DatabaseConfig {
            conn: Some(BackendConfig::from_url(url).unwrap()),
            pool: PoolConfig {
                min_connections: 3,
                max_connections: 15,
                ..Default::default()
            },
        };
        let yaml = serde_yaml::to_string(&original).unwrap();
        let restored: DatabaseConfig = serde_yaml::from_str(&yaml).unwrap();

        assert_eq!(
            original.conn.as_ref().unwrap().url().unwrap().as_str(),
            restored.conn.as_ref().unwrap().url().unwrap().as_str()
        );
        assert_eq!(restored.pool.min_connections, 3);
        assert_eq!(restored.pool.max_connections, 15);
    }

    // ── PoolConfig defaults ───────────────────────────────────────────

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
