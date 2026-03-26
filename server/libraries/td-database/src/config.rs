//
// Copyright 2026 Tabs Data Inc.
//

use crate::dynamic::DynamicPool;
use crate::error::DbError;
use serde::{Deserialize, Serialize};
use serde_with::DurationSeconds;
use serde_with::serde_as;
use sqlx::any::AnyConnectOptions;
use sqlx::pool::PoolOptions;
use sqlx::{ConnectOptions, Database};
use std::fs;
use std::str::FromStr;
use std::time::Duration;
use tracing::log::LevelFilter;
use tracing::{debug, info, warn};
use url::Url;

const SLOW_QUERIES_THRESHOLD: u64 = 5000;

/// Configuration for a database.
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    /// The database URI.
    #[serde(default)]
    pub url: Option<Url>,
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
    #[serde(default = "log_level")]
    pub log_level: String,
    /// Database specifics
    pub sqlite: Option<SqliteConfig>,
    pub postgres: Option<PostgresConfig>,
    pub mysql: Option<MySqlConfig>,
}

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

fn log_level() -> String {
    LevelFilter::Off.to_string()
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SqliteConfig {}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PostgresConfig {}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MySqlConfig {}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            url: None,
            min_connections: min_connections(),
            max_connections: max_connections(),
            acquire_timeout: acquire_timeout(),
            max_lifetime: max_lifetime(),
            idle_timeout: idle_timeout(),
            test_before_acquire: test_before_acquire(),
            log_level: log_level(),
            sqlite: Default::default(),
            postgres: Default::default(),
            mysql: Default::default(),
        }
    }
}

impl DatabaseConfig {
    pub fn new<P: Into<Url>>(db_url: P) -> Self {
        Self {
            url: Some(db_url.into()),
            min_connections: min_connections(),
            max_connections: max_connections(),
            acquire_timeout: acquire_timeout(),
            max_lifetime: max_lifetime(),
            idle_timeout: idle_timeout(),
            test_before_acquire: test_before_acquire(),
            log_level: log_level(),
            sqlite: Default::default(),
            postgres: Default::default(),
            mysql: Default::default(),
        }
    }

    pub async fn connect(&self) -> Result<DynamicPool, DbError> {
        if self.max_connections <= 0 {
            Err(DbError::InvalidMaxConnections(self.max_connections))?
        }
        if self.min_connections > self.max_connections {
            Err(DbError::InvalidConnections(
                self.max_connections,
                self.min_connections,
            ))?
        }

        let url = self.url.as_ref().ok_or(DbError::MissingDatabaseLocation)?;
        let conn_options = self.conn_options(url)?;

        match url.scheme() {
            #[cfg(feature = "sqlite")]
            s if sqlx::Sqlite::URL_SCHEMES.contains(&s) || s.eq_ignore_ascii_case("file") => {
                Self::try_create_missing_dir(url.clone());
                let (rw, ro) = (
                    // important to create rw first so create_if_missing kicks in
                    self.connect_sqlite(&conn_options, false).await?,
                    self.connect_sqlite(&conn_options, true).await?,
                );
                Ok(DynamicPool::Sqlite { ro, rw })
            }
            #[cfg(feature = "postgres")]
            s if sqlx::Postgres::URL_SCHEMES.contains(&s) => {
                let (rw, ro) = (
                    self.connect_postgres(&conn_options, false).await?,
                    self.connect_postgres(&conn_options, true).await?,
                );
                Ok(DynamicPool::Postgres { ro, rw })
            }
            #[cfg(feature = "mysql")]
            s if sqlx::MySql::URL_SCHEMES.contains(&s) => {
                let (rw, ro) = (
                    self.connect_mysql(&conn_options, false).await?,
                    self.connect_mysql(&conn_options, true).await?,
                );
                Ok(DynamicPool::MySql { ro, rw })
            }
            _ => Err(DbError::UnsupportedDatabase(url.scheme().to_string())),
        }
    }

    fn conn_options(&self, url: &Url) -> Result<AnyConnectOptions, DbError> {
        let conn_options = AnyConnectOptions::from_url(url)
            .map_err(DbError::FailedToConnectToDatabase)?
            .log_statements({
                match LevelFilter::from_str(&self.log_level) {
                    Ok(level) => {
                        info!("Using sqlx log level: {level}");
                        level
                    }
                    Err(_) => {
                        warn!(
                            "Invalid sqlx log level configured: {}. Disabling logging.",
                            self.log_level
                        );
                        LevelFilter::Off
                    }
                }
            })
            .log_slow_statements(
                LevelFilter::Warn,
                Duration::from_millis(SLOW_QUERIES_THRESHOLD),
            );
        Ok(conn_options)
    }

    fn pool_options<DB: Database>(&self, read_only: bool) -> PoolOptions<DB> {
        PoolOptions::new()
            .min_connections(if read_only { self.min_connections } else { 1 })
            .max_connections(if read_only { self.max_connections } else { 1 })
            .acquire_timeout(self.acquire_timeout)
            .max_lifetime(self.max_lifetime)
            .idle_timeout(self.idle_timeout)
            .test_before_acquire(self.test_before_acquire)
    }

    #[cfg(feature = "sqlite")]
    fn try_create_missing_dir(url: Url) {
        if let Ok(path) = url.to_file_path() {
            if let Some(parent) = path.parent() {
                if !parent.exists() {
                    if let Err(err) = fs::create_dir_all(parent) {
                        warn!("Failed to create database directory: {parent:?}. Error: {err}");
                    } else {
                        debug!("Created database directory: {parent:?}");
                    }
                }
            }
        }
    }

    #[cfg(feature = "sqlite")]
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

    #[cfg(feature = "postgres")]
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

    #[cfg(feature = "mysql")]
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
        debug!("Database(read_only:{read_only}). Pool options:. {pool_options:#?}");

        let pool = pool_options.connect_with(conn_options).await?;
        Ok(pool)
    }
}
