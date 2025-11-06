//
// Copyright 2025 Tabs Data Inc.
//

use derive_builder::Builder;
use futures_util::future::BoxFuture;
use futures_util::stream::BoxStream;
use itertools::Either;
use regex::Regex;
use serde::{Deserialize, Serialize};
use sqlx::migrate::{MigrateError, Migrator};
use sqlx::pool::PoolConnection;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions, SqliteRow};
use sqlx::{
    ConnectOptions, Database, Describe, Error, Execute, Executor, FromRow, Pool, Sqlite,
    Transaction,
};
use std::cmp::Ordering;
use std::fmt::Display;
use std::future::Future;
use std::time::Duration;
use td_error::td_error;
use td_schema::{DB_EDITION_NAME, DB_VERSION_NAME, DB_VERSION_VALUE};
use te_system::edition::{Compatible, Edition, TabsdataEdition};
use tracing::log::LevelFilter;

const SLOW_QUERIES_THRESHOLD: u64 = 5000;
const PRAGMA_TEMP_STORE: (&str, &str) = ("temp_store", "MEMORY");

/// Configuration for a SQLite database.
#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
#[builder(default)]
pub struct SqliteConfig {
    /// The Sqlite URI, required.
    #[builder(setter(into))]
    pub url: Option<String>,
    /// The minimum number of database connections, defaults to `1`.
    min_connections: u32,
    /// The maximum number of database connections, defaults to `10`.
    max_connections: u32,
    /// The maximum time to wait for a database connection to be acquired, defaults to `30 seconds`.
    acquire_timeout: u64,
    /// The maximum lifetime of a database connection, defaults to `60 minutes`.
    max_lifetime: u64,
    /// The maximum time a database connection can be idle, defaults to `60 seconds`.
    idle_timeout: u64,
    /// Whether to test the connection before acquiring it, defaults to `true`.
    test_before_acquire: bool,
}

impl Default for SqliteConfig {
    fn default() -> Self {
        SqliteConfig {
            url: None,
            min_connections: 1,
            max_connections: 10,
            acquire_timeout: 30,
            max_lifetime: 60 * 60,
            idle_timeout: 60,
            test_before_acquire: true,
        }
    }
}

impl SqliteConfig {
    pub fn acquire_timeout(&self) -> Duration {
        Duration::from_secs(self.acquire_timeout)
    }

    pub fn idle_timeout(&self) -> Duration {
        Duration::from_secs(self.idle_timeout)
    }

    pub fn max_lifetime(&self) -> Duration {
        Duration::from_secs(self.max_lifetime)
    }

    pub fn to_builder(&self) -> SqliteConfigBuilder {
        let mut builder = SqliteConfigBuilder::default();

        builder.url(self.url.clone());
        builder.min_connections(self.min_connections);
        builder.max_connections(self.max_connections);
        builder.acquire_timeout(self.acquire_timeout);
        builder.max_lifetime(self.max_lifetime);
        builder.idle_timeout(self.idle_timeout);
        builder.test_before_acquire(self.test_before_acquire);
        builder
    }

    pub fn rw_pool_options(&self) -> SqlitePoolOptions {
        SqlitePoolOptions::new()
            .min_connections(1)
            .max_connections(1)
            .acquire_timeout(self.acquire_timeout())
            .max_lifetime(self.max_lifetime())
            .idle_timeout(self.idle_timeout())
            .test_before_acquire(self.test_before_acquire)
    }

    pub fn ro_pool_options(&self) -> SqlitePoolOptions {
        SqlitePoolOptions::new()
            .min_connections(self.min_connections)
            .max_connections(self.max_connections)
            .acquire_timeout(self.acquire_timeout())
            .max_lifetime(self.max_lifetime())
            .idle_timeout(self.idle_timeout())
            .test_before_acquire(self.test_before_acquire)
    }
}

pub fn create_bindings_literal(offset: usize, bindings: usize) -> String {
    let mut s = String::with_capacity(bindings * 5);
    for i in offset + 1..=offset + bindings {
        //SQL uses base 1
        s.push('?');
        s.push_str(&i.to_string());
        if i < offset + bindings {
            s.push(',');
        }
    }
    s
}

/// A database schema. Alias for Sqlx's [`Migrator`].
///
/// Use sqlx-cli to define/maintain the database schema. DDL files should be created using
/// `sqlx migrate add -r <name>` command (Run 'cargo install sqlx-cli' in the dev environment
/// to install sqlx-cli).
pub type DbSchema = Migrator;

#[td_error]
pub enum DbError {
    #[error("Tabsdata database schema has not be created. It must be created")]
    DatabaseSchemaDoesNotExist = 5000,
    #[error("Tabsdata database must be upgraded: {0}")]
    DatabaseNeedsUpgrade(String) = 5001,
    #[error(
        "Tabsdata database version is '{0}', binary database version is '{1}'. Binary must be upgraded"
    )]
    DatabaseIsNewer(String, usize) = 5002,
    #[error("Tabsdata database corrupted. {0}")]
    DatabaseCorrupted(String) = 5003,
    #[error("Database location is missing in the given configuration")]
    MissingDatabaseLocation = 5004,
    #[error("Failed to check database existence: {0}")]
    FailedToCheckDatabaseExistence(#[source] Error) = 5005,
    #[error("Failed to create database: {0}")]
    FailedToCreateDatabase(#[source] Error) = 5006,
    #[error("Failed to connect to the database: {0}")]
    FailedToConnectToDatabase(#[source] Error) = 5007,
    #[error("Failed to create or upgrade the database: {0}")]
    FailedToCreateOrUpgradeDatabaseSchema(#[source] MigrateError) = 5008,
    #[error("Sql error: {0}")]
    SqlError(#[source] Error) = 5009,
    #[error("Database does not exist")]
    DatabaseDoesNotExist = 5010,
    #[error("Failed to create database directory {0}: {1}")]
    FailedToCreateDatabaseDir(String, #[source] std::io::Error) = 5011,
    #[error(
        "Tabsdata instance edition is '{0}', and binary edition is '{1}'. This instance cannot run with '{1}' edition"
    )]
    InvalidEdition(String, String) = 5012,
    #[error("Failed to create or upgrade the database tabsdata edition: {0}")]
    FailedToCreateOrUpgradeDatabaseEdition(#[source] Error) = 5013,
    #[error("Failed to upgrade the database tabsdata edition: {0}")]
    CannotUpgradeEdition(String) = 5014,
}

/// Sqlite database connection provider using Sqlx.
///
/// Databases are automatically created and their schema is upgraded if necessary
/// when the connection is created.
pub struct Db;

impl Db {
    /// Returns a database connection provider for a database with the given schema.
    pub fn schema() -> Self {
        Db
    }

    fn db_location_path(config: &SqliteConfig) -> Result<String, DbError> {
        let mut db_url = config
            .url
            .as_ref()
            .ok_or(DbError::MissingDatabaseLocation)?
            .to_string();

        db_url = remove_leading_file_protocol(&db_url);
        db_url = remove_leading_slash(&db_url);

        let dir = std::path::Path::new(&db_url).parent().unwrap();
        if !dir.exists() {
            std::fs::create_dir_all(dir).map_err(|err| {
                DbError::FailedToCreateDatabaseDir(dir.to_str().unwrap().to_string(), err)
            })?;
        }
        Ok(db_url)
    }

    async fn connect(
        &self,
        config: &SqliteConfig,
        read_only: bool,
    ) -> Result<Pool<Sqlite>, DbError> {
        let db_location = Self::db_location_path(config)?;

        let pool_options = if read_only {
            config.ro_pool_options()
        } else {
            config.rw_pool_options()
        };

        let db_options = SqliteConnectOptions::new()
            .filename(&db_location)
            .create_if_missing(!read_only)
            .busy_timeout(Duration::from_secs(10))
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .read_only(read_only)
            .log_slow_statements(
                LevelFilter::Warn,
                Duration::from_millis(SLOW_QUERIES_THRESHOLD),
            )
            .pragma(PRAGMA_TEMP_STORE.0, PRAGMA_TEMP_STORE.1);

        let db_options = if !cfg!(feature = "sqlx_log") {
            db_options.clone().log_statements(LevelFilter::Trace)
        } else {
            db_options
        };

        let pool = pool_options
            .connect_with(db_options)
            .await
            .map_err(DbError::FailedToConnectToDatabase)?;
        Ok(pool)
    }

    /// Connects to the database specified in the given configuration, if the database does not
    /// exist it creates it, if the schema is out of date, it upgrades it.
    ///
    /// Returns a RW Sqlx connection pool to the database. After this point using the database is
    /// vanilla Sqlx.
    pub async fn rw_pool(&self, config: &SqliteConfig) -> Result<Pool<Sqlite>, DbError> {
        Self::connect(self, config, false).await
    }

    /// Connects to the database specified in the given configuration, if the database does not
    /// exist fails.
    ///
    /// Returns a RO Sqlx connection pool to the database. After this point using the database is
    /// vanilla Sqlx.
    pub async fn ro_connect(&self, config: &SqliteConfig) -> Result<Pool<Sqlite>, DbError> {
        Self::connect(self, config, true).await
    }
}

#[derive(Debug, Clone)]
pub struct DbPool {
    pub schema: &'static DbSchema,
    pub ro_pool: Pool<Sqlite>,
    pub rw_pool: Pool<Sqlite>,
}

/// Specialized Sqlx Sqlite [`Pool`] that uses two pools, one for read-only operations and one for
/// read-write operations.
impl DbPool {
    /// Connects to a database using the given configuration.
    ///
    /// The schema is assumed to be up to date.
    pub async fn connect(
        config: &SqliteConfig,
        schema: &'static DbSchema,
    ) -> Result<Self, DbError> {
        let rw_pool = Db::schema().rw_pool(config).await?;
        let ro_pool = Db::schema().ro_connect(config).await?;
        Ok(Self {
            schema,
            ro_pool,
            rw_pool,
        })
    }

    /// Creates a database using the given configuration.
    ///
    /// Creates the schema.
    pub async fn create(config: &SqliteConfig, schema: &'static DbSchema) -> Result<Self, DbError> {
        let rw_pool = Db::schema().rw_pool(config).await?;
        let ro_pool = Db::schema().ro_connect(config).await?;
        let db = Self {
            schema,
            ro_pool,
            rw_pool,
        };
        db.upgrade().await?;
        Ok(db)
    }

    pub async fn check(&self) -> Result<(), DbError> {
        self.check_db_version().await?;
        self.check_tabsdata_edition().await?;
        Ok(())
    }

    pub async fn upgrade(&self) -> Result<(), DbError> {
        self.upgrade_db_version().await?;
        self.upgrade_tabsdata_edition().await?;
        Ok(())
    }

    fn map_system_db_error(err: Error, row: impl Display) -> DbError {
        match &err {
            Error::RowNotFound => DbError::DatabaseCorrupted(
                format!("Missing '{row}' row in 'tabsdata_system'").to_string(),
            ),
            Error::Database(database_err) => {
                if database_err.message().contains("no such table") {
                    DbError::DatabaseSchemaDoesNotExist
                } else {
                    DbError::SqlError(err)
                }
            }
            _ => DbError::SqlError(err),
        }
    }

    async fn check_db_version(&self) -> Result<(), DbError> {
        let res: String =
            sqlx::QueryBuilder::new("SELECT value FROM tabsdata_system WHERE name = ")
                .push_bind(DB_VERSION_NAME)
                .build_query_scalar()
                .fetch_one(&self.ro_pool)
                .await
                .map_err(|e| Self::map_system_db_error(e, DB_VERSION_NAME))?;
        let version = res.parse::<usize>().map_err(|_| {
            DbError::DatabaseCorrupted(format!(
                "'{}' value '{}' must be an integer",
                DB_VERSION_NAME, res
            ))
        })?;

        match version.cmp(&DB_VERSION_VALUE) {
            Ordering::Equal => Ok(()),
            Ordering::Less => Err(DbError::DatabaseNeedsUpgrade(format!(
                "Tabsdata database version is '{}', binary database version is '{}'",
                version, *DB_VERSION_VALUE
            ))),
            Ordering::Greater => Err(DbError::DatabaseIsNewer(
                version.to_string(),
                *DB_VERSION_VALUE,
            )),
        }
    }

    async fn upgrade_db_version(&self) -> Result<(), DbError> {
        self.schema
            .run(&self.rw_pool)
            .await
            .map_err(DbError::FailedToCreateOrUpgradeDatabaseSchema)?;
        Ok(())
    }

    async fn parse_tabsdata_edition(&self) -> Result<Option<String>, DbError> {
        sqlx::QueryBuilder::new("SELECT value FROM tabsdata_system WHERE name = ")
            .push_bind(DB_EDITION_NAME)
            .build_query_scalar()
            .fetch_optional(&self.ro_pool)
            .await
            .map_err(|e| Self::map_system_db_error(e, DB_EDITION_NAME))
    }

    async fn check_tabsdata_edition(&self) -> Result<(), DbError> {
        let instance_edition = self.parse_tabsdata_edition().await?;
        let runtime_edition = TabsdataEdition;

        match instance_edition {
            None => Err(DbError::DatabaseNeedsUpgrade(format!(
                "instance edition is undefined, binary edition is '{}'",
                runtime_edition.label(),
            ))),
            Some(instance_edition) => {
                if !runtime_edition.is_compatible(&instance_edition) {
                    Err(DbError::InvalidEdition(
                        instance_edition,
                        runtime_edition.label().to_string(),
                    ))
                } else if runtime_edition.requires_upgrade(&instance_edition) {
                    Err(DbError::DatabaseNeedsUpgrade(format!(
                        "instance edition is '{instance_edition}', binary edition is '{}'",
                        runtime_edition.label()
                    )))
                } else {
                    Ok(())
                }
            }
        }
    }

    async fn upgrade_tabsdata_edition(&self) -> Result<(), DbError> {
        let instance_edition = self.parse_tabsdata_edition().await?;
        let runtime_edition = TabsdataEdition;

        match instance_edition {
            None => {
                // if not set, set it to the runtime edition
                sqlx::QueryBuilder::new("INSERT INTO tabsdata_system values (")
                    .push_bind(DB_EDITION_NAME)
                    .push(", ")
                    .push_bind(runtime_edition.label())
                    .push(")")
                    .build()
                    .execute(&self.rw_pool)
                    .await
                    .map_err(DbError::FailedToCreateOrUpgradeDatabaseEdition)?;
                Ok(())
            }
            Some(instance_edition) => {
                if !runtime_edition.is_compatible(&instance_edition) {
                    Err(DbError::CannotUpgradeEdition(format!(
                        "instance edition is '{instance_edition}' cannot be upgraded to '{}'",
                        runtime_edition.label()
                    )))
                } else if runtime_edition.requires_upgrade(&instance_edition) {
                    sqlx::QueryBuilder::new("UPDATE tabsdata_system SET value = ")
                        .push_bind(runtime_edition.label())
                        .push(" WHERE name = ")
                        .push_bind(DB_EDITION_NAME)
                        .build()
                        .execute(&self.rw_pool)
                        .await
                        .map_err(DbError::FailedToCreateOrUpgradeDatabaseEdition)?;
                    Ok(())
                } else {
                    Ok(())
                }
            }
        }
    }

    /// Delegates to the read-only pool's [`Pool::acquire`] method.
    pub fn acquire(
        &self,
    ) -> impl Future<Output = Result<PoolConnection<Sqlite>, Error>> + 'static + use<> {
        self.ro_pool.acquire()
    }

    /// Delegates to the read-write pool's [`Pool::begin`] method.
    pub async fn begin(&self) -> Result<Transaction<'static, Sqlite>, Error> {
        self.rw_pool.begin().await
    }

    /// Returns if the pool is closed.
    pub fn is_closed(&self) -> bool {
        self.ro_pool.is_closed() && self.rw_pool.is_closed()
    }
}

impl From<&DbPool> for DbPool {
    fn from(db_pool: &DbPool) -> Self {
        db_pool.clone()
    }
}

/// Trait for types that can be fetched from a database row.
pub trait DbData: for<'a> FromRow<'a, SqliteRow> + Send + Unpin {}

/// [`DbPool`] implements Sqlx [`Executor`] so it can be used with sqlx API.
///
/// The implementation delegates to the corresponding methods of the read-only pool for
/// transactions with only read operations, and to the read-write pool for transactions
/// with read (optional) & write operations.
//TODO Joaquin please check lifetimes here
impl<'c> Executor<'c> for &'_ DbPool {
    type Database = Sqlite;

    fn execute<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxFuture<'e, Result<<Self::Database as Database>::QueryResult, Error>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Self::Database>,
    {
        self.rw_pool.execute(query)
    }

    fn execute_many<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxStream<'e, Result<<Self::Database as Database>::QueryResult, Error>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Self::Database>,
    {
        self.rw_pool.execute_many(query)
    }

    fn fetch<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxStream<'e, Result<<Self::Database as Database>::Row, Error>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Self::Database>,
    {
        self.ro_pool.fetch(query)
    }

    fn fetch_many<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxStream<
        'e,
        Result<
            Either<<Self::Database as Database>::QueryResult, <Self::Database as Database>::Row>,
            Error,
        >,
    >
    where
        'c: 'e,
        E: 'q + Execute<'q, Self::Database>,
    {
        self.ro_pool.fetch_many(query)
    }

    fn fetch_all<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxFuture<'e, Result<Vec<<Self::Database as Database>::Row>, Error>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Self::Database>,
    {
        self.ro_pool.fetch_all(query)
    }

    fn fetch_one<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxFuture<'e, Result<<Self::Database as Database>::Row, Error>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Self::Database>,
    {
        self.ro_pool.fetch_one(query)
    }

    fn fetch_optional<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxFuture<'e, Result<Option<<Self::Database as Database>::Row>, Error>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Self::Database>,
    {
        self.ro_pool.fetch_optional(query)
    }

    fn prepare<'e, 'q: 'e>(
        self,
        query: &'q str,
    ) -> BoxFuture<'e, Result<<Self::Database as Database>::Statement<'q>, Error>>
    where
        'c: 'e,
    {
        self.ro_pool.prepare(query)
    }

    fn prepare_with<'e, 'q: 'e>(
        self,
        sql: &'q str,
        parameters: &'e [<Self::Database as Database>::TypeInfo],
    ) -> BoxFuture<'e, Result<<Self::Database as Database>::Statement<'q>, Error>>
    where
        'c: 'e,
    {
        self.ro_pool.prepare_with(sql, parameters)
    }

    fn describe<'e, 'q: 'e>(
        self,
        sql: &'q str,
    ) -> BoxFuture<'e, Result<Describe<Self::Database>, Error>>
    where
        'c: 'e,
    {
        self.rw_pool.describe(sql)
    }
}

fn remove_leading_file_protocol(url: &str) -> String {
    if url.starts_with("file://") {
        return url.strip_prefix("file://").unwrap().to_string();
    }
    url.to_string()
}

fn remove_leading_slash(url: &str) -> String {
    let pattern = Regex::new(r"^/([a-zA-Z]:)").unwrap();
    pattern.replace(url, "$1").to_string()
}

#[cfg(test)]
mod tests {
    use crate::sql;
    use crate::sql::{Db, DbError, DbPool, remove_leading_file_protocol, remove_leading_slash};
    use std::time::Duration;
    use te_system::edition::{Edition, TabsdataEdition};
    use testdir::testdir;
    use url::Url;

    #[test]
    fn test_db_location_path_ok() {
        let db_path = testdir!().join("dir1").join("dir2").join("test.db");

        let config = sql::SqliteConfigBuilder::default()
            .url(Url::from_file_path(&db_path).unwrap().as_str().to_string())
            .build()
            .unwrap();

        assert!(!db_path.parent().unwrap().exists());
        Db::db_location_path(&config).unwrap();
        assert!(db_path.parent().unwrap().exists());
    }

    // test sqlite config into sqlx pool options
    #[test]
    fn test_sqlite_config() {
        let config = sql::SqliteConfigBuilder::default()
            .url(String::from("sqlite::memory:"))
            .min_connections(2)
            .max_connections(10)
            .acquire_timeout(10)
            .max_lifetime(60 * 60)
            .idle_timeout(60)
            .test_before_acquire(true)
            .build()
            .unwrap();
        assert_eq!(config.url.as_ref().unwrap(), "sqlite::memory:");
        assert_eq!(config.min_connections, 2);
        assert_eq!(config.max_connections, 10);
        assert_eq!(config.acquire_timeout(), Duration::from_secs(10));
        assert_eq!(config.max_lifetime(), Duration::from_secs(60 * 60));
        assert_eq!(config.idle_timeout(), Duration::from_secs(60));
        assert!(config.test_before_acquire);
    }

    //TODO
    #[tokio::test]
    async fn creates_database_and_schema() {
        // testing db_schema macro
        let db_file = testdir!().join("test.db");
        let config = sql::SqliteConfigBuilder::default()
            .url(db_file.to_str().map(str::to_string))
            .build()
            .unwrap();
        let db = crate::db_with_schema(&config, td_schema::test_schema())
            .await
            .unwrap();
        db.upgrade_db_version().await.unwrap();
        let _ = sqlx::query("SELECT * FROM foo").execute(&db).await.unwrap();
        assert!(db_file.exists());
    }

    #[tokio::test]
    async fn open_existing_database() {
        let schema = td_schema::test_schema();

        let db_file = testdir!().join("test.db");
        let config = sql::SqliteConfigBuilder::default()
            .url(db_file.to_str().map(str::to_string))
            .build()
            .unwrap();
        {
            let db = DbPool::connect(&config, schema).await.unwrap();
            db.upgrade_db_version().await.unwrap();
            sqlx::query("INSERT INTO foo values('a', 'A')")
                .execute(&db)
                .await
                .unwrap();
        }

        let db = DbPool::connect(&config, schema).await.unwrap();
        db.upgrade_db_version().await.unwrap();
        let res = sqlx::query("SELECT * FROM foo")
            .fetch_all(&db)
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        drop(db);
    }

    #[test]
    fn test_create_bindings_literal() {
        assert_eq!(sql::create_bindings_literal(0, 0), "");
        assert_eq!(sql::create_bindings_literal(0, 1), "?1");
        assert_eq!(sql::create_bindings_literal(0, 2), "?1,?2");
        assert_eq!(sql::create_bindings_literal(0, 3), "?1,?2,?3");
        assert_eq!(sql::create_bindings_literal(1, 0), "");
        assert_eq!(sql::create_bindings_literal(1, 1), "?2");
        assert_eq!(sql::create_bindings_literal(1, 2), "?2,?3");
        assert_eq!(sql::create_bindings_literal(1, 3), "?2,?3,?4");
    }

    #[test]
    fn test_remove_leading_file_protocol() {
        assert_eq!(
            remove_leading_file_protocol("file:///C:/path/to/file"),
            "/C:/path/to/file"
        );
        assert_eq!(
            remove_leading_file_protocol("file:///another/path"),
            "/another/path"
        );
        assert_eq!(
            remove_leading_file_protocol("file:///E:/no/leading/slash"),
            "/E:/no/leading/slash"
        );
        assert_eq!(
            remove_leading_file_protocol("file:///not/a/windows/path"),
            "/not/a/windows/path"
        );
        assert_eq!(
            remove_leading_file_protocol("C:/no/protocol"),
            "C:/no/protocol"
        );
        assert_eq!(remove_leading_file_protocol(""), "");
    }

    #[test]
    fn test_remove_leading_slash() {
        assert_eq!(remove_leading_slash("/C:/path/to/file"), "C:/path/to/file");
        assert_eq!(remove_leading_slash("/D:/another/path"), "D:/another/path");
        assert_eq!(
            remove_leading_slash("E:/no/leading/slash"),
            "E:/no/leading/slash"
        );
        assert_eq!(
            remove_leading_slash("/not/a/windows/path"),
            "/not/a/windows/path"
        );
        assert_eq!(remove_leading_slash(""), "");
    }

    #[tokio::test]
    async fn test_tabsdata_database_schema_does_not_exist() {
        let schema = td_schema::test_schema();
        let db_file = testdir!().join("test.db");
        let config = sql::SqliteConfigBuilder::default()
            .url(db_file.to_str().map(str::to_string))
            .build()
            .unwrap();

        // tabsdata schema does not exist
        let db = DbPool::connect(&config, schema).await.unwrap();
        db.upgrade_db_version().await.unwrap();
        let res = db.check_db_version().await;
        assert!(matches!(res, Err(DbError::DatabaseSchemaDoesNotExist)));
    }

    #[tokio::test]
    async fn test_tabsdata_database_schema_ok() {
        let schema = td_schema::schema();
        let db_file = testdir!().join("test.db");
        let config = sql::SqliteConfigBuilder::default()
            .url(db_file.to_str().map(str::to_string))
            .build()
            .unwrap();

        let db = DbPool::create(&config, schema).await.unwrap();
        assert!(db.check_db_version().await.is_ok());
    }

    #[tokio::test]
    async fn test_tabsdata_database_schema_missing_db_version() {
        let schema = td_schema::schema();
        let db_file = testdir!().join("test.db");
        let config = sql::SqliteConfigBuilder::default()
            .url(db_file.to_str().map(str::to_string))
            .build()
            .unwrap();

        let db = DbPool::create(&config, schema).await.unwrap();

        sqlx::query("DELETE FROM tabsdata_system")
            .execute(&db)
            .await
            .unwrap();

        let res = db.check_db_version().await;
        assert!(matches!(res, Err(DbError::DatabaseCorrupted(_))));
    }

    #[tokio::test]
    async fn test_tabsdata_database_schema_invalid_db_version() {
        let schema = td_schema::schema();
        let db_file = testdir!().join("test.db");
        let config = sql::SqliteConfigBuilder::default()
            .url(db_file.to_str().map(str::to_string))
            .build()
            .unwrap();

        let db = DbPool::create(&config, schema).await.unwrap();

        sqlx::query("UPDATE tabsdata_system set value = 'invalid' WHERE name = 'db_version'")
            .execute(&db)
            .await
            .unwrap();

        let res = db.check_db_version().await;
        assert!(matches!(res, Err(DbError::DatabaseCorrupted(_))));
    }

    #[tokio::test]
    async fn test_tabsdata_database_schema_db_needs_upgrade() {
        let schema = td_schema::schema();
        let db_file = testdir!().join("test.db");
        let config = sql::SqliteConfigBuilder::default()
            .url(db_file.to_str().map(str::to_string))
            .build()
            .unwrap();

        let db = DbPool::create(&config, schema).await.unwrap();

        sqlx::query("UPDATE tabsdata_system set value = '0' WHERE name = 'db_version'")
            .execute(&db)
            .await
            .unwrap();

        let res = db.check_db_version().await;
        assert!(matches!(res, Err(DbError::DatabaseNeedsUpgrade(_))));
    }

    #[tokio::test]
    async fn test_tabsdata_database_schema_app_needs_upgrade() {
        let schema = td_schema::schema();
        let db_file = testdir!().join("test.db");
        let config = sql::SqliteConfigBuilder::default()
            .url(db_file.to_str().map(str::to_string))
            .build()
            .unwrap();

        let db = DbPool::create(&config, schema).await.unwrap();

        sqlx::query("UPDATE tabsdata_system set value = '1000' WHERE name = 'db_version'")
            .execute(&db)
            .await
            .unwrap();

        let res = db.check_db_version().await;
        assert!(matches!(res, Err(DbError::DatabaseIsNewer(_, _))));
    }

    #[tokio::test]
    async fn test_tabsdata_database_edition_upgrade() {
        let schema = td_schema::schema();
        let db_file = testdir!().join("test.db");
        let config = sql::SqliteConfigBuilder::default()
            .url(db_file.to_str().map(str::to_string))
            .build()
            .unwrap();

        let db = DbPool::create(&config, schema).await.unwrap();
        let db_edition = db.parse_tabsdata_edition().await.unwrap();
        let runtime_edition = TabsdataEdition;
        assert_eq!(db_edition.unwrap(), runtime_edition.label());
    }

    #[tokio::test]
    async fn test_tabsdata_database_schema_invalid_edition() {
        let schema = td_schema::schema();
        let db_file = testdir!().join("test.db");
        let config = sql::SqliteConfigBuilder::default()
            .url(db_file.to_str().map(str::to_string))
            .build()
            .unwrap();

        let db = DbPool::create(&config, schema).await.unwrap();

        sqlx::query("UPDATE tabsdata_system set value = 'invalid' WHERE name = 'edition'")
            .execute(&db)
            .await
            .unwrap();

        let res = db.check_tabsdata_edition().await;
        assert!(matches!(res, Err(DbError::InvalidEdition(_, _))));
    }
}
