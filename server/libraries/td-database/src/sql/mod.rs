//
// Copyright 2025 Tabs Data Inc.
//

use derive_builder::Builder;
use futures_util::future::BoxFuture;
use futures_util::stream::BoxStream;
use getset::{CopyGetters, Getters};
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
use std::future::Future;
use std::time::Duration;
use td_error::td_error;
use tracing::log::LevelFilter;

const SLOW_QUERIES_THRESHOLD: u64 = 5000;
const PRAGMA_TEMP_STORE: (&str, &str) = ("temp_store", "MEMORY");

/// Configuration for a SQLite database.
#[derive(Debug, Clone, Serialize, Deserialize, Builder, Getters, CopyGetters)]
#[builder(default)]
#[getset(get_copy = "pub")]
pub struct SqliteConfig {
    /// The Sqlite URI, required.
    #[getset(skip)]
    #[getset(get = "pub")]
    #[builder(setter(into))]
    url: Option<String>,
    /// The minimum number of database connections, defaults to `1`.
    min_connections: u32,
    /// The maximum number of database connections, defaults to `10`.
    max_connections: u32,
    /// The maximum time to wait for a database connection to be acquired, defaults to `30 seconds`.
    #[getset(skip)]
    acquire_timeout: u64,
    /// The maximum lifetime of a database connection, defaults to `60 minutes`.
    #[getset(skip)]
    max_lifetime: u64,
    /// The maximum time a database connection can be idle, defaults to `60 seconds`.
    #[getset(skip)]
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
    #[error("Database location is missing in the given configuration")]
    MissingDatabaseLocation = 5000,
    #[error("Failed to database existence: {0}")]
    FailedToCheckDatabaseExistence(#[source] Error) = 5001,
    #[error("Failed to database existence: {0}")]
    FailedToCreateDatabase(#[source] Error) = 5002,
    #[error("Failed to connect to the database: {0}")]
    FailedToConnectToDatabase(#[source] Error) = 5003,
    #[error("Failed to connect to the database: {0}")]
    FailedToCreateOrUpdateDatabaseSchema(#[source] MigrateError) = 5004,
    #[error("Sql error: {0}")]
    SqlError(#[source] Error) = 5005,
    #[error("Database does not exist")]
    DatabaseDoesNotExist = 5006,
    #[error("Failed to create database directory {0}: {1}")]
    FailedToCreateDatabaseDir(String, #[source] std::io::Error) = 5007,
}

/// Sqlite database connection provider using Sqlx.
///
/// Databases are automatically created and their schema is updated if necessary
/// when the connection is created.
pub struct Db {
    schema: &'static DbSchema,
}

impl Db {
    /// Returns a database connection provider for a database with the given schema.
    pub fn schema(schema: &'static DbSchema) -> Self {
        Db { schema }
    }

    fn db_location_path(config: &SqliteConfig) -> Result<String, DbError> {
        let mut db_url = config
            .url()
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
        if !read_only {
            self.schema
                .run(&pool)
                .await
                .map_err(DbError::FailedToCreateOrUpdateDatabaseSchema)?;
        }
        Ok(pool)
    }

    /// Connects to the database specified in the given configuration, if the database does not
    /// exist it creates it, if the schema is out of date, it updates it.
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
    pub ro_pool: Pool<Sqlite>,
    pub rw_pool: Pool<Sqlite>,
}

/// Specialized Sqlx Sqlite [`Pool`] that uses two pools, one for read-only operations and one for
/// read-write operations.
impl DbPool {
    /// Creates a new [`DbPool`] with the given configuration.
    ///
    /// The schema is created or updated to match the given [`DbSchema`].
    pub async fn new(config: &SqliteConfig, schema: &'static DbSchema) -> Result<Self, DbError> {
        let rw_ool = Db::schema(schema).rw_pool(config).await?;
        let ro_pool = Db::schema(schema).ro_connect(config).await?;
        Ok(Self {
            ro_pool,
            rw_pool: rw_ool,
        })
    }

    /// Delegates to the read-only pool's [`Pool::acquire`] method.
    pub fn acquire(&self) -> impl Future<Output = Result<PoolConnection<Sqlite>, Error>> + 'static {
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
    use crate::sql::{remove_leading_file_protocol, remove_leading_slash, Db, DbPool};
    use std::time::Duration;
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
        assert_eq!(config.url().as_ref().unwrap(), "sqlite::memory:");
        assert_eq!(config.min_connections(), 2);
        assert_eq!(config.max_connections(), 10);
        assert_eq!(config.acquire_timeout(), Duration::from_secs(10));
        assert_eq!(config.max_lifetime(), Duration::from_secs(60 * 60));
        assert_eq!(config.idle_timeout(), Duration::from_secs(60));
        assert!(config.test_before_acquire());
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
            let db = DbPool::new(&config, schema).await.unwrap();
            sqlx::query("INSERT INTO foo values('a', 'A')")
                .execute(&db)
                .await
                .unwrap();
        }

        let db = DbPool::new(&config, schema).await.unwrap();
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
}
