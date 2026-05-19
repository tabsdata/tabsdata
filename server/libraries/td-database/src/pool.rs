//
// Copyright 2026 Tabs Data Inc.
//

use crate::config::DatabaseConfig;
use crate::dynamic::{DynamicConnection, DynamicPool, DynamicTransaction};
#[cfg(feature = "embedded-postgres")]
use crate::embedded::EmbeddedPostgres;
use crate::error::DbError;
use crate::sea_query::builder::SeaQueryExt;
use crate::sea_query::kind::DatabaseKind;
use sea_query::{Expr, ExprTrait, Iden, Query};
use sqlx::Error;
use std::cmp::Ordering;
use std::fmt::Display;
use std::sync::Arc;
use td_schema::schema::SchemaVersion;
use td_schema::{
    CURRENT_TABSDATA_VERSION, CURRENT_VERSION, DB_EDITION_NAME, DB_VERSION_NAME,
    TABSDATA_VERSION_NAME, TabsdataSystem,
};
use te_system::edition::{Compatible, Edition, TabsdataEdition};
use tracing::{debug, info, instrument, warn};
use url::Url;

#[derive(Debug, Clone)]
pub struct TdPool {
    pub pool: DynamicPool,
    /// URL the pool is connected to. `Arc` so `TdPool::clone` stays cheap —
    /// the URL is set once at connect time and never mutated.
    url: Arc<Url>,
    // Owns the embedded postgres server when this pool was booted via the
    // embedded path. Shared across clones via `Arc` so the server stops
    // only when the last `TdPool` clone drops. `pool` is declared first
    // so the sqlx pool drops before the embedded handle within a single
    // struct drop.
    #[cfg(feature = "embedded-postgres")]
    _embedded: Option<Arc<EmbeddedPostgres>>,
}

impl AsRef<DynamicPool> for TdPool {
    fn as_ref(&self) -> &DynamicPool {
        &self.pool
    }
}

/// Specialized [`Pool`] that uses two pools, one for read-only operations and one for
/// read-write operations.
impl TdPool {
    /// Connects to a database using the given configuration.
    ///
    /// When `config` configured the postgres backend with `embedded: Some(_)`,
    /// the embedded server is booted inside [`DatabaseConfig::connect`] and
    /// the live handle parks here (under `Arc`) so the server stops cleanly
    /// when the last clone of this pool drops. The schema is assumed to be
    /// up to date.
    #[instrument(name = "db.pool.connect", skip_all, level = "info", err(Debug))]
    pub async fn connect(config: &DatabaseConfig) -> Result<Self, DbError> {
        info!("db.pool.connect: opening pool");
        let out = config.connect().await?;
        debug!("Connected to database pool: {:#?}", out.pool);
        Ok(Self {
            pool: out.pool,
            url: Arc::new(out.url),
            #[cfg(feature = "embedded-postgres")]
            _embedded: out.embedded.map(Arc::new),
        })
    }

    /// The URL the pool is actually connected to. For external backends this
    /// matches what was configured; for embedded postgres this is the URL
    /// the booted server is listening on.
    pub fn url(&self) -> &Url {
        &self.url
    }

    /// Creates a database using the given configuration.
    ///
    /// Creates the schema.
    #[instrument(name = "db.pool.create", skip_all, level = "info", err(Debug))]
    pub async fn create(config: &DatabaseConfig) -> Result<Self, DbError> {
        let this = Self::connect(config).await?;
        this.upgrade().await?;
        Ok(this)
    }

    #[instrument(name = "db.pool.check", skip_all, level = "debug", err(Debug))]
    pub async fn check(&self) -> Result<(), DbError> {
        self.check_tabsdata_version().await?;
        debug!("db.pool.check: tabsdata version OK");
        self.check_db_version().await?;
        debug!("db.pool.check: db version OK");
        self.check_tabsdata_edition().await?;
        debug!("db.pool.check: edition OK");
        Ok(())
    }

    pub async fn upgrade(&self) -> Result<(), DbError> {
        info!("Starting database upgrade");
        self.upgrade_db_version().await?;
        self.upgrade_tabsdata_edition().await?;
        info!("Database upgrade complete");
        Ok(())
    }

    fn map_system_db_error(err: Error, row: impl Display) -> DbError {
        match &err {
            Error::RowNotFound => DbError::DatabaseCorrupted(
                format!(
                    "Missing '{row}' row in '{}'",
                    TabsdataSystem::Table.to_string()
                )
                .to_string(),
            ),
            Error::Database(database_err) => {
                if is_undefined_table_error(database_err.code().as_deref(), database_err.message())
                {
                    DbError::DatabaseSchemaDoesNotExist
                } else {
                    DbError::SqlError(err)
                }
            }
            _ => DbError::SqlError(err),
        }
    }

    /// Read a single `value` from `tabsdata_system` keyed by `name`. Missing
    /// rows return `Ok(None)`; only the read itself can fail.
    async fn read_system_value(&self, name: &str) -> Result<Option<String>, DbError> {
        Query::select()
            .column(TabsdataSystem::Value)
            .from(TabsdataSystem::Table)
            .and_where(Expr::col(TabsdataSystem::Name).eq(name))
            .build_query_scalar()
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| Self::map_system_db_error(e, name))
    }

    /// Read the current `db_version` from the database.
    async fn try_read_db_version(&self) -> Result<usize, DbError> {
        let res = self
            .read_system_value(DB_VERSION_NAME)
            .await?
            .ok_or_else(|| {
                DbError::DatabaseCorrupted(format!(
                    "Missing '{DB_VERSION_NAME}' row in '{}'",
                    TabsdataSystem::Table.to_string()
                ))
            })?;
        res.parse::<usize>().map_err(|_| {
            DbError::DatabaseCorrupted(format!(
                "'{DB_VERSION_NAME}' value '{res}' must be an integer"
            ))
        })
    }

    async fn check_db_version(&self) -> Result<(), DbError> {
        let version = self.try_read_db_version().await?;
        match version.cmp(&CURRENT_VERSION) {
            Ordering::Equal => Ok(()),
            Ordering::Less => Err(DbError::DatabaseNeedsUpgrade(format!(
                "Tabsdata database version is '{}', binary database version is '{}'",
                version, CURRENT_VERSION
            ))),
            Ordering::Greater => Err(DbError::DatabaseIsNewer(
                version.to_string(),
                CURRENT_VERSION,
            )),
        }
    }

    /// Read the major Tabsdata version recorded in `tabsdata_system`.
    ///
    /// Pre-2.0 databases lack this row; we map a missing row to `1` so the
    /// startup gate can refuse them with a clear cross-major message.
    async fn try_read_tabsdata_version(&self) -> Result<usize, DbError> {
        match self.read_system_value(TABSDATA_VERSION_NAME).await? {
            None => Ok(1),
            Some(raw) => raw.parse::<usize>().map_err(|_| {
                DbError::DatabaseCorrupted(format!(
                    "'{TABSDATA_VERSION_NAME}' value '{raw}' must be an integer"
                ))
            }),
        }
    }

    async fn check_tabsdata_version(&self) -> Result<(), DbError> {
        let version = self.try_read_tabsdata_version().await?;
        if version == CURRENT_TABSDATA_VERSION {
            Ok(())
        } else {
            Err(DbError::IncompatibleMajorVersion(
                version,
                CURRENT_TABSDATA_VERSION,
            ))
        }
    }

    /// Schema-counter check for the current major version.
    ///
    /// Cross-major upgrade is not supported (see [`check_tabsdata_version`]).
    /// In-major upgrades walk the delta ladder
    /// (`td_schema::schema::deltas::get`) from the stored `db_version` up to
    /// `CURRENT_VERSION`; each delta applies in its own transaction. A
    /// missing delta in the range panics with the upstream
    /// `"No delta migration for version N"` error.
    pub async fn upgrade_db_version(&self) -> Result<(), DbError> {
        match self.try_read_db_version().await {
            Ok(version) if version == CURRENT_VERSION => {
                debug!(version, "Schema version is up to date");
            }
            Ok(version) if version < CURRENT_VERSION => {
                info!(from = version, to = CURRENT_VERSION, "Upgrading schema");
                for target in (version + 1)..=CURRENT_VERSION {
                    let stmts = td_schema::schema::deltas::get(target, self.kind());
                    let mut tx = self
                        .pool
                        .begin()
                        .await
                        .map_err(DbError::FailedToCreateOrUpgradeDatabaseSchema)?;
                    tx.as_mut_conn()
                        .execute_batch(&stmts)
                        .await
                        .map_err(DbError::FailedToCreateOrUpgradeDatabaseSchema)?;
                    tx.commit()
                        .await
                        .map_err(DbError::FailedToCreateOrUpgradeDatabaseSchema)?;
                    info!(target, "Schema delta applied");
                }
            }
            Ok(version) => {
                warn!(
                    database_version = version,
                    binary_version = CURRENT_VERSION,
                    "Database is newer than binary"
                );
                return Err(DbError::DatabaseIsNewer(
                    version.to_string(),
                    CURRENT_VERSION,
                ));
            }
            Err(DbError::DatabaseSchemaDoesNotExist) => {
                info!(
                    version = CURRENT_VERSION,
                    "No existing schema found, creating fresh install"
                );
                let mut tx = self
                    .pool
                    .begin()
                    .await
                    .map_err(DbError::FailedToCreateOrUpgradeDatabaseSchema)?;
                let stmts = td_schema::schema::current::Schema::build(self.kind());
                tx.as_mut_conn()
                    .execute_batch(&stmts)
                    .await
                    .map_err(DbError::FailedToCreateOrUpgradeDatabaseSchema)?;
                tx.commit()
                    .await
                    .map_err(DbError::FailedToCreateOrUpgradeDatabaseSchema)?;
                info!(version = CURRENT_VERSION, "Fresh schema created");
            }
            Err(e) => return Err(e),
        }
        Ok(())
    }

    async fn parse_tabsdata_edition(&self) -> Result<Option<String>, DbError> {
        self.read_system_value(DB_EDITION_NAME).await
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
                info!(edition = runtime_edition.label(), "Setting initial edition");
                Query::insert()
                    .into_table(TabsdataSystem::Table)
                    .columns([TabsdataSystem::Name, TabsdataSystem::Value])
                    .values([DB_EDITION_NAME.into(), runtime_edition.label().into()])
                    .map_err(|e| DbError::CannotUpgradeEdition(e.to_string()))?
                    .build_query()
                    .execute(self)
                    .await
                    .map_err(DbError::FailedToCreateOrUpgradeDatabaseEdition)?;
                Ok(())
            }
            Some(ref instance_edition) => {
                if !runtime_edition.is_compatible(instance_edition) {
                    warn!(
                        instance = %instance_edition,
                        runtime = runtime_edition.label(),
                        "Incompatible edition"
                    );
                    Err(DbError::CannotUpgradeEdition(format!(
                        "instance edition is '{instance_edition}' cannot be upgraded to '{}'",
                        runtime_edition.label()
                    )))
                } else if runtime_edition.requires_upgrade(instance_edition) {
                    info!(
                        from = %instance_edition,
                        to = runtime_edition.label(),
                        "Upgrading edition"
                    );
                    Query::update()
                        .table(TabsdataSystem::Table)
                        .value(TabsdataSystem::Value, runtime_edition.label())
                        .and_where(Expr::col(TabsdataSystem::Name).eq(DB_EDITION_NAME))
                        .build_query()
                        .execute(self)
                        .await
                        .map_err(DbError::FailedToCreateOrUpgradeDatabaseEdition)?;
                    Ok(())
                } else {
                    debug!(
                        edition = %instance_edition,
                        "Edition is up to date"
                    );
                    Ok(())
                }
            }
        }
    }

    /// Delegates to the read-only pool's [`Pool::acquire`] method.
    pub async fn acquire<'c>(&self) -> Result<DynamicConnection<'c>, Error> {
        let start = std::time::Instant::now();
        let result = self.pool.acquire().await;
        let elapsed_ms = start.elapsed().as_millis() as u64;
        if elapsed_ms > 100 {
            warn!(elapsed_ms, "db.pool.acquire: slow acquisition");
        }
        result
    }

    /// Delegates to the read-write pool's [`Pool::begin`] method.
    pub async fn begin<'c>(&self) -> Result<DynamicTransaction<'c>, Error> {
        let start = std::time::Instant::now();
        let result = self.pool.begin().await;
        let elapsed_ms = start.elapsed().as_millis() as u64;
        if elapsed_ms > 100 {
            warn!(elapsed_ms, "db.pool.begin: slow transaction start");
        }
        result
    }

    /// Returns if the pool is closed.
    pub fn is_closed(&self) -> bool {
        self.pool.is_closed()
    }

    /// Close the pool, waiting for all connections to be returned and closed.
    pub async fn close(&self) {
        self.pool.close().await;
    }
}

/// True if a database error reports a missing `tabsdata_system` table for any
/// backend enabled at compile time. Each arm is gated on its backend feature so
/// the check stays tight to what's actually linked in.
fn is_undefined_table_error(code: Option<&str>, message: &str) -> bool {
    #[cfg(postgres)]
    if matches!(code, Some("42P01")) {
        return true;
    }
    #[cfg(mysql)]
    if matches!(code, Some("42S02")) {
        return true;
    }
    #[cfg(sqlite)]
    if message.contains("no such table") {
        return true;
    }
    let _ = (code, message);
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raw_query;
    use td_common::tdtestdir;
    use te_system::edition::{Edition, TabsdataEdition};
    use url::Url;

    #[cfg(postgres)]
    #[test]
    fn undefined_table_error_pg_sqlstate() {
        assert!(is_undefined_table_error(Some("42P01"), ""));
        assert!(!is_undefined_table_error(Some("23505"), ""));
    }

    #[cfg(mysql)]
    #[test]
    fn undefined_table_error_mysql_sqlstate() {
        assert!(is_undefined_table_error(Some("42S02"), ""));
        assert!(!is_undefined_table_error(Some("HY000"), ""));
    }

    #[cfg(sqlite)]
    #[test]
    fn undefined_table_error_sqlite_message() {
        assert!(is_undefined_table_error(None, "no such table: foo"));
        assert!(!is_undefined_table_error(None, "syntax error near 'x'"));
    }

    // Sqlite-only pool-level tests. Postgres-side equivalents for the
    // cross-backend-meaningful parts (schema_applies, db_version, edition)
    // live in the top-level `tests` module in `lib.rs`.
    #[cfg(sqlite)]
    #[tokio::test]
    async fn creates_database_and_schema() {
        let db_file = tdtestdir!().join("test.db");
        let db_url = Url::from_file_path(db_file.clone()).unwrap();
        let config = DatabaseConfig::new(db_url).unwrap();
        let db = TdPool::create(&config).await.unwrap();
        let _ = raw_query!(db, "SELECT * FROM tabsdata_system")
            .build_query()
            .execute(&db)
            .await
            .unwrap();
        assert!(db_file.exists());
    }

    #[cfg(sqlite)]
    #[tokio::test]
    async fn open_existing_database() {
        let db_file = tdtestdir!().join("test.db");
        let db_url = Url::from_file_path(db_file).unwrap();
        let config = DatabaseConfig::new(db_url).unwrap();
        {
            let db = TdPool::create(&config).await.unwrap();
            raw_query!(
                db,
                "INSERT INTO tabsdata_system values('test_key', 'test_val')"
            )
            .build_query()
            .execute(&db)
            .await
            .unwrap();
        }

        let db = TdPool::connect(&config).await.unwrap();
        let res = raw_query!(db, "SELECT * FROM tabsdata_system WHERE name = 'test_key'")
            .build_query()
            .fetch_all(&db)
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        drop(db);
    }

    #[cfg(sqlite)]
    #[tokio::test]
    async fn test_tabsdata_database_schema_does_not_exist() {
        let db_file = tdtestdir!().join("test.db");
        let db_url = Url::from_file_path(db_file).unwrap();
        let config = DatabaseConfig::new(db_url).unwrap();
        // Connect without upgrade — DB has no tables
        let db = TdPool::connect(&config).await.unwrap();
        let res = db.check_db_version().await;
        assert!(matches!(res, Err(DbError::DatabaseSchemaDoesNotExist)));
    }

    #[cfg(sqlite)]
    #[tokio::test]
    async fn test_tabsdata_database_schema_ok() {
        let db_file = tdtestdir!().join("test.db");
        let db_url = Url::from_file_path(db_file).unwrap();
        let config = DatabaseConfig::new(db_url).unwrap();
        let db = TdPool::create(&config).await.unwrap();
        assert!(db.check_db_version().await.is_ok());
    }

    #[cfg(sqlite)]
    #[tokio::test]
    async fn test_tabsdata_database_schema_missing_db_version() {
        let db_file = tdtestdir!().join("test.db");
        let db_url = Url::from_file_path(db_file).unwrap();
        let config = DatabaseConfig::new(db_url).unwrap();
        let db = TdPool::create(&config).await.unwrap();

        raw_query!(db, "DELETE FROM tabsdata_system")
            .build_query()
            .execute(&db)
            .await
            .unwrap();

        let res = db.check_db_version().await;
        assert!(matches!(res, Err(DbError::DatabaseCorrupted(_))));
    }

    #[cfg(sqlite)]
    #[tokio::test]
    async fn test_tabsdata_database_schema_invalid_db_version() {
        let db_file = tdtestdir!().join("test.db");
        let db_url = Url::from_file_path(db_file).unwrap();
        let config = DatabaseConfig::new(db_url).unwrap();
        let db = TdPool::create(&config).await.unwrap();

        raw_query!(
            db,
            "UPDATE tabsdata_system set value = 'invalid' WHERE name = 'db_version'"
        )
        .build_query()
        .execute(&db)
        .await
        .unwrap();

        let res = db.check_db_version().await;
        assert!(matches!(res, Err(DbError::DatabaseCorrupted(_))));
    }

    #[cfg(sqlite)]
    #[tokio::test]
    async fn test_tabsdata_database_schema_db_needs_upgrade() {
        let db_file = tdtestdir!().join("test.db");
        let db_url = Url::from_file_path(db_file).unwrap();
        let config = DatabaseConfig::new(db_url).unwrap();
        let db = TdPool::create(&config).await.unwrap();

        raw_query!(
            db,
            "UPDATE tabsdata_system set value = '0' WHERE name = 'db_version'"
        )
        .build_query()
        .execute(&db)
        .await
        .unwrap();

        let res = db.check_db_version().await;
        assert!(matches!(res, Err(DbError::DatabaseNeedsUpgrade(_))));
    }

    #[cfg(sqlite)]
    #[tokio::test]
    async fn test_tabsdata_database_schema_app_needs_upgrade() {
        let db_file = tdtestdir!().join("test.db");
        let db_url = Url::from_file_path(db_file).unwrap();
        let config = DatabaseConfig::new(db_url).unwrap();
        let db = TdPool::create(&config).await.unwrap();

        raw_query!(
            db,
            "UPDATE tabsdata_system set value = '1000' WHERE name = 'db_version'"
        )
        .build_query()
        .execute(&db)
        .await
        .unwrap();

        let res = db.check_db_version().await;
        assert!(matches!(res, Err(DbError::DatabaseIsNewer(_, _))));
    }

    #[cfg(sqlite)]
    #[tokio::test]
    async fn test_tabsdata_database_edition_upgrade() {
        let db_file = tdtestdir!().join("test.db");
        let db_url = Url::from_file_path(db_file).unwrap();
        let config = DatabaseConfig::new(db_url).unwrap();
        let db = TdPool::create(&config).await.unwrap();
        let db_edition = db.parse_tabsdata_edition().await.unwrap();
        let runtime_edition = TabsdataEdition;
        assert_eq!(db_edition.unwrap(), runtime_edition.label());
    }

    #[cfg(sqlite)]
    #[tokio::test]
    async fn test_tabsdata_database_schema_invalid_edition() {
        let db_file = tdtestdir!().join("test.db");
        let db_url = Url::from_file_path(db_file).unwrap();
        let config = DatabaseConfig::new(db_url).unwrap();
        let db = TdPool::create(&config).await.unwrap();

        raw_query!(
            db,
            "UPDATE tabsdata_system set value = 'invalid' WHERE name = 'edition'"
        )
        .build_query()
        .execute(&db)
        .await
        .unwrap();

        let res = db.check_tabsdata_edition().await;
        assert!(matches!(res, Err(DbError::InvalidEdition(_, _))));
    }

    #[cfg(sqlite)]
    #[tokio::test]
    async fn test_upgrade_idempotent() {
        let db_file = tdtestdir!().join("test.db");
        let db_url = Url::from_file_path(db_file).unwrap();
        let config = DatabaseConfig::new(db_url).unwrap();
        let db = TdPool::connect(&config).await.unwrap();

        // First upgrade: fresh install
        db.upgrade().await.unwrap();
        db.check().await.unwrap();

        // Second upgrade: should succeed (no-op)
        db.upgrade().await.unwrap();
        db.check().await.unwrap();
    }

    #[cfg(sqlite)]
    #[tokio::test]
    async fn test_execute_batch() {
        let db_file = tdtestdir!().join("test.db");
        let db_url = Url::from_file_path(db_file).unwrap();
        let config = DatabaseConfig::new(db_url).unwrap();
        let db = TdPool::connect(&config).await.unwrap();

        let stmts = vec![
            "CREATE TABLE test_batch (id TEXT PRIMARY KEY, val TEXT)".to_string(),
            "INSERT INTO test_batch VALUES ('a', 'one')".to_string(),
            "INSERT INTO test_batch VALUES ('b', 'two')".to_string(),
        ];
        let mut tx = db.pool.begin().await.unwrap();
        tx.as_mut_conn().execute_batch(&stmts).await.unwrap();
        tx.commit().await.unwrap();

        let rows = raw_query!(db, "SELECT * FROM test_batch")
            .build_query()
            .fetch_all(&db)
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[cfg(sqlite)]
    #[tokio::test]
    async fn test_backend_returns_sqlite() {
        let db_file = tdtestdir!().join("test.db");
        let db_url = Url::from_file_path(db_file).unwrap();
        let config = DatabaseConfig::new(db_url).unwrap();
        let db = TdPool::connect(&config).await.unwrap();
        let backend: td_schema::schema::backend::Backend = db.kind().into();
        assert_eq!(backend, td_schema::schema::backend::Backend::Sqlite);
    }
}
