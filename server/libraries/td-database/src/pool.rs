//
// Copyright 2026 Tabs Data Inc.
//

use crate::config::DatabaseConfig;
use crate::dynamic::{DynamicConnection, DynamicPool, DynamicTransaction};
use crate::error::DbError;
use crate::sea_query::builder::SeaQueryExt;
use crate::sea_query::kind::DatabaseKind;
use sea_query::{Expr, ExprTrait, Iden, Query};
use sqlx::Error;
use std::cmp::Ordering;
use std::fmt::Display;
use td_schema::schema::SchemaVersion;
use td_schema::{CURRENT_VERSION, DB_EDITION_NAME, DB_VERSION_NAME, TabsdataSystem};
use te_system::edition::{Compatible, Edition, TabsdataEdition};
use tracing::{debug, info, warn};

#[derive(Debug, Clone)]
pub struct TdPool {
    pub pool: DynamicPool,
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
    /// The schema is assumed to be up to date.
    pub async fn connect(config: &DatabaseConfig) -> Result<Self, DbError> {
        let pool = config.connect().await?;
        debug!("Connected to database pool: {pool:#?}");
        Ok(Self { pool })
    }

    /// Creates a database using the given configuration.
    ///
    /// Creates the schema.
    pub async fn create(config: &DatabaseConfig) -> Result<Self, DbError> {
        let this = Self::connect(config).await?;
        this.upgrade().await?;
        Ok(this)
    }

    pub async fn check(&self) -> Result<(), DbError> {
        self.check_db_version().await?;
        self.check_tabsdata_edition().await?;
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
                if database_err.message().contains("no such table") {
                    DbError::DatabaseSchemaDoesNotExist
                } else {
                    DbError::SqlError(err)
                }
            }
            _ => DbError::SqlError(err),
        }
    }

    /// Read the current `db_version` from the database.
    async fn try_read_db_version(&self) -> Result<usize, DbError> {
        let res: String = Query::select()
            .column(TabsdataSystem::Value)
            .from(TabsdataSystem::Table)
            .and_where(Expr::col(TabsdataSystem::Name).eq(DB_VERSION_NAME))
            .build_query_scalar()
            .fetch_one(&self.pool)
            .await
            .map_err(|e| Self::map_system_db_error(e, DB_VERSION_NAME))?;
        res.parse::<usize>().map_err(|_| {
            DbError::DatabaseCorrupted(format!(
                "'{}' value '{}' must be an integer",
                DB_VERSION_NAME, res
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

    pub async fn upgrade_db_version(&self) -> Result<(), DbError> {
        match self.try_read_db_version().await {
            Ok(version) if version == CURRENT_VERSION => {
                debug!(version, "Schema version is up to date");
            }
            Ok(version) if version < CURRENT_VERSION => {
                info!(
                    from = version,
                    to = CURRENT_VERSION,
                    "Upgrading schema version"
                );

                let mut tx = self
                    .pool
                    .begin()
                    .await
                    .map_err(DbError::FailedToCreateOrUpgradeDatabaseSchema)?;

                let mut current = version;

                // Legacy upgrade: apply historic SQLite migrations for databases
                // from before the programmatic migration system (versions 0–5).
                // We use <= so that v5 databases created by the old sqlx system
                // also get normalized (correct type spellings and indices).
                if current <= td_schema::schema::archive::MIN_ARCHIVE {
                    #[allow(unreachable_patterns)]
                    match &self.pool {
                        #[cfg(feature = "sqlite")]
                        DynamicPool::Sqlite { .. } => {
                            info!(
                                from = current,
                                to = td_schema::schema::archive::MIN_ARCHIVE,
                                "Applying legacy SQLite migrations"
                            );
                            for v in (current + 1)..=td_schema::schema::archive::MIN_ARCHIVE {
                                debug!(delta = v, "Applying historic SQLite migration");
                                let stmts = td_schema::schema::archive::historic::get(v);
                                tx.as_mut_conn()
                                    .execute_batch(&stmts)
                                    .await
                                    .map_err(DbError::FailedToCreateOrUpgradeDatabaseSchema)?;
                                debug!(delta = v, "Historic migration applied");
                            }

                            // Normalize schema types and indices to match the
                            // programmatic v5 archive.
                            debug!("Normalizing schema to match v5 archive");
                            let stmts = td_schema::schema::archive::historic::normalize();
                            tx.as_mut_conn()
                                .execute_batch(&stmts)
                                .await
                                .map_err(DbError::FailedToCreateOrUpgradeDatabaseSchema)?;

                            // Clean up the legacy sqlx migrations tracking table.
                            tx.as_mut_conn()
                                .execute_batch(&[
                                    "DROP TABLE IF EXISTS _sqlx_migrations".to_string()
                                ])
                                .await
                                .map_err(DbError::FailedToCreateOrUpgradeDatabaseSchema)?;
                            current = td_schema::schema::archive::MIN_ARCHIVE;
                            info!(version = current, "Legacy SQLite migrations complete");
                        }
                        _ => {
                            return Err(DbError::DatabaseCorrupted(format!(
                                "Database version {} predates multi-backend support \
                                 and can only be upgraded on SQLite",
                                current
                            )));
                        }
                    }
                }

                // Programmatic deltas for versions >= MIN_ARCHIVE.
                for v in (current + 1)..=CURRENT_VERSION {
                    debug!(delta = v, "Applying schema delta");
                    let stmts = td_schema::schema::deltas::get(v, self.kind());
                    tx.as_mut_conn()
                        .execute_batch(&stmts)
                        .await
                        .map_err(DbError::FailedToCreateOrUpgradeDatabaseSchema)?;
                    debug!(delta = v, "Schema delta applied");
                }

                tx.commit()
                    .await
                    .map_err(DbError::FailedToCreateOrUpgradeDatabaseSchema)?;
                info!(version = CURRENT_VERSION, "Schema upgrade complete");
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
        Query::select()
            .column(TabsdataSystem::Value)
            .from(TabsdataSystem::Table)
            .and_where(Expr::col(TabsdataSystem::Name).eq(DB_EDITION_NAME))
            .build_query_scalar()
            .fetch_optional(&self.pool)
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
        self.pool.acquire().await
    }

    /// Delegates to the read-write pool's [`Pool::begin`] method.
    pub async fn begin<'c>(&self) -> Result<DynamicTransaction<'c>, Error> {
        self.pool.begin().await
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raw_query;
    use td_common::tdtestdir;
    use te_system::edition::{Edition, TabsdataEdition};
    use url::Url;

    #[tokio::test]
    async fn creates_database_and_schema() {
        let db_file = tdtestdir!().join("test.db");
        let db_url = Url::from_file_path(db_file.clone()).unwrap();
        let config = DatabaseConfig::new(db_url);
        let db = TdPool::create(&config).await.unwrap();
        let _ = raw_query!(db, "SELECT * FROM tabsdata_system")
            .build_query()
            .execute(&db)
            .await
            .unwrap();
        assert!(db_file.exists());
    }

    #[tokio::test]
    async fn open_existing_database() {
        let db_file = tdtestdir!().join("test.db");
        let db_url = Url::from_file_path(db_file).unwrap();
        let config = DatabaseConfig::new(db_url);
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

    #[tokio::test]
    async fn test_tabsdata_database_schema_does_not_exist() {
        let db_file = tdtestdir!().join("test.db");
        let db_url = Url::from_file_path(db_file).unwrap();
        let config = DatabaseConfig::new(db_url);
        // Connect without upgrade — DB has no tables
        let db = TdPool::connect(&config).await.unwrap();
        let res = db.check_db_version().await;
        assert!(matches!(res, Err(DbError::DatabaseSchemaDoesNotExist)));
    }

    #[tokio::test]
    async fn test_tabsdata_database_schema_ok() {
        let db_file = tdtestdir!().join("test.db");
        let db_url = Url::from_file_path(db_file).unwrap();
        let config = DatabaseConfig::new(db_url);
        let db = TdPool::create(&config).await.unwrap();
        assert!(db.check_db_version().await.is_ok());
    }

    #[tokio::test]
    async fn test_tabsdata_database_schema_missing_db_version() {
        let db_file = tdtestdir!().join("test.db");
        let db_url = Url::from_file_path(db_file).unwrap();
        let config = DatabaseConfig::new(db_url);
        let db = TdPool::create(&config).await.unwrap();

        raw_query!(db, "DELETE FROM tabsdata_system")
            .build_query()
            .execute(&db)
            .await
            .unwrap();

        let res = db.check_db_version().await;
        assert!(matches!(res, Err(DbError::DatabaseCorrupted(_))));
    }

    #[tokio::test]
    async fn test_tabsdata_database_schema_invalid_db_version() {
        let db_file = tdtestdir!().join("test.db");
        let db_url = Url::from_file_path(db_file).unwrap();
        let config = DatabaseConfig::new(db_url);
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

    #[tokio::test]
    async fn test_tabsdata_database_schema_db_needs_upgrade() {
        let db_file = tdtestdir!().join("test.db");
        let db_url = Url::from_file_path(db_file).unwrap();
        let config = DatabaseConfig::new(db_url);
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

    #[tokio::test]
    async fn test_tabsdata_database_schema_app_needs_upgrade() {
        let db_file = tdtestdir!().join("test.db");
        let db_url = Url::from_file_path(db_file).unwrap();
        let config = DatabaseConfig::new(db_url);
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

    #[tokio::test]
    async fn test_tabsdata_database_edition_upgrade() {
        let db_file = tdtestdir!().join("test.db");
        let db_url = Url::from_file_path(db_file).unwrap();
        let config = DatabaseConfig::new(db_url);
        let db = TdPool::create(&config).await.unwrap();
        let db_edition = db.parse_tabsdata_edition().await.unwrap();
        let runtime_edition = TabsdataEdition;
        assert_eq!(db_edition.unwrap(), runtime_edition.label());
    }

    #[tokio::test]
    async fn test_tabsdata_database_schema_invalid_edition() {
        let db_file = tdtestdir!().join("test.db");
        let db_url = Url::from_file_path(db_file).unwrap();
        let config = DatabaseConfig::new(db_url);
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

    #[tokio::test]
    async fn test_upgrade_idempotent() {
        let db_file = tdtestdir!().join("test.db");
        let db_url = Url::from_file_path(db_file).unwrap();
        let config = DatabaseConfig::new(db_url);
        let db = TdPool::connect(&config).await.unwrap();

        // First upgrade: fresh install
        db.upgrade().await.unwrap();
        db.check().await.unwrap();

        // Second upgrade: should succeed (no-op)
        db.upgrade().await.unwrap();
        db.check().await.unwrap();
    }

    #[tokio::test]
    async fn test_execute_batch() {
        let db_file = tdtestdir!().join("test.db");
        let db_url = Url::from_file_path(db_file).unwrap();
        let config = DatabaseConfig::new(db_url);
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

    #[tokio::test]
    async fn test_backend_returns_sqlite() {
        let db_file = tdtestdir!().join("test.db");
        let db_url = Url::from_file_path(db_file).unwrap();
        let config = DatabaseConfig::new(db_url);
        let db = TdPool::connect(&config).await.unwrap();
        let backend: td_schema::schema::backend::Backend = db.kind().into();
        assert_eq!(backend, td_schema::schema::backend::Backend::Sqlite);
    }
}
