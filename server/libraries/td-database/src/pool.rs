//
// Copyright 2026 Tabs Data Inc.
//

use crate::config::DatabaseConfig;
use crate::dynamic::{DynamicConnection, DynamicPool, DynamicTransaction};
use crate::error::DbError;
use crate::sea_query::builder::SeaQueryExt;
use sea_query::{Expr, ExprTrait, Iden, Query};
use sqlx::Error;
use sqlx::migrate::Migrator;
use std::cmp::Ordering;
use std::fmt::Display;
use td_schema::{DB_EDITION_NAME, DB_VERSION_NAME, DB_VERSION_VALUE, TabsdataSystem};
use te_system::edition::{Compatible, Edition, TabsdataEdition};
use tracing::{debug, trace};

#[derive(Debug, Clone)]
pub struct TdPool {
    pub schema: &'static Migrator,
    pub pool: DynamicPool,
}

impl AsRef<DynamicPool> for TdPool {
    fn as_ref(&self) -> &DynamicPool {
        &self.pool
    }
}

/// Specialized Sqlx [`Pool`] that uses two pools, one for read-only operations and one for
/// read-write operations.
impl TdPool {
    /// Connects to a database using the given configuration.
    ///
    /// The schema is assumed to be up to date.
    pub async fn connect(
        config: &DatabaseConfig,
        schema: &'static Migrator,
    ) -> Result<Self, DbError> {
        let pool = config.connect().await?;
        debug!("Connected to database pool: {pool:#?}");
        trace!("Connected to database with migrator: {schema:#?}");
        Ok(Self { schema, pool })
    }

    /// Creates a database using the given configuration.
    ///
    /// Creates the schema.
    pub async fn create(
        config: &DatabaseConfig,
        schema: &'static Migrator,
    ) -> Result<Self, DbError> {
        let this = Self::connect(config, schema).await?;
        this.upgrade().await?;
        Ok(this)
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

    async fn check_db_version(&self) -> Result<(), DbError> {
        let res: String = Query::select()
            .column(TabsdataSystem::Value)
            .from(TabsdataSystem::Table)
            .and_where(Expr::col(TabsdataSystem::Name).eq(DB_VERSION_NAME))
            .build_query_scalar()
            .fetch_one(&self.pool)
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

    pub async fn upgrade_db_version(&self) -> Result<(), DbError> {
        match &self.pool {
            DynamicPool::Sqlite { rw, .. } => self.schema.run(rw).await,
            DynamicPool::Postgres { rw, .. } => self.schema.run(rw).await,
            DynamicPool::MySql { rw, .. } => self.schema.run(rw).await,
        }
        .map_err(DbError::FailedToCreateOrUpgradeDatabaseSchema)?;
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
                // if not set, set it to the runtime edition
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
            Some(instance_edition) => {
                if !runtime_edition.is_compatible(&instance_edition) {
                    Err(DbError::CannotUpgradeEdition(format!(
                        "instance edition is '{instance_edition}' cannot be upgraded to '{}'",
                        runtime_edition.label()
                    )))
                } else if runtime_edition.requires_upgrade(&instance_edition) {
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
        // testing db_schema macro
        let db_file = tdtestdir!().join("test.db");
        let db_url = Url::from_file_path(db_file.clone()).unwrap();
        let config = DatabaseConfig::new(db_url);
        let db = crate::db_with_schema(&config, td_schema::test_schema())
            .await
            .unwrap();
        db.upgrade_db_version().await.unwrap();
        let _ = raw_query!(db, "SELECT * FROM foo")
            .build_query()
            .execute(&db)
            .await
            .unwrap();
        assert!(db_file.exists());
    }

    #[tokio::test]
    async fn open_existing_database() {
        let schema = td_schema::test_schema();

        let db_file = tdtestdir!().join("test.db");
        let db_url = Url::from_file_path(db_file).unwrap();
        let config = DatabaseConfig::new(db_url);
        {
            let db = TdPool::connect(&config, schema).await.unwrap();
            db.upgrade_db_version().await.unwrap();
            raw_query!(db, "INSERT INTO foo values('a', 'A')")
                .build_query()
                .execute(&db)
                .await
                .unwrap();
        }

        let db = TdPool::connect(&config, schema).await.unwrap();
        db.upgrade_db_version().await.unwrap();
        let res = raw_query!(db, "SELECT * FROM foo")
            .build_query()
            .fetch_all(&db)
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        drop(db);
    }

    #[tokio::test]
    async fn test_tabsdata_database_schema_does_not_exist() {
        let schema = td_schema::test_schema();
        let db_file = tdtestdir!().join("test.db");
        let db_url = Url::from_file_path(db_file).unwrap();
        let config = DatabaseConfig::new(db_url);
        // tabsdata schema does not exist
        let db = TdPool::connect(&config, schema).await.unwrap();
        db.upgrade_db_version().await.unwrap();
        let res = db.check_db_version().await;
        assert!(matches!(res, Err(DbError::DatabaseSchemaDoesNotExist)));
    }

    #[tokio::test]
    async fn test_tabsdata_database_schema_ok() {
        let schema = td_schema::schema();
        let db_file = tdtestdir!().join("test.db");
        let db_url = Url::from_file_path(db_file).unwrap();
        let config = DatabaseConfig::new(db_url);
        let db = TdPool::create(&config, schema).await.unwrap();
        assert!(db.check_db_version().await.is_ok());
    }

    #[tokio::test]
    async fn test_tabsdata_database_schema_missing_db_version() {
        let schema = td_schema::schema();
        let db_file = tdtestdir!().join("test.db");
        let db_url = Url::from_file_path(db_file).unwrap();
        let config = DatabaseConfig::new(db_url);
        let db = TdPool::create(&config, schema).await.unwrap();

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
        let schema = td_schema::schema();
        let db_file = tdtestdir!().join("test.db");
        let db_url = Url::from_file_path(db_file).unwrap();
        let config = DatabaseConfig::new(db_url);
        let db = TdPool::create(&config, schema).await.unwrap();

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
        let schema = td_schema::schema();
        let db_file = tdtestdir!().join("test.db");
        let db_url = Url::from_file_path(db_file).unwrap();
        let config = DatabaseConfig::new(db_url);
        let db = TdPool::create(&config, schema).await.unwrap();

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
        let schema = td_schema::schema();
        let db_file = tdtestdir!().join("test.db");
        let db_url = Url::from_file_path(db_file).unwrap();
        let config = DatabaseConfig::new(db_url);
        let db = TdPool::create(&config, schema).await.unwrap();

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
        let schema = td_schema::schema();
        let db_file = tdtestdir!().join("test.db");
        let db_url = Url::from_file_path(db_file).unwrap();
        let config = DatabaseConfig::new(db_url);
        let db = TdPool::create(&config, schema).await.unwrap();
        let db_edition = db.parse_tabsdata_edition().await.unwrap();
        let runtime_edition = TabsdataEdition;
        assert_eq!(db_edition.unwrap(), runtime_edition.label());
    }

    #[tokio::test]
    async fn test_tabsdata_database_schema_invalid_edition() {
        let schema = td_schema::schema();
        let db_file = tdtestdir!().join("test.db");
        let db_url = Url::from_file_path(db_file).unwrap();
        let config = DatabaseConfig::new(db_url);
        let db = TdPool::create(&config, schema).await.unwrap();

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
}
