//
// Copyright 2026 Tabs Data Inc.
//

use crate::error::DbError;
use crate::pool::TdPool;
use async_trait::async_trait;
use tracing::{debug, info};

#[async_trait]
pub trait SchemaManager: Send + Sync {
    async fn upgrade(&self, pool: &TdPool) -> Result<(), DbError>;
    async fn check(&self, pool: &TdPool) -> Result<(), DbError>;
}

#[derive(Default)]
pub struct ApiserverSchemaManager;

#[async_trait]
impl SchemaManager for ApiserverSchemaManager {
    async fn upgrade(&self, pool: &TdPool) -> Result<(), DbError> {
        info!("Starting database upgrade");
        pool.upgrade_db_version().await?;
        pool.upgrade_tabsdata_edition().await?;
        pool.ensure_app_role().await?;
        info!("Database upgrade complete");
        Ok(())
    }

    async fn check(&self, pool: &TdPool) -> Result<(), DbError> {
        pool.check_tabsdata_version().await?;
        debug!("db.pool.check: tabsdata version OK");
        pool.check_db_version().await?;
        debug!("db.pool.check: db version OK");
        pool.check_tabsdata_edition().await?;
        debug!("db.pool.check: edition OK");
        pool.check_pg_min_version().await?;
        pool.check_rls_posture().await?;
        pool.check_upgrader_permissions().await?;
        Ok(())
    }
}

#[derive(Default)]
pub struct SystemSchemaManager;

#[async_trait]
impl SchemaManager for SystemSchemaManager {
    async fn upgrade(&self, _pool: &TdPool) -> Result<(), DbError> {
        Ok(())
    }

    async fn check(&self, _pool: &TdPool) -> Result<(), DbError> {
        Ok(())
    }
}
