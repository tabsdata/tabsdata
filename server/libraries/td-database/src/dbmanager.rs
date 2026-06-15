//
// Copyright 2026 Tabs Data Inc.
//

use crate::config::{DatabaseConfig, Schema};
use crate::error::DbError;
use crate::pool::TdPool;
use tracing::info;

pub struct SchemaOwner {
    pub schema: Schema,
    pub owner: String,
    pub password: String,
}

pub async fn create(admin: &DatabaseConfig, owners: &[SchemaOwner]) -> Result<(), DbError> {
    let pool = TdPool::connect(admin).await?;
    for owner in owners {
        pool.provision_schema(&owner.schema, &owner.owner, &owner.password)
            .await?;
        info!(schema = %owner.schema, owner = %owner.owner, "Provisioned schema and owner");
    }
    Ok(())
}

pub async fn populate(config: &DatabaseConfig) -> Result<(), DbError> {
    let pool = TdPool::connect(config).await?;
    pool.upgrade().await?;
    pool.check().await?;
    Ok(())
}

pub async fn upgrade(config: &DatabaseConfig) -> Result<(), DbError> {
    let pool = TdPool::connect(config).await?;
    pool.upgrade().await?;
    pool.check().await?;
    Ok(())
}
