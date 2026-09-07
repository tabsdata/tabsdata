//
// Copyright 2026 Tabsdata Inc.
//

//! The built-in apiserver schema component.

use crate::components::{
    AdminPool, ComponentRegistration, SchemaComponent, SchemaSource, UpgradeTransaction,
};
use crate::domain::{Apiserver, DbDomain};
use crate::error::DbError;
use crate::pool::PoolKind;
use crate::pool::TdPool;
use async_trait::async_trait;
use tracing::debug;

/// Registry/override key identifying the apiserver component.
pub const APISERVER_COMPONENT_KEY: &str = "apiserver";

/// The apiserver component's default schema.
const APISERVER_SCHEMA: &str = "td_apiserver";

/// The apiserver component: `td-schema`'s current schema plus the edition
/// gate and RLS/app-role steps.
pub struct ApiserverComponent;

#[async_trait]
impl SchemaComponent for ApiserverComponent {
    fn key(&self) -> &'static str {
        APISERVER_COMPONENT_KEY
    }

    fn default_schema(&self) -> &'static str {
        APISERVER_SCHEMA
    }

    fn pools(&self) -> &'static [PoolKind] {
        Apiserver::POOLS
    }

    fn schema_source(&self) -> &'static dyn SchemaSource {
        &td_schema::CurrentSource
    }

    /// Fail fast on server version / lock capacity / upgrader privileges
    /// before any delta.
    async fn preflight(&self, admin: &AdminPool) -> Result<(), DbError> {
        admin.check_pg_min_version().await?;
        admin.check_pg_lock_capacity().await?;
        admin.check_upgrader_permissions().await
    }

    async fn upgrade(&self, tx: &mut UpgradeTransaction<'_>) -> Result<(), DbError> {
        tx.upgrade_db_version(self.schema_source()).await?;
        tx.upgrade_tabsdata_edition().await?;
        tx.grant_rls_role_privileges().await
    }

    async fn check(&self, serving: &TdPool) -> Result<(), DbError> {
        serving.check_tabsdata_version().await?;
        debug!("db.check: tabsdata version OK");
        serving.check_db_version(self.schema_source()).await?;
        debug!("db.check: db version OK");
        serving.check_tabsdata_edition().await?;
        debug!("db.check: edition OK");
        serving.check_pg_min_version().await?;
        serving.check_pg_lock_capacity().await?;
        serving.check_rls_posture().await?;
        Ok(())
    }
}

// Self-registers in always-linked td-database, so every edition gets it.
inventory::submit! {
    ComponentRegistration { component: &ApiserverComponent }
}
