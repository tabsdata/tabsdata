//
// Copyright 2026 Tabs Data Inc.
//

//! The built-in apiserver schema component.

use crate::system::SystemPool;
use crate::system::{ComponentRegistration, SchemaComponent, SchemaSource};
use async_trait::async_trait;
use crate::error::DbError;
use crate::pool::TdPool;
use tracing::{debug, info};

/// Registry/override key identifying the apiserver component.
pub const APISERVER_COMPONENT_KEY: &str = "apiserver";

/// The apiserver component's default schema (a deployment may mount it elsewhere
/// via the `components:` override).
const APISERVER_SCHEMA: &str = "td_apiserver";

/// The apiserver schema: the full component backed by `td-schema`'s current
/// schema, with the edition gate and RLS / app-role steps layered on top of the
/// lean defaults.
pub struct ApiserverComponent;

#[async_trait]
impl SchemaComponent for ApiserverComponent {
    fn key(&self) -> &'static str {
        APISERVER_COMPONENT_KEY
    }

    fn default_schema(&self) -> &'static str {
        APISERVER_SCHEMA
    }

    fn schema_source(&self) -> &'static dyn SchemaSource {
        &td_schema::CurrentSource
    }

    async fn upgrade(&self, system: &SystemPool) -> Result<(), DbError> {
        info!("Starting database upgrade");
        // Upgrade-time prerequisites: a new-enough server and an upgrader role
        // that can actually create/alter in the schema. Fail fast and clearly
        // before applying any deltas.
        system.check_pg_min_version().await?;
        system.check_upgrader_permissions().await?;
        system.upgrade_db_version(self.schema_source()).await?;
        system.upgrade_tabsdata_edition().await?;
        system.ensure_app_role().await?;
        info!("Database upgrade complete");
        Ok(())
    }

    async fn check(&self, serving: &TdPool) -> Result<(), DbError> {
        // Runtime "is this database serveable?" check — all reads on the serving
        // (app) pool, so an already-upgraded apiserver needs no system access.
        serving.check_tabsdata_version().await?;
        debug!("db.check: tabsdata version OK");
        serving.check_db_version(self.schema_source()).await?;
        debug!("db.check: db version OK");
        serving.check_tabsdata_edition().await?;
        debug!("db.check: edition OK");
        serving.check_pg_min_version().await?;
        serving.check_rls_posture().await?;
        Ok(())
    }
}

// The built-in apiserver component self-registers here in `td-database`, which is
// always linked, so every edition and binary gets it. The enterprise `te-database`
// adds only its own components (e.g. the agent) on top.
inventory::submit! {
    ComponentRegistration { component: &ApiserverComponent }
}
