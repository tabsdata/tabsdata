//
// Copyright 2026 Tabs Data Inc.
//

//! The schema-component contract and the inventory registry used to discover
//! components at runtime; the edition extension (`te-database`) registers
//! components via `inventory::submit!`.

use crate::components::AdminPool;
use crate::config::DatabaseComponents;
use crate::error::DbError;
use crate::pool::TdPool;
use async_trait::async_trait;

pub use td_schema::SchemaSource;
pub use td_schema::schema::backend::{BACKEND_COUNT, Backend};

/// Per-entry inputs to provisioning, resolved from the component's config entry.
pub struct ProvisionSpec<'a> {
    pub schema: &'a str,
    pub owner: &'a str,
    pub password: &'a str,
}

/// One self-describing schema component. Each phase takes the pool whose
/// privilege it needs: `provision` on the privileged server pool, `upgrade` on
/// the per-schema admin pool (owner DDL), `check` on the serving [`TdPool`]
/// only — so an already-upgraded apiserver verifies with just its app pool.
#[async_trait]
pub trait SchemaComponent: Send + Sync + 'static {
    /// This component's identity in the registry and in the `database:` map.
    fn key(&self) -> &'static str;

    /// The default schema name (also the default owner-role name); a
    /// deployment may mount the component elsewhere via its entry's `schema:`.
    fn default_schema(&self) -> &'static str;

    /// This component's schema content.
    fn schema_source(&self) -> &'static dyn SchemaSource;

    /// Create the schema and its owner role.
    async fn provision(&self, admin: &AdminPool, spec: &ProvisionSpec<'_>) -> Result<(), DbError> {
        admin
            .provision_schema(spec.schema, spec.owner, spec.password)
            .await
    }

    /// Bring the schema to its current version, then grant the RLS app role.
    async fn upgrade(&self, admin: &AdminPool) -> Result<(), DbError> {
        admin.upgrade_db_version(self.schema_source()).await?;
        admin.ensure_app_role().await
    }

    /// Verify the schema is serveable as-is, on the serving pool only.
    async fn check(&self, serving: &TdPool) -> Result<(), DbError> {
        serving.check_db_version(self.schema_source()).await
    }

    /// Seed the release-baseline *content* on the serving [`TdPool`], after
    /// `upgrade`. Idempotent full sync (re-seed prunes orphaned baseline rows,
    /// keeps the user overlay). Default: no-op — most components ship schema
    /// structure only and seed nothing.
    async fn seed(&self, _serving: &TdPool) -> Result<(), DbError> {
        Ok(())
    }
}

/// Inventory registration slot, one submission per component.
pub struct ComponentRegistration {
    pub component: &'static dyn SchemaComponent,
}
inventory::collect!(ComponentRegistration);

/// All components registered (linked) into this binary.
pub fn registered() -> impl Iterator<Item = &'static dyn SchemaComponent> {
    inventory::iter::<ComponentRegistration>
        .into_iter()
        .map(|r| r.component)
}

/// Resolve a component by its key.
pub fn component(key: &str) -> Result<&'static dyn SchemaComponent, DbError> {
    registered()
        .find(|c| c.key() == key)
        .ok_or_else(|| DbError::UnknownSchemaEntry(key.to_owned()))
}

/// Validate the `database:` map against the registry: every registered
/// component has its own entry and every key names a registered component. An
/// empty registry means the binary forgot the `force_link!` anchor, which
/// compiles fine but fails every config at runtime.
pub fn validate_components(components: &DatabaseComponents) -> Result<(), DbError> {
    if registered().next().is_none() {
        return Err(DbError::NoComponentsRegistered);
    }
    for registered_component in registered() {
        super::component_entry(components, registered_component)?;
    }
    for key in components.keys() {
        let _ = component(key)?;
    }
    Ok(())
}

/// Snapshot of one component's recorded state, for `status` reporting.
pub struct ComponentStatus {
    pub key: &'static str,
    pub schema: String,
    pub db_version: Option<usize>,
    pub current: bool,
}
