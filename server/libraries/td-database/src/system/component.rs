//
// Copyright 2026 Tabs Data Inc.
//

//! The schema-component contract and the inventory registry used to discover
//! components at runtime. An out-of-tree (e.g. enterprise) component depends on
//! this crate and implements [`SchemaComponent`]; the edition extension
//! (`te-database`) registers it via `inventory::submit!`.

use crate::system::SystemPool;
use async_trait::async_trait;
use crate::config::DatabaseDescriptor;
use crate::error::DbError;
use crate::pool::TdPool;

pub use td_schema::SchemaSource;
pub use td_schema::schema::backend::{BACKEND_COUNT, Backend};

/// Per-entry inputs to provisioning, computed by the descriptor.
pub struct ProvisionSpec<'a> {
    pub schema: &'a str,
    pub owner: &'a str,
    pub password: &'a str,
}

/// One self-describing schema component. Defaults are lean: a plain schema that
/// tracks its own `db_version` and grants the RLS app role so the serving pool
/// can read it. The apiserver component overrides
/// [`upgrade`](SchemaComponent::upgrade)/[`check`](SchemaComponent::check) to add
/// the edition gate and version prerequisites on top.
///
/// Each phase takes the pool whose privilege it needs: [`provision`](SchemaComponent::provision)
/// on the system pool (`CREATE SCHEMA`/`CREATE ROLE`), [`upgrade`](SchemaComponent::upgrade)
/// on the per-schema system pool (owner DDL), and [`check`](SchemaComponent::check)
/// on the serving [`TdPool`] only — so an already-upgraded apiserver verifies with
/// just its app pool.
#[async_trait]
pub trait SchemaComponent: Send + Sync + 'static {
    /// This component's identity in the registry and in the descriptor's
    /// `components:` override map. Stable; not necessarily the schema name.
    fn key(&self) -> &'static str;

    /// The component's **default** schema name (also the default owner-role name).
    /// A deployment may mount the component on a different schema via the
    /// `components:` override, so this is a default, not a fixed identity — the
    /// registry [`key`](Self::key) is the identity, independent of the schema name,
    /// so the same content can be mounted on any schema.
    fn default_schema(&self) -> &'static str;

    /// This component's schema content.
    fn schema_source(&self) -> &'static dyn SchemaSource;

    /// Create the schema and its owner role on the privileged system pool.
    async fn provision(&self, system: &SystemPool, spec: &ProvisionSpec<'_>) -> Result<(), DbError> {
        system
            .provision_schema(spec.schema, spec.owner, spec.password)
            .await
    }

    /// Bring the schema to its current version on its system pool, then grant the
    /// RLS app role the serving pool connects as.
    async fn upgrade(&self, system: &SystemPool) -> Result<(), DbError> {
        system.upgrade_db_version(self.schema_source()).await?;
        system.ensure_app_role().await
    }

    /// Verify the schema is serveable as-is, on the serving pool only — at
    /// minimum that it is at the current version. Runs with just the app pool.
    async fn check(&self, serving: &TdPool) -> Result<(), DbError> {
        serving.check_db_version(self.schema_source()).await
    }
}

/// Inventory registration slot. Each component crate submits exactly one of
/// these per component; the edition extension and binaries link the submitting
/// crates so the submissions reach the final binary.
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

/// Every `components:` override key must name a registered component — the only
/// thing the config can get wrong, since the registry (not the config) decides
/// which components exist. Also guards that the registry is non-empty: an empty
/// one means the binary forgot the [`force_link!`](crate::system) anchor, which
/// compiles fine but fails every descriptor at runtime (and would pass this check
/// vacuously when the config carries no `components:` overrides).
pub fn validate_descriptor(descriptor: &DatabaseDescriptor) -> Result<(), DbError> {
    if registered().next().is_none() {
        return Err(DbError::NoComponentsRegistered);
    }
    for key in descriptor.override_keys() {
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
