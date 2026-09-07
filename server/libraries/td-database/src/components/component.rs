//
// Copyright 2026 Tabsdata Inc.
//

//! The schema-component contract and the inventory registry used to discover
//! components at runtime; the edition extension (`te-database`) registers
//! components via `inventory::submit!`.

use crate::components::{AdminPool, UpgradeTransaction, resolved_schema};
use crate::config::{DatabaseComponents, RlsRoles};
use crate::error::DbError;
use crate::pool::PoolKind;
use crate::pool::TdPool;
use async_trait::async_trait;
use std::collections::BTreeMap;
pub use td_schema::SchemaSource;

/// Per-entry inputs to provisioning, resolved from the component's config entry.
pub struct ProvisionSpec<'a> {
    pub(crate) schema: &'a str,
    pub(crate) owner: &'a str,
    pub(crate) password: &'a str,
    /// The RLS roles this component's serving connections switch between —
    /// the entry's own, not the privileged server entry's.
    pub(crate) roles: &'a RlsRoles,
}

/// One self-describing schema component. Each phase takes the handle whose
/// privilege it needs: `provision` on the privileged server pool, `preflight`
/// on the per-schema admin pool (owner DDL), `upgrade` on the transaction that
/// pool opens, `check` on the serving [`TdPool`] only — so an already-upgraded
/// apiserver verifies with just its app pool.
#[async_trait]
pub trait SchemaComponent: Send + Sync + 'static {
    /// This component's identity in the registry and in the `database:` map.
    fn key(&self) -> &'static str;

    /// The default schema name (also the default owner-role name); a
    /// deployment may mount the component elsewhere via its entry's `schema:`.
    fn default_schema(&self) -> &'static str;

    /// The pools this component uses — cite the domain marker's
    /// [`POOLS`](crate::domain::DbDomain::POOLS) so the declaration exists
    /// once. Its config entry must carry exactly these `pool:` blocks.
    fn pools(&self) -> &'static [PoolKind];

    /// This component's schema content.
    fn schema_source(&self) -> &'static dyn SchemaSource;

    /// Create the schema, its owner role, and its RLS roles.
    async fn provision(&self, admin: &AdminPool, spec: &ProvisionSpec<'_>) -> Result<(), DbError> {
        admin.provision_schema(spec).await
    }

    /// Read-only gates run before the upgrade transaction opens, so a server
    /// or privilege problem fails fast instead of half way through a ladder.
    /// Default: nothing to check.
    async fn preflight(&self, _admin: &AdminPool) -> Result<(), DbError> {
        Ok(())
    }

    /// Bring the schema to its current version, then grant the RLS roles.
    /// Runs inside the single upgrade transaction [`AdminPool::upgrade`] owns:
    /// every step here commits together or not at all.
    async fn upgrade(&self, tx: &mut UpgradeTransaction<'_>) -> Result<(), DbError> {
        tx.upgrade_db_version(self.schema_source()).await?;
        tx.grant_rls_role_privileges().await
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
/// component has its own entry, every key names a registered component, and no
/// two components resolve to the same schema — provisioning is idempotent, so a
/// collision has the second component silently share the first one's tables. An
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
    let resolved: Vec<(&'static str, String)> = registered()
        .map(|c| Ok((c.key(), resolved_schema(components, c)?)))
        .collect::<Result<_, DbError>>()?;
    require_distinct_schemas(&resolved)
}

/// No two components may own the same schema.
fn require_distinct_schemas(resolved: &[(&str, String)]) -> Result<(), DbError> {
    let mut owners: BTreeMap<&str, &str> = BTreeMap::new();
    for (key, schema) in resolved {
        if let Some(owner) = owners.insert(schema, key) {
            return Err(DbError::DuplicateComponentSchema(
                schema.clone(),
                owner.to_owned(),
                (*key).to_owned(),
            ));
        }
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

#[cfg(test)]
mod tests {
    use super::require_distinct_schemas;
    use crate::error::DbError;

    #[test]
    fn distinct_schemas_pass() {
        let resolved = [
            ("apiserver", "td_apiserver".to_owned()),
            ("aiagent", "td_aiagent".to_owned()),
        ];
        assert!(require_distinct_schemas(&resolved).is_ok());
    }

    #[test]
    fn shared_schema_is_rejected_naming_both_components() {
        let resolved = [
            ("apiserver", "shared".to_owned()),
            ("aiagent", "shared".to_owned()),
        ];
        let error = require_distinct_schemas(&resolved).unwrap_err();
        let DbError::DuplicateComponentSchema(schema, first, second) = error else {
            panic!("expected a duplicate-schema error, got {error:?}");
        };
        assert_eq!(schema, "shared");
        assert_eq!(first, "apiserver");
        assert_eq!(second, "aiagent");
    }
}
