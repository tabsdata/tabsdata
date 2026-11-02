//
// Copyright 2026 Tabsdata Inc.
//

//! The schema-component contract and the [`Registry`] of the components a
//! binary manages: the built-in apiserver component and the ones the edition
//! passes in.

use crate::components::extensions::OptionalExtension;
use crate::components::{AdminPool, ApiserverComponent, UpgradeTransaction, resolved_schema};
use crate::config::{DatabaseComponents, RlsRoles};
use crate::domain::DomainPools;
use crate::error::DbError;
use crate::pool::PoolKind;
use crate::pool::TdPool;
use async_trait::async_trait;
use futures_util::future::BoxFuture;
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

    /// The optional extensions this component's schema uses when the database
    /// has them: `td_dbmanager db extensions add` creates one into its schema,
    /// marks it in its ledger and builds its dependent objects. Default: none.
    fn extensions(&self) -> &'static [OptionalExtension] {
        &[]
    }

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

/// Baseline content a crate above `td-database` seeds into a component's
/// schema, run by [`seed_all`](crate::components::seed_all) after the
/// component's own [`SchemaComponent::seed`], on the component's serving
/// pools. An error fails the create or upgrade.
pub struct BaselineSeeder {
    pub component: &'static str,
    pub name: &'static str,
    pub seed: fn(DomainPools) -> BoxFuture<'static, Result<(), String>>,
}

/// The schema components a binary manages, the built-in apiserver component
/// first, and the baseline content seeded into them.
#[derive(Clone)]
pub struct Registry {
    components: Vec<&'static dyn SchemaComponent>,
    seeders: &'static [BaselineSeeder],
}

impl Registry {
    /// The apiserver component plus the edition's `components` and `seeders`.
    pub fn new(
        components: &'static [&'static dyn SchemaComponent],
        seeders: &'static [BaselineSeeder],
    ) -> Self {
        let mut all: Vec<&'static dyn SchemaComponent> = vec![&ApiserverComponent];
        all.extend_from_slice(components);
        Self {
            components: all,
            seeders,
        }
    }

    pub fn components(&self) -> impl Iterator<Item = &'static dyn SchemaComponent> + '_ {
        self.components.iter().copied()
    }

    /// Resolve a component by its key.
    pub fn component(&self, key: &str) -> Result<&'static dyn SchemaComponent, DbError> {
        self.components()
            .find(|c| c.key() == key)
            .ok_or_else(|| DbError::UnknownSchemaEntry(key.to_owned()))
    }

    /// The seeders contributed to `component`.
    pub fn seeders<'a>(
        &self,
        component: &'a str,
    ) -> impl Iterator<Item = &'static BaselineSeeder> + 'a {
        self.seeders
            .iter()
            .filter(move |seeder| seeder.component == component)
    }
}

/// Validate the `database:` map against the registry: every registered
/// component has its own entry, every key names a registered component, and no
/// two components resolve to the same schema — provisioning is idempotent, so a
/// collision has the second component silently share the first one's tables.
pub fn validate_components(
    registry: &Registry,
    components: &DatabaseComponents,
) -> Result<(), DbError> {
    for registered_component in registry.components() {
        super::component_entry(components, registered_component)?;
    }
    for key in components.keys() {
        let _ = registry.component(key)?;
    }
    let resolved: Vec<(&'static str, String)> = registry
        .components()
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
