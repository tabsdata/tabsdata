//
// Copyright 2026 Tabs Data Inc.
//

//! Database **management**: the privileged [`SystemPool`], the schema-component
//! contract and registry, the built-in apiserver component, and the provisioning /
//! upgrade / check orchestration over the registry and the [`DatabaseDescriptor`].
//!
//! The system/admin counterpart to the serving [`TdPool`] — nothing here is needed
//! to serve a request. Component registration is the edition extension's job
//! (`te-database`), so this module stays edition-agnostic.

mod apiserver;
mod component;
mod pool;
pub(crate) mod sql;

#[cfg(test)]
mod tests;

pub use apiserver::{APISERVER_COMPONENT_KEY, ApiserverComponent};
pub use component::*;
pub use pool::SystemPool;

use crate::config::{ComponentOverride, DatabaseConfig, DatabaseDescriptor, DatabaseResolver};
use crate::error::DbError;
use crate::pool::TdPool;
use std::collections::BTreeMap;

// ── Orchestration over the registry + descriptor ───────────────────────
//
// The registry (`registered()`) is the source of truth for which components
// this binary manages; the descriptor supplies the database-wide connection and
// any per-component overrides (looked up by `component.key()`).

/// The schema a component is mounted on: its `components:` override, else the
/// component's default. The owner role takes the same name.
fn resolved_schema<'a>(
    ovr: Option<&'a ComponentOverride>,
    component: &'a dyn SchemaComponent,
) -> &'a str {
    ovr.and_then(|o| o.schema.as_deref())
        .unwrap_or(component.default_schema())
}

/// Build a component's connectable against the already-running server, applying
/// its per-component override. The live server URL comes from the resolver (the
/// embedded server is booted out of band), so no privileged handle is needed.
/// Shared by the system and serving pool openers.
fn component_config(
    resolver: &DatabaseResolver,
    component: &dyn SchemaComponent,
) -> Result<DatabaseConfig, DbError> {
    let ovr = resolver.component_override(component.key());
    resolver.serving_config(resolved_schema(ovr, component), ovr)
}

/// Open a component's per-schema system pool (connects as the schema owner role).
async fn system_pool_for(
    resolver: &DatabaseResolver<'_>,
    component: &dyn SchemaComponent,
) -> Result<SystemPool, DbError> {
    SystemPool::connect(&component_config(resolver, component)?).await
}

/// Open a component's serving pool against the running server — the same path
/// the apiserver uses at runtime.
async fn serving_for(
    resolver: &DatabaseResolver<'_>,
    component: &dyn SchemaComponent,
) -> Result<TdPool, DbError> {
    crate::db(&component_config(resolver, component)?).await
}

/// Create every registered component's schema and owner role on the system pool.
/// A component may customize creation by overriding [`SchemaComponent::provision`].
pub async fn provision_all(
    descriptor: &DatabaseDescriptor,
    system: &SystemPool,
) -> Result<(), DbError> {
    let resolver = descriptor.resolve()?;
    for component in registered() {
        let ovr = resolver.component_override(component.key());
        let schema = resolved_schema(ovr, component);
        let (owner, password) = resolver.owner_credentials(schema, ovr)?;
        component
            .provision(
                system,
                &ProvisionSpec {
                    schema,
                    owner: &owner,
                    password: &password,
                },
            )
            .await?;
    }
    Ok(())
}

/// Bring every registered component to its current version, each on its own
/// per-schema pool connected as the schema owner — the owner has DDL on its
/// schema, so no superuser handle is needed.
pub async fn upgrade_all(descriptor: &DatabaseDescriptor) -> Result<(), DbError> {
    let resolver = descriptor.resolve()?;
    for component in registered() {
        let pool = system_pool_for(&resolver, component).await?;
        component.upgrade(&pool).await?;
    }
    Ok(())
}

/// Verify every registered component is serveable and up to date, each on its own
/// serving pool opened from the descriptor — the same app path and the same check
/// the apiserver runs, so it validates the descriptor against the database it
/// actually connects to. No system handle.
pub async fn check_all(descriptor: &DatabaseDescriptor) -> Result<(), DbError> {
    for component in registered() {
        let serving = connect_serving(descriptor, component.key()).await?;
        component.check(&serving).await?;
    }
    Ok(())
}

/// Verify every registered component on its already-open serving pool — the
/// apiserver's boot check, reusing the pools from [`connect_app_all`] rather than
/// reconnecting (cf. [`check_all`], which opens fresh pools from the descriptor).
pub async fn check_pools(pools: &ServingPools) -> Result<(), DbError> {
    for component in registered() {
        component.check(pools.get(component.key())?).await?;
    }
    Ok(())
}

/// Verify one component is serveable on the given serving pool. Needs no
/// system access — an already-upgraded apiserver boots with only its app
/// pool and calls this on it.
pub async fn check(key: &str, serving: &TdPool) -> Result<(), DbError> {
    component(key)?.check(serving).await
}

/// Drop every registered component's schema and owner role on the system pool.
/// Destructive; the caller guards.
pub async fn drop_all(descriptor: &DatabaseDescriptor, system: &SystemPool) -> Result<(), DbError> {
    let resolver = descriptor.resolve()?;
    for component in registered() {
        let ovr = resolver.component_override(component.key());
        let schema = resolved_schema(ovr, component);
        let (owner, _) = resolver.owner_credentials(schema, ovr)?;
        system.drop_schema(schema, &owner).await?;
    }
    Ok(())
}

/// Report each component's recorded version and whether it is current, read from
/// its serving pool opened from the descriptor — the app path, no system handle.
pub async fn status(descriptor: &DatabaseDescriptor) -> Result<Vec<ComponentStatus>, DbError> {
    let resolver = descriptor.resolve()?;
    let mut out = Vec::new();
    for component in registered() {
        let ovr = resolver.component_override(component.key());
        let schema = resolved_schema(ovr, component);
        let db_version = connect_serving(descriptor, component.key())
            .await?
            .db_version()
            .await?;
        out.push(ComponentStatus {
            key: component.key(),
            schema: schema.to_string(),
            current: db_version == Some(component.schema_source().current_version()),
            db_version,
        });
    }
    Ok(out)
}

/// Build one component's serving pool — the same path the apiserver uses.
pub async fn build_pool(descriptor: &DatabaseDescriptor, key: &str) -> Result<TdPool, DbError> {
    serving_for(&descriptor.resolve()?, component(key)?).await
}

/// Open the privileged system pool from a descriptor — used only by the
/// `td_dbmanager` management commands that need superuser rights
/// (`create`/`drop`). Connects to the already-running server (the embedded server
/// is booted out of band by [`start_server`], never here), so opening it never
/// spawns a database.
pub async fn connect_system(descriptor: &DatabaseDescriptor) -> Result<SystemPool, DbError> {
    SystemPool::connect(&descriptor.resolve()?.system_connectable_running()?).await
}

/// Start the database **server** if the deployment hosts one in-process (the
/// embedded backend): boot it detached so it outlives this call and later commands
/// and the apiserver connect to it. A no-op for sidecar/remote, whose server is not
/// ours to start. Idempotent. Driven by `td_dbmanager start`.
pub async fn start_server(descriptor: &DatabaseDescriptor) -> Result<(), DbError> {
    #[cfg(all(postgres, feature = "embedded-postgres"))]
    {
        if let Some(ecfg) = descriptor.resolve()?.embedded_config() {
            return crate::embedded::start_detached(&ecfg).await;
        }
        tracing::info!("database is not embedded; nothing to start");
        Ok(())
    }
    #[cfg(not(all(postgres, feature = "embedded-postgres")))]
    {
        let _ = descriptor;
        tracing::info!("embedded postgres not built in; nothing to start");
        Ok(())
    }
}

/// Stop the in-process database **server** (embedded backend); a no-op for
/// sidecar/remote. Idempotent. Driven by `td_dbmanager stop`, run after the
/// apiserver's serving pools have closed.
pub async fn stop_server(descriptor: &DatabaseDescriptor) -> Result<(), DbError> {
    #[cfg(all(postgres, feature = "embedded-postgres"))]
    {
        if let Some(ecfg) = descriptor.resolve()?.embedded_config() {
            return crate::embedded::stop_detached(&ecfg).await;
        }
        tracing::info!("database is not embedded; nothing to stop");
        Ok(())
    }
    #[cfg(not(all(postgres, feature = "embedded-postgres")))]
    {
        let _ = descriptor;
        tracing::info!("embedded postgres not built in; nothing to stop");
        Ok(())
    }
}

/// Serving pools for every registered component, keyed by
/// [`SchemaComponent::key`]. Built once at startup by [`connect_app_all`]; each
/// context grabs the pool it needs by key rather than opening its own.
#[derive(Clone)]
pub struct ServingPools(BTreeMap<&'static str, TdPool>);

impl ServingPools {
    /// The serving pool for a registered component. Errors if the key is not in
    /// the set built at startup (i.e. no such registered component).
    pub fn get(&self, key: &str) -> Result<&TdPool, DbError> {
        self.0
            .get(key)
            .ok_or_else(|| DbError::UnknownSchemaEntry(key.to_owned()))
    }

    /// Close every component's serving pool — on shutdown, before the system
    /// handle (if any) tears the server down.
    pub async fn shutdown(&self) {
        for pool in self.0.values() {
            pool.shutdown().await;
        }
    }
}

/// Connect the application's database for the running server: an **app-only**
/// serving pool for every registered component, keyed by component. The same path
/// in every mode — the apiserver never boots or manages the database (the embedded
/// server is brought up by `td_dbmanager start`; schemas are provisioned/upgraded
/// by `td_dbmanager create`/`upgrade`), so it holds no privileged system access.
/// One descriptor-driven step builds them all up front so each context grabs the
/// pool it needs ([`ServingPools::get`]).
pub async fn connect_app_all(descriptor: &DatabaseDescriptor) -> Result<ServingPools, DbError> {
    let mut pools = BTreeMap::new();
    for component in registered() {
        pools.insert(
            component.key(),
            connect_serving(descriptor, component.key()).await?,
        );
    }
    Ok(ServingPools(pools))
}

/// A component's serving pool straight from the descriptor, with **no system
/// handle** — the app-only path, the same way the apiserver reaches the database.
/// Url-based backends use their configured URL; an embedded server's live address
/// is read from disk (it is booted by the apiserver, so it must already be running).
pub async fn connect_serving(
    descriptor: &DatabaseDescriptor,
    key: &str,
) -> Result<TdPool, DbError> {
    let component = component(key)?;
    let resolver = descriptor.resolve()?;
    let ovr = resolver.component_override(component.key());
    crate::db(&resolver.serving_config(resolved_schema(ovr, component), ovr)?).await
}

/// Verify every registered component's schema is reachable: open its serving pool
/// from the descriptor and ping it — the app path, so a successful ping proves the
/// database is reachable exactly as the application sees it. No system handle.
pub async fn ping_all(descriptor: &DatabaseDescriptor) -> Result<(), DbError> {
    for component in registered() {
        connect_serving(descriptor, component.key())
            .await?
            .acquire()
            .await?
            .ping()
            .await?;
    }
    Ok(())
}
