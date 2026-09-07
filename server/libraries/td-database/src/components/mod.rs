//
// Copyright 2026 Tabsdata Inc.
//

//! The registry schema components and their management: the privileged
//! [`AdminPool`] and the provision / upgrade / check orchestration over the
//! configured [`DatabaseComponents`]. Component registration is the edition
//! extension's job (`te-database`), so this module stays edition-agnostic.
//!
//! Logging shape, kept by every phase here: `info!` announces the phase and
//! confirms it, once per component, with the component key and its schema;
//! `debug!` covers the steps inside one; statements are sqlx's business.

mod admin_pool;
mod apiserver;
mod component;
mod plan;
pub(crate) mod sql;
mod upgrade;

#[cfg(test)]
mod tests;

use crate::config::{
    ComponentConfig, DatabaseComponents, DatabaseConfig, EMBEDDED_CREDENTIAL, PostgresConnect,
    Schema,
};
use crate::domain::DomainPools;
use crate::error::DbError;
use crate::pool::PoolKind;
use crate::pool::{ComponentPools, TdPool};
use crate::server::{EmbeddedPassword, resolve_password, server_connectable};
pub use admin_pool::AdminPool;
pub use apiserver::{APISERVER_COMPONENT_KEY, ApiserverComponent};
pub use component::{
    ComponentRegistration, ComponentStatus, ProvisionSpec, SchemaComponent, SchemaSource,
    component, registered, validate_components,
};
pub use plan::{ComponentUpgrade, UpgradeAction};
use sqlx::Connection;
use std::collections::{BTreeMap, BTreeSet};
use tracing::{debug, info, warn};
pub use upgrade::UpgradeTransaction;

/// Copies the connection target of the `database_server:` entry onto every
/// component. The server entry says where the database is; a component says
/// only what is its own (its password, schema, app role and pool), so the
/// address is authored once and cannot drift between the two files.
///
/// Only the location travels: credentials stay per component, since the server
/// entry logs in as the privileged admin and a component as its own owner role.
pub fn propagate_server_target(components: &mut DatabaseComponents, server: &ComponentConfig) {
    let source = &server.postgres;
    for entry in components.values_mut() {
        let target = &mut entry.postgres;
        target.host = source.host.clone();
        target.port = source.port;
        target.socket = source.socket.clone();
        target.database = source.database.clone();
        target.ssl_mode = source.ssl_mode;
        target.ssl_root_cert = source.ssl_root_cert.clone();
        target.ssl_root_cert_pem = source.ssl_root_cert_pem.clone();
        target.ssl_client_cert = source.ssl_client_cert.clone();
        target.ssl_client_cert_pem = source.ssl_client_cert_pem.clone();
        target.ssl_client_key = source.ssl_client_key.clone();
        target.ssl_client_key_pem = source.ssl_client_key_pem.clone();
    }
}

/// The config entry for a component; every registered component must have one.
pub fn component_entry<'a>(
    components: &'a DatabaseComponents,
    component: &dyn SchemaComponent,
) -> Result<&'a ComponentConfig, DbError> {
    components
        .get(component.key())
        .ok_or_else(|| DbError::MissingComponentEntry(component.key().to_owned()))
}

/// The schema a component is mounted on (the owner role takes the same name):
/// [`ComponentConfig::schema`] with this component's key and static default.
fn schema_of(entry: &ComponentConfig, component: &dyn SchemaComponent) -> Result<String, DbError> {
    entry.schema(component.key(), component.default_schema())
}

/// [`schema_of`], resolving the component's entry from the map.
pub fn resolved_schema(
    components: &DatabaseComponents,
    component: &dyn SchemaComponent,
) -> Result<String, DbError> {
    schema_of(component_entry(components, component)?, component)
}

/// [`resolved_schema`] for a component identified by key alone, for callers
/// reading a config whose components are not linked into their binary. The
/// static default is only reachable on an entry carrying no database name.
pub fn resolved_schema_of(
    components: &DatabaseComponents,
    key: &str,
    default_schema: &str,
) -> Result<String, DbError> {
    let entry = components
        .get(key)
        .ok_or_else(|| DbError::MissingComponentEntry(key.to_owned()))?;
    entry.schema(key, default_schema)
}

/// The role a component's serving connection logs in as: the entry's explicit
/// `username`, else the owner role its schema is named after.
fn username_of(username: Option<&str>, schema: &str) -> Result<String, DbError> {
    match username {
        Some(EMBEDDED_CREDENTIAL) => Err(DbError::EmbeddedUsernameNotAllowed),
        Some(username) => Ok(username.to_owned()),
        None => Ok(schema.to_owned()),
    }
}

/// [`username_of`], resolving the component's entry and schema from the map.
pub fn resolved_username(
    components: &DatabaseComponents,
    component: &dyn SchemaComponent,
) -> Result<String, DbError> {
    let entry = component_entry(components, component)?;
    let schema = schema_of(entry, component)?;
    username_of(entry.postgres.username.as_deref(), &schema)
}

/// [`resolved_username`] for a component identified by key alone — see
/// [`resolved_schema_of`].
pub fn resolved_username_of(
    components: &DatabaseComponents,
    key: &str,
    default_schema: &str,
) -> Result<String, DbError> {
    let entry = components
        .get(key)
        .ok_or_else(|| DbError::MissingComponentEntry(key.to_owned()))?;
    let schema = resolved_schema_of(components, key, default_schema)?;
    username_of(entry.postgres.username.as_deref(), &schema)
}

/// The identity a component's serving connection logs in as: its
/// [`username_of`] and its resolved password — required, so a missing one is a
/// config error at resolution, not a failed login later.
fn serving_credentials(
    username: Option<&str>,
    password: Option<&str>,
    schema: &str,
) -> Result<(String, String), DbError> {
    let username = username_of(username, schema)?;
    let password = resolve_password(password, EmbeddedPassword::Owner { schema })?
        .ok_or(DbError::MissingCredentials)?;
    Ok((username, password))
}

/// Serving connectable for a component: its entry, mounted on its resolved
/// schema, logging in with its [`serving_credentials`].
pub fn serving_config(
    components: &DatabaseComponents,
    component: &dyn SchemaComponent,
) -> Result<DatabaseConfig, DbError> {
    let entry = component_entry(components, component)?;
    let schema = schema_of(entry, component)?;
    let pg = &entry.postgres;
    let (username, password) =
        serving_credentials(pg.username.as_deref(), pg.password.as_deref(), &schema)?;
    Ok(DatabaseConfig {
        conn: PostgresConnect {
            options: pg.clone(),
            username,
            password,
        },
        schema: Some(Schema::new(&schema)),
        app_role: Some(entry.app_role()?),
        pool: entry.pool.clone(),
    })
}

/// The owner role (the schema name) and its password for provisioning. Role
/// creation needs a password, so absence is an error.
fn owner_credentials(
    entry: &ComponentConfig,
    component: &dyn SchemaComponent,
) -> Result<(String, String), DbError> {
    let schema = schema_of(entry, component)?;
    let password = resolve_password(
        entry.postgres.password.as_deref(),
        EmbeddedPassword::OwnerCreate { schema: &schema },
    )?
    .ok_or(DbError::MissingCredentials)?;
    Ok((schema, password))
}

async fn admin_pool_for(
    components: &DatabaseComponents,
    component: &dyn SchemaComponent,
) -> Result<AdminPool, DbError> {
    AdminPool::connect(&serving_config(components, component)?).await
}

async fn serving_for(
    components: &DatabaseComponents,
    component: &dyn SchemaComponent,
) -> Result<TdPool, DbError> {
    crate::db(&serving_config(components, component)?).await
}

/// The state of one component's top-level objects.
#[derive(Debug, Clone)]
pub struct ComponentObjects {
    pub key: &'static str,
    pub schema: String,
    pub schema_present: bool,
    pub owner: String,
    pub owner_present: bool,
    pub app_role: String,
    pub app_role_present: bool,
}

/// Inspect every registered component's schema, owner role and app role.
pub async fn inspect_all(
    components: &DatabaseComponents,
    admin: &AdminPool,
) -> Result<Vec<ComponentObjects>, DbError> {
    let mut found = Vec::new();
    for component in registered() {
        let entry = component_entry(components, component)?;
        let (owner, _) = owner_credentials(entry, component)?;
        let schema = schema_of(entry, component)?;
        let app_role = entry.app_role()?.to_owned();
        found.push(ComponentObjects {
            key: component.key(),
            schema_present: admin.schema_exists(&schema).await?,
            owner_present: admin.role_exists(&owner).await?,
            app_role_present: admin.role_exists(&app_role).await?,
            schema,
            owner,
            app_role,
        });
    }
    Ok(found)
}

/// Refuse to create when a schema or its owner is already present. App roles
/// are cluster-global and shared, so their presence is reported, not fatal.
pub fn require_absent(found: &[ComponentObjects]) -> Result<(), DbError> {
    let present: Vec<String> = found
        .iter()
        .flat_map(|objects| {
            let mut held = Vec::new();
            if objects.schema_present {
                held.push(format!(
                    "component '{}' schema '{}'",
                    objects.key, objects.schema
                ));
            }
            if objects.owner_present {
                held.push(format!(
                    "component '{}' owner role '{}'",
                    objects.key, objects.owner
                ));
            }
            held
        })
        .collect();
    if present.is_empty() {
        return Ok(());
    }
    Err(DbError::CreatePreconditionFailed(present.join(", ")))
}

/// Refuse to upgrade when a schema or its owner is missing.
pub fn require_present(found: &[ComponentObjects]) -> Result<(), DbError> {
    let absent: Vec<String> = found
        .iter()
        .flat_map(|objects| {
            let mut held = Vec::new();
            if !objects.schema_present {
                held.push(format!(
                    "component '{}' schema '{}'",
                    objects.key, objects.schema
                ));
            }
            if !objects.owner_present {
                held.push(format!(
                    "component '{}' owner role '{}'",
                    objects.key, objects.owner
                ));
            }
            held
        })
        .collect();
    if absent.is_empty() {
        return Ok(());
    }
    Err(DbError::UpgradePreconditionFailed(absent.join(", ")))
}

/// Create every registered component's schema, owner role and app role on the
/// privileged pool. Each component provisions its own entry's app role — not
/// the privileged entry's — so serving `SET ROLE` targets what was created.
///
/// Provisioning owns the create precondition: its DDL is idempotent, so
/// without the check a `create` over a live database would adopt its schemas
/// instead of failing. Returns the pre-provision state for the caller's report.
pub async fn provision_all(
    components: &DatabaseComponents,
    admin: &AdminPool,
) -> Result<Vec<ComponentObjects>, DbError> {
    let before = inspect_all(components, admin).await?;
    require_absent(&before)?;
    for objects in &before {
        if objects.app_role_present {
            warn!(
                component = objects.key,
                app_role = %objects.app_role,
                "App role already exists; provisioning reuses it"
            );
        }
    }
    for component in registered() {
        let entry = component_entry(components, component)?;
        let (owner, password) = owner_credentials(entry, component)?;
        let app_role = entry.app_role()?;
        info!(
            component = component.key(),
            schema = %owner,
            owner = %owner,
            app_role = %app_role,
            version = component.schema_source().current_version(),
            "Provisioning component"
        );
        component
            .provision(
                admin,
                &ProvisionSpec {
                    schema: &owner,
                    owner: &owner,
                    password: &password,
                    app_role: &app_role,
                },
            )
            .await?;
    }
    Ok(before)
}

/// Bring every registered component to its current version, each as its schema
/// owner (DDL on its own schema — no server handle needed). Reports the version
/// transition each component took.
pub async fn upgrade_all(
    components: &DatabaseComponents,
) -> Result<Vec<ComponentUpgrade>, DbError> {
    let mut upgraded = Vec::new();
    for component in registered() {
        let schema = resolved_schema(components, component)?;
        info!(
            component = component.key(),
            schema = %schema,
            version = component.schema_source().current_version(),
            "Upgrading component"
        );
        let pool = admin_pool_for(components, component).await?;
        let action = pool.upgrade(component).await?;
        if action.is_none() {
            warn!(
                component = component.key(),
                schema = %schema,
                "Component reported no schema version transition"
            );
        }
        upgraded.push(ComponentUpgrade {
            key: component.key(),
            schema,
            action,
        });
    }
    Ok(upgraded)
}

/// Every declared pool must have its `pool:` block and every serving block
/// must be declared — a block for a pool the component does not have is a
/// config error, same as a missing one.
pub fn validate_component_pools(components: &DatabaseComponents) -> Result<(), DbError> {
    for component in registered() {
        let entry = component_entry(components, component)?;
        for kind in [PoolKind::Regular, PoolKind::Exclusive] {
            let declared = component.pools().contains(&kind);
            let configured = entry.pool.get(kind).is_some();
            if declared && !configured {
                return Err(DbError::MissingComponentPoolConfig(
                    component.key(),
                    kind.as_str(),
                ));
            }
            if configured && !declared {
                return Err(DbError::UnexpectedPoolConfig(
                    component.key(),
                    kind.as_str(),
                ));
            }
        }
    }
    Ok(())
}

/// Verify every registered component is serveable and up to date, on fresh
/// serving pools opened from the config.
pub async fn check_all(components: &DatabaseComponents) -> Result<(), DbError> {
    for component in registered() {
        let serving = connect_serving(components, component.key()).await?;
        component.check(&serving).await?;
    }
    Ok(())
}

/// Seed every registered component's release baseline on fresh serving pools
/// opened from the config. Run after `upgrade_all` on both create and upgrade;
/// each `seed` is an idempotent full sync, and components that ship structure
/// only inherit the trait no-op.
pub async fn seed_all(components: &DatabaseComponents) -> Result<(), DbError> {
    for component in registered() {
        let schema = resolved_schema(components, component)?;
        debug!(component = component.key(), schema = %schema, "Seeding baseline");
        let serving = connect_serving(components, component.key()).await?;
        component.seed(&serving).await?;
        info!(component = component.key(), schema = %schema, "Baseline seeded");
    }
    Ok(())
}

/// Verify every registered component on its already-open serving pool — the
/// apiserver's boot check.
pub async fn check_pools(pools: &ServingPools) -> Result<(), DbError> {
    for component in registered() {
        let regular = pools.get(component.key())?.get(PoolKind::Regular).ok_or(
            DbError::MissingComponentPool(component.key(), PoolKind::Regular),
        )?;
        component.check(regular).await?;
    }
    Ok(())
}

/// Verify one component is serveable on the given serving pool.
pub async fn check(key: &str, serving: &TdPool) -> Result<(), DbError> {
    component(key)?.check(serving).await
}

/// Drop every registered component's schema and owner role. Destructive; the
/// caller guards.
pub async fn drop_all(components: &DatabaseComponents, admin: &AdminPool) -> Result<(), DbError> {
    let mut app_roles = BTreeSet::new();
    for component in registered() {
        let entry = component_entry(components, component)?;
        let (owner, _) = owner_credentials(entry, component)?;
        debug!(component = component.key(), schema = %owner, "Dropping component");
        admin.drop_schema(&owner, &owner).await?;
        info!(component = component.key(), schema = %owner, "Component dropped");
        app_roles.insert(entry.app_role()?);
    }
    for app_role in app_roles {
        admin.drop_app_role(&app_role).await?;
    }
    Ok(())
}

/// Report each component's recorded version and whether it is current.
pub async fn status(components: &DatabaseComponents) -> Result<Vec<ComponentStatus>, DbError> {
    let mut out = Vec::new();
    for component in registered() {
        let schema = resolved_schema(components, component)?;
        let db_version = connect_serving(components, component.key())
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
pub async fn build_pool(components: &DatabaseComponents, key: &str) -> Result<TdPool, DbError> {
    serving_for(components, component(key)?).await
}

/// Open the privileged pool on the `database_server:` entry. Connects to the
/// already-running server (booted out of band by
/// [`start_server`](crate::server::start_server)) — never spawns one.
pub async fn connect_server(server: &ComponentConfig) -> Result<AdminPool, DbError> {
    AdminPool::connect(&server_connectable(server)?)
        .await
        .map_err(auth_hint)
}

/// Map PostgreSQL `28P01` to the likely cause: the data directory keeps the
/// credentials it was initialized with, so a later secret rotation no longer
/// matches.
fn auth_hint(error: DbError) -> DbError {
    match &error {
        DbError::FailedToConnectToDatabase(sqlx::Error::Database(db))
            if db.code().as_deref() == Some("28P01") =>
        {
            DbError::ServerAuthenticationFailed(db.to_string())
        }
        _ => error,
    }
}

/// Serving pools for every registered component, keyed by
/// [`SchemaComponent::key`]. Built once at startup by [`connect_app_all`].
#[derive(Clone)]
pub struct ServingPools(BTreeMap<&'static str, ComponentPools>);

impl ServingPools {
    /// The serving pools of a registered component.
    pub fn get(&self, key: &str) -> Result<&ComponentPools, DbError> {
        self.0
            .get(key)
            .ok_or_else(|| DbError::UnknownSchemaEntry(key.to_owned()))
    }

    /// Close every component's serving pools — on shutdown.
    pub async fn shutdown(&self) {
        for pools in self.0.values() {
            pools.shutdown().await;
        }
    }

    /// These pools keyed by domain (component keys are the domains'
    /// `SCHEMA_KEY`s).
    pub fn domain_pools(&self) -> DomainPools {
        DomainPools::from_map(self.0.clone())
    }
}

/// **App-only** serving pools for every registered component: one pool per
/// declared kind, each sized by its own explicit `pool:` block. The apiserver
/// never boots or manages the database, so it holds no privileged access.
pub async fn connect_app_all(components: &DatabaseComponents) -> Result<ServingPools, DbError> {
    validate_component_pools(components)?;
    let mut pools = BTreeMap::new();
    for component in registered() {
        let key = component.key();
        let config = serving_config(components, component)?;
        let mut component_pools = ComponentPools::default();
        let mut kinds = component.pools().to_vec();
        kinds.sort_by_key(|kind| !matches!(kind, PoolKind::Regular));
        for kind in kinds {
            let pool = TdPool::connect_as(&config, kind).await?.of_component(key);
            component_pools.set(kind, pool);
        }
        pools.insert(key, component_pools);
    }
    Ok(ServingPools(pools))
}

/// One writable app-only pool straight from the component's `pool.regular`
/// block — the dbmanager lifecycle path (check/seed/status/ping).
pub async fn connect_serving(
    components: &DatabaseComponents,
    key: &str,
) -> Result<TdPool, DbError> {
    serving_for(components, component(key)?).await
}

/// Ping every registered component's serving pool — proves the database is
/// reachable exactly as the application sees it.
pub async fn ping_all(components: &DatabaseComponents) -> Result<(), DbError> {
    for component in registered() {
        connect_serving(components, component.key())
            .await?
            .begin()
            .await?
            .ping()
            .await?;
    }
    Ok(())
}
