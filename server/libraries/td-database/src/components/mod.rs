//
// Copyright 2026 Tabsdata Inc.
//

//! The registry schema components and their management: the privileged
//! [`AdminPool`] and the provision / upgrade / check orchestration over the
//! configured [`DatabaseComponents`]. Component registration is the edition
//! extension's job (`te-database`), so this module stays edition-agnostic.

mod apiserver;
mod component;
mod pool;
pub(crate) mod sql;

#[cfg(test)]
mod tests;

pub use apiserver::{APISERVER_COMPONENT_KEY, ApiserverComponent};
pub use component::*;
pub use pool::AdminPool;

#[cfg(mysql)]
use crate::config::MySqlConnect;
#[cfg(postgres)]
use crate::config::PostgresConnect;
use crate::config::{
    BackendConfig, ComponentConfig, ConnectConfig, DatabaseComponents, DatabaseConfig,
    EMBEDDED_CREDENTIAL, Schema,
};
use crate::domain::DomainPools;
use crate::error::DbError;
use crate::pool::TdPool;
use crate::server::{EmbeddedPassword, resolve_password, server_connectable};
use std::collections::{BTreeMap, BTreeSet};

/// The config entry for a component; every registered component must have one.
pub(crate) fn component_entry<'a>(
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

/// The identity a component's serving connection logs in as: the entry's
/// explicit `username` (else the schema-named owner role) and its resolved
/// password — required, so a missing one is a config error at resolution, not
/// a failed login later.
fn serving_credentials(
    username: Option<&str>,
    password: Option<&str>,
    schema: &str,
) -> Result<(String, String), DbError> {
    let username = match username {
        Some(EMBEDDED_CREDENTIAL) => return Err(DbError::EmbeddedUsernameNotAllowed),
        Some(username) => username.to_owned(),
        None => schema.to_owned(),
    };
    let password = resolve_password(password, EmbeddedPassword::Owner { schema })?
        .ok_or(DbError::MissingCredentials)?;
    Ok((username, password))
}

/// The entry's configured password, from the backends that authenticate.
fn entry_password(conn: &BackendConfig) -> Option<&str> {
    match conn {
        #[cfg(sqlite)]
        BackendConfig::Sqlite(_) => None,
        #[cfg(postgres)]
        BackendConfig::Postgres(c) => c.password.as_deref(),
        #[cfg(mysql)]
        BackendConfig::MySql(c) => c.password.as_deref(),
    }
}

/// Serving connectable for a component: its entry, mounted on its resolved
/// schema, logging in with its [`serving_credentials`].
pub fn serving_config(
    components: &DatabaseComponents,
    component: &dyn SchemaComponent,
) -> Result<DatabaseConfig, DbError> {
    let entry = component_entry(components, component)?;
    let schema = schema_of(entry, component)?;
    let conn = match &entry.conn {
        #[cfg(sqlite)]
        BackendConfig::Sqlite(c) => ConnectConfig::Sqlite(c.clone()),
        #[cfg(postgres)]
        BackendConfig::Postgres(c) => {
            let (username, password) =
                serving_credentials(c.username.as_deref(), c.password.as_deref(), &schema)?;
            ConnectConfig::Postgres(PostgresConnect {
                options: c.clone(),
                username,
                password,
            })
        }
        #[cfg(mysql)]
        BackendConfig::MySql(c) => {
            let (username, password) =
                serving_credentials(c.username.as_deref(), c.password.as_deref(), &schema)?;
            ConnectConfig::MySql(MySqlConnect {
                options: c.clone(),
                username,
                password,
            })
        }
    };
    Ok(DatabaseConfig {
        conn,
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
        entry_password(&entry.conn),
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

/// Create every registered component's schema, owner role and app role on the
/// privileged pool. Each component provisions its own entry's app role — not
/// the privileged entry's — so serving `SET ROLE` targets what was created.
pub async fn provision_all(
    components: &DatabaseComponents,
    admin: &AdminPool,
) -> Result<(), DbError> {
    for component in registered() {
        let entry = component_entry(components, component)?;
        let (owner, password) = owner_credentials(entry, component)?;
        let app_role = entry.app_role()?;
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
    Ok(())
}

/// Bring every registered component to its current version, each as its schema
/// owner (DDL on its own schema — no server handle needed).
pub async fn upgrade_all(components: &DatabaseComponents) -> Result<(), DbError> {
    for component in registered() {
        let pool = admin_pool_for(components, component).await?;
        component.upgrade(&pool).await?;
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
        let serving = connect_serving(components, component.key()).await?;
        component.seed(&serving).await?;
    }
    Ok(())
}

/// Verify every registered component on its already-open serving pool — the
/// apiserver's boot check.
pub async fn check_pools(pools: &ServingPools) -> Result<(), DbError> {
    for component in registered() {
        component.check(pools.get(component.key())?).await?;
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
        admin.drop_schema(&owner, &owner).await?;
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
pub struct ServingPools(BTreeMap<&'static str, TdPool>);

impl ServingPools {
    /// The serving pool for a registered component.
    pub fn get(&self, key: &str) -> Result<&TdPool, DbError> {
        self.0
            .get(key)
            .ok_or_else(|| DbError::UnknownSchemaEntry(key.to_owned()))
    }

    /// Close every component's serving pool — on shutdown.
    pub async fn shutdown(&self) {
        for pool in self.0.values() {
            pool.shutdown().await;
        }
    }

    /// These pools keyed by domain (component keys are the domains'
    /// `SCHEMA_KEY`s).
    pub fn domain_pools(&self) -> DomainPools {
        DomainPools::from_map(self.0.clone())
    }
}

/// An **app-only** serving pool for every registered component. The apiserver
/// never boots or manages the database, so it holds no privileged access.
pub async fn connect_app_all(components: &DatabaseComponents) -> Result<ServingPools, DbError> {
    let mut pools = BTreeMap::new();
    for component in registered() {
        pools.insert(
            component.key(),
            connect_serving(components, component.key()).await?,
        );
    }
    Ok(ServingPools(pools))
}

/// A component's serving pool straight from the config — the app-only path,
/// the same way the apiserver reaches the database.
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
            .acquire()
            .await?
            .ping()
            .await?;
    }
    Ok(())
}
