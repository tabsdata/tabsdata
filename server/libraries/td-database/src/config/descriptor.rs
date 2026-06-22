//
// Copyright 2026 Tabs Data Inc.
//

//! [`DatabaseDescriptor`] — the `database:` configuration section: the
//! [`DatabaseMode`], the database-wide backend, admin credentials, and an
//! optional [`ComponentOverride`] per component. Which components exist and the
//! schema each owns comes from the linked component registry, not the config; it
//! resolves the privileged admin connectable and per-component connectables on
//! demand.

use super::DatabaseMode;
use super::Schema;
use super::backend::{BackendConfig, ConnectableBackend};
use super::connection::DatabaseConfig;
#[cfg(all(postgres, feature = "embedded-postgres"))]
use super::embedded::EmbeddedConfig;
use super::pool::PoolConfig;
use super::system_config::SystemConfig;
use crate::error::DbError;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;

/// Optional per-component deployment settings, keyed by component identity in
/// [`DatabaseDescriptor`]'s `components:` map. Supplies only the deployment-
/// specific bits: in [`DatabaseMode::Remote`] the owner password (the embedded
/// modes generate it), plus optional pool tuning. Omit when nothing to override.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ComponentOverride {
    /// Schema to mount this component on, overriding its
    /// [`default_schema`](crate::system::SchemaComponent::default_schema). The
    /// owner role takes the same name. Lets the same component populate any schema.
    #[serde(default)]
    pub schema: Option<Schema>,
    /// Owner-role password. Required when not [`embedded`](crate::config::DatabaseMode::is_embedded)
    /// (embedded generates and persists it in a sidecar file); supply it for
    /// sidecar and remote.
    #[serde(default)]
    pub password: Option<String>,
    /// Pool settings for this component's serving pool; unset fields default.
    #[serde(default, flatten)]
    pub pool: PoolConfig,
}

/// The `database:` section. A **managed** database (`embedded`/`sidecar`) carries
/// a [`system`](Self::resolve) connection — the privileged target we boot or
/// connect to and manage; its serving pools derive from the live system URL. A
/// **remote** database carries only the top-level backend — the app-only serving
/// connection — and no `system` entry. The component set and each component's
/// schema come from the registry; `components:` only carries per-deployment
/// specifics (owner passwords, schema overrides, pool tuning).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DatabaseDescriptor {
    #[serde(default)]
    pub mode: DatabaseMode,
    /// Serving/app backend for a **remote** database — the only connection we have
    /// to it. Absent for managed databases (serving derives from the system URL).
    #[serde(default, flatten)]
    pub conn: Option<BackendConfig>,
    /// The privileged system connection — present iff the mode is managed.
    /// [`Self::resolve`] enforces that invariant.
    #[serde(default)]
    pub system: Option<SystemConfig>,
    /// Optional per-component deployment overrides, keyed by component identity.
    #[serde(default)]
    pub components: BTreeMap<String, ComponentOverride>,
}

impl DatabaseDescriptor {
    pub fn mode(&self) -> DatabaseMode {
        self.mode
    }

    /// The top-level (remote serving) backend, or `None` for a managed database
    /// (whose backend lives in the `system` connection) or when unconfigured.
    pub fn backend(&self) -> Option<&BackendConfig> {
        self.conn.as_ref()
    }

    /// The per-component override for `key`, if the config supplies one.
    pub fn component_override(&self, key: &str) -> Option<&ComponentOverride> {
        self.components.get(key)
    }

    /// Keys present in the `components:` map. These are deployment *overrides*,
    /// not the set of components (which comes from the registry).
    pub fn override_keys(&self) -> impl Iterator<Item = &String> {
        self.components.keys()
    }

    /// Validate the connection shape and borrow as a [`DatabaseResolver`] whose
    /// backend is guaranteed present. Enforces the managed/system invariant:
    ///
    /// * **managed** (`embedded`/`sidecar`) must carry a `system` entry — its
    ///   backend is the source of truth (what we boot or connect to);
    /// * **remote** must *not* carry one, and must supply the top-level backend.
    ///
    /// Per-component owner passwords and `components:` keys are checked elsewhere
    /// (lazily / against the registry).
    pub fn resolve(&self) -> Result<DatabaseResolver<'_>, DbError> {
        let backend = match (self.mode.is_managed(), &self.system) {
            (true, None) => return Err(DbError::MissingSystemEntry),
            (false, Some(_)) => return Err(DbError::UnexpectedSystemEntry),
            (true, Some(system)) => &system.conn,
            (false, None) => self.conn.as_ref().ok_or(DbError::MissingDatabaseLocation)?,
        };
        Ok(DatabaseResolver {
            descriptor: self,
            backend,
            system: self.system.as_ref(),
        })
    }

    /// Validate the connection shape (see [`Self::resolve`]).
    pub fn validate(&self) -> Result<(), DbError> {
        self.resolve().map(|_| ())
    }
}

/// A validated borrow of a [`DatabaseDescriptor`] whose backend is guaranteed
/// present, so building the per-component connectables is infallible (only the
/// embedded sidecar-password read can still fail). For a managed database the
/// backend is the `system` connection's; for remote it is the top-level one.
/// Obtained via [`DatabaseDescriptor::resolve`].
pub struct DatabaseResolver<'a> {
    descriptor: &'a DatabaseDescriptor,
    backend: &'a BackendConfig,
    system: Option<&'a SystemConfig>,
}

impl DatabaseResolver<'_> {
    pub fn mode(&self) -> DatabaseMode {
        self.descriptor.mode
    }

    /// The guaranteed-present database-wide backend.
    pub fn backend(&self) -> &BackendConfig {
        self.backend
    }

    /// The per-component override for `key`, if the config supplies one.
    pub fn component_override(&self, key: &str) -> Option<&ComponentOverride> {
        self.descriptor.component_override(key)
    }

    /// The privileged system connectable, from the database's `system` entry.
    /// Errors with [`DbError::MissingSystemEntry`] for a remote (unmanaged)
    /// database, which has none. For url-based system backends only — embedded goes
    /// through [`Self::system_connectable_running`] (it needs the persisted creds).
    pub fn system_connectable(&self) -> Result<DatabaseConfig, DbError> {
        self.system.ok_or(DbError::MissingSystemEntry)?.connectable()
    }

    // (system_connectable_running below handles the embedded case.)

    /// Admin connectable to the **already-running** system server, for management
    /// (`provision`/`upgrade`/`drop`) that connects rather than boots. Url-based
    /// system backends use their configured connectable; embedded targets the live
    /// URL with the persisted superuser credentials — the server is started out of
    /// band (`td_dbmanager start`), never booted here.
    pub fn system_connectable_running(&self) -> Result<DatabaseConfig, DbError> {
        let system = self.system.ok_or(DbError::MissingSystemEntry)?;
        #[cfg(all(postgres, feature = "embedded-postgres"))]
        if let Some(ecfg) = self.embedded_config() {
            let (url, username, password) = crate::embedded::live_admin(&ecfg)?;
            return Ok(DatabaseConfig {
                conn: ConnectableBackend::from_url(url)?,
                schema: None,
                username: Some(username),
                password: Some(password),
                app_role: None,
                pool: system.pool.clone(),
            });
        }
        system.connectable()
    }

    /// The embedded boot config with the `system` entry's superuser credentials
    /// overlaid (the same overlay the connect path applies). `None` when the backend
    /// is not embedded postgres. The single place the embedded server's boot knobs
    /// are derived — used by the database server lifecycle (`start`/`stop`) and to
    /// resolve the live admin connectable.
    #[cfg(all(postgres, feature = "embedded-postgres"))]
    pub fn embedded_config(&self) -> Option<EmbeddedConfig> {
        let BackendConfig::EmbeddedPostgres(ecfg) = self.backend else {
            return None;
        };
        let mut ecfg = ecfg.clone();
        if let Some(system) = self.system {
            if system.username.is_some() {
                ecfg.username = system.username.clone();
            }
            if system.password.is_some() {
                ecfg.password = system.password.clone();
            }
        }
        Some(ecfg)
    }

    /// Connectable for a component owning `schema`: the database backend lowered to
    /// a [`ConnectableBackend`](super::ConnectableBackend) (embedded → its live URL),
    /// logging in as the schema-named owner role with the override's password.
    pub fn assemble(
        &self,
        schema: &str,
        ovr: Option<&ComponentOverride>,
    ) -> Result<DatabaseConfig, DbError> {
        Ok(DatabaseConfig {
            conn: self.backend.clone().into_connectable()?,
            schema: Some(Schema::new(schema)),
            username: Some(schema.to_owned()),
            password: ovr.and_then(|o| o.password.clone()),
            app_role: None,
            pool: ovr.map(|o| o.pool.clone()).unwrap_or_default(),
        })
    }

    /// Serving connectable for a component owning `schema` against the running
    /// server. The owner connectable from [`Self::assemble`] (which lowers embedded
    /// to its live URL); the credential-generating modes additionally fill the owner
    /// password persisted at provisioning when no override supplies one. Backend-
    /// general — only reading the persisted password is embedded-specific.
    pub fn serving_config(
        &self,
        schema: &str,
        ovr: Option<&ComponentOverride>,
    ) -> Result<DatabaseConfig, DbError> {
        let mut config = self.assemble(schema, ovr)?;
        if self.mode().is_managed() && config.password.is_none() {
            config.password = Some(self.generated_owner_password(schema)?);
        }
        Ok(config)
    }

    /// The owner password persisted at provisioning. Only **embedded** generates
    /// and persists owner passwords (in sidecar files next to `data_dir`); sidecar
    /// is managed but not spawned by us, so it has no sidecar file and must supply
    /// the password via the `components:` override (else [`DbError::MissingCredentials`]).
    #[cfg(feature = "embedded-postgres")]
    fn generated_owner_password(&self, schema: &str) -> Result<String, DbError> {
        if !self.mode().is_embedded() {
            return Err(DbError::MissingCredentials);
        }
        crate::embedded::load_owner_password(self.embedded_data_dir()?, schema)
    }
    #[cfg(not(feature = "embedded-postgres"))]
    fn generated_owner_password(&self, _schema: &str) -> Result<String, DbError> {
        Err(DbError::MissingCredentials)
    }

    /// The embedded data directory the backend boots from, the parent of the
    /// sidecar password files.
    #[cfg(all(postgres, feature = "embedded-postgres"))]
    fn embedded_data_dir(&self) -> Result<&Path, DbError> {
        match self.backend {
            BackendConfig::EmbeddedPostgres(embedded) => embedded
                .data_dir
                .as_deref()
                .ok_or(DbError::MissingDatabaseLocation),
            _ => Err(DbError::MissingDatabaseLocation),
        }
    }

    /// The owner role (the schema name) and its password for a component owning
    /// `schema`: the override's password when set, otherwise (embedded modes)
    /// generated and persisted in a sidecar file. [`DatabaseMode::Remote`] has no
    /// fallback, so it must be supplied.
    pub fn owner_credentials(
        &self,
        schema: &str,
        ovr: Option<&ComponentOverride>,
    ) -> Result<(String, String), DbError> {
        let password = match ovr.and_then(|o| o.password.clone()) {
            Some(password) => password,
            // Only embedded generates+persists owner passwords (sidecar files);
            // sidecar/remote must supply them via the component override.
            #[cfg(all(postgres, feature = "embedded-postgres"))]
            None if self.mode().is_embedded() => {
                crate::embedded::load_or_create_owner_password(self.embedded_data_dir()?, schema)?
            }
            None => return Err(DbError::MissingCredentials),
        };
        Ok((schema.to_owned(), password))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A managed (embedded) database carries its backend in the `system` entry;
    /// the resolver's backend is the system one, and `assemble` overlays it onto a
    /// component's schema as that schema's owner role.
    #[cfg(all(postgres, feature = "embedded-postgres"))]
    #[test]
    fn descriptor_managed_embedded_shape() {
        let yaml = "
mode: embedded
system:
  data_dir: /tmp/x
  persistent: true
  max_connections: 2
";
        let descriptor: DatabaseDescriptor = serde_yaml::from_str(yaml).unwrap();
        // Managed: no top-level backend; it lives in the system connection.
        assert!(descriptor.backend().is_none());
        let resolver = descriptor.resolve().unwrap();
        assert!(matches!(
            resolver.backend(),
            BackendConfig::EmbeddedPostgres(_)
        ));
        // The system pool flattens alongside the embedded backend fields and is
        // independent of any component pool. (Asserted on the declarative config:
        // building a connectable for embedded would resolve the live URL, which
        // needs a running server.)
        assert_eq!(resolver.system.unwrap().pool.max_connections, 2);
    }

    /// A fixed `port` on an embedded `system` connection survives the untagged
    /// `BackendConfig` + flattened `SystemConfig` shape (the flatten does not
    /// swallow it), so a deployment can pin the embedded server's port.
    #[test]
    fn embedded_fixed_port_deserializes() {
        let yaml = "
mode: embedded
system:
  data_dir: /tmp/x
  port: 7542
  persistent: true
";
        let descriptor: DatabaseDescriptor = serde_yaml::from_str(yaml).unwrap();
        let resolver = descriptor.resolve().unwrap();
        assert!(matches!(
            resolver.backend(),
            BackendConfig::EmbeddedPostgres(embedded) if embedded.port == Some(7542)
        ));
    }

    /// The managed/system invariant: managed needs a `system` entry, remote must
    /// not have one (remote is app-only, never managed by us).
    #[cfg(postgres)]
    #[test]
    fn descriptor_system_entry_invariant() {
        // Managed without a system entry → MissingSystemEntry.
        let descriptor: DatabaseDescriptor = serde_yaml::from_str("mode: sidecar").unwrap();
        assert!(matches!(
            descriptor.validate(),
            Err(DbError::MissingSystemEntry)
        ));

        // Remote carrying a system entry → UnexpectedSystemEntry.
        let yaml = "
mode: remote
url: postgres://db/tabsdata
system:
  url: postgres://db/tabsdata
  username: admin
  password: secret
";
        let descriptor: DatabaseDescriptor = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(
            descriptor.validate(),
            Err(DbError::UnexpectedSystemEntry)
        ));

        // Remote with just its app backend (and a per-component owner password).
        let yaml = "
mode: remote
url: postgres://db/tabsdata
components:
  apiserver:
    password: owner_secret
";
        let descriptor: DatabaseDescriptor = serde_yaml::from_str(yaml).unwrap();
        descriptor.validate().unwrap();
        assert_eq!(
            descriptor
                .component_override("apiserver")
                .and_then(|o| o.password.as_deref()),
            Some("owner_secret")
        );
    }

    /// The default descriptor (managed mode, no system entry) fails with
    /// [`DbError::MissingSystemEntry`].
    #[test]
    fn descriptor_default_missing_system() {
        let descriptor = DatabaseDescriptor::default();
        assert!(matches!(
            descriptor.validate(),
            Err(DbError::MissingSystemEntry)
        ));
    }
}
