//
// Copyright 2025 Tabsdata Inc.
//

//! API Server CLI configuration and parameters.

use clap::{Args, ValueEnum};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::net::{AddrParseError, SocketAddr};
use std::path::PathBuf;
use strum::{Display, EnumString, ParseError};
use ta_services::factory::FieldAccessors;
use td_bridge::Orchestrator;
use td_common::jwt::JwtConfig;
use td_database::config::DatabaseComponents;
use td_error::{TdError, td_error};
use td_execution::{SchedulerCheckFrequency, StuckRequestedTimeout, TransactionBy};
use td_objects::types::addresses::{
    ApiServerAddresses, InternalServerAddresses, NonEmptyAddresses,
};
use td_security::config::PasswordHashingConfig;
use td_storage::{MountDef, StorageError};
use te_apiserver::config::{ExtendedConfig, ExtendedParams};

#[derive(Clone, Serialize, Deserialize, FieldAccessors)]
pub struct Config {
    #[serde(default)]
    pub addresses: ApiServerAddresses,
    #[serde(default)]
    pub internal_addresses: InternalServerAddresses,
    pub password: PasswordHashingConfig,
    pub jwt: JwtConfig,
    pub request_timeout: i64, // in seconds
    #[serde(default)]
    pub check_frequency: SchedulerCheckFrequency,
    #[serde(default)]
    pub stuck_requested_timeout: StuckRequestedTimeout,
    pub ssl_folder: PathBuf,
    #[serde(default)]
    pub database: DatabaseComponents,
    #[serde(default)]
    pub storage: Option<StorageConfig>,
    /// Root directory served by `GET /api/v1/resources/{*path}`. A request for
    /// `/resources/a/b.svg` returns `<resources_folder>/a/b.svg`. When unset,
    /// the route returns `404` for every path.
    #[serde(default)]
    pub resources_folder: Option<PathBuf>,
    #[serde(default)]
    pub transaction_by: TransactionBy,
    #[serde(default)]
    pub orchestrator: Orchestrator,
    /// Which security view the apiserver exposes:
    /// `simple` (default) exposes the flat `/permission-grants` facade and
    /// blocks the raw RBAC role/permission/user-role management endpoints;
    /// `rbac` exposes the RBAC management endpoints and blocks the facade.
    #[serde(default)]
    pub security_mode: SecurityMode,
    #[serde(flatten)]
    pub extended_config: ExtendedConfig,
}

/// The security view the apiserver exposes. See [`Config::security_mode`].
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    ValueEnum,
    Display,
    EnumString,
    Default,
)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub enum SecurityMode {
    /// Simple Security Mode View — flat permissions, `/permission-grants`
    /// facade available, RBAC management endpoints blocked.
    #[default]
    Simple,
    /// RBAC Mode View — role/permission/user-role management endpoints
    /// available, `/permission-grants` facade blocked.
    Rbac,
}

impl SecurityMode {
    pub fn is_simple(&self) -> bool {
        matches!(self, SecurityMode::Simple)
    }

    pub fn is_rbac(&self) -> bool {
        matches!(self, SecurityMode::Rbac)
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, Default)]
pub struct StorageConfig {
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    mounts: Option<Vec<MountDef>>,
}

impl Config {
    pub fn storage_mounts(&self) -> Result<Vec<MountDef>, ConfigError> {
        let Some(storage) = &self.storage else {
            return Err(ConfigError::MissingStorage);
        };

        match (&storage.url, &storage.mounts) {
            (Some(_), Some(_)) => Err(ConfigError::DoubleStorageConfig),
            (None, None) => Err(ConfigError::MissingStorageConfig),
            (Some(url), None) => {
                let mount_def = MountDef::builder()
                    .id("TDS_MOUNT_ROOT")
                    .path("/")
                    .uri(url)
                    .build()
                    .map_err(ConfigError::InvalidMountDefinition)?;
                Ok(vec![mount_def])
            }
            (None, Some(mounts)) => Ok(mounts.clone()),
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            addresses: ApiServerAddresses::default(),
            internal_addresses: InternalServerAddresses::default(),
            password: PasswordHashingConfig::default(),
            jwt: JwtConfig::default(),
            request_timeout: 60,
            check_frequency: SchedulerCheckFrequency::default(),
            stuck_requested_timeout: StuckRequestedTimeout::default(),
            ssl_folder: PathBuf::default(),
            database: DatabaseComponents::default(),
            storage: Some(StorageConfig::default()),
            resources_folder: None,
            transaction_by: TransactionBy::default(),
            orchestrator: Orchestrator::default(),
            security_mode: SecurityMode::default(),
            extended_config: ExtendedConfig::default(),
        }
    }
}

impl Display for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let storage_url = self
            .storage
            .as_ref()
            .and_then(|s| s.url.as_ref())
            .map(|s| s.as_str())
            .unwrap_or("None");

        // hide sensitive configs from displaying
        write!(
            f,
            "{{ addresses: {:?}, internal_address: {:?}, database: {:?}, storage {} }}",
            self.addresses, self.internal_addresses, self.database, storage_url
        )
    }
}

impl td_process::launcher::config::Config for Config {}

impl From<&Config> for JwtConfig {
    fn from(config: &Config) -> Self {
        config.jwt.clone()
    }
}

impl From<&Config> for PasswordHashingConfig {
    fn from(config: &Config) -> Self {
        config.password.clone()
    }
}

/// CLI parameters that overlay the YAML [`Config`]. Database configuration is
/// not among them: the apiserver sources its entire database setup from the
/// `database:` section of its config file, and both the server lifecycle and
/// schema management are the `td_dbmanager` command's job, so there are no
/// `--database-*` / `--db-schema` flags.
#[derive(Debug, Clone, Args)]
pub struct Params {
    #[clap(long)]
    /// Storage location (it must be a file:// URI)
    storage_url: Option<String>,
    #[clap(short, long, value_parser = parse_socket_addr, num_args = 1.., value_delimiter = ',')]
    /// List of addresses to bind the server to
    address: Option<Vec<SocketAddr>>,
    #[clap(short, long, value_parser = parse_socket_addr, num_args = 1.., value_delimiter = ',')]
    /// List of internal addresses to bind the server to
    internal_address: Option<Vec<SocketAddr>>,
    #[clap(long)]
    /// SSL folder path
    ssl_folder: Option<PathBuf>,
    #[clap(long)]
    /// Directory served by `GET /api/v1/resources/{*path}`. Overrides the
    /// config file's `resources_folder`; when unset the route returns 404.
    resources_folder: Option<PathBuf>,
    #[clap(long)]
    /// JWT Secret
    jwt_secret: Option<String>,
    #[clap(long)]
    /// JWT Access token expiration time in seconds
    access_jwt_expiration: Option<i64>,
    #[clap(long)]
    /// Request timeout in seconds
    request_timeout: Option<i64>,
    #[clap(long)]
    /// Failover polling interval in seconds for the scheduler loops
    check_frequency: Option<i64>,
    #[clap(long)]
    /// Seconds a dispatched function run may go without a worker callback
    /// before the scheduler auto-recovers it; `<= 0` disables the sweep
    stuck_requested_timeout: Option<i64>,
    #[clap(long, value_parser = parse_transaction_by)]
    /// Transaction by
    transaction_by: Option<TransactionBy>,
    #[clap(long)]
    /// The etc directory
    etc: Option<String>, // not used via clap.Added for etc_service to work correctly with CLI option
    #[clap(long)]
    /// The supervisor message queue directory
    msg: Option<String>, // not used via clap. Added for queue_service to work correctly with CLI option
    #[clap(long, value_parser = parse_orchestrator)]
    /// Orchestrator backend the apiserver dispatches workloads to.
    orchestrator: Option<Orchestrator>,
    #[clap(long, value_enum)]
    /// Security view the apiserver exposes: `simple` (default) or `rbac`.
    security_mode: Option<SecurityMode>,
    #[clap(flatten)]
    extended_params: ExtendedParams,
}

impl Params {
    pub fn resolve(&self, config: Config) -> Result<Config, TdError> {
        let mut resolved_storage: StorageConfig = config.storage.clone().unwrap_or_default();
        if self.storage_url.is_some() {
            let resolved_storage_url = self
                .storage_url
                .as_ref()
                .map(|url| url.to_string())
                .or(self.storage_url.clone());
            resolved_storage.url = resolved_storage_url;
            resolved_storage.mounts = None;
        }
        let resolved_storage = Some(resolved_storage);

        let config = Config {
            addresses: self
                .address
                .clone()
                .map(NonEmptyAddresses::from_vec)
                .transpose()?
                .map(ApiServerAddresses)
                .unwrap_or(config.addresses.clone()),
            internal_addresses: self
                .internal_address
                .clone()
                .map(NonEmptyAddresses::from_vec)
                .transpose()?
                .map(InternalServerAddresses)
                .unwrap_or(config.internal_addresses.clone()),
            password: config.password.clone(),
            jwt: {
                let secret = self
                    .jwt_secret
                    .to_owned()
                    .or_else(|| config.jwt.secret.clone())
                    .ok_or(ConfigError::MissingJWTSecret)?;
                let expiration = self
                    .access_jwt_expiration
                    .unwrap_or(config.jwt.access_token_expiration);
                JwtConfig::new(secret, expiration)
            },
            request_timeout: self.request_timeout.unwrap_or(config.request_timeout),
            check_frequency: self
                .check_frequency
                .map(SchedulerCheckFrequency::from_secs)
                .unwrap_or(config.check_frequency),
            stuck_requested_timeout: self
                .stuck_requested_timeout
                .map(StuckRequestedTimeout::from_secs)
                .unwrap_or(config.stuck_requested_timeout),
            ssl_folder: self
                .ssl_folder
                .clone()
                .unwrap_or_else(|| config.ssl_folder.clone()),
            database: config.database,
            storage: resolved_storage,
            resources_folder: self.resources_folder.clone().or(config.resources_folder),
            transaction_by: self
                .transaction_by
                .clone()
                .unwrap_or_else(|| config.transaction_by.clone()),
            orchestrator: self.orchestrator.unwrap_or(config.orchestrator),
            security_mode: self.security_mode.unwrap_or(config.security_mode),
            extended_config: self
                .extended_params
                .resolve(config.extended_config.clone())?,
        };

        if config.storage_mounts()?.is_empty() {
            Err(ConfigError::MissingStorage)?;
        }
        Ok(config)
    }
}

fn parse_socket_addr(addr: &str) -> Result<SocketAddr, AddrParseError> {
    addr.parse()
}

fn parse_transaction_by(transaction_by: &str) -> Result<TransactionBy, ParseError> {
    TransactionBy::try_from(transaction_by)
}

fn parse_orchestrator(orchestrator: &str) -> Result<Orchestrator, ParseError> {
    use std::str::FromStr;
    Orchestrator::from_str(orchestrator)
}

#[td_error]
pub enum ConfigError {
    #[error("No address was specified for the API Server to bind to.")]
    MissingAddress = 0,
    #[error("No JWT Secret was specified.")]
    MissingJWTSecret = 1,
    #[error("No storage settings.")]
    MissingStorage = 4,
    #[error("Both storage.url and storage.mounts have been configured.")]
    DoubleStorageConfig = 5,
    #[error("Neither storage.url nor storage.mounts has been configured.")]
    MissingStorageConfig = 6,
    #[error("The url specified in storage.url is wrong: {0}")]
    InvalidMountDefinition(#[source] StorageError) = 7,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use nonempty::nonempty;
    use std::net::SocketAddr;
    use std::str::FromStr;
    use td_common::id;
    use td_database::components::{ApiserverComponent, resolved_schema};
    use td_objects::types::addresses::NonEmptyAddresses;
    use tracing::log::LevelFilter;

    #[test]
    fn config_crate_loads_database_components() {
        let yaml = r#"
database:
  apiserver:
    postgres:
      host: localhost
      database: tabsdata
      password: owner_secret
      log_statements: off
    pool:
      max_connections: 25
"#;
        let built = config::Config::builder()
            .add_source(config::Config::try_from(&Config::default()).unwrap())
            .add_source(config::File::from_str(yaml, config::FileFormat::Yaml))
            .add_source(config::Environment::with_prefix("apiserver"))
            .build()
            .unwrap();
        let config: Config = built.try_deserialize().unwrap();
        let entry = config.database.get("apiserver").unwrap();
        let pg = entry.conn.postgres().unwrap();
        assert_eq!(pg.host, "localhost");
        assert_eq!(pg.database.as_deref(), Some("tabsdata"));
        assert_eq!(pg.password.as_deref(), Some("owner_secret"));
        assert_eq!(pg.log.log_statements, LevelFilter::Off);
        assert_eq!(entry.pool.max_connections, 25);
        assert_eq!(entry.pool.min_connections, 1);
    }

    #[test]
    fn config_merges_split_apiserver_database_storage_files() {
        let apiserver_yaml = r#"
request_timeout: 90
orchestrator: kubernetes
"#;
        let database_yaml = r#"
database:
  apiserver:
    postgres:
      host: localhost
      database: tabsdata
      password: owner_secret
"#;
        let pool_yaml = r#"
database:
  apiserver:
    pool:
      max_connections: 40
"#;
        let storage_yaml = r#"
storage:
  mounts:
    - id: ROOT
      path: /
      uri: file:///tmp/storage/data/
"#;
        let built = config::Config::builder()
            .add_source(config::Config::try_from(&Config::default()).unwrap())
            .add_source(config::File::from_str(
                apiserver_yaml,
                config::FileFormat::Yaml,
            ))
            .add_source(config::File::from_str(
                database_yaml,
                config::FileFormat::Yaml,
            ))
            .add_source(config::File::from_str(pool_yaml, config::FileFormat::Yaml))
            .add_source(config::File::from_str(
                storage_yaml,
                config::FileFormat::Yaml,
            ))
            .add_source(config::Environment::with_prefix("apiserver"))
            .build()
            .unwrap();
        let config: Config = built.try_deserialize().unwrap();
        assert_eq!(config.request_timeout, 90);
        assert_eq!(config.orchestrator, Orchestrator::Kubernetes);
        // The apiserver entry omits `schema:`, so it resolves to the
        // component's default.
        assert_eq!(
            resolved_schema(&config.database, &ApiserverComponent).unwrap(),
            "td_apiserver"
        );
        // The pool override from the split file merges into the same entry
        // without clobbering the backend block.
        let entry = config.database.get("apiserver").unwrap();
        assert_eq!(entry.pool.max_connections, 40);
        assert_eq!(
            entry.conn.postgres().unwrap().password.as_deref(),
            Some("owner_secret")
        );
        assert_eq!(config.storage_mounts().unwrap().len(), 1);
    }

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert!(config.database.is_empty());
        assert_eq!(
            *config.addresses,
            NonEmptyAddresses::new(nonempty![SocketAddr::from(([0, 0, 0, 0], 2457))])
        );
        id::Id::try_from(config.jwt.secret.as_ref().unwrap()).unwrap();
        assert!(config.storage.is_some());
        assert!(config.clone().storage.unwrap().url.is_none());
        assert_eq!(
            config.jwt.access_token_expiration,
            Duration::hours(1).num_seconds()
        );
        assert_eq!(config.request_timeout, 60);
        // security mode defaults to Simple.
        assert_eq!(config.security_mode, SecurityMode::Simple);
        assert!(config.security_mode.is_simple());
    }

    #[test]
    fn test_security_mode_serde_and_parse() {
        // serde round-trips lowercase names.
        assert_eq!(
            serde_json::to_string(&SecurityMode::Rbac).unwrap(),
            "\"rbac\""
        );
        assert_eq!(
            serde_json::from_str::<SecurityMode>("\"simple\"").unwrap(),
            SecurityMode::Simple
        );
        // CLI / strum parsing accepts lowercase tokens.
        assert_eq!(
            <SecurityMode as FromStr>::from_str("rbac").unwrap(),
            SecurityMode::Rbac
        );
        assert_eq!(
            <SecurityMode as FromStr>::from_str("simple").unwrap(),
            SecurityMode::Simple
        );
        // a config that omits security_mode falls back to the Simple default
        // (the field carries #[serde(default)]).
        let mut value = serde_json::to_value(Config::default()).unwrap();
        value.as_object_mut().unwrap().remove("security_mode");
        let cfg: Config = serde_json::from_value(value).unwrap();
        assert_eq!(cfg.security_mode, SecurityMode::Simple);
    }

    #[test]
    fn test_params_resolve() {
        let default_config = Config::default();

        let params = Params {
            #[cfg(target_os = "windows")]
            storage_url: Some(String::from("file:///c:/storage")),
            #[cfg(not(target_os = "windows"))]
            storage_url: Some(String::from("file:///storage")),
            address: Some(vec!["127.0.0.1:2457".parse().unwrap()]),
            internal_address: Some(vec!["127.0.0.1:2458".parse().unwrap()]),
            ssl_folder: Some(PathBuf::from("file:///ssl")),
            resources_folder: Some(PathBuf::from("/srv/resources")),
            jwt_secret: Some(String::from("NEW_SECRET")),
            access_jwt_expiration: Some(7200),
            request_timeout: Some(120),
            check_frequency: None,
            stuck_requested_timeout: None,
            transaction_by: Some(TransactionBy::default()),
            etc: None,
            msg: None,
            orchestrator: None,
            security_mode: None,
            extended_params: ExtendedParams::default(),
        };

        let resolved_config = params.resolve(default_config.clone()).unwrap();

        assert_eq!(resolved_config.jwt.secret, Some("NEW_SECRET".to_string()));
        assert_eq!(
            *resolved_config.addresses,
            NonEmptyAddresses::new(nonempty![SocketAddr::from(([127, 0, 0, 1], 2457))])
        );
        assert!(resolved_config.clone().storage.is_some());
        #[cfg(target_os = "windows")]
        assert_eq!(
            resolved_config
                .clone()
                .storage
                .unwrap()
                .url
                .as_ref()
                .unwrap(),
            "file:///c:/storage"
        );
        #[cfg(not(target_os = "windows"))]
        assert_eq!(
            resolved_config
                .clone()
                .storage
                .unwrap()
                .url
                .as_ref()
                .unwrap(),
            "file:///storage"
        );
        assert_eq!(resolved_config.request_timeout, 120);
        // The `--resources-folder` CLI param overrides the config-file value.
        assert_eq!(
            resolved_config.resources_folder,
            Some(PathBuf::from("/srv/resources"))
        );

        // Test with some fields not set in Params
        let partial_params = Params {
            #[cfg(target_os = "windows")]
            storage_url: Some(String::from("file:///c:/storage")),
            #[cfg(not(target_os = "windows"))]
            storage_url: Some(String::from("file:///storage")),
            address: None,
            internal_address: None,
            ssl_folder: None,
            resources_folder: None,
            jwt_secret: Some(String::from("NEW_SECRET")),
            access_jwt_expiration: Some(1800),
            request_timeout: None,
            check_frequency: None,
            stuck_requested_timeout: None,
            transaction_by: Some(TransactionBy::default()),
            etc: None,
            msg: None,
            orchestrator: None,
            security_mode: None,
            extended_params: ExtendedParams::default(),
        };

        let partially_resolved_config = partial_params.resolve(default_config.clone()).unwrap();

        assert_eq!(
            partially_resolved_config.addresses,
            default_config.addresses
        );
        assert_eq!(
            partially_resolved_config.internal_addresses,
            default_config.internal_addresses
        );
        assert_eq!(resolved_config.jwt.secret, Some("NEW_SECRET".to_string()));
        assert_eq!(partially_resolved_config.jwt.access_token_expiration, 1800);
        assert_eq!(
            partially_resolved_config.request_timeout,
            default_config.request_timeout
        );
    }

    #[test]
    fn test_missing_jwt_secret_errors_without_panicking() {
        let mut config = Config::default();
        config.jwt.secret = None;

        let params = Params {
            #[cfg(target_os = "windows")]
            storage_url: Some(String::from("file:///c:/storage")),
            #[cfg(not(target_os = "windows"))]
            storage_url: Some(String::from("file:///storage")),
            address: None,
            internal_address: None,
            ssl_folder: None,
            resources_folder: None,
            jwt_secret: None,
            access_jwt_expiration: None,
            request_timeout: None,
            check_frequency: None,
            stuck_requested_timeout: None,
            transaction_by: None,
            etc: None,
            msg: None,
            orchestrator: None,
            security_mode: None,
            extended_params: ExtendedParams::default(),
        };

        let result = params.resolve(config);
        assert!(
            result.is_err(),
            "expected an error for a missing JWT secret"
        );
        let err = result.err().unwrap();
        assert!(err.to_string().contains("JWT Secret"), "got: {err}");
    }
}
