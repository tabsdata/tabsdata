//
// Copyright 2025 Tabsdata Inc.
//

use clap::Args;
use http::uri::Authority;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::net::{AddrParseError, SocketAddr};
use std::path::PathBuf;
use std::str::FromStr;
use strum::ParseError;
use ta_apiserver::router::SecurityMode;
use ta_services::factory::FieldAccessors;
use td_bridge::Orchestrator;
use td_common::jwt::JwtConfig;
use td_database::components::propagate_server_target;
use td_database::config::{ComponentConfig, DatabaseComponents};
use td_error::{TdError, td_error};
use td_execution::{
    DispatchGrace, MaxCompletionTime, MaxFunctionsPerTick, MaxPreRunningTime, MaxRecoveriesPerTick,
    MaxRunningTime, RecoveryTickTimeout, SchedulerCheckFrequency, SchedulerTickTimeout,
    StuckRequestedTimeout, TransactionBy,
};
use td_objects::types::addresses::{
    ApiServerAddresses, InternalServerAddresses, NonEmptyAddresses,
};
use td_security::config::PasswordHashingConfig;
use td_security::token::config::ServiceTokenConfig;
use td_storage::{MountDef, StorageError};
use te_apiserver::config::{ExtendedConfig, ExtendedParams};

fn deserialize_optional_authority<'de, D>(deserializer: D) -> Result<Option<Authority>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw: Option<String> = Option::deserialize(deserializer)?;
    match raw {
        None => Ok(None),
        Some(raw) if raw.trim().is_empty() => Ok(None),
        Some(raw) => Authority::from_str(raw.trim())
            .map(Some)
            .map_err(serde::de::Error::custom),
    }
}

fn serialize_optional_authority<S>(
    authority: &Option<Authority>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match authority {
        None => serializer.serialize_none(),
        Some(authority) => serializer.serialize_str(authority.as_str()),
    }
}

#[derive(Clone, Serialize, Deserialize, FieldAccessors)]
pub struct Config {
    #[serde(default)]
    pub addresses: ApiServerAddresses,
    #[serde(default)]
    pub internal_addresses: InternalServerAddresses,
    /// Host other workloads reach the internal server at, replacing the bound
    /// one in the callback urls handed to workers. Needed wherever the bind
    /// address names nothing reachable from another network namespace, as a
    /// wildcard does. Any port here is ignored: the bound one is used. Unset
    /// keeps the bound address.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_authority",
        serialize_with = "serialize_optional_authority"
    )]
    pub advertised_internal_host: Option<Authority>,
    pub password: PasswordHashingConfig,
    pub jwt: JwtConfig,
    /// Unset by default, which refuses token creation.
    #[serde(default)]
    pub service_tokens: ServiceTokenConfig,
    pub request_timeout: i64, // in seconds
    #[serde(default)]
    pub check_frequency: SchedulerCheckFrequency,
    #[serde(default)]
    pub stuck_requested_timeout: StuckRequestedTimeout,
    #[serde(default)]
    pub max_functions_per_tick: MaxFunctionsPerTick,
    #[serde(default)]
    pub tick_timeout: SchedulerTickTimeout,
    #[serde(default)]
    pub recovery_tick_timeout: RecoveryTickTimeout,
    #[serde(default)]
    pub dispatch_grace: DispatchGrace,
    #[serde(default)]
    pub max_pre_running_time: MaxPreRunningTime,
    #[serde(default)]
    pub max_running_time: MaxRunningTime,
    #[serde(default)]
    pub max_completion_time: MaxCompletionTime,
    #[serde(default)]
    pub max_recoveries_per_tick: MaxRecoveriesPerTick,
    pub ssl_folder: PathBuf,
    #[serde(default)]
    pub database: DatabaseComponents,
    /// The `database_server:` entry, mounted from the dbmanager config. It owns
    /// where the database is; the components under `database:` carry only what
    /// is theirs (their password, schema and pool).
    #[serde(default)]
    pub database_server: Option<ComponentConfig>,
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
            advertised_internal_host: None,
            password: PasswordHashingConfig::default(),
            jwt: JwtConfig::default(),
            service_tokens: ServiceTokenConfig::default(),
            request_timeout: 60,
            check_frequency: SchedulerCheckFrequency::default(),
            stuck_requested_timeout: StuckRequestedTimeout::default(),
            max_functions_per_tick: MaxFunctionsPerTick::default(),
            tick_timeout: SchedulerTickTimeout::default(),
            recovery_tick_timeout: RecoveryTickTimeout::default(),
            dispatch_grace: DispatchGrace::default(),
            max_pre_running_time: MaxPreRunningTime::default(),
            max_running_time: MaxRunningTime::default(),
            max_completion_time: MaxCompletionTime::default(),
            max_recoveries_per_tick: MaxRecoveriesPerTick::default(),
            ssl_folder: PathBuf::default(),
            database: DatabaseComponents::default(),
            database_server: None,
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
    /// before the scheduler auto-recovers it; `<= 0` falls back to the
    /// default (the sweep cannot be disabled)
    stuck_requested_timeout: Option<i64>,
    #[clap(long)]
    /// Maximum function runs dispatched per scheduler tick; `<= 0` falls
    /// back to the default
    max_functions_per_tick: Option<i64>,
    #[clap(long)]
    /// Seconds a scheduler tick may run before it is cancelled and retried;
    /// `<= 0` falls back to the default
    tick_timeout: Option<i64>,
    #[clap(long)]
    /// Seconds a recovery tick may run before it is cancelled and retried;
    /// `<= 0` falls back to the default
    recovery_tick_timeout: Option<i64>,
    #[clap(long)]
    /// Seconds a dispatched run must be old before recovery may judge its
    /// missing workload dead; `<= 0` falls back to the default
    dispatch_grace: Option<i64>,
    #[clap(long)]
    /// Seconds a function workload may spend before its run reports Running
    /// before recovery kills the attempt; `<= 0` falls back to the default
    max_pre_running_time: Option<i64>,
    #[clap(long)]
    /// Optional cap in seconds on a function run's execution time; `<= 0`
    /// keeps it disabled
    max_running_time: Option<i64>,
    #[clap(long)]
    /// Seconds a finished function workload may go without its terminal
    /// callback landing before recovery kills the attempt; `<= 0` falls
    /// back to the default
    max_completion_time: Option<i64>,
    #[clap(long)]
    /// Maximum recovery actions of each kind per recovery tick; `<= 0`
    /// falls back to the default
    max_recoveries_per_tick: Option<i64>,
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
            advertised_internal_host: config.advertised_internal_host.clone(),
            password: config.password.clone(),
            service_tokens: config.service_tokens.clone(),
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
            max_functions_per_tick: self
                .max_functions_per_tick
                .map(MaxFunctionsPerTick::from_count)
                .unwrap_or(config.max_functions_per_tick),
            tick_timeout: self
                .tick_timeout
                .map(SchedulerTickTimeout::from_secs)
                .unwrap_or(config.tick_timeout),
            recovery_tick_timeout: self
                .recovery_tick_timeout
                .map(RecoveryTickTimeout::from_secs)
                .unwrap_or(config.recovery_tick_timeout),
            dispatch_grace: self
                .dispatch_grace
                .map(DispatchGrace::from_secs)
                .unwrap_or(config.dispatch_grace),
            max_pre_running_time: self
                .max_pre_running_time
                .map(MaxPreRunningTime::from_secs)
                .unwrap_or(config.max_pre_running_time),
            max_running_time: self
                .max_running_time
                .map(MaxRunningTime::from_secs)
                .unwrap_or(config.max_running_time),
            max_completion_time: self
                .max_completion_time
                .map(MaxCompletionTime::from_secs)
                .unwrap_or(config.max_completion_time),
            max_recoveries_per_tick: self
                .max_recoveries_per_tick
                .map(MaxRecoveriesPerTick::from_count)
                .unwrap_or(config.max_recoveries_per_tick),
            ssl_folder: self
                .ssl_folder
                .clone()
                .unwrap_or_else(|| config.ssl_folder.clone()),
            database: {
                let mut components = config.database;
                if let Some(server) = &config.database_server {
                    propagate_server_target(&mut components, server);
                }
                components
            },
            database_server: config.database_server,
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
    #[error("Could not resolve the advertised internal host '{0}': {1}")]
    UnresolvableAdvertisedHost(String, String) = 8,
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
      regular:
        min_connections: 1
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
        let pg = &entry.postgres;
        assert_eq!(pg.host, "localhost");
        assert_eq!(pg.database.as_deref(), Some("tabsdata"));
        assert_eq!(pg.password.as_deref(), Some("owner_secret"));
        assert_eq!(pg.log.log_statements, LevelFilter::Off);
        let regular = entry.pool.regular.as_ref().unwrap();
        assert_eq!(regular.max_connections, 25);
        assert_eq!(regular.min_connections, 1);
        assert!(entry.pool.exclusive.is_none());
    }

    #[test]
    fn the_server_entry_supplies_the_target_of_every_component() {
        // The two files a pod mounts: the components carry only what is theirs,
        // the server entry says where the database is.
        let apiserver_yaml = r#"
database:
  apiserver:
    postgres:
      password: apiserver_secret
  aiagent:
    postgres:
      password: aiagent_secret
"#;
        let dbmanager_yaml = r#"
database_server:
  postgres:
    host: rds.example.com
    port: 5433
    database: tabsdata
    username: admin
    password: admin_secret
    ssl_mode: verify-ca
"#;
        let built = config::Config::builder()
            .add_source(config::Config::try_from(&Config::default()).unwrap())
            .add_source(config::File::from_str(
                apiserver_yaml,
                config::FileFormat::Yaml,
            ))
            .add_source(config::File::from_str(
                dbmanager_yaml,
                config::FileFormat::Yaml,
            ))
            .build()
            .unwrap();
        let config: Config = built.try_deserialize().unwrap();

        let mut components = config.database.clone();
        propagate_server_target(&mut components, config.database_server.as_ref().unwrap());

        for component in ["apiserver", "aiagent"] {
            let pg = &components.get(component).unwrap().postgres;
            assert_eq!(pg.host, "rds.example.com", "host for {component}");
            assert_eq!(pg.port, 5433, "port for {component}");
            assert_eq!(
                pg.database.as_deref(),
                Some("tabsdata"),
                "db for {component}"
            );
        }
        // Credentials stay per component: only the location travels.
        assert_eq!(
            components
                .get("apiserver")
                .unwrap()
                .postgres
                .password
                .as_deref(),
            Some("apiserver_secret")
        );
        assert_eq!(
            components
                .get("aiagent")
                .unwrap()
                .postgres
                .password
                .as_deref(),
            Some("aiagent_secret")
        );
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
      regular:
        min_connections: 1
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
        // The apiserver entry omits `schema:`, so it derives from the
        // database name.
        assert_eq!(
            resolved_schema(&config.database, &ApiserverComponent).unwrap(),
            "tabsdata_apiserver"
        );
        // The pool override from the split file merges into the same entry
        // without clobbering the backend block.
        let entry = config.database.get("apiserver").unwrap();
        assert_eq!(entry.pool.regular.as_ref().unwrap().max_connections, 40);
        assert_eq!(entry.postgres.password.as_deref(), Some("owner_secret"));
        assert_eq!(config.storage_mounts().unwrap().len(), 1);
    }

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert!(config.database.is_empty());
        assert_eq!(
            *config.addresses,
            NonEmptyAddresses::new(nonempty![
                SocketAddr::from(([0, 0, 0, 0], 2457)),
                SocketAddr::from(([0u16; 8], 2457)),
            ])
        );
        assert_eq!(
            *config.internal_addresses,
            NonEmptyAddresses::new(nonempty![
                SocketAddr::from(([0, 0, 0, 0], 2458)),
                SocketAddr::from(([0u16; 8], 2458)),
            ])
        );
        id::Id::try_from(config.jwt.secret.as_ref().unwrap()).unwrap();
        assert!(config.storage.is_some());
        assert!(config.clone().storage.unwrap().url.is_none());
        assert_eq!(
            config.jwt.access_token_expiration,
            Duration::hours(1).num_seconds()
        );
        assert_eq!(config.request_timeout, 60);
        assert_eq!(config.security_mode, SecurityMode::Simple);
        assert!(config.security_mode.is_simple());
    }

    #[test]
    fn test_security_mode_serde_and_parse() {
        assert_eq!(
            serde_json::to_string(&SecurityMode::Rbac).unwrap(),
            "\"rbac\""
        );
        assert_eq!(
            serde_json::from_str::<SecurityMode>("\"simple\"").unwrap(),
            SecurityMode::Simple
        );
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
            max_functions_per_tick: None,
            tick_timeout: None,
            recovery_tick_timeout: None,
            dispatch_grace: None,
            max_pre_running_time: None,
            max_running_time: None,
            max_completion_time: None,
            max_recoveries_per_tick: None,
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
            max_functions_per_tick: None,
            tick_timeout: None,
            recovery_tick_timeout: None,
            dispatch_grace: None,
            max_pre_running_time: None,
            max_running_time: None,
            max_completion_time: None,
            max_recoveries_per_tick: None,
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
            max_functions_per_tick: None,
            tick_timeout: None,
            recovery_tick_timeout: None,
            dispatch_grace: None,
            max_pre_running_time: None,
            max_running_time: None,
            max_completion_time: None,
            max_recoveries_per_tick: None,
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
