//
// Copyright 2025 Tabs Data Inc.
//

//! API Server CLI configuration and parameters.

use clap::{Args, ValueEnum};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::net::{AddrParseError, SocketAddr};
use std::path::PathBuf;
use strum::{Display, EnumString, ParseError};
use ta_services::factory::FieldAccessors;
use td_authz::jwt::JwtConfig;
use td_database::config::{BackendConfig, DatabaseConfig};
use td_error::{TdError, td_error};
use td_execution::TransactionBy;
use td_objects::types::addresses::{
    ApiServerAddresses, InternalServerAddresses, NonEmptyAddresses,
};
use td_security::config::PasswordHashingConfig;
use td_storage::{MountDef, StorageError};
use te_apiserver::config::{ExtendedConfig, ExtendedParams};
use tracing::log::LevelFilter;
use url::Url;

#[derive(Clone, Serialize, Deserialize, FieldAccessors)]
pub struct Config {
    #[serde(default)]
    pub addresses: ApiServerAddresses,
    #[serde(default)]
    pub internal_addresses: InternalServerAddresses,
    pub password: PasswordHashingConfig,
    pub jwt: JwtConfig,
    pub request_timeout: i64, // in seconds
    pub ssl_folder: PathBuf,
    pub database: DatabaseConfig,
    #[serde(default)]
    pub storage: Option<StorageConfig>,
    #[serde(default)]
    pub transaction_by: TransactionBy,
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
            password: PasswordHashingConfig::default(),
            jwt: JwtConfig::default(),
            request_timeout: 60,
            ssl_folder: PathBuf::default(),
            database: DatabaseConfig::default(),
            storage: Some(StorageConfig::default()),
            transaction_by: TransactionBy::default(),
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

#[derive(Debug, Clone, ValueEnum, Display, EnumString)]
#[strum(serialize_all = "lowercase")]
pub enum DbSchema {
    /// Creates Tabsdata database
    Create,
    /// Upgrades Tabsdata database
    Upgrade,
    /// Creates or upgrades Tabsdata database as needed
    Auto,
}

#[derive(Debug, Clone, Args)]
pub struct Params {
    #[clap(long, alias = "db")]
    /// Database URL
    database_url: Option<Url>,
    #[clap(long)]
    /// Database log level
    database_log_level: Option<LevelFilter>,
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
    /// JWT Secret
    jwt_secret: Option<String>,
    #[clap(long)]
    /// JWT Access token expiration time in seconds
    access_jwt_expiration: Option<i64>,
    #[clap(long)]
    /// Request timeout in seconds
    request_timeout: Option<i64>,
    #[clap(long, value_parser = parse_transaction_by)]
    /// Transaction by
    transaction_by: Option<TransactionBy>,
    #[clap(long)]
    /// The apiserver will create or upgrade the DB schema on startup, default is false
    pub db_schema: Option<DbSchema>,
    #[clap(long)]
    /// The etc directory
    etc: Option<String>, // not used via clap.Added for etc_service to work correctly with CLI option
    #[clap(long)]
    /// The supervisor message queue directory
    msg: Option<String>, // not used via clap. Added for queue_service to work correctly with CLI option
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
                    .unwrap_or_else(|| config.jwt.secret.clone().unwrap());
                let expiration = self
                    .access_jwt_expiration
                    .unwrap_or(config.jwt.access_token_expiration);
                JwtConfig::new(secret, expiration)
            },
            request_timeout: self.request_timeout.unwrap_or(config.request_timeout),
            ssl_folder: self
                .ssl_folder
                .clone()
                .unwrap_or_else(|| config.ssl_folder.clone()),
            database: {
                let mut config = config.database;
                if let Some(url) = &self.database_url {
                    config.conn = Some(match config.conn {
                        Some(existing) => existing.with_url(url.clone())?,
                        None => BackendConfig::from_url(url.clone())?,
                    });
                }
                if let Some(log_level) = self.database_log_level {
                    config.pool.log_level = log_level;
                }
                config
            },
            storage: resolved_storage,
            transaction_by: self
                .transaction_by
                .clone()
                .unwrap_or_else(|| config.transaction_by.clone()),
            extended_config: self
                .extended_params
                .resolve(config.extended_config.clone())?,
        };

        if config.jwt.secret.is_none() {
            Err(ConfigError::MissingJWTSecret)?
        }

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
    use td_objects::types::addresses::NonEmptyAddresses;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.database.url(), None);
        assert_eq!(
            *config.addresses,
            NonEmptyAddresses::new(nonempty![SocketAddr::from(([127, 0, 0, 1], 2457))])
        );
        id::Id::try_from(config.jwt.secret.as_ref().unwrap()).unwrap();
        assert!(config.storage.is_some());
        assert!(config.clone().storage.unwrap().url.is_none());
        assert_eq!(
            config.jwt.access_token_expiration,
            Duration::hours(1).num_seconds()
        );
        assert_eq!(config.request_timeout, 60);
    }

    #[test]
    fn test_params_resolve() {
        let default_config = Config::default();

        let params = Params {
            #[cfg(target_os = "windows")]
            database_url: Some(Url::from_str("file:///c:/test.db").unwrap()),
            #[cfg(not(target_os = "windows"))]
            database_url: Some(Url::from_str("file:///test.db").unwrap()),
            database_log_level: None,
            #[cfg(target_os = "windows")]
            storage_url: Some(String::from("file:///c:/storage")),
            #[cfg(not(target_os = "windows"))]
            storage_url: Some(String::from("file:///storage")),
            address: Some(vec!["127.0.0.1:2457".parse().unwrap()]),
            internal_address: Some(vec!["127.0.0.1:2458".parse().unwrap()]),
            ssl_folder: Some(PathBuf::from("file:///ssl")),
            jwt_secret: Some(String::from("NEW_SECRET")),
            access_jwt_expiration: Some(7200),
            request_timeout: Some(120),
            transaction_by: Some(TransactionBy::default()),
            db_schema: None,
            etc: None,
            msg: None,
            extended_params: ExtendedParams::default(),
        };

        let resolved_config = params.resolve(default_config.clone()).unwrap();

        assert_eq!(resolved_config.jwt.secret, Some("NEW_SECRET".to_string()));
        assert_eq!(
            *resolved_config.addresses,
            NonEmptyAddresses::new(nonempty![SocketAddr::from(([127, 0, 0, 1], 2457))])
        );
        #[cfg(target_os = "windows")]
        assert_eq!(
            resolved_config.database.url(),
            Some(&Url::from_str("file:///test.db").unwrap())
        );
        #[cfg(not(target_os = "windows"))]
        assert_eq!(
            resolved_config.database.url(),
            Some(&Url::from_str("file:///test.db").unwrap())
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

        // Test with some fields not set in Params
        let partial_params = Params {
            #[cfg(target_os = "windows")]
            database_url: Some(Url::from_str("file:///c:/test.db").unwrap()),
            #[cfg(not(target_os = "windows"))]
            database_url: Some(Url::from_str("file:///test.db").unwrap()),
            database_log_level: None,
            #[cfg(target_os = "windows")]
            storage_url: Some(String::from("file:///c:/storage")),
            #[cfg(not(target_os = "windows"))]
            storage_url: Some(String::from("file:///storage")),
            address: None,
            internal_address: None,
            ssl_folder: None,
            jwt_secret: Some(String::from("NEW_SECRET")),
            access_jwt_expiration: Some(1800),
            request_timeout: None,
            transaction_by: Some(TransactionBy::default()),
            db_schema: None,
            etc: None,
            msg: None,
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
}
