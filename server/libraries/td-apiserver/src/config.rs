//
//  Copyright 2024 Tabs Data Inc.
//

//! API Server CLI configuration and parameters.

use clap_derive::{Args, ValueEnum};
use getset::Getters;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::net::{AddrParseError, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use strum::{Display, EnumString, ParseError};
use td_database::sql::SqliteConfig;
use td_error::td_error;
use td_security::config::PasswordHashingConfig;
use td_services::auth::services::JwtConfig;
use td_storage::MountDef;
use te_execution::transaction::TransactionBy;

#[derive(Clone, Serialize, Deserialize, Getters)]
#[getset(get = "pub")]
pub struct Config {
    #[serde(default = "addresses_default")]
    addresses: Vec<SocketAddr>,
    #[serde(default = "internal_addresses_default")]
    internal_addresses: Vec<SocketAddr>,
    password: PasswordHashingConfig,
    jwt: JwtConfig,
    request_timeout: i64, // in seconds
    ssl_folder: PathBuf,
    database: SqliteConfig,
    #[serde(default)]
    storage: Option<StorageConfig>,
    #[serde(default)]
    transaction_by: TransactionBy,
}

pub fn addresses_default() -> Vec<SocketAddr> {
    const DEFAULT_PORT: u16 = 2457;
    vec![SocketAddr::new(Ipv4Addr::LOCALHOST.into(), DEFAULT_PORT)]
}

pub fn internal_addresses_default() -> Vec<SocketAddr> {
    const DEFAULT_PORT: u16 = 2458;
    vec![SocketAddr::new(Ipv4Addr::LOCALHOST.into(), DEFAULT_PORT)]
}

#[derive(Clone, Serialize, Deserialize, Getters, Debug)]
#[getset(get = "pub")]
#[derive(Default)]
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

        let has_url = storage.url.is_some();
        let has_mounts = storage.mounts.is_some();

        match (has_url, has_mounts) {
            (true, true) => Err(ConfigError::DoubleStorageConfig),
            (false, false) => Err(ConfigError::MissingStorageConfig),
            (true, false) => {
                let mount_def = MountDef::builder()
                    .id("TDS_MOUNT_ROOT")
                    .path("/")
                    .uri(storage.url.as_ref().unwrap())
                    .build()
                    .map_err(|_| ConfigError::InvalidMountDefinition)?;
                Ok(vec![mount_def])
            }
            (false, true) => Ok(storage.mounts.as_ref().unwrap().clone()),
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            addresses: addresses_default(),
            internal_addresses: internal_addresses_default(),
            password: PasswordHashingConfig::default(),
            jwt: JwtConfig::default(),
            request_timeout: 60,
            ssl_folder: PathBuf::default(),
            database: SqliteConfig::default(),
            storage: Some(StorageConfig::default()),
            transaction_by: TransactionBy::default(),
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

#[derive(Debug, Clone, Getters, Args)]
#[getset(get = "pub")]
pub struct Params {
    #[clap(long, alias = "db")]
    /// Database URL (it must be a file:// URI)
    database_url: Option<String>,
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
    db_schema: Option<DbSchema>,
    #[clap(long)]
    /// The etc directory
    etc: Option<String>, // not used via clap.Added for etc_service to work correctly with CLI option
    #[clap(long)]
    /// The supervisor message queue directory
    msg: Option<String>, // not used via clap. Added for queue_service to work correctly with CLI option
}

impl Params {
    pub fn resolve(&self, config: Config) -> Result<Config, ConfigError> {
        let mut resolved_storage: StorageConfig = config.storage.clone().unwrap_or_default();
        if self.storage_url.is_some() {
            let resolved_storage_url = self
                .storage_url
                .as_ref()
                .map(|url| url.to_string())
                .or(self.storage_url().clone());
            resolved_storage.url = resolved_storage_url;
        }
        let resolved_storage = Some(resolved_storage);
        let config = Config {
            addresses: self
                .address
                .clone()
                .unwrap_or_else(|| config.addresses().clone()),
            internal_addresses: self
                .internal_address
                .clone()
                .unwrap_or_else(|| config.internal_addresses().clone()),
            password: config.password().clone(),
            jwt: {
                let secret = self
                    .jwt_secret
                    .to_owned()
                    .unwrap_or_else(|| config.jwt().secret().to_owned().unwrap());
                let expiration = self
                    .access_jwt_expiration
                    .unwrap_or_else(|| *config.jwt().access_token_expiration());
                JwtConfig::new(secret, expiration)
            },
            request_timeout: self
                .request_timeout
                .unwrap_or_else(|| *config.request_timeout()),
            ssl_folder: self
                .ssl_folder
                .clone()
                .unwrap_or_else(|| config.ssl_folder().clone()),
            database: self
                .database_url
                .as_ref()
                .map(|db_url| {
                    let mut database_config_builder = config.database().to_builder();
                    database_config_builder.url(Some(db_url.to_string()));
                    database_config_builder.build().unwrap()
                })
                .unwrap_or_else(|| config.database().clone()),
            storage: resolved_storage,
            transaction_by: self
                .transaction_by
                .clone()
                .unwrap_or_else(|| config.transaction_by().clone()),
        };

        if config.addresses().is_empty() {
            Err(ConfigError::MissingAddress)?;
        }

        if config.jwt().secret().is_none() {
            Err(ConfigError::MissingJWTSecret)?
        }

        match config.database().url() {
            None => Err(ConfigError::MissingDatabaseUrl)?,
            Some(url) if !url.starts_with("file://") => {
                Err(ConfigError::InvalidDatabaseUrl(url.to_string()))?;
            }
            _ => {}
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
    #[error("No database URL (must be a file:// URL) was provided.")]
    MissingDatabaseUrl = 2,
    #[error("The database URL '{0}' must be a file:// URL.")]
    InvalidDatabaseUrl(String) = 3,
    #[error("No storage settings.")]
    MissingStorage = 4,
    #[error("Both storage.url and storage.mounts have been configured.")]
    DoubleStorageConfig = 5,
    #[error("Neither storage.url nor storage.mounts has been configured.")]
    MissingStorageConfig = 6,
    #[error("The url specified in storage.url is wrong.")]
    InvalidMountDefinition = 7,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use std::net::SocketAddr;
    use td_common::id;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.database().url(), &None);
        assert_eq!(
            config.addresses(),
            &vec![SocketAddr::from(([127, 0, 0, 1], 2457))]
        );
        id::Id::try_from(config.jwt().secret().as_ref().unwrap()).unwrap();
        assert!(config.storage.is_some());
        assert!(config.clone().storage.unwrap().url.is_none());
        assert_eq!(
            *config.jwt().access_token_expiration(),
            Duration::hours(1).num_seconds()
        );
        assert_eq!(*config.request_timeout(), 60);
    }

    #[test]
    fn test_params_resolve() {
        let default_config = Config::default();

        let params = Params {
            #[cfg(target_os = "windows")]
            database_url: Some(String::from("file:///c:/test.db")),
            #[cfg(not(target_os = "windows"))]
            database_url: Some(String::from("file:///test.db")),
            #[cfg(target_os = "windows")]
            storage_url: Some(String::from("file:///c:/storage")),
            #[cfg(not(target_os = "windows"))]
            storage_url: Some(String::from("file:///storage")),
            address: Some(vec!["127.0.0.1:8080".parse().unwrap()]),
            internal_address: Some(vec!["127.0.0.1:8081".parse().unwrap()]),
            ssl_folder: Some(PathBuf::from("file:///ssl")),
            jwt_secret: Some(String::from("NEW_SECRET")),
            access_jwt_expiration: Some(7200),
            request_timeout: Some(120),
            transaction_by: Some(TransactionBy::default()),
            db_schema: None,
            etc: None,
            msg: None,
        };

        let resolved_config = params.resolve(default_config.clone()).unwrap();

        assert_eq!(
            resolved_config.jwt().secret(),
            &Some("NEW_SECRET".to_string())
        );
        assert_eq!(
            resolved_config.addresses(),
            &vec!["127.0.0.1:8080".parse().unwrap()]
        );
        #[cfg(target_os = "windows")]
        assert_eq!(
            resolved_config.database().url().as_ref().unwrap(),
            "file:///c:/test.db"
        );
        #[cfg(not(target_os = "windows"))]
        assert_eq!(
            resolved_config.database().url().as_ref().unwrap(),
            "file:///test.db"
        );
        assert!(resolved_config.clone().storage.is_some());
        #[cfg(target_os = "windows")]
        assert_eq!(
            resolved_config
                .clone()
                .storage
                .unwrap()
                .url()
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
                .url()
                .as_ref()
                .unwrap(),
            "file:///storage"
        );
        assert_eq!(*resolved_config.request_timeout(), 120);

        // Test with some fields not set in Params
        let partial_params = Params {
            #[cfg(target_os = "windows")]
            database_url: Some(String::from("file:///c:/test.db")),
            #[cfg(not(target_os = "windows"))]
            database_url: Some(String::from("file:///test.db")),
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
        };

        let partially_resolved_config = partial_params.resolve(default_config.clone()).unwrap();

        assert_eq!(
            partially_resolved_config.addresses(),
            default_config.addresses()
        );
        assert_eq!(
            partially_resolved_config.internal_addresses(),
            default_config.internal_addresses()
        );
        assert_eq!(
            resolved_config.jwt().secret(),
            &Some("NEW_SECRET".to_string())
        );
        assert_eq!(
            *partially_resolved_config.jwt().access_token_expiration(),
            1800
        );
        assert_eq!(
            *partially_resolved_config.request_timeout(),
            *default_config.request_timeout()
        );
    }
}
