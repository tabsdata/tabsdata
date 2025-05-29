//
//  Copyright 2024 Tabs Data Inc.
//

//! API Server CLI configuration and parameters.

use crate::addresses_default;
use clap_derive::{Args, ValueEnum};
use getset::Getters;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::net::{AddrParseError, SocketAddr};
use strum::ParseError;
use td_database::sql::SqliteConfig;
use td_error::td_error;
use td_services::auth::services::{JwtConfig, PasswordHashConfig};
use td_storage::MountDef;
use te_execution::transaction::TransactionBy;

#[derive(Clone, Serialize, Deserialize, Getters)]
#[getset(get = "pub")]
pub struct Config {
    #[serde(default)]
    addresses: Vec<SocketAddr>,
    password: PasswordHashConfig,
    jwt: JwtConfig,
    request_timeout: i64, // in seconds
    database: SqliteConfig,
    storage_url: Option<String>,
    #[getset(skip)]
    storage_mounts: Option<Vec<MountDef>>,
    #[serde(default)]
    transaction_by: TransactionBy,
}

impl Config {
    pub fn storage_mounts(&self) -> Result<Vec<MountDef>, ConfigError> {
        if self.storage_url.is_some() && self.storage_mounts.is_some() {
            Err(ConfigError::DoubleStorageConfig)
        } else if self.storage_url.is_none() && self.storage_mounts.is_none() {
            Err(ConfigError::MissingStorageConfig)
        } else if self.storage_url.is_some() {
            let mount_def = MountDef::builder()
                .id("TDS_MOUNT_ROOT")
                .mount_path("/")
                .uri(self.storage_url.as_ref().unwrap())
                .build()
                .unwrap();
            Ok(vec![mount_def])
        } else {
            Ok(self.storage_mounts.as_ref().unwrap().clone())
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            addresses: addresses_default(),
            password: PasswordHashConfig::default(),
            jwt: JwtConfig::default(),
            request_timeout: 60,
            database: SqliteConfig::default(),
            storage_url: None,
            storage_mounts: None,
            transaction_by: TransactionBy::default(),
        }
    }
}

impl Display for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // hide sensitive configs from displaying
        write!(
            f,
            "{{ addresses: {:?}, database: {:?}, storage {:?} }}",
            self.addresses, self.database, self.storage_url
        )
    }
}

impl td_common::config::Config for Config {}

impl From<&Config> for JwtConfig {
    fn from(config: &Config) -> Self {
        config.jwt.clone()
    }
}

impl From<&Config> for PasswordHashConfig {
    fn from(config: &Config) -> Self {
        config.password.clone()
    }
}

#[derive(Debug, Clone, ValueEnum)]
pub enum DbSchema {
    /// Creates Tabsdata database
    Create,
    /// Updates Tabsdata database
    Update,
    /// Creates or updates Tabsdata database as needed
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
    /// The apiserver will create or update the DB schema on startup, default is false
    db_schema: Option<DbSchema>,
}

impl Params {
    pub fn resolve(&self, config: Config) -> Result<Config, ConfigError> {
        let config = Config {
            database: self
                .database_url
                .as_ref()
                .map(|db_url| {
                    let mut database_config_builder = config.database().to_builder();
                    database_config_builder.url(Some(db_url.to_string()));
                    database_config_builder.build().unwrap()
                })
                .unwrap_or_else(|| config.database().clone()),
            storage_url: self
                .storage_url
                .as_ref()
                .map(|url| url.to_string())
                .or(self.storage_url().clone()),
            storage_mounts: config.storage_mounts.clone(),
            addresses: self
                .address
                .clone()
                .unwrap_or_else(|| config.addresses().clone()),
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
            Err(ConfigError::MissingStorageUrl)?;
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
    #[error("No storage URL (must be a file:// URL) was provided.")]
    MissingStorageUrl = 4,
    #[error("Cannot define both storage-url and storage-mounts in the configuration")]
    DoubleStorageConfig = 5,
    #[error("No storage URL no mounts configuration")]
    MissingStorageConfig = 6,
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
            &vec![SocketAddr::from(([127, 0, 0, 1], 0))]
        );
        id::Id::try_from(config.jwt().secret().as_ref().unwrap()).unwrap();
        assert_eq!(config.storage_url(), &None);
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
            jwt_secret: Some(String::from("NEW_SECRET")),
            access_jwt_expiration: Some(7200),
            request_timeout: Some(120),
            transaction_by: Some(TransactionBy::default()),
            db_schema: None,
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
        assert_eq!(
            resolved_config.database().url().as_ref().unwrap(),
            "file:///test.db"
        );
        assert_eq!(
            resolved_config.storage_url().as_ref().unwrap(),
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
            jwt_secret: Some(String::from("NEW_SECRET")),
            access_jwt_expiration: Some(1800),
            request_timeout: None,
            transaction_by: Some(TransactionBy::default()),
            db_schema: None,
        };

        let partially_resolved_config = partial_params.resolve(default_config.clone()).unwrap();

        assert_eq!(
            partially_resolved_config.addresses(),
            default_config.addresses()
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
