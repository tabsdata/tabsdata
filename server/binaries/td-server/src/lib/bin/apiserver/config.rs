//
//  Copyright 2024 Tabs Data Inc.
//

//! API Server CLI configuration and parameters.

use crate::logic::apiserver::addresses_default;
use chrono::Duration;
use clap_derive::Args;
use getset::Getters;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::net::{AddrParseError, SocketAddr};
use strum::ParseError;
use td_database::sql::SqliteConfig;
use td_transaction::TransactionBy;

#[derive(Clone, Serialize, Deserialize, Getters)]
#[getset(get = "pub")]
pub struct Config {
    storage_url: Option<String>,
    #[serde(default)]
    addresses: Vec<SocketAddr>,
    jwt_secret: Option<String>,
    access_jwt_expiration: i64,  // in seconds
    refresh_jwt_expiration: i64, // in seconds
    request_timeout: i64,        // in seconds
    database: SqliteConfig,
    #[serde(default)]
    transaction_by: TransactionBy,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            storage_url: None,
            addresses: addresses_default(),
            jwt_secret: None,
            access_jwt_expiration: Duration::hours(1).num_seconds(),
            refresh_jwt_expiration: Duration::hours(24).num_seconds(),
            request_timeout: 60,
            database: SqliteConfig::default(),
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
    /// JWT Refresh token expiration time in seconds
    refresh_jwt_expiration: Option<i64>,
    #[clap(long)]
    /// Request timeout in seconds
    request_timeout: Option<i64>,
    #[clap(long, value_parser = parse_transaction_by)]
    /// Transaction by
    transaction_by: Option<TransactionBy>,
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
                .or(config.storage_url().clone()),
            addresses: self
                .address
                .clone()
                .unwrap_or_else(|| config.addresses().clone()),
            jwt_secret: self
                .jwt_secret
                .as_ref()
                .map(|secret| secret.to_string())
                .or(config.jwt_secret().clone()),
            access_jwt_expiration: self
                .access_jwt_expiration
                .unwrap_or_else(|| *config.access_jwt_expiration()),
            refresh_jwt_expiration: self
                .refresh_jwt_expiration
                .unwrap_or_else(|| *config.refresh_jwt_expiration()),
            request_timeout: self
                .request_timeout
                .unwrap_or_else(|| *config.request_timeout()),
            transaction_by: self
                .transaction_by
                .clone()
                .unwrap_or_else(|| config.transaction_by().clone()),
        };

        if config.addresses().is_empty() {
            return Err(ConfigError::MissingAddress);
        }

        if config.jwt_secret().is_none() {
            return Err(ConfigError::MissingJWTSecret);
        }

        match config.database.url() {
            None => return Err(ConfigError::MissingDatabaseUrl),
            Some(url) if !url.starts_with("file://") => {
                return Err(ConfigError::InvalidDatabaseUrl(url.to_string()));
            }
            _ => {}
        }

        match config.storage_url {
            None => return Err(ConfigError::MissingStorageUrl),
            Some(url) if !url.starts_with("file://") => {
                return Err(ConfigError::InvalidStorageUrl(url.to_string()));
            }
            _ => {}
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

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("No address was specified for the API Server to bind to.")]
    MissingAddress,
    #[error("No JWT Secret was specified.")]
    MissingJWTSecret,
    #[error("No database URL (must be a file:// URL) was provided.")]
    MissingDatabaseUrl,
    #[error("The database URL '{0}' must be a file:// URL.")]
    InvalidDatabaseUrl(String),
    #[error("No storage URL (must be a file:// URL) was provided.")]
    MissingStorageUrl,
    #[error("The storage URL '{0}' must be a file:// URL.")]
    InvalidStorageUrl(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.database().url(), &None);
        assert_eq!(
            config.addresses(),
            &vec![SocketAddr::from(([127, 0, 0, 1], 0))]
        );
        assert_eq!(config.jwt_secret(), &None);
        assert_eq!(config.storage_url(), &None);
        assert_eq!(
            *config.access_jwt_expiration(),
            Duration::hours(1).num_seconds()
        );
        assert_eq!(
            *config.refresh_jwt_expiration(),
            Duration::hours(24).num_seconds()
        );
        assert_eq!(*config.request_timeout(), 60);
    }

    #[test]
    fn test_params_resolve() {
        let default_config = Config::default();

        let params = Params {
            database_url: Some(String::from("file:///test.db")),
            storage_url: Some(String::from("file:///storage")),
            address: Some(vec!["127.0.0.1:8080".parse().unwrap()]),
            jwt_secret: Some(String::from("NEW_SECRET")),
            access_jwt_expiration: Some(7200),
            refresh_jwt_expiration: Some(14400),
            request_timeout: Some(120),
            transaction_by: Some(TransactionBy::default()),
        };

        let resolved_config = params.resolve(default_config.clone()).unwrap();

        assert_eq!(
            resolved_config.jwt_secret(),
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
        assert_eq!(*resolved_config.access_jwt_expiration(), 7200);
        assert_eq!(*resolved_config.refresh_jwt_expiration(), 14400);
        assert_eq!(*resolved_config.request_timeout(), 120);

        // Test with some fields not set in Params
        let partial_params = Params {
            database_url: Some(String::from("file:///test.db")),
            storage_url: Some(String::from("file:///storage")),
            address: None,
            jwt_secret: Some(String::from("NEW_SECRET")),
            access_jwt_expiration: Some(1800),
            refresh_jwt_expiration: None,
            request_timeout: None,
            transaction_by: Some(TransactionBy::default()),
        };

        let partially_resolved_config = partial_params.resolve(default_config.clone()).unwrap();

        assert_eq!(
            partially_resolved_config.addresses(),
            default_config.addresses()
        );
        assert_eq!(
            resolved_config.jwt_secret(),
            &Some("NEW_SECRET".to_string())
        );
        assert_eq!(*partially_resolved_config.access_jwt_expiration(), 1800);
        assert_eq!(
            *partially_resolved_config.request_timeout(),
            *default_config.request_timeout()
        );
    }
}
