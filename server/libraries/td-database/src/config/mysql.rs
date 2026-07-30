//
// Copyright 2026 Tabsdata Inc.
//

//! [`MySqlConfig`] — the full sqlx `MySqlConnectOptions` surface.

use super::logging::StatementLogConfig;
use serde::{Deserialize, Serialize};
use sqlx::mysql::MySqlSslMode;
use std::path::PathBuf;

/// Mirror of [`MySqlSslMode`] with a serde surface (sqlx's enum has none).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MySqlSslModeConfig {
    Disabled,
    #[default]
    Preferred,
    Required,
    VerifyCa,
    VerifyIdentity,
}

impl From<MySqlSslModeConfig> for MySqlSslMode {
    fn from(mode: MySqlSslModeConfig) -> Self {
        match mode {
            MySqlSslModeConfig::Disabled => MySqlSslMode::Disabled,
            MySqlSslModeConfig::Preferred => MySqlSslMode::Preferred,
            MySqlSslModeConfig::Required => MySqlSslMode::Required,
            MySqlSslModeConfig::VerifyCa => MySqlSslMode::VerifyCa,
            MySqlSslModeConfig::VerifyIdentity => MySqlSslMode::VerifyIdentity,
        }
    }
}

/// MySQL connect options. `username`/`password` are optional here — component
/// resolution fills them in (see [`crate::components`]); the connect path only
/// sees the resolved, mandatory credentials. Unset optional fields leave
/// sqlx's own defaults in place.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct MySqlConfig {
    /// Server host, defaults to `localhost`.
    pub host: String,
    /// Server port, defaults to `3306`.
    pub port: u16,
    /// Unix socket path, used instead of `host`/`port` when set.
    pub socket: Option<PathBuf>,
    /// Database name.
    pub database: Option<String>,
    /// Login username (component default: the schema-named owner role).
    pub username: Option<String>,
    /// Login password; `${embedded}` opts into the persisted one.
    pub password: Option<String>,
    /// TLS posture, defaults to `preferred`.
    pub ssl_mode: MySqlSslModeConfig,
    /// CA certificate file for server verification.
    pub ssl_ca: Option<PathBuf>,
    /// CA certificate as inline PEM.
    pub ssl_ca_pem: Option<String>,
    /// Client certificate file for mutual TLS.
    pub ssl_client_cert: Option<PathBuf>,
    /// Client certificate as inline PEM.
    pub ssl_client_cert_pem: Option<String>,
    /// Client private key file for mutual TLS.
    pub ssl_client_key: Option<PathBuf>,
    /// Client private key as inline PEM.
    pub ssl_client_key_pem: Option<String>,
    /// Prepared-statement cache size per connection.
    pub statement_cache_capacity: Option<usize>,
    /// Connection character set; sqlx defaults to `utf8mb4`.
    pub charset: Option<String>,
    /// Connection collation.
    pub collation: Option<String>,
    /// `PIPES_AS_CONCAT` sql mode, defaults to `true`.
    pub pipes_as_concat: bool,
    /// Allow the cleartext authentication plugin.
    pub enable_cleartext_plugin: bool,
    /// `NO_ENGINE_SUBSTITUTION` sql mode, defaults to `true`.
    pub no_engine_substitution: bool,
    /// Session timezone; sqlx defaults to `+00:00`.
    pub timezone: Option<String>,
    /// Issue `SET NAMES` on connect, defaults to `true`.
    pub set_names: bool,
    /// Statement logging.
    #[serde(flatten)]
    pub log: StatementLogConfig,
}

impl Default for MySqlConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 3306,
            socket: None,
            database: None,
            username: None,
            password: None,
            ssl_mode: MySqlSslModeConfig::default(),
            ssl_ca: None,
            ssl_ca_pem: None,
            ssl_client_cert: None,
            ssl_client_cert_pem: None,
            ssl_client_key: None,
            ssl_client_key_pem: None,
            statement_cache_capacity: None,
            charset: None,
            collation: None,
            pipes_as_concat: true,
            enable_cleartext_plugin: false,
            no_engine_substitution: true,
            timezone: None,
            set_names: true,
            log: StatementLogConfig::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mysql_config_minimal_defaults() {
        let cfg: MySqlConfig = serde_yaml::from_str("{}").unwrap();
        assert_eq!(cfg, MySqlConfig::default());
        assert_eq!(cfg.host, "localhost");
        assert_eq!(cfg.port, 3306);
        assert_eq!(cfg.ssl_mode, MySqlSslModeConfig::Preferred);
        assert!(cfg.pipes_as_concat);
        assert!(cfg.no_engine_substitution);
        assert!(cfg.set_names);
    }

    #[test]
    fn mysql_config_full_surface() {
        let yaml = "
host: db.example.com
port: 3307
socket: /var/run/mysqld/mysqld.sock
database: td
username: td
password: secret
ssl_mode: verify_identity
ssl_ca: /certs/ca.pem
ssl_ca_pem: ca-pem
ssl_client_cert: /certs/client.pem
ssl_client_cert_pem: cert-pem
ssl_client_key: /certs/client.key
ssl_client_key_pem: key-pem
statement_cache_capacity: 128
charset: utf8mb4
collation: utf8mb4_unicode_ci
pipes_as_concat: false
enable_cleartext_plugin: true
no_engine_substitution: false
timezone: '+01:00'
set_names: false
";
        let cfg: MySqlConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.host, "db.example.com");
        assert_eq!(cfg.port, 3307);
        assert_eq!(
            cfg.socket.as_deref(),
            Some("/var/run/mysqld/mysqld.sock".as_ref())
        );
        assert_eq!(cfg.database.as_deref(), Some("td"));
        assert_eq!(cfg.username.as_deref(), Some("td"));
        assert_eq!(cfg.password.as_deref(), Some("secret"));
        assert_eq!(cfg.ssl_mode, MySqlSslModeConfig::VerifyIdentity);
        assert_eq!(cfg.ssl_ca.as_deref(), Some("/certs/ca.pem".as_ref()));
        assert_eq!(cfg.ssl_ca_pem.as_deref(), Some("ca-pem"));
        assert_eq!(cfg.statement_cache_capacity, Some(128));
        assert_eq!(cfg.charset.as_deref(), Some("utf8mb4"));
        assert_eq!(cfg.collation.as_deref(), Some("utf8mb4_unicode_ci"));
        assert!(!cfg.pipes_as_concat);
        assert!(cfg.enable_cleartext_plugin);
        assert!(!cfg.no_engine_substitution);
        assert_eq!(cfg.timezone.as_deref(), Some("+01:00"));
        assert!(!cfg.set_names);
    }

    #[test]
    fn mysql_config_bad_ssl_mode_errors() {
        assert!(serde_yaml::from_str::<MySqlConfig>("ssl_mode: verify-ca").is_err());
    }
}
