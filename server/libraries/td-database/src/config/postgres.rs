//
// Copyright 2026 Tabsdata Inc.
//

//! [`PostgresConfig`] — the full sqlx `PgConnectOptions` surface. External
//! servers and the managed in-process server alike (the latter is booted out
//! of band from [`crate::server`]) are addressed by `host`/`port`.

use super::logging::StatementLogConfig;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgSslMode;
use std::collections::BTreeMap;
use std::path::PathBuf;

/// Mirror of [`PgSslMode`] with a serde surface (sqlx's enum has none).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PgSslModeConfig {
    Disable,
    Allow,
    #[default]
    Prefer,
    Require,
    VerifyCa,
    VerifyFull,
}

impl From<PgSslModeConfig> for PgSslMode {
    fn from(mode: PgSslModeConfig) -> Self {
        match mode {
            PgSslModeConfig::Disable => PgSslMode::Disable,
            PgSslModeConfig::Allow => PgSslMode::Allow,
            PgSslModeConfig::Prefer => PgSslMode::Prefer,
            PgSslModeConfig::Require => PgSslMode::Require,
            PgSslModeConfig::VerifyCa => PgSslMode::VerifyCa,
            PgSslModeConfig::VerifyFull => PgSslMode::VerifyFull,
        }
    }
}

/// PostgreSQL connect options. `username`/`password` are optional here —
/// component/server resolution fills them in (see [`crate::components`] and
/// [`crate::server`]); the connect path only sees the resolved, mandatory
/// credentials. Unset optional fields leave sqlx's own defaults in place.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct PostgresConfig {
    /// Server host, defaults to `localhost`.
    pub host: String,
    /// Server port, defaults to `5432`.
    pub port: u16,
    /// Unix socket directory, used instead of `host`/`port` when set.
    pub socket: Option<PathBuf>,
    /// Database name; sqlx defaults it to the username when unset.
    pub database: Option<String>,
    /// Login username (component default: the schema-named owner role;
    /// `${embedded}` on the server entry: the boot superuser).
    pub username: Option<String>,
    /// Login password; `${embedded}` opts into the persisted one.
    pub password: Option<String>,
    /// TLS posture, defaults to `prefer`.
    pub ssl_mode: PgSslModeConfig,
    /// CA certificate file for server verification.
    pub ssl_root_cert: Option<PathBuf>,
    /// CA certificate as inline PEM.
    pub ssl_root_cert_pem: Option<String>,
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
    /// `application_name` reported to the server.
    pub application_name: Option<String>,
    /// `extra_float_digits` session setting.
    pub extra_float_digits: Option<i8>,
    /// Additional server settings (GUCs) applied on connect.
    pub options: BTreeMap<String, String>,
    /// Statement logging.
    #[serde(flatten)]
    pub log: StatementLogConfig,
}

impl Default for PostgresConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 5432,
            socket: None,
            database: None,
            username: None,
            password: None,
            ssl_mode: PgSslModeConfig::default(),
            ssl_root_cert: None,
            ssl_root_cert_pem: None,
            ssl_client_cert: None,
            ssl_client_cert_pem: None,
            ssl_client_key: None,
            ssl_client_key_pem: None,
            statement_cache_capacity: None,
            application_name: None,
            extra_float_digits: None,
            options: BTreeMap::new(),
            log: StatementLogConfig::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tracing::log::LevelFilter;

    #[test]
    fn postgres_config_minimal_defaults() {
        let cfg: PostgresConfig = serde_yaml::from_str("{}").unwrap();
        assert_eq!(cfg, PostgresConfig::default());
        assert_eq!(cfg.host, "localhost");
        assert_eq!(cfg.port, 5432);
        assert_eq!(cfg.ssl_mode, PgSslModeConfig::Prefer);
        assert!(cfg.username.is_none());
    }

    #[test]
    fn postgres_config_full_surface() {
        let yaml = "
host: db.example.com
port: 6432
socket: /var/run/postgresql
database: postgres
username: td
password: secret
ssl_mode: verify-full
ssl_root_cert: /certs/ca.pem
ssl_root_cert_pem: root-pem
ssl_client_cert: /certs/client.pem
ssl_client_cert_pem: cert-pem
ssl_client_key: /certs/client.key
ssl_client_key_pem: key-pem
statement_cache_capacity: 256
application_name: tabsdata
extra_float_digits: 3
options:
  search_path: td
  statement_timeout: 5s
log_statements: info
log_slow_statements: error
log_slow_statements_threshold: 2
";
        let cfg: PostgresConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.host, "db.example.com");
        assert_eq!(cfg.port, 6432);
        assert_eq!(cfg.socket.as_deref(), Some("/var/run/postgresql".as_ref()));
        assert_eq!(cfg.database.as_deref(), Some("postgres"));
        assert_eq!(cfg.username.as_deref(), Some("td"));
        assert_eq!(cfg.password.as_deref(), Some("secret"));
        assert_eq!(cfg.ssl_mode, PgSslModeConfig::VerifyFull);
        assert_eq!(cfg.ssl_root_cert.as_deref(), Some("/certs/ca.pem".as_ref()));
        assert_eq!(cfg.ssl_root_cert_pem.as_deref(), Some("root-pem"));
        assert_eq!(
            cfg.ssl_client_cert.as_deref(),
            Some("/certs/client.pem".as_ref())
        );
        assert_eq!(cfg.ssl_client_cert_pem.as_deref(), Some("cert-pem"));
        assert_eq!(
            cfg.ssl_client_key.as_deref(),
            Some("/certs/client.key".as_ref())
        );
        assert_eq!(cfg.ssl_client_key_pem.as_deref(), Some("key-pem"));
        assert_eq!(cfg.statement_cache_capacity, Some(256));
        assert_eq!(cfg.application_name.as_deref(), Some("tabsdata"));
        assert_eq!(cfg.extra_float_digits, Some(3));
        assert_eq!(cfg.options["search_path"], "td");
        assert_eq!(cfg.options["statement_timeout"], "5s");
        assert_eq!(cfg.log.log_statements, LevelFilter::Info);
        assert_eq!(cfg.log.log_slow_statements, LevelFilter::Error);
        assert_eq!(
            cfg.log.log_slow_statements_threshold,
            Duration::from_secs(2)
        );
    }

    #[test]
    fn postgres_config_bad_ssl_mode_errors() {
        let result = serde_yaml::from_str::<PostgresConfig>("ssl_mode: full");
        assert!(result.is_err());
    }
}
