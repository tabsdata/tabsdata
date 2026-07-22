//
// Copyright 2026 Tabsdata Inc.
//

//! [`DatabaseConfig`] — a single **connectable** database target: its
//! [`BackendConfig`], schema, credentials, and [`PoolConfig`]. The resolved
//! config that component/server resolution produces and
//! [`crate::pool::connect`] consumes, so the backend is mandatory (a target
//! with no backend isn't connectable).

use super::Schema;
use super::backend::BackendConfig;
use super::pool::PoolConfig;
use crate::error::DbError;
use serde::{Deserialize, Serialize};
use url::Url;

/// Configuration for a connectable database pool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    /// The backend target — always reachable by URL.
    #[serde(flatten)]
    pub conn: BackendConfig,
    /// PostgreSQL schema the connection's `search_path` is pinned to. Mandatory on
    /// the PostgreSQL path (no `public` default, so each component stays isolated);
    /// [`Self::connect`] fails with [`DbError::MissingSchema`] when unset. Ignored
    /// by SQLite and MySQL, which have no `search_path`.
    #[serde(default)]
    pub schema: Option<Schema>,
    /// Connection username. Required by PostgreSQL and MySQL (their URL must not
    /// embed credentials); rejected by SQLite, which is a file with no
    /// credentials. Validated against the backend by [`Self::connect`].
    #[serde(default)]
    pub username: Option<String>,
    /// Connection password, paired with [`Self::username`] under the same rules.
    #[serde(default)]
    pub password: Option<String>,
    /// The RLS app role serving connections `SET ROLE` to. `None` means the global
    /// default [`APP_ROLE`](crate::APP_ROLE) (`td_app`) — what production always
    /// uses. Tests set a per-test role so they can isolate role-level state (e.g.
    /// flipping `BYPASSRLS`) on the shared server.
    #[serde(default)]
    pub app_role: Option<String>,
    /// Connection pool settings.
    #[serde(default, flatten)]
    pub pool: PoolConfig,
}

impl DatabaseConfig {
    pub fn new<P: Into<Url>>(db_url: P) -> Result<Self, DbError> {
        Ok(Self {
            conn: BackendConfig::from_url(db_url.into())?,
            schema: None,
            username: None,
            password: None,
            app_role: None,
            pool: PoolConfig::default(),
        })
    }

    /// The RLS app role serving connections `SET ROLE` to: the configured override,
    /// else the global default [`APP_ROLE`](crate::APP_ROLE).
    pub fn app_role(&self) -> &str {
        self.app_role.as_deref().unwrap_or(crate::APP_ROLE)
    }

    pub fn url(&self) -> &Url {
        self.conn.url()
    }

    /// The configured PostgreSQL schema, or [`DbError::MissingSchema`] when
    /// unset. Only meaningful on the PostgreSQL path; SQLite/MySQL connect
    /// without consulting it.
    pub fn require_schema(&self) -> Result<&Schema, DbError> {
        self.schema.as_ref().ok_or(DbError::MissingSchema)
    }

    /// Check the credential rules for the backend: SQLite rejects any credentials
    /// (a file has none); other backends must keep credentials in the explicit
    /// fields, never embedded in the URL. Presence is not required here.
    pub fn validate(&self) -> Result<(), DbError> {
        match &self.conn {
            #[cfg(sqlite)]
            BackendConfig::Sqlite(_) => {
                if self.username.is_some() || self.password.is_some() {
                    return Err(DbError::UnexpectedCredentials);
                }
            }
            _ => {
                let url = self.conn.url();
                if !url.username().is_empty() || url.password().is_some() {
                    return Err(DbError::CredentialsInUrl);
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::schema;
    use super::*;
    use std::time::Duration;
    use tracing::log::LevelFilter;

    #[cfg(sqlite)]
    #[test]
    fn database_config_conn_inferred() {
        let yaml = "
url: file:///tmp/test.db
min_connections: 2
max_connections: 20
";
        let cfg: DatabaseConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(cfg.conn, BackendConfig::Sqlite(_)));
        assert_eq!(cfg.url().as_str(), "file:///tmp/test.db");
        assert_eq!(cfg.pool.min_connections, 2);
        assert_eq!(cfg.pool.max_connections, 20);
        // defaults kick in for unset fields
        assert_eq!(cfg.pool.acquire_timeout, Duration::from_secs(30));
        assert!(cfg.pool.test_before_acquire);
    }

    #[cfg(all(postgres, not(sqlite)))]
    #[test]
    fn database_config_conn_inferred() {
        let yaml = "
url: postgres://localhost/mydb
schema: td_apiserver
username: user
password: pass
min_connections: 2
max_connections: 20
";
        let cfg: DatabaseConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(cfg.conn, BackendConfig::Postgres(_)));
        assert_eq!(cfg.username.as_deref(), Some("user"));
        assert_eq!(cfg.password.as_deref(), Some("pass"));
        assert_eq!(cfg.pool.min_connections, 2);
        assert_eq!(cfg.pool.max_connections, 20);
        assert_eq!(cfg.pool.acquire_timeout, Duration::from_secs(30));
        assert!(cfg.pool.test_before_acquire);
    }

    /// The flattened [`PoolConfig`] parses alongside the backend; unset pool
    /// fields fall back to their defaults. A backend is mandatory, so the body
    /// carries one (a postgres URL here).
    #[cfg(postgres)]
    #[test]
    fn database_config_pool_defaults_and_log_level() {
        let parse = |extra: &str| -> DatabaseConfig {
            serde_yaml::from_str(&format!("url: postgres://localhost/db\n{extra}")).unwrap()
        };

        // Unset pool fields default.
        let cfg = parse("");
        assert_eq!(cfg.pool.min_connections, 1);
        assert_eq!(cfg.pool.max_connections, 10);

        assert_eq!(parse("log_level: off").pool.log_level, LevelFilter::Off);
        assert_eq!(
            parse(r#"log_level: "off""#).pool.log_level,
            LevelFilter::Off
        );
        assert_eq!(parse("log_level: trace").pool.log_level, LevelFilter::Trace);
        assert_eq!(parse("log_level: dEbuG").pool.log_level, LevelFilter::Debug);
    }

    #[test]
    fn database_config_round_trip() {
        #[cfg(sqlite)]
        let (url, username, password) = (Url::parse("file:///tmp/rt.db").unwrap(), None, None);
        #[cfg(all(postgres, not(sqlite)))]
        let (url, username, password) = (
            Url::parse("postgres://localhost/rt").unwrap(),
            Some("user".to_owned()),
            Some("pass".to_owned()),
        );

        let original = DatabaseConfig {
            conn: BackendConfig::from_url(url).unwrap(),
            schema: Some(Schema::new(schema::TEST_SCHEMA)),
            username,
            password,
            app_role: None,
            pool: PoolConfig {
                min_connections: 3,
                max_connections: 15,
                ..Default::default()
            },
        };
        let yaml = serde_yaml::to_string(&original).unwrap();
        let restored: DatabaseConfig = serde_yaml::from_str(&yaml).unwrap();

        assert_eq!(original.url().as_str(), restored.url().as_str());
        assert_eq!(restored.schema, original.schema);
        assert_eq!(restored.username, original.username);
        assert_eq!(restored.password, original.password);
        assert_eq!(restored.pool.min_connections, 3);
        assert_eq!(restored.pool.max_connections, 15);
    }
}
