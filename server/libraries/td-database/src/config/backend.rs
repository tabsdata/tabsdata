//
// Copyright 2026 Tabs Data Inc.
//

//! [`BackendConfig`] — which database backend a connection targets, plus the
//! per-backend URL/boot config. The variants are the per-backend structs in the
//! sibling modules (sqlite, postgres, embedded, mysql).

#[cfg(all(postgres, feature = "embedded-postgres"))]
use super::embedded::EmbeddedConfig;
#[cfg(mysql)]
use super::mysql::{MySqlConfig, MySqlUrl};
#[cfg(postgres)]
use super::postgres::{PostgresConfig, PostgresUrl};
#[cfg(sqlite)]
use super::sqlite::{SqliteConfig, SqliteUrl};
use crate::error::DbError;
use serde::{Deserialize, Serialize};
use url::Url;

/// Database connection configuration. Each variant pairs a URL (or, for
/// embedded postgres, the boot knobs) with the backend-specific config.
///
/// The backend is determined by which config key is present (`sqlite`,
/// `postgres`, or `mysql`). When none is present, the backend is inferred
/// from the URL scheme.
///
/// ```yaml
/// url: file:///tmp/app.db
/// sqlite:
///   journal_mode: wal
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum BackendConfig {
    #[cfg(sqlite)]
    Sqlite(SqliteConfig),
    #[cfg(postgres)]
    Postgres(PostgresConfig),
    /// In-process Postgres, booted out of band by `td_dbmanager start`
    /// ([`crate::embedded::start_detached`]) and resolved to its live URL before
    /// connecting (see [`Self::into_connectable`]). A *deployment* backend only —
    /// it never appears on a [`ConnectableBackend`]. Mutually exclusive with
    /// [`Self::Postgres`] — pick external (URL set) or embedded (boot knobs), not both.
    #[cfg(all(postgres, feature = "embedded-postgres"))]
    EmbeddedPostgres(EmbeddedConfig),
    #[cfg(mysql)]
    MySql(MySqlConfig),
}

impl BackendConfig {
    /// The configured URL. `None` for [`Self::EmbeddedPostgres`], since the
    /// URL isn't known until the server boots. Query the live URL via
    /// [`crate::pool::TdPool::url`] instead.
    pub fn url(&self) -> Option<&Url> {
        match self {
            #[cfg(sqlite)]
            Self::Sqlite(c) => Some(&c.url),
            #[cfg(postgres)]
            Self::Postgres(c) => Some(&c.url),
            #[cfg(all(postgres, feature = "embedded-postgres"))]
            Self::EmbeddedPostgres(_) => None,
            #[cfg(mysql)]
            Self::MySql(c) => Some(&c.url),
        }
    }

    /// Build the embedded-postgres variant from boot knobs.
    #[cfg(all(postgres, feature = "embedded-postgres"))]
    pub fn embedded_postgres(cfg: EmbeddedConfig) -> Self {
        Self::EmbeddedPostgres(cfg)
    }

    /// Infer the backend variant from the URL scheme. The URL carries the
    /// connection target only — credentials are explicit fields on
    /// [`DatabaseConfig`](super::DatabaseConfig), so a credentialed URL is
    /// rejected for the backends that take separate credentials (PostgreSQL,
    /// MySQL).
    pub fn from_url(url: Url) -> Result<Self, DbError> {
        #[cfg(sqlite)]
        if let Ok(sqlite_url) = SqliteUrl::try_from(url.clone()) {
            return Ok(Self::Sqlite(SqliteConfig { url: sqlite_url }));
        }
        #[cfg(postgres)]
        if let Ok(pg_url) = PostgresUrl::try_from(url.clone()) {
            return Ok(Self::Postgres(PostgresConfig { url: pg_url }));
        }
        #[cfg(mysql)]
        if let Ok(my_url) = MySqlUrl::try_from(url.clone()) {
            return Ok(Self::MySql(MySqlConfig { url: my_url }));
        }
        Err(DbError::UnsupportedDatabase(url.scheme().to_string()))
    }

    /// Lower a *deployment* backend to a *connectable* one. The url-based variants
    /// map across directly; [`Self::EmbeddedPostgres`] **resolves** to its live URL
    /// (read from the running server) — the same lowering as [`Self::serving_url`].
    /// Errors only if an embedded server is asked for before it is running.
    /// Credentials are not in the URL; they travel in the config's explicit fields.
    pub fn into_connectable(self) -> Result<ConnectableBackend, DbError> {
        match self {
            #[cfg(sqlite)]
            Self::Sqlite(c) => Ok(ConnectableBackend::Sqlite(c)),
            #[cfg(postgres)]
            Self::Postgres(c) => Ok(ConnectableBackend::Postgres(c)),
            #[cfg(all(postgres, feature = "embedded-postgres"))]
            Self::EmbeddedPostgres(cfg) => ConnectableBackend::from_url(crate::embedded::live_url(&cfg)?),
            #[cfg(mysql)]
            Self::MySql(c) => Ok(ConnectableBackend::MySql(c)),
        }
    }
}

/// A backend a [`DatabaseConfig`](super::DatabaseConfig) can actually **connect**
/// to — always reachable by URL. Distinct from [`BackendConfig`] (the *deployment*
/// type), which also models the not-yet-running embedded server: resolution lowers
/// `BackendConfig` to this (embedded → its live URL) before any connect, so the
/// connect path never has to boot a server.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ConnectableBackend {
    #[cfg(sqlite)]
    Sqlite(SqliteConfig),
    #[cfg(postgres)]
    Postgres(PostgresConfig),
    #[cfg(mysql)]
    MySql(MySqlConfig),
}

impl ConnectableBackend {
    /// The connection URL (always present — every connectable backend has one).
    pub fn url(&self) -> &Url {
        match self {
            #[cfg(sqlite)]
            Self::Sqlite(c) => &c.url,
            #[cfg(postgres)]
            Self::Postgres(c) => &c.url,
            #[cfg(mysql)]
            Self::MySql(c) => &c.url,
        }
    }

    /// Infer the connectable backend from the URL scheme — see
    /// [`BackendConfig::from_url`] for the credential rules.
    pub fn from_url(url: Url) -> Result<Self, DbError> {
        BackendConfig::from_url(url)?.into_connectable()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(sqlite)]
    #[test]
    fn conn_sqlite_explicit() {
        let yaml = "url: file:///tmp/test.db";
        let conn: BackendConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(conn, BackendConfig::Sqlite(_)));
        assert_eq!(conn.url().unwrap().as_str(), "file:///tmp/test.db");
    }

    #[cfg(sqlite)]
    #[test]
    fn conn_sqlite_round_trip() {
        let conn = BackendConfig::from_url(Url::parse("file:///tmp/test.db").unwrap()).unwrap();
        let yaml = serde_yaml::to_string(&conn).unwrap();
        let conn2: BackendConfig = serde_yaml::from_str(&yaml).unwrap();
        assert!(matches!(conn2, BackendConfig::Sqlite(_)));
        assert_eq!(conn.url(), conn2.url());
    }

    #[cfg(postgres)]
    #[test]
    fn conn_postgres_round_trip() {
        let conn =
            BackendConfig::from_url(Url::parse("postgres://user:pass@localhost/mydb").unwrap())
                .unwrap();
        let yaml = serde_yaml::to_string(&conn).unwrap();
        let conn2: BackendConfig = serde_yaml::from_str(&yaml).unwrap();
        assert!(matches!(conn2, BackendConfig::Postgres(_)));
        assert_eq!(conn.url(), conn2.url());
    }

    #[cfg(mysql)]
    #[test]
    fn conn_mysql_round_trip() {
        let conn = BackendConfig::from_url(Url::parse("mysql://user:pass@localhost/mydb").unwrap())
            .unwrap();
        let yaml = serde_yaml::to_string(&conn).unwrap();
        let conn2: BackendConfig = serde_yaml::from_str(&yaml).unwrap();
        assert!(matches!(conn2, BackendConfig::MySql(_)));
        assert_eq!(conn.url(), conn2.url());
    }

    #[cfg(sqlite)]
    #[test]
    fn conn_infer_sqlite_from_url() {
        let yaml = "url: file:///tmp/inferred.db";
        let conn: BackendConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(conn, BackendConfig::Sqlite(_)));
        assert_eq!(conn.url().unwrap().as_str(), "file:///tmp/inferred.db");
    }

    #[cfg(postgres)]
    #[test]
    fn conn_infer_postgres_from_url() {
        let yaml = "url: postgres://user:pass@localhost/mydb";
        let conn: BackendConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(conn, BackendConfig::Postgres(_)));
    }

    #[cfg(mysql)]
    #[test]
    fn conn_infer_mysql_from_url() {
        let yaml = "url: mysql://user:pass@localhost/mydb";
        let conn: BackendConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(conn, BackendConfig::MySql(_)));
    }

    #[test]
    fn conn_unknown_scheme_errors() {
        let yaml = "url: oracle://localhost/mydb";
        let result = serde_yaml::from_str::<BackendConfig>(yaml);
        assert!(result.is_err());
    }

    /// `sqlite: {}` has no `url`, so deserialization fails. Empty bodies are
    /// rejected by serde when no untagged variant accepts them.
    #[cfg(all(sqlite, not(postgres)))]
    #[test]
    fn conn_missing_url_errors() {
        let yaml = "sqlite: {}";
        let result = serde_yaml::from_str::<BackendConfig>(yaml);
        assert!(result.is_err());
    }
}
