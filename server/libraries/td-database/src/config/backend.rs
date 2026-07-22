//
// Copyright 2026 Tabsdata Inc.
//

//! [`BackendConfig`] — which database backend a connection targets, plus the
//! per-backend URL config. The variants are the per-backend structs in the
//! sibling modules (sqlite, postgres, mysql). Every backend is reachable by
//! URL; the embedded postgres server is a *deployment* concern (booted by
//! `td_dbmanager` from [`crate::server`]) and is addressed through its
//! localhost URL like any other server.

#[cfg(mysql)]
use super::mysql::{MySqlConfig, MySqlUrl};
#[cfg(postgres)]
use super::postgres::{PostgresConfig, PostgresUrl};
#[cfg(sqlite)]
use super::sqlite::{SqliteConfig, SqliteUrl};
use crate::error::DbError;
use serde::{Deserialize, Serialize};
use url::Url;

/// Database connection configuration. Each variant pairs a URL with the
/// backend-specific config.
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
    #[cfg(mysql)]
    MySql(MySqlConfig),
}

impl BackendConfig {
    /// The connection URL (always present — every backend has one).
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

    /// Whether this backend is PostgreSQL.
    pub fn is_postgres(&self) -> bool {
        match self {
            #[cfg(postgres)]
            Self::Postgres(_) => true,
            #[allow(unreachable_patterns)]
            _ => false,
        }
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
        assert_eq!(conn.url().as_str(), "file:///tmp/test.db");
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
        assert_eq!(conn.url().as_str(), "file:///tmp/inferred.db");
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

    /// A body with no `url` matches no variant, so deserialization fails —
    /// the config surface requires every backend to state its URL.
    #[test]
    fn conn_missing_url_errors() {
        let yaml = "persistent: true";
        let result = serde_yaml::from_str::<BackendConfig>(yaml);
        assert!(result.is_err());
    }
}
