//
// Copyright 2026 Tabsdata Inc.
//

//! [`BackendConfig`] — which database backend a connection targets, plus the
//! per-backend connect options. The variants are the per-backend structs in
//! the sibling modules (sqlite, postgres, mysql); the key selects the backend.
//! The embedded postgres server is a *deployment* concern (booted by
//! `td_dbmanager` from [`crate::server`]) and is addressed through its
//! localhost host/port like any other server.

#[cfg(mysql)]
use super::mysql::MySqlConfig;
#[cfg(postgres)]
use super::postgres::PostgresConfig;
#[cfg(sqlite)]
use super::sqlite::SqliteConfig;
use serde::Serialize;

/// Database connection configuration. The backend is selected by which config
/// key is present, and the key's body is that backend's full connect options.
///
/// ```yaml
/// postgres:
///   host: 127.0.0.1
///   port: 5432
///   database: postgres
///   username: td_apiserver
///   password: ${embedded}
/// ```
///
/// Deserialization happens through
/// [`ComponentConfig`](super::ComponentConfig)'s shape (which enforces exactly
/// one backend key); this enum only serializes, flattened, back into that
/// single-key map.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum BackendConfig {
    #[cfg(sqlite)]
    Sqlite(SqliteConfig),
    #[cfg(postgres)]
    Postgres(PostgresConfig),
    #[cfg(mysql)]
    MySql(MySqlConfig),
}

impl BackendConfig {
    /// The postgres options when this backend is PostgreSQL.
    #[cfg(postgres)]
    pub fn postgres(&self) -> Option<&PostgresConfig> {
        match self {
            Self::Postgres(c) => Some(c),
            #[allow(unreachable_patterns)]
            _ => None,
        }
    }

    /// The postgres options when this backend is PostgreSQL, for modification.
    #[cfg(postgres)]
    pub fn postgres_mut(&mut self) -> Option<&mut PostgresConfig> {
        match self {
            Self::Postgres(c) => Some(c),
            #[allow(unreachable_patterns)]
            _ => None,
        }
    }

    /// The configured database name, on the backends that have one.
    pub fn database(&self) -> Option<&str> {
        match self {
            #[cfg(sqlite)]
            Self::Sqlite(_) => None,
            #[cfg(postgres)]
            Self::Postgres(c) => c.database.as_deref(),
            #[cfg(mysql)]
            Self::MySql(c) => c.database.as_deref(),
        }
    }
}
