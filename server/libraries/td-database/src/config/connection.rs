//
// Copyright 2026 Tabsdata Inc.
//

//! [`DatabaseConfig`] — a single **connectable** database target: its resolved
//! [`ConnectConfig`], schema, and [`PoolConfig`]. Produced by component/server
//! resolution and consumed by [`crate::pool::connect`]. Not config surface —
//! it never deserializes; the declarative shape is [`ComponentConfig`]
//! (see [`super::component`]).

#[cfg(mysql)]
use super::MySqlConfig;
#[cfg(postgres)]
use super::PostgresConfig;
use super::Schema;
#[cfg(sqlite)]
use super::SqliteConfig;
use super::pool::PoolConfig;
use crate::error::DbError;

/// A resolved, connectable backend target. Unlike the declarative
/// [`BackendConfig`](super::BackendConfig), the backends that authenticate
/// carry mandatory credentials — resolution already ran.
#[derive(Debug, Clone)]
pub enum ConnectConfig {
    #[cfg(sqlite)]
    Sqlite(SqliteConfig),
    #[cfg(postgres)]
    Postgres(PostgresConnect),
    #[cfg(mysql)]
    MySql(MySqlConnect),
}

/// PostgreSQL connect options with the resolved login identity.
#[cfg(postgres)]
#[derive(Debug, Clone)]
pub struct PostgresConnect {
    pub options: PostgresConfig,
    pub username: String,
    pub password: String,
}

/// MySQL connect options with the resolved login identity.
#[cfg(mysql)]
#[derive(Debug, Clone)]
pub struct MySqlConnect {
    pub options: MySqlConfig,
    pub username: String,
    pub password: String,
}

/// Configuration for a connectable database pool.
#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    /// The resolved backend target.
    pub conn: ConnectConfig,
    /// PostgreSQL schema the connection's `search_path` is pinned to. Mandatory on
    /// the PostgreSQL path (no `public` default, so each component stays isolated);
    /// [`Self::connect`] fails with [`DbError::MissingSchema`] when unset. Ignored
    /// by SQLite and MySQL, which have no `search_path`.
    pub schema: Option<Schema>,
    /// The RLS app role serving connections `SET ROLE` to. `None` means the global
    /// default [`APP_ROLE`](crate::APP_ROLE) (`td_app`) — what production always
    /// uses. Tests set a per-test role so they can isolate role-level state (e.g.
    /// flipping `BYPASSRLS`) on the shared server.
    pub app_role: Option<String>,
    /// Connection pool settings.
    pub pool: PoolConfig,
}

impl DatabaseConfig {
    /// The RLS app role serving connections `SET ROLE` to: the configured override,
    /// else the global default [`APP_ROLE`](crate::APP_ROLE).
    pub fn app_role(&self) -> &str {
        self.app_role.as_deref().unwrap_or(crate::APP_ROLE)
    }

    /// The configured PostgreSQL schema, or [`DbError::MissingSchema`] when
    /// unset. Only meaningful on the PostgreSQL path; SQLite/MySQL connect
    /// without consulting it.
    pub fn require_schema(&self) -> Result<&Schema, DbError> {
        self.schema.as_ref().ok_or(DbError::MissingSchema)
    }
}
