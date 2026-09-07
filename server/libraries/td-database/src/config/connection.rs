//
// Copyright 2026 Tabsdata Inc.
//

//! [`DatabaseConfig`] — a single **connectable** database target: its resolved
//! [`PostgresConnect`], schema, and [`PoolsConfig`]. Produced by
//! component/server resolution and consumed by [`crate::pool::connect`]. Not
//! config surface — it never deserializes; the declarative shape is
//! [`ComponentConfig`](super::ComponentConfig) (see [`super::component`]).

use super::PostgresConfig;
use super::Schema;
use super::pool::PoolsConfig;
use crate::error::DbError;

/// PostgreSQL connect options with the resolved login identity. Unlike the
/// declarative [`PostgresConfig`], the credentials are mandatory — resolution
/// already ran.
#[derive(Debug, Clone)]
pub struct PostgresConnect {
    pub options: PostgresConfig,
    pub username: String,
    pub password: String,
}

/// Configuration for a connectable database pool.
#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    /// The resolved connect target.
    pub conn: PostgresConnect,
    /// Schema the connection's `search_path` is pinned to. Mandatory on the
    /// serving path (no `public` default, so each component stays isolated);
    /// [`connect_serving`](DatabaseConfig::connect_serving) fails with
    /// [`DbError::MissingSchema`] when unset. The privileged system entry runs
    /// without a home schema — it provisions across schemas with explicit,
    /// schema-qualified DDL.
    pub schema: Option<Schema>,
    /// The RLS app role serving connections `SET ROLE` to. `None` means the
    /// global default [`APP_ROLE`](crate::APP_ROLE) (`td_app`). Deployments
    /// sharing a PostgreSQL cluster set a per-instance role (the fargate
    /// templates derive one from the database name); tests set a per-test
    /// role so they can isolate role-level state (e.g. flipping `BYPASSRLS`)
    /// on the shared server.
    pub app_role: Option<String>,
    /// Connection pool settings.
    pub pool: PoolsConfig,
}

/// The single home of the app-role default: provisioning and serving must
/// resolve the same role or `SET ROLE` targets one that was never created.
pub fn app_role_or_default(app_role: Option<&str>) -> &str {
    app_role.unwrap_or(crate::APP_ROLE)
}

impl DatabaseConfig {
    /// The RLS app role serving connections `SET ROLE` to: the configured override,
    /// else the global default [`APP_ROLE`](crate::APP_ROLE).
    pub fn app_role(&self) -> &str {
        app_role_or_default(self.app_role.as_deref())
    }

    /// The configured schema, or [`DbError::MissingSchema`] when unset.
    pub(crate) fn require_schema(&self) -> Result<&Schema, DbError> {
        self.schema.as_ref().ok_or(DbError::MissingSchema)
    }
}
