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
    /// The RLS roles a connection switches between. `None` on the privileged
    /// system entry, which never serves.
    pub roles: Option<RlsRoles>,
    /// Connection pool settings.
    pub pool: PoolsConfig,
}

/// The resolved names of a connection's two RLS roles. Provisioning and
/// serving must resolve the same pair or `SET ROLE` targets a role that was
/// never created.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RlsRoles {
    /// Confined to one project by `project_isolation`; the session role of
    /// every serving connection.
    pub in_project: String,
    /// Holds membership in
    /// [`ACROSS_PROJECTS_ROLE`](td_schema::ACROSS_PROJECTS_ROLE), which the
    /// `across_projects` policy admits. The tenant role must never hold it.
    pub across_projects: String,
}

impl DatabaseConfig {
    /// The configured RLS roles, or [`DbError::MissingRlsRoleName`] when
    /// unset (the privileged system entry).
    pub(crate) fn require_roles(&self) -> Result<&RlsRoles, DbError> {
        self.roles.as_ref().ok_or(DbError::MissingRlsRoleName)
    }

    /// The configured schema, or [`DbError::MissingSchema`] when unset.
    pub(crate) fn require_schema(&self) -> Result<&Schema, DbError> {
        self.schema.as_ref().ok_or(DbError::MissingSchema)
    }
}
