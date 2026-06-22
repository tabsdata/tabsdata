//
// Copyright 2025 Tabs Data Inc.
//

#[cfg(not(any(feature = "sqlite", feature = "postgres", feature = "mysql")))]
compile_error!("You must enable at least one database feature: `sqlite`, `postgres` or `mysql`.");

use crate::config::DatabaseConfig;
use crate::error::DbError;
use crate::pool::TdPool;

/// Non-superuser, `NOBYPASSRLS` PostgreSQL role the serving pool switches to in
/// `after_connect` (`SET ROLE`), so row-level security is enforced even when the
/// pool authenticated as a superuser (e.g. embedded). On embedded it is
/// auto-provisioned at startup; external deployments pre-create it and grant
/// the connecting role membership.
pub const APP_ROLE: &str = "td_app";

// The PostgreSQL schema the application's objects live in is the single source
// of truth for connection setup: every connection's `search_path` is pinned to
// it, `APP_ROLE` is granted access to it, the upgrader's privileges are checked
// against it, and schema introspection scopes to it. It is sourced from
// `DatabaseConfig::schema` (mandatory on the PostgreSQL path — no `public`
// default, so each component's objects stay isolated in their own schema) and
// carried on the live pool via `TdPool::schema`.

pub mod config;
pub mod dynamic;
#[cfg(feature = "embedded-postgres")]
pub mod embedded;
pub mod error;
pub mod pool;
pub mod sea_query;
pub mod system;

#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

/// Creates a connection pool for the `tabsdata` database.
pub async fn db(config: &DatabaseConfig) -> Result<TdPool, DbError> {
    TdPool::connect(config).await
}
