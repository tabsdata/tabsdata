//
// Copyright 2025 Tabs Data Inc.
//

//! Tabsdata database layer:
//!
//! * [`config`] — what a deployment writes: the `database:` components map
//!   and the `database_server:` entry.
//! * [`server`] — runs the managed PostgreSQL server behind `database_server:`.
//! * [`components`] — provisions, upgrades and checks the registry schema
//!   components under `database:`.
//! * [`pool`] / [`domain`] — the serving runtime.

#[cfg(not(any(feature = "sqlite", feature = "postgres", feature = "mysql")))]
compile_error!("You must enable at least one database feature: `sqlite`, `postgres` or `mysql`.");

use crate::config::DatabaseConfig;
use crate::error::DbError;
use crate::pool::TdPool;

/// Non-superuser, `NOBYPASSRLS` PostgreSQL role the serving pool switches to in
/// `after_connect` (`SET ROLE`), so row-level security is enforced even when the
/// pool authenticated as a superuser.
pub const APP_ROLE: &str = "td_app";

pub mod components;
pub mod config;
pub mod domain;
pub mod dynamic;
pub mod error;
pub mod pool;
pub mod sea_query;
pub mod server;

#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

/// Creates a connection pool for the `tabsdata` database.
pub async fn db(config: &DatabaseConfig) -> Result<TdPool, DbError> {
    TdPool::connect(config).await
}
