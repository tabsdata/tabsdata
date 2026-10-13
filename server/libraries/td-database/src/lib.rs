//
// Copyright 2025 Tabsdata Inc.
//

//! Tabsdata database layer:
//!
//! * [`config`] — what a deployment writes: the `database:` components map
//!   and the `database_server:` entry.
//! * [`server`] — runs the managed PostgreSQL server behind `database_server:`.
//! * [`components`] — provisions, upgrades and checks the registry schema
//!   components under `database:`.
//! * [`pool`] / [`domain`] / [`connection`] — the serving runtime.
//! * [`access`] — what service bodies hold: scoped, grant-gated connections.

use crate::config::DatabaseConfig;
use crate::error::DbError;
use crate::pool::TdPool;

pub mod access;
pub mod components;
pub mod config;
pub mod connection;
pub mod domain;
pub mod error;
mod generated_sql;
pub mod pool;
pub mod sea_query;
pub mod server;

#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

/// Creates a connection pool for the `tabsdata` database.
pub async fn db(config: &DatabaseConfig) -> Result<TdPool, DbError> {
    TdPool::connect(config).await
}
