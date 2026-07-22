//
// Copyright 2026 Tabsdata Inc.
//

//! Database connection configuration.
//!
//! * [`BackendConfig`] picks a backend (sqlite, postgres, mysql) by URL scheme.
//! * [`PoolConfig`] holds the sqlx pool settings.
//! * [`ComponentConfig`] is one named connection; [`DatabaseComponents`] (the
//!   `database:` section) is a plain map of them, one per registry component.
//!   The `database_server:` section reuses the same shape (gated by
//!   [`crate::server`], its only reader).
//! * [`DatabaseConfig`] is one connectable target, consumed by
//!   [`crate::pool::connect`].
//!
//! Declarative types only: connecting lives in [`crate::pool::connect`],
//! running the managed server in [`crate::server`], and resolving components
//! against the registry in [`crate::components`].

// Shared definitions.
pub mod schema;

// Per-backend URL configs.
#[cfg(mysql)]
pub mod mysql;
#[cfg(postgres)]
pub mod postgres;
#[cfg(sqlite)]
pub mod sqlite;

// Configuration types.
mod backend;
mod component;
mod connection;
mod pool;

#[cfg(mysql)]
pub use mysql::{MySqlConfig, MySqlUrl};
#[cfg(postgres)]
pub use postgres::{PostgresConfig, PostgresUrl};
pub use schema::Schema;
#[cfg(sqlite)]
pub use sqlite::{SqliteConfig, SqliteUrl};

pub use backend::BackendConfig;
pub use component::{ComponentConfig, DatabaseComponents, EMBEDDED_CREDENTIAL};
pub use connection::DatabaseConfig;
pub use pool::PoolConfig;
