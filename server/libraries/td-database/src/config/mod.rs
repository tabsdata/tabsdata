//
// Copyright 2026 Tabsdata Inc.
//

//! Database connection configuration.
//!
//! * [`PostgresConfig`] is the connect-options surface, held under each
//!   entry's `postgres:` key.
//! * [`PoolsConfig`] holds the per-mode sqlx pool settings, under the `pool:` key.
//! * [`ComponentConfig`] is one named connection; [`DatabaseComponents`] (the
//!   `database:` section) is a plain map of them, one per registry component.
//!   The `database_server:` section reuses the same shape (gated by
//!   [`crate::server`], its only reader).
//! * [`DatabaseConfig`] is one resolved connectable target, consumed by
//!   [`crate::pool::connect`].
//!
//! Declarative types only: connecting lives in [`crate::pool::connect`],
//! running the managed server in [`crate::server`], and resolving components
//! against the registry in [`crate::components`].

// Shared definitions.
mod logging;
pub(crate) mod schema;

// Connect options.
pub(crate) mod postgres;

// Configuration types.
mod component;
mod connection;
mod pool;

pub use postgres::{PgSslModeConfig, PostgresConfig};
pub use schema::Schema;

pub use component::{ComponentConfig, DatabaseComponents, EMBEDDED_CREDENTIAL};
pub(crate) use connection::DatabaseConfig;
pub use connection::PostgresConnect;
pub(crate) use logging::StatementLogConfig;
pub use pool::{PoolConfig, PoolsConfig};
