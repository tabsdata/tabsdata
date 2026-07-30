//
// Copyright 2026 Tabsdata Inc.
//

//! Database connection configuration.
//!
//! * [`BackendConfig`] picks a backend by config key (`sqlite:`, `postgres:`,
//!   `mysql:`); the key's body is that backend's full sqlx connect options.
//! * [`PoolConfig`] holds the sqlx pool settings, under the `pool:` key.
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
pub mod schema;

// Per-backend connect options.
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
pub use mysql::{MySqlConfig, MySqlSslModeConfig};
#[cfg(postgres)]
pub use postgres::{PgSslModeConfig, PostgresConfig};
pub use schema::Schema;
#[cfg(sqlite)]
pub use sqlite::{
    PragmaConfig, SqliteAutoVacuumConfig, SqliteConfig, SqliteExtensionConfig,
    SqliteJournalModeConfig, SqliteLockingModeConfig, SqliteSynchronousConfig,
};

pub use backend::BackendConfig;
pub use component::{ComponentConfig, DatabaseComponents, EMBEDDED_CREDENTIAL};
#[cfg(mysql)]
pub use connection::MySqlConnect;
#[cfg(postgres)]
pub use connection::PostgresConnect;
pub use connection::{ConnectConfig, DatabaseConfig};
pub use logging::StatementLogConfig;
pub use pool::PoolConfig;
