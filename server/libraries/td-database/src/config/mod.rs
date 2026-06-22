//
// Copyright 2026 Tabs Data Inc.
//

//! Database connection configuration.
//!
//! * [`BackendConfig`] picks one of the supported backends (sqlite, postgres,
//!   embedded postgres, mysql); each has its own per-backend config struct in a
//!   submodule ([`sqlite`], [`postgres`], [`embedded`], [`mysql`]).
//! * [`PoolConfig`] holds the sqlx pool settings.
//! * [`DatabaseConfig`] is one connectable target (backend + schema +
//!   credentials + pool); [`crate::pool::connect`] turns it into live pools.
//! * [`DatabaseDescriptor`] is the whole `database:` section: the mode, the
//!   database-wide backend and admin credentials, and an optional
//!   [`ComponentOverride`] per component. Which components exist (and the schema
//!   each owns) comes from the linked component registry, not the config — so
//!   the descriptor never enumerates schemas; it only carries the connection and
//!   any per-component deployment overrides (e.g. remote owner passwords).
//!
//! These are the declarative types only. The act of connecting (opening pools,
//! booting embedded postgres) is behavior and lives in [`crate::pool::connect`].

// Shared definitions.
pub mod mode;
pub mod schema;
pub mod system_config;

// Per-backend URL / boot configs.
#[cfg(all(postgres, feature = "embedded-postgres"))]
pub mod embedded;
#[cfg(mysql)]
pub mod mysql;
#[cfg(postgres)]
pub mod postgres;
#[cfg(sqlite)]
pub mod sqlite;

// Configuration types.
mod backend;
mod connection;
mod descriptor;
mod pool;

pub use mode::DatabaseMode;
pub use schema::Schema;
pub use system_config::SystemConfig;
#[cfg(all(postgres, feature = "embedded-postgres"))]
pub use embedded::EmbeddedConfig;
#[cfg(mysql)]
pub use mysql::{MySqlConfig, MySqlUrl};
#[cfg(postgres)]
pub use postgres::{PostgresConfig, PostgresUrl};
#[cfg(sqlite)]
pub use sqlite::{SqliteConfig, SqliteUrl};

pub use backend::{BackendConfig, ConnectableBackend};
pub use connection::DatabaseConfig;
pub use descriptor::{ComponentOverride, DatabaseDescriptor, DatabaseResolver};
pub use pool::PoolConfig;
