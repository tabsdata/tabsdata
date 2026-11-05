//
// Copyright 2026 Tabsdata Inc.
//

//! The serving pool and the generic connection machinery it is built on:
//!
//! * [`serving`] — [`TdPool`], the serving application pool a request runs on;
//!   [`ComponentPools`] pairs it with a component's system pool, by [`PoolKind`].
//! * [`options`] — mapping the config onto sqlx's connect options.
//! * [`connect`] — turning a [`DatabaseConfig`](crate::config::DatabaseConfig)
//!   into live pools (the serving pool, the privileged admin connection, the
//!   app-role provisioning).
//! * [`state`] — reading a schema's recorded version/edition state.
//! * [`scoped`] — project scoping ([`Scoped`](scoped::Scoped)).

mod component_pools;
pub mod connect;
mod kind;
pub mod options;
pub mod scoped;
mod serving;
pub mod state;

pub use component_pools::ComponentPools;
pub use kind::{PoolKind, RegularPool, ServingPool, SystemPool};
pub use serving::TdPool;
