//
// Copyright 2026 Tabsdata Inc.
//

//! The serving pool and the generic connection machinery it is built on:
//!
//! * [`serving`] — [`TdPool`], the serving application pool a request runs on.
//! * [`connect`] — turning a [`DatabaseConfig`](crate::config::DatabaseConfig)
//!   into live pools (the [`ConnectRole`](connect::ConnectRole) selector, the
//!   embedded-postgres boot, the app-role provisioning).
//! * [`state`] — reading a schema's recorded version/edition state.
//! * [`scoped`] — project scoping ([`Scoped`](scoped::Scoped)).

pub mod connect;
pub mod scoped;
mod serving;
pub(crate) mod state;

pub use serving::TdPool;
