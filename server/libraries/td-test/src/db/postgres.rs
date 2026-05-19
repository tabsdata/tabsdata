//
// Copyright 2026 Tabs Data Inc.
//

//! Postgres provider. Thin delegate over
//! [`td_database::test_utils::postgres::pool`]. The returned `TdPool` owns
//! the embedded server inside itself (parked under `Arc`); when the pool
//! drops at the end of the test, the server shuts down and releases its
//! shared-memory segment.

use crate::db::DbBackendProvider;
use std::future::Future;
use td_database::pool::TdPool;

/// Marker type for the postgres provider. The trait impl is gated on
/// `cfg(postgres)`; the type itself is always present so the macro can name
/// it inside a `#[cfg(postgres)]`-gated test arm without confusing path
/// resolution when postgres is disabled.
pub struct PostgresProvider;

#[cfg(postgres)]
impl DbBackendProvider for PostgresProvider {
    const NAME: &'static str = "postgres";
    fn provision() -> impl Future<Output = TdPool> + Send {
        td_database::test_utils::postgres::pool()
    }
}

// Keep the type usable in `cfg(not(postgres))` builds — emitted test arms
// reference `PostgresProvider` unconditionally and are stripped by their own
// `#[cfg(postgres)]` gate at the call site.
#[cfg(not(postgres))]
impl PostgresProvider {
    #[allow(dead_code)]
    pub fn provision() -> impl Future<Output = TdPool> + Send {
        async { panic!("postgres backend not enabled") }
    }
}
