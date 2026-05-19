//
// Copyright 2026 Tabs Data Inc.
//

//! Per-backend test pool factories. The canonical home for "give me a fresh
//! `TdPool` for one test" — used by td-test's `DbBackendProvider` impls and,
//! directly, by td-database's own internal tests (which can't go through the
//! `#[td_test::test(db)]` macro because td-test depends on td-database).
//!
//! Each backend lives in its own submodule with a matching shape:
//!
//! * `sqlite::pool() -> TdPool` (cfg(sqlite))
//! * `postgres::pool() -> TdPool` (cfg(postgres))
//!
//! Returned pools are freshly connected but **un-upgraded**. Callers that
//! need the schema applied call `.upgrade().await` themselves —
//! `DbTestSetup` does this conditionally (depending on whether a fixture
//! provides its own schema), and td-database's tests do it inline.

#[cfg(postgres)]
pub mod postgres;
#[cfg(sqlite)]
pub mod sqlite;
