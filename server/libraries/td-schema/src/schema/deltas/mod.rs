//
// Copyright 2026 Tabsdata Inc.
//

#![allow(dead_code)]

//! Incremental schema deltas for upgrading between versions.
//!
//! Each delta module (e.g., `v1_to_v2`) implements the [`SchemaDelta`] trait
//! on a `Migration` marker type:
//!
//! ```ignore
//! pub struct Migration;
//!
//! impl SchemaDelta for Migration {
//!     const FROM_VERSION: usize = N;
//!     const TO_VERSION: usize = N + 1;
//!     fn retired() -> Vec<String> { /* ... */ }
//!     fn up() -> Vec<String> { /* ... */ }
//! }
//! ```
//!
//! Tabsdata 2.0 reset the ladder: `current/` was the v1 baseline of the 2.x
//! line. Each further bump of `current::VERSION` adds one delta here — see
//! the workflow below.
//!
//! ## Adding a new delta
//!
//! 1. Create `vN_to_v{N+1}.rs` with `pub struct Migration;` and
//!    `impl SchemaDelta`.
//! 2. Add `pub(crate) mod vN_to_v{N+1};` to this file.
//! 3. Add a match arm to `get()`:
//!    `{N+1} => rung::<vN_to_v{N+1}::Migration>(),`.
//! 4. Freeze its DDL snapshots under `snapshots/v{N}_to_v{N+1}/` by running
//!    `test_delta_ddl_snapshots` with `TD_SNAPSHOT_UPDATE=1`.
//!
//! The last statement in every delta **must** update `db_version` using the
//! helper: `update_db_version(Self::TO_VERSION)`.
//!
//! The whole ladder runs as one transaction, so a delta must emit only
//! transaction-safe SQL — no `CREATE INDEX CONCURRENTLY`, no `VACUUM`.
//!
//! See the project README for a full guide and example.

use crate::schema::source::Delta;
use crate::schema::version::SchemaDelta;

pub(crate) mod v10_to_v11;
pub(crate) mod v11_to_v12;
pub(crate) mod v12_to_v13;
pub(crate) mod v1_to_v2;
pub(crate) mod v2_to_v3;
pub(crate) mod v3_to_v4;
pub(crate) mod v4_to_v5;
pub(crate) mod v5_to_v6;
pub(crate) mod v6_to_v7;
pub(crate) mod v7_to_v8;
pub(crate) mod v8_to_v9;
pub(crate) mod v9_to_v10;

/// The v10 derivation of `metrics_facts` from the base rows. Exposed so the
/// DAO gate can hold the live fact writers to the same rows the rung derives.
#[cfg(feature = "td-test")]
pub fn metrics_facts_backfill() -> String {
    v9_to_v10::backfill_statement()
}

#[cfg(feature = "td-test")]
pub fn status_rollup_backfill() -> Vec<String> {
    v12_to_v13::status_rollup_backfill()
}

/// The rung upgrading from version `target_version - 1` to `target_version`.
///
/// # Panics
///
/// Panics if no delta exists for the given target version.
pub fn get(target_version: usize) -> Delta {
    fn rung<D: SchemaDelta>() -> Delta {
        Delta {
            up: D::up(),
            retired: D::retired(),
        }
    }
    match target_version {
        2 => rung::<v1_to_v2::Migration>(),
        3 => rung::<v2_to_v3::Migration>(),
        4 => rung::<v3_to_v4::Migration>(),
        5 => rung::<v4_to_v5::Migration>(),
        6 => rung::<v5_to_v6::Migration>(),
        7 => rung::<v6_to_v7::Migration>(),
        8 => rung::<v7_to_v8::Migration>(),
        9 => rung::<v8_to_v9::Migration>(),
        10 => rung::<v9_to_v10::Migration>(),
        11 => rung::<v10_to_v11::Migration>(),
        12 => rung::<v11_to_v12::Migration>(),
        13 => rung::<v12_to_v13::Migration>(),
        _ => panic!("No delta migration for version {target_version}"),
    }
}
