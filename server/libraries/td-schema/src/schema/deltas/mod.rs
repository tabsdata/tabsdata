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
//!     fn up(b: Backend) -> Vec<String> { /* ... */ }
//! }
//! ```
//!
//! Tabsdata 2.0 resets the ladder: `current/` is the v1 baseline of the 2.x
//! line and there are no deltas yet. The first upgrade
//! (`current::VERSION` bumped to 2) creates `v1_to_v2.rs` here — see
//! the workflow below.
//!
//! ## Adding a new delta
//!
//! 1. Create `vN_to_v{N+1}.rs` with `pub struct Migration;` and
//!    `impl SchemaDelta`.
//! 2. Add `pub(crate) mod vN_to_v{N+1};` to this file.
//! 3. Add a match arm to `get()`:
//!    `{N+1} => vN_to_v{N+1}::Migration::up(backend),`.
//! 4. Freeze its DDL snapshots under `snapshots/v{N}_to_v{N+1}/` by running
//!    `test_delta_ddl_snapshots` with `TD_SNAPSHOT_UPDATE=1`.
//!
//! The last statement in every delta **must** update `db_version` using the
//! helper: `b.update_db_version(Self::TO_VERSION)`.
//!
//! See the project README for a full guide and example.

use crate::schema::backend::Backend;

/// Returns the SQL statements to upgrade from version `target_version - 1` to
/// `target_version` for the given backend.
///
/// # Panics
///
/// Panics if no delta exists for the given target version.
#[allow(clippy::match_single_binding)]
pub fn get(target_version: usize, backend: impl Into<Backend>) -> Vec<String> {
    let _backend = backend.into();
    match target_version {
        _ => panic!("No delta migration for version {target_version}"),
    }
}
