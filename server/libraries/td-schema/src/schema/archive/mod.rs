//
// Copyright 2026 Tabsdata Inc.
//

#![allow(dead_code)]

//! Archived full schema snapshots for previous versions.
//!
//! Each archive (e.g., `v1/`) is a frozen copy of the `current/` directory as
//! it existed at that version. Archives are used together with deltas to test
//! the migration invariant: `archive(N) + delta(N→N+1) ≡ current(N+1)`.
//!
//! Tabsdata 2.0 resets the schema ladder: `current/` is the v1 baseline of the
//! 2.x line and there are no archives yet. The first upgrade
//! (`current::Schema::VERSION` bumped to 2) is the trigger to create
//! `archive/v1/` — see the workflow below.
//!
//! ## Creating an archive (on the first upgrade)
//!
//! When bumping to version N+1:
//!
//! 1. Copy the entire `current/` directory to `archive/vN/`, including its
//!    `snapshots/` dir — the frozen copy keeps its committed DDL snapshots.
//!    (`current/` already uses `super::` imports, so the copy works as-is.)
//! 2. In `vN/mod.rs`: hardcode `const VERSION: usize = N;` and remove the
//!    delegating `pub fn build()`.
//! 3. Add `pub(crate) mod vN;` to this file.
//! 4. Add a match arm to `build()`: `N => vN::Schema::build(backend),`.
//!
//! `test_archive_ddl_snapshots` then guards the frozen copy automatically.
//! See the project README for a full guide and example.

use crate::schema::backend::Backend;

/// Build the full schema for an archived version.
///
/// # Panics
///
/// Panics if no archive exists for the given version.
#[allow(clippy::match_single_binding)]
pub fn build(version: usize, backend: impl Into<Backend>) -> Vec<String> {
    let _backend = backend.into();
    match version {
        _ => panic!("No archived schema for version {version}"),
    }
}
