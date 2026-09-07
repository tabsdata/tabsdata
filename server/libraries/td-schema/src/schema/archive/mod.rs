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
//! Tabsdata 2.0 reset the schema ladder: `v1/` is the frozen 2.x baseline.
//! Follow the workflow below for each further bump.
//!
//! ## Creating an archive
//!
//! When bumping to version N+1:
//!
//! 1. Copy the entire `current/` directory to `archive/vN/`, including its
//!    `snapshots/` dir — the frozen copy keeps its committed DDL snapshots.
//!    (`current/` already uses `super::` imports, so the copy works as-is.)
//! 2. In `vN/mod.rs`: hardcode `pub const VERSION: usize = N;`, remove the
//!    delegating `pub fn build()`, and drop the copied `Source`'s `delta`
//!    override (plus the now-unused imports) — an archive is only ever
//!    installed as a starting point, so the default empty `delta` is correct.
//! 3. Add `pub(crate) mod vN;` to this file.
//! 4. Append `&vN::Source` to [`ARCHIVES`] — its length is tied to
//!    `current::VERSION`, so the bump does not compile until you do.
//!
//! `test_archive_ddl_snapshots` then guards the frozen copy automatically.
//! See the project README for a full guide and example.

use crate::schema::current;
use crate::schema::source::{SchemaSource, install_script};

pub(crate) mod sql;
pub(crate) mod v1;
pub(crate) mod v2;
pub(crate) mod v3;
pub(crate) mod v4;
pub(crate) mod v5;
pub(crate) mod v6;

const ARCHIVED: usize = current::VERSION - 1;

/// Every version below the current one, in ascending order. The length is tied
/// to `current::VERSION`, so bumping the version without archiving the one it
/// replaces does not compile.
pub(crate) const ARCHIVES: [&'static dyn SchemaSource; ARCHIVED] =
    [&v1::Source, &v2::Source, &v3::Source, &v4::Source, &v5::Source, &v6::Source];

/// The [`SchemaSource`] for an archived version.
///
/// # Panics
///
/// Panics outside `1..current::VERSION`, which has no archive by construction.
pub fn source(version: usize) -> &'static dyn SchemaSource {
    ARCHIVES[version - 1]
}

/// The full install script for an archived version. Panics if no archive
/// exists for it.
pub fn build(version: usize) -> Vec<String> {
    install_script(source(version))
}
