//
// Copyright 2026 Tabs Data Inc.
//

//! Incremental schema deltas for upgrading between versions.
//!
//! Each delta module (e.g., `v5_to_v6`) implements the [`SchemaDelta`] trait
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
//! ## Adding a new delta
//!
//! 1. Create `vN_to_v{N+1}.rs` with `pub struct Migration;` and `impl SchemaDelta`.
//! 2. Add `pub(crate) mod vN_to_v{N+1};` to this file.
//! 3. Add a match arm: `{N+1} => vN_to_v{N+1}::Migration::up(backend),`.
//!
//! The last statement in every delta **must** update `db_version` using
//! the helper: `b.update_db_version(Self::TO_VERSION)`.
//!
//! See the project README for a full guide and example.

pub(crate) mod v5_to_v6;
pub(crate) mod v6_to_v7;
pub(crate) mod v7_to_v8;

use crate::schema::SchemaDelta;
use crate::schema::backend::Backend;

/// Returns the SQL statements to upgrade from version `target_version - 1` to
/// `target_version` for the given backend.
///
/// # Panics
///
/// Panics if no delta exists for the given target version.
pub fn get(target_version: usize, backend: impl Into<Backend>) -> Vec<String> {
    let backend = backend.into();
    match target_version {
        6 => v5_to_v6::Migration::up(backend),
        7 => v6_to_v7::Migration::up(backend),
        8 => v7_to_v8::Migration::up(backend),
        _ => panic!("No delta migration for version {target_version}"),
    }
}

/// Return the stored fingerprints for a delta targeting the given version.
///
/// # Panics
///
/// Panics if no delta exists for the given target version.
pub fn fingerprints(target_version: usize) -> [&'static str; 3] {
    match target_version {
        6 => v5_to_v6::Migration::FINGERPRINTS,
        7 => v6_to_v7::Migration::FINGERPRINTS,
        8 => v7_to_v8::Migration::FINGERPRINTS,
        _ => panic!("No delta migration for version {target_version}"),
    }
}
