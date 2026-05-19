//
// Copyright 2026 Tabs Data Inc.
//

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
//!     const FINGERPRINTS: [&'static str; 3] = ["", "", ""];
//!     fn up(b: Backend) -> Vec<String> { /* ... */ }
//! }
//! ```
//!
//! Tabsdata 2.0 resets the ladder: `current/` is the v1 baseline of the 2.x
//! line and there are no deltas yet. The first upgrade
//! (`current::Schema::VERSION` bumped to 2) creates `v1_to_v2.rs` here — see
//! the workflow below.
//!
//! ## Adding a new delta
//!
//! 1. Create `vN_to_v{N+1}.rs` with `pub struct Migration;` and
//!    `impl SchemaDelta` (include `FINGERPRINTS` — leave empty initially,
//!    fill after running tests).
//! 2. Add `pub(crate) mod vN_to_v{N+1};` to this file.
//! 3. Add a match arm to `get()`:
//!    `{N+1} => vN_to_v{N+1}::Migration::up(backend),`.
//! 4. Add a match arm to `fingerprints()`:
//!    `{N+1} => vN_to_v{N+1}::Migration::FINGERPRINTS,`.
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
/// Panics if no delta exists for the given target version. Until the first
/// 2.x upgrade lands, every call panics.
pub fn get(target_version: usize, _backend: impl Into<Backend>) -> Vec<String> {
    panic!("No delta migration for version {target_version}");
}

/// Return the stored fingerprints for a delta targeting the given version.
///
/// # Panics
///
/// Panics if no delta exists for the given target version. Until the first
/// 2.x upgrade lands, every call panics.
pub fn fingerprints(target_version: usize) -> [&'static str; 3] {
    panic!("No delta migration for version {target_version}");
}
