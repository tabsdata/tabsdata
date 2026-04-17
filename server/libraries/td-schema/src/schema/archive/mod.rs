//
// Copyright 2026 Tabs Data Inc.
//

//! Archived full schema snapshots for previous versions.
//!
//! Each archive (e.g., `v5/`) is a frozen copy of the `current/` directory as it
//! existed at that version. Archives are used together with deltas to test the
//! migration invariant: `archive(N) + delta(N→N+1) ≡ current(N+1)`.
//!
//! ## Creating an archive
//!
//! When creating version N+1:
//!
//! 1. Copy the entire `current/` directory to `archive/vN/`.
//!    (`current/` already uses `super::` imports, so the copy works as-is.)
//! 2. In `vN/mod.rs`: hardcode `const VERSION: usize = N;`, keep `FINGERPRINTS`
//!    with the real hash values, and remove the delegating `pub fn build()`.
//! 3. Add `pub(crate) mod vN;` to this file.
//! 4. Add a match arm to `build()`: `N => vN::Schema::build(backend),`.
//! 5. Add a match arm to `fingerprints()`: `N => vN::Schema::FINGERPRINTS,`.
//! 6. Set `MIN_ARCHIVE` to N (or keep the existing value if earlier archives exist).
//!
//! See the project README for a full guide and example.

pub mod historic;
pub(crate) mod v5;
pub(crate) mod v6;
pub(crate) mod v7;
pub(crate) mod v8;

use crate::schema::SchemaVersion;
use crate::schema::backend::Backend;

/// Build the full schema for an archived version.
///
/// # Panics
///
/// Panics if no archive exists for the given version.
pub fn build(version: usize, backend: impl Into<Backend>) -> Vec<String> {
    let backend = backend.into();
    match version {
        5 => v5::Schema::build(backend),
        6 => v6::Schema::build(backend),
        7 => v7::Schema::build(backend),
        8 => v8::Schema::build(backend),
        _ => panic!("No archived schema for version {version}"),
    }
}

/// Return the stored fingerprints for an archived version.
///
/// # Panics
///
/// Panics if no archive exists for the given version.
pub fn fingerprints(version: usize) -> [&'static str; 3] {
    match version {
        5 => v5::Schema::FINGERPRINTS,
        6 => v6::Schema::FINGERPRINTS,
        7 => v7::Schema::FINGERPRINTS,
        8 => v8::Schema::FINGERPRINTS,
        _ => panic!("No archived schema for version {version}"),
    }
}

/// The minimum archived version. Archives exist for versions
/// `MIN_ARCHIVE..CURRENT_VERSION` (exclusive of current since current is live).
pub const MIN_ARCHIVE: usize = 5;
