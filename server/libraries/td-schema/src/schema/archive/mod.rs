//
// Copyright 2026 Tabs Data Inc.
//

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
//! 1. Copy the entire `current/` directory to `archive/vN/`.
//!    (`current/` already uses `super::` imports, so the copy works as-is.)
//! 2. In `vN/mod.rs`: hardcode `const VERSION: usize = N;`, keep `FINGERPRINTS`
//!    with the real hash values, and remove the delegating `pub fn build()`.
//! 3. Add `pub(crate) mod vN;` to this file.
//! 4. Add a match arm to `build()`: `N => vN::Schema::build(backend),`.
//! 5. Add a match arm to `fingerprints()`: `N => vN::Schema::FINGERPRINTS,`.
//! 6. Set `MIN_ARCHIVE` to N (or keep the existing value if earlier archives
//!    exist).
//!
//! See the project README for a full guide and example.

use crate::schema::backend::Backend;

/// Build the full schema for an archived version.
///
/// # Panics
///
/// Panics if no archive exists for the given version. Until the first 2.x
/// upgrade lands, every call panics.
pub fn build(version: usize, _backend: impl Into<Backend>) -> Vec<String> {
    panic!("No archived schema for version {version}");
}

/// Return the stored fingerprints for an archived version.
///
/// # Panics
///
/// Panics if no archive exists for the given version. Until the first 2.x
/// upgrade lands, every call panics.
pub fn fingerprints(version: usize) -> [&'static str; 3] {
    panic!("No archived schema for version {version}");
}

/// The minimum archived version. Archives exist for versions
/// `MIN_ARCHIVE..CURRENT_VERSION` (exclusive of current since current is live).
///
/// Set to `CURRENT_VERSION + 1` (effectively empty range) until the first
/// archive is created.
pub const MIN_ARCHIVE: usize = 2;
