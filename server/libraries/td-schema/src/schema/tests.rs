//
// Copyright 2026 Tabs Data Inc.
//

//! Generation-only tests for the schema system.
//!
//! These tests validate SQL generation and migration guards without touching a
//! database. The tests that execute SQL against a real database live in
//! `td-database` where `TdPool` / `DynamicPool` are available.

use super::archive;
use super::backend::Backend;
use super::current;
use super::deltas;
use super::{SchemaDelta, SchemaVersion};

const ALL_BACKENDS: &[Backend] = &[
    #[cfg(feature = "sqlite")]
    Backend::Sqlite,
    #[cfg(feature = "postgres")]
    Backend::Postgres,
    #[cfg(feature = "mysql")]
    Backend::MySql,
];

// ---------------------------------------------------------------------------
// Generic trait-based validation helpers
// ---------------------------------------------------------------------------

/// Validate that a [`SchemaVersion`] implementation produces consistent,
/// non-empty output across all backends.
pub(crate) fn validate_schema_version<S: SchemaVersion>() {
    let mut counts: Vec<(Backend, usize)> = Vec::new();
    for &b in ALL_BACKENDS {
        let stmts = S::build(b);
        assert!(
            !stmts.is_empty(),
            "SchemaVersion v{} produced empty output on {b:?}",
            S::VERSION
        );
        counts.push((b, stmts.len()));
    }
    let first = counts[0].1;
    for (b, count) in &counts[1..] {
        assert_eq!(
            first,
            *count,
            "SchemaVersion v{}: {:?} produced {first} stmts but {b:?} produced {count}",
            S::VERSION,
            counts[0].0
        );
    }
}

/// Validate that a [`SchemaDelta`] implementation:
/// - Is a single-step migration (`FROM_VERSION + 1 == TO_VERSION`)
/// - Produces non-empty output across all backends
/// - Has a last statement that contains the target version (version bump guard)
/// - Produces consistent statement counts across backends
pub(crate) fn validate_delta<D: SchemaDelta>() {
    assert_eq!(
        D::FROM_VERSION + 1,
        D::TO_VERSION,
        "Delta must be single-step: expected {}->{}+1, got {}->{}>",
        D::FROM_VERSION,
        D::FROM_VERSION,
        D::FROM_VERSION,
        D::TO_VERSION,
    );

    let mut counts: Vec<(Backend, usize)> = Vec::new();
    for &b in ALL_BACKENDS {
        let stmts = D::up(b);
        assert!(
            !stmts.is_empty(),
            "SchemaDelta v{}->v{} produced empty output on {b:?}",
            D::FROM_VERSION,
            D::TO_VERSION,
        );
        // Verify last statement bumps version
        let last = stmts.last().unwrap();
        assert!(
            last.contains(&format!("'{}'", D::TO_VERSION)),
            "Last statement of delta v{}->v{} must update db_version to {}. Got: {last}",
            D::FROM_VERSION,
            D::TO_VERSION,
            D::TO_VERSION,
        );
        counts.push((b, stmts.len()));
    }
    let first = counts[0].1;
    for (b, count) in &counts[1..] {
        assert_eq!(
            first,
            *count,
            "SchemaDelta v{}->v{}: {:?} produced {first} stmts but {b:?} produced {count}",
            D::FROM_VERSION,
            D::TO_VERSION,
            counts[0].0
        );
    }
}

// ---------------------------------------------------------------------------
// Trait-based validation test for the current schema
// ---------------------------------------------------------------------------

#[test]
fn test_validate_current_schema() {
    validate_schema_version::<current::Schema>();
}

// ---------------------------------------------------------------------------
// Schema fingerprint — prevents in-place edits without a migration
// ---------------------------------------------------------------------------

/// Compute the SHA-256 fingerprint of SQL statements.
fn compute_fingerprint(stmts: &[String]) -> String {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    for stmt in stmts {
        hasher.update(stmt.as_bytes());
        hasher.update(b"\n");
    }
    hex::encode(hasher.finalize())
}

/// Recomputes the SHA-256 fingerprint of the SQL output for every schema version
/// (archived + current + deltas) per backend and compares each to its stored
/// constant. If someone modifies any schema without following the migration
/// process, this test fails with clear instructions.
#[test]
fn test_schema_fingerprint() {
    for &b in ALL_BACKENDS {
        let idx = b.index();

        // Check archived versions
        for v in archive::MIN_ARCHIVE..current::Schema::VERSION {
            let actual = compute_fingerprint(&archive::build(v, b));
            let expected = archive::fingerprints(v)[idx];
            assert_eq!(
                actual, expected,
                "\n\n\
                 Archived schema v{v} ({b:?}) SQL output changed!\n\n\
                 Actual fingerprint:   {actual}\n\
                 Expected fingerprint: {expected}\n\n\
                 Archives are frozen and must not be modified after creation.\n",
            );
        }

        // Check deltas
        for v in (archive::MIN_ARCHIVE + 1)..=current::Schema::VERSION {
            let actual = compute_fingerprint(&deltas::get(v, b));
            let expected = deltas::fingerprints(v)[idx];
            assert_eq!(
                actual,
                expected,
                "\n\n\
                 Delta v{from}->v{v} ({b:?}) SQL output changed!\n\n\
                 Actual fingerprint:   {actual}\n\
                 Expected fingerprint: {expected}\n\n\
                 Deltas are frozen and must not be modified after creation.\n",
                from = v - 1,
            );
        }

        // Check current version
        let actual = compute_fingerprint(&current::build(b));
        let expected = current::Schema::FINGERPRINTS[idx];
        assert_eq!(
            actual,
            expected,
            "\n\n\
             Schema SQL output ({b:?}) changed but VERSION ({ver}) was not bumped!\n\n\
             Actual fingerprint:   {actual}\n\
             Expected fingerprint: {expected}\n\n\
             If you intentionally modified the schema, follow the migration process:\n\
             1. Copy current/ to archive/v{ver}/\n\
             2. Create deltas/v{ver}_to_v{next}.rs\n\
             3. Wire up the new archive and delta in their respective mod.rs files\n\
             4. Bump VERSION to {next}\n\
             5. Update FINGERPRINTS for {b:?} to: {actual}\n",
            ver = current::Schema::VERSION,
            next = current::Schema::VERSION + 1,
        );
    }
}

// ---------------------------------------------------------------------------
// Migration guard tests — always on, no feature gates, no #[ignore].
// Checked for ALL backends.
// ---------------------------------------------------------------------------

/// If CURRENT_VERSION has been bumped beyond the initial sea-query version (5),
/// archives must exist for every version from MIN_ARCHIVE to CURRENT_VERSION-1,
/// and must produce non-empty output for every backend.
#[test]
fn test_migration_guard_archives_exist() {
    assert!(
        archive::MIN_ARCHIVE > 0,
        "CURRENT_VERSION is {} but MIN_ARCHIVE is 0. \
         You must create archives before bumping the version.",
        current::Schema::VERSION
    );

    for v in archive::MIN_ARCHIVE..current::Schema::VERSION {
        for &b in ALL_BACKENDS {
            let result = std::panic::catch_unwind(|| archive::build(v, b));
            assert!(
                result.is_ok(),
                "Missing archive for version {v} on {b:?}. When bumping CURRENT_VERSION to {}, \
                 you must copy current/ to archive/v{v}/.",
                current::Schema::VERSION
            );
            let stmts = result.unwrap();
            assert!(
                !stmts.is_empty(),
                "Archive v{v} produced empty output on {b:?}"
            );
        }
    }
}

/// If CURRENT_VERSION has been bumped beyond the initial sea-query version (5),
/// deltas must exist for every version from MIN_ARCHIVE+1 to CURRENT_VERSION,
/// and must produce non-empty output for every backend.
#[test]
fn test_migration_guard_deltas_exist() {
    for v in (archive::MIN_ARCHIVE + 1)..=current::Schema::VERSION {
        for &b in ALL_BACKENDS {
            let result = std::panic::catch_unwind(|| deltas::get(v, b));
            assert!(
                result.is_ok(),
                "Missing delta for version {v} on {b:?}. When bumping CURRENT_VERSION to {}, \
                 you must create deltas/v{}_to_v{v}.rs.",
                current::Schema::VERSION,
                v - 1
            );
            let stmts = result.unwrap();
            assert!(
                !stmts.is_empty(),
                "Delta v{v} produced empty output on {b:?}"
            );
        }
    }
}

/// Archives and deltas for every backend must produce the same statement count,
/// ensuring no backend is missing statements.
#[test]
fn test_migration_guard_backend_consistency() {
    // Archives: each version must produce the same count across backends.
    for v in archive::MIN_ARCHIVE..current::Schema::VERSION {
        let counts: Vec<(Backend, usize)> = ALL_BACKENDS
            .iter()
            .map(|&b| (b, archive::build(v, b).len()))
            .collect();
        let first = counts[0].1;
        for (b, count) in &counts[1..] {
            assert_eq!(
                first, *count,
                "Archive v{v}: {:?} produced {first} stmts but {b:?} produced {count}",
                counts[0].0
            );
        }
    }

    // Deltas: each version must produce the same count across backends.
    for v in (archive::MIN_ARCHIVE + 1)..=current::Schema::VERSION {
        let counts: Vec<(Backend, usize)> = ALL_BACKENDS
            .iter()
            .map(|&b| (b, deltas::get(v, b).len()))
            .collect();
        let first = counts[0].1;
        for (b, count) in &counts[1..] {
            assert_eq!(
                first, *count,
                "Delta v{v}: {:?} produced {first} stmts but {b:?} produced {count}",
                counts[0].0
            );
        }
    }
}
