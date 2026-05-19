//
// Copyright 2026 Tabs Data Inc.
//

//! Generation-only tests for the schema system.
//!
//! These tests validate SQL generation without touching a database. The tests
//! that execute SQL against a real database live in `td-database` where
//! `TdPool` / `DynamicPool` are available.

use super::backend::Backend;
use super::{SchemaDelta, SchemaVersion, archive, current, deltas};

const ALL_BACKENDS: &[Backend] = &[
    Backend::Sqlite,
    Backend::Postgres,
    Backend::MySql,
];

/// Validate that a [`SchemaVersion`] implementation produces non-empty output
/// for every enabled backend.
///
/// Per-backend statement counts are not asserted equal: Postgres emits a
/// supporting `CREATE FUNCTION` alongside each trigger because sqlx-postgres
/// rejects multi-statement strings, so the backends legitimately diverge in
/// total statement count even when the schema is logically identical.
pub(crate) fn validate_schema_version<S: SchemaVersion>() {
    for &b in ALL_BACKENDS {
        let stmts = S::build(b);
        assert!(
            !stmts.is_empty(),
            "SchemaVersion v{} produced empty output on {b:?}",
            S::VERSION
        );
    }
}

#[test]
fn test_validate_current_schema() {
    validate_schema_version::<current::Schema>();
}

/// Validate that a [`SchemaDelta`] implementation:
/// - Is a single-step migration (`FROM_VERSION + 1 == TO_VERSION`)
/// - Produces non-empty output across all backends
/// - Last statement updates `db_version` to `TO_VERSION` (the bump guard)
/// - Produces consistent statement counts across backends
///
/// Each delta module's own tests call this against its `Migration` impl.
#[allow(dead_code)]
pub(crate) fn validate_delta<D: SchemaDelta>() {
    assert_eq!(
        D::FROM_VERSION + 1,
        D::TO_VERSION,
        "Delta must be single-step: got {}->{}",
        D::FROM_VERSION,
        D::TO_VERSION,
    );

    for &b in ALL_BACKENDS {
        let stmts = D::up(b);
        assert!(
            !stmts.is_empty(),
            "SchemaDelta v{}->v{} produced empty output on {b:?}",
            D::FROM_VERSION,
            D::TO_VERSION,
        );
        let last = stmts.last().unwrap();
        assert!(
            last.contains(&format!("'{}'", D::TO_VERSION)),
            "Last statement of delta v{}->v{} must update db_version to {}. Got: {last}",
            D::FROM_VERSION,
            D::TO_VERSION,
            D::TO_VERSION,
        );
    }
}

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

/// Print the fingerprint of the current schema for every enabled backend.
/// Run with `cargo test -p td-schema --lib print_schema_fingerprints --
/// --ignored --nocapture` to refresh `FINGERPRINTS` on schema edits.
#[test]
#[ignore]
fn print_schema_fingerprints() {
    for &b in ALL_BACKENDS {
        println!(
            "{b:?} (idx {}): {}",
            b.index(),
            compute_fingerprint(&current::build(b)),
        );
    }
}

/// Pins the SHA-256 fingerprint of every schema version (archives,
/// deltas, current) per backend. Catches silent edits to frozen
/// archives/deltas and to `current/`. The archive/delta loops are
/// no-ops at the v1 baseline (range `1..1` is empty) and activate
/// automatically on the first version bump.
#[allow(clippy::reversed_empty_ranges)]
#[test]
fn test_schema_fingerprint() {
    for &b in ALL_BACKENDS {
        let idx = b.index();

        for v in 1..current::Schema::VERSION {
            let actual = compute_fingerprint(&archive::build(v, b));
            let expected = archive::fingerprints(v)[idx];
            assert_eq!(
                actual, expected,
                "\n\nArchived schema v{v} ({b:?}) SQL output changed.\n\
                 Actual:   {actual}\nExpected: {expected}\n\n\
                 Archives are frozen and must not be modified after creation.\n",
            );
        }

        for v in 2..=current::Schema::VERSION {
            let actual = compute_fingerprint(&deltas::get(v, b));
            let expected = deltas::fingerprints(v)[idx];
            assert_eq!(
                actual,
                expected,
                "\n\nDelta v{from}->v{v} ({b:?}) SQL output changed.\n\
                 Actual:   {actual}\nExpected: {expected}\n\n\
                 Deltas are frozen and must not be modified after creation.\n",
                from = v - 1,
            );
        }

        let actual = compute_fingerprint(&current::build(b));
        let expected = current::Schema::FINGERPRINTS[idx];
        assert_eq!(
            actual, expected,
            "\n\nSchema SQL output ({b:?}) changed.\n\
             Actual:   {actual}\nExpected: {expected}\n\n\
             If the change is intentional, update FINGERPRINTS for {b:?} \
             to: {actual}\n",
        );
    }
}

/// On a version bump, archives must exist for every version in
/// `1..CURRENT_VERSION` and produce non-empty output for every backend.
/// Guards against bumping `CURRENT_VERSION` without creating
/// `archive/vN/`. No-op at v1 baseline.
#[allow(clippy::reversed_empty_ranges)]
#[test]
fn test_migration_guard_archives_exist() {
    for v in 1..current::Schema::VERSION {
        for &b in ALL_BACKENDS {
            let result = std::panic::catch_unwind(|| archive::build(v, b));
            assert!(
                result.is_ok(),
                "Missing archive for version {v} on {b:?}. When bumping \
                 CURRENT_VERSION to {}, copy current/ to archive/v{v}/.",
                current::Schema::VERSION
            );
            assert!(
                !result.unwrap().is_empty(),
                "Archive v{v} produced empty output on {b:?}"
            );
        }
    }
}

/// On a version bump, deltas must exist for every version in
/// `2..=CURRENT_VERSION` and produce non-empty output for every backend.
/// Guards against bumping `CURRENT_VERSION` without creating
/// `deltas/vN_to_vM.rs`. No-op at v1 baseline.
#[allow(clippy::reversed_empty_ranges)]
#[test]
fn test_migration_guard_deltas_exist() {
    for v in 2..=current::Schema::VERSION {
        for &b in ALL_BACKENDS {
            let result = std::panic::catch_unwind(|| deltas::get(v, b));
            assert!(
                result.is_ok(),
                "Missing delta for version {v} on {b:?}. When bumping \
                 CURRENT_VERSION to {}, create deltas/v{}_to_v{v}.rs.",
                current::Schema::VERSION,
                v - 1
            );
            assert!(
                !result.unwrap().is_empty(),
                "Delta v{v} produced empty output on {b:?}"
            );
        }
    }
}

/// Every backend must produce the same statement count for each
/// archived/delta version — guards against silently skipping a backend
/// in an archive or delta. No-op while the ranges are empty.
#[allow(clippy::reversed_empty_ranges)]
#[test]
fn test_migration_guard_backend_consistency() {
    let assert_consistent = |kind: &str, v: usize, counts: Vec<(Backend, usize)>| {
        let (first_b, first) = counts[0];
        for (b, count) in &counts[1..] {
            assert_eq!(
                first, *count,
                "{kind} v{v}: {first_b:?} produced {first} stmts but {b:?} produced {count}",
            );
        }
    };

    for v in 1..current::Schema::VERSION {
        let counts = ALL_BACKENDS
            .iter()
            .map(|&b| (b, archive::build(v, b).len()))
            .collect();
        assert_consistent("Archive", v, counts);
    }
    for v in 2..=current::Schema::VERSION {
        let counts = ALL_BACKENDS
            .iter()
            .map(|&b| (b, deltas::get(v, b).len()))
            .collect();
        assert_consistent("Delta", v, counts);
    }
}
