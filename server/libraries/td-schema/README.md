<!--
Copyright 2026 Tabs Data Inc.
-->

# td-schema — Tabsdata SQL Schema

Cross-database schema definitions built with [sea-query](https://docs.rs/sea-query).
A single set of Rust builders produces correct SQL for **SQLite**, **Postgres**,
and **MySQL**.

## Directory layout

```
src/schema/
├── mod.rs              # SchemaVersion + SchemaDelta trait definitions
├── backend.rs          # Backend enum (Sqlite, Postgres, MySql) + helpers
├── current/            # Live schema — the source of truth
│   ├── mod.rs          # Schema (impl SchemaVersion with VERSION + FINGERPRINT)
│   ├── system.rs       # tabsdata_system table + seed data
│   ├── users.rs        # users table, views, seed
│   ├── collections.rs
│   ├── roles.rs
│   ├── functions.rs
│   ├── sessions.rs
│   ├── permissions.rs
│   └── indices.rs      # All secondary indices
├── archive/            # Frozen snapshots of previous versions
│   ├── mod.rs          # build(version, backend), fingerprints(version), MIN_ARCHIVE
│   ├── v5/             # Schema (impl SchemaVersion) — frozen copy at v5
│   ├── v6/             # Schema (impl SchemaVersion) — frozen copy at v6
│   ├── v7/             # Schema (impl SchemaVersion) — frozen copy at v7
│   ├── v8/             # Schema (impl SchemaVersion) — frozen copy at v8
│   └── historic/       # Legacy sqlx migration SQL files (v0–v5, SQLite-only)
│       ├── mod.rs      # get(), initial_v0_schema(), normalize()
│       ├── live/       # Frozen .up.sql / .down.sql migration files
│       └── README.md   # Documentation — DO NOT MODIFY the SQL files
├── deltas/             # Incremental migration scripts
│   ├── mod.rs          # get(target_version, backend), fingerprints(target_version)
│   ├── v5_to_v6.rs     # Migration (impl SchemaDelta) — v5 to v6
│   ├── v6_to_v7.rs     # Migration (impl SchemaDelta) — v6 to v7
│   ├── v7_to_v8.rs     # Migration (impl SchemaDelta) — v7 to v8
│   └── v8_to_v9.rs     # Migration (impl SchemaDelta) — v8 to v9
└── tests.rs            # Trait validation, fingerprint, migration guards
```

## Traits

Every version snapshot and every delta implements a trait that formalizes its
contract:

```rust
/// A complete schema snapshot for a specific database version.
pub trait SchemaVersion {
    const VERSION: usize;
    const FINGERPRINTS: [&'static str; backend::BACKEND_COUNT];
    fn build(backend: impl Into<Backend>) -> Vec<String>;
}

/// An incremental migration from one database version to the next.
pub trait SchemaDelta {
    const FROM_VERSION: usize;
    const TO_VERSION: usize;
    const FINGERPRINTS: [&'static str; backend::BACKEND_COUNT];
    fn up(backend: Backend) -> Vec<String>;
}
```

- `current::Schema` implements `SchemaVersion` (the live version) with
  `FINGERPRINTS` containing the SHA-256 hashes of the SQL output, one per
  backend (Sqlite, Postgres, MySql).
- Each `archive::vN::Schema` implements `SchemaVersion` (frozen snapshots)
  with `FINGERPRINTS` containing the hashes at the time the archive was created.
- Each `deltas::vN_to_vM::Migration` implements `SchemaDelta` with
  `FINGERPRINTS` containing the hashes of the delta output per backend.

## How it works

| Path | Purpose |
|------|---------|
| `current::Schema::build(backend)` | Generates the **full schema** for a fresh install at `Schema::VERSION`. |
| `archive::build(version, backend)` | Dispatches to the archived `SchemaVersion` for a **previous** version. |
| `archive::fingerprints(version)` | Returns the stored per-backend fingerprints for an archived version. |
| `deltas::get(target, backend)` | Dispatches to the `SchemaDelta` that upgrades from `target - 1` to `target`. |
| `deltas::fingerprints(target)` | Returns the stored per-backend fingerprints for a delta. |

**Fresh installs** run `current::Schema::build()`.
**Existing databases (v6+)** apply deltas **incrementally**: going from v6 to
v7 runs delta 7 (see `TdPool::upgrade_db_version` in td-database).
**Legacy databases (v0–v5)** are upgraded via the `archive/historic/` module:
the frozen sqlx SQL files bring the database to v5, then `normalize()`
rebuilds all tables using the v5 archive to ensure type and index parity with
a fresh install, then programmatic deltas continue to CURRENT. The entire
upgrade runs in a single transaction.

## Changing the schema

Follow these steps **every time** you modify tables, views, indices, or seed
data in `current/`. The `test_schema_fingerprint` test will fail if you skip
any step.

### 1. Archive the current version

Copy the entire `current/` directory to `archive/v{Schema::VERSION}/`:

```bash
cp -r src/schema/current src/schema/archive/v6
```

Then clean up the archive copy:
- Freeze the version in the `SchemaVersion` impl: change `const VERSION` from
  `Self::VERSION` (dynamic) to a literal (e.g., `8`).
- Remove the delegating `pub fn build()` convenience function at the bottom
  of the archive's `mod.rs` (only `current/mod.rs` needs it).
- Keep `FINGERPRINTS` as-is — the hashes are already correct for the frozen
  version.

The result should look like:

```rust
use crate::schema::SchemaVersion;
use crate::schema::backend::Backend;

pub struct Schema;

impl SchemaVersion for Schema {
    const VERSION: usize = 8;

    const FINGERPRINTS: [&'static str; 3] = [
        "...", // Sqlite
        "...", // Postgres
        "...", // MySql
    ];

    fn build(backend: impl Into<Backend>) -> Vec<String> {
        let backend = backend.into();
        // ... existing build body ...
    }
}
```

Wire it up in `archive/mod.rs`:
```rust
pub(crate) mod v8;

pub fn build(version: usize, backend: impl Into<Backend>) -> Vec<String> {
    let backend = backend.into();
    match version {
        // ... existing arms ...
        8 => v8::Schema::build(backend),   // <- new
        _ => panic!("No archived schema for version {version}"),
    }
}

pub fn fingerprints(version: usize) -> [&'static str; 3] {
    match version {
        // ... existing arms ...
        8 => v8::Schema::FINGERPRINTS,   // <- new
        _ => panic!("No archived schema for version {version}"),
    }
}
```

### 2. Create the delta

Create `deltas/v8_to_v9.rs`:

```rust
use crate::schema::SchemaDelta;
use crate::schema::backend::Backend;

pub struct Migration;

impl SchemaDelta for Migration {
    const FROM_VERSION: usize = 8;
    const TO_VERSION: usize = 9;
    const FINGERPRINTS: [&'static str; 3] = [
        "", // Sqlite  — filled in by step 6
        "", // Postgres
        "", // MySql
    ];

    fn up(b: Backend) -> Vec<String> {
        vec![
            // Your migration SQL here, e.g.:
            // b.build_schema(Table::alter().table(...).add_column(...).to_owned()),

            // MANDATORY: always bump db_version as the last statement.
            b.update_db_version(Self::TO_VERSION),
        ]
    }
}
```

Wire it up in `deltas/mod.rs`:
```rust
pub(crate) mod v8_to_v9;

pub fn get(target_version: usize, backend: impl Into<Backend>) -> Vec<String> {
    let backend = backend.into();
    match target_version {
        // ... existing arms ...
        9 => v8_to_v9::Migration::up(backend),  // <- new
        _ => panic!("No delta migration for version {target_version}"),
    }
}

pub fn fingerprints(target_version: usize) -> [&'static str; 3] {
    match target_version {
        // ... existing arms ...
        9 => v8_to_v9::Migration::FINGERPRINTS,  // <- new
        _ => panic!("No delta migration for version {target_version}"),
    }
}
```

### 3. Add validation tests

Add a `#[cfg(test)] mod tests` block to each new module. The shared helpers
in `crate::schema::tests` do the heavy lifting:

In `archive/v6/mod.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::Schema;
    use crate::schema::tests::validate_schema_version;

    #[test]
    fn test_validate_archive_v6() {
        validate_schema_version::<Schema>();
    }
}
```

In `deltas/v6_to_v7.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::Migration;
    use crate::schema::tests::validate_delta;

    #[test]
    fn test_validate_delta_v6_to_v7() {
        validate_delta::<Migration>();
    }
}
```

### 4. Make your schema changes in `current/`

Edit whichever files need to change (tables, views, indices, seed data).

### 5. Bump the version

In `current/mod.rs`, update `Schema::VERSION` in the `SchemaVersion` impl:

```rust
impl SchemaVersion for Schema {
    const VERSION: usize = 7;  // <- bumped
    // ...
}
```

### 6. Update the fingerprints

Run the tests — the fingerprint test will fail and print the new hashes:

```bash
cargo test -p td-schema
```

Copy the `Actual fingerprints` values from the error output and update
`FINGERPRINTS` in the `SchemaVersion` impl in `current/mod.rs`. Also update
`FINGERPRINTS` in the new delta's `SchemaDelta` impl.

### 7. Verify

```bash
cargo test -p td-schema
cargo test -p td-database --features sqlite
```

Both should pass. The key tests are:

| Test | What it checks |
|------|---------------|
| `test_schema_fingerprint` | SQL output hasn't changed without a version bump. |
| `test_validate_current_schema` | Current schema produces non-empty, consistent output across all backends. |
| `test_validate_archive_vN` | Each archive produces non-empty, consistent output across all backends. |
| `test_validate_delta_vN_to_vM` | Each delta is single-step, non-empty, consistent, and bumps `db_version`. |
| `test_migration_guard_archives_exist` | An archive exists for every version from `MIN_ARCHIVE` to `Schema::VERSION - 1`. |
| `test_migration_guard_deltas_exist` | A delta exists for every version from `MIN_ARCHIVE + 1` to `Schema::VERSION`. |
| `test_migration_guard_backend_consistency` | Archives and deltas produce the same number of statements across all backends. |
| `test_migration_invariant` *(td-database)* | `archive(N) + deltas(N+1..=current)` produces a schema identical to a fresh install, validated against a real SQLite database. |
| `test_historic_migration_invariant` *(td-database)* | `v0 + historic + normalize + deltas` produces a schema identical to a fresh install (full parity: tables, views, indices, column types). |
| `test_historic_upgrade_end_to_end` *(td-database)* | Production `upgrade_db_version()` path from v0 to CURRENT succeeds and cleans up `_sqlx_migrations`. |
| `test_upgrade_rollback_on_failure` *(td-database)* | A failed upgrade rolls back — database stays at its original version. |

## Example: the v5 to v6 showcase migration

The `v5_to_v6::Migration` delta is an empty migration (no schema changes,
only the mandatory `db_version` bump). It exists to demonstrate the migration
system end-to-end and to activate all the guard tests. Use it as a template
when creating your first real migration.
