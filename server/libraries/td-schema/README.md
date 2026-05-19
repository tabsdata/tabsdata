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
│   ├── mod.rs          # Schema (impl SchemaVersion with VERSION + FINGERPRINTS)
│   ├── system.rs       # tabsdata_system table + seed data
│   ├── users.rs        # users table, views, seed
│   ├── functions.rs    # functions, function_patches, current views, indices
│   ├── logical.rs      # logical_function_runs, LTDVs, pointer event logs
│   └── …               # one file per subsystem
├── archive/            # Frozen snapshots of previous versions (empty in 2.0)
│   └── mod.rs          # build(version, backend), fingerprints(version)
├── deltas/             # Incremental migration scripts (empty in 2.0)
│   └── mod.rs          # get(target_version, backend), fingerprints(target_version)
└── tests.rs            # Trait validation, fingerprint, migration guards
```

`archive/` and `deltas/` are wired (constants, dispatch fns, guard tests) but
hold no archived versions yet — the 2.x line starts at `Schema::VERSION = 1` so
there is nothing to archive against until the first in-major bump. See
**Changing the schema within a major** below for the workflow that fills them.

## Major versions and upgradeability

Tabsdata 2.0 is **not** in-place upgradeable from any earlier release. A 2.0
binary refuses to open any 1.x database and requires a fresh install.

The gate is the `tabsdata_version` row in `tabsdata_system`:

| `tabsdata_system.tabsdata_version` | Binary `CURRENT_TABSDATA_VERSION` | Outcome                                |
|------------------------------------|-----------------------------------|----------------------------------------|
| row missing                        | 2                                 | refused — treated as `1` (pre-2.0)     |
| 2                                  | 2                                 | proceed to `db_version` check          |
| anything else                      | 2                                 | refused with `IncompatibleMajorVersion`|

Pre-2.0 databases do not write `tabsdata_version`, so the absence of the row is
the unambiguous signal that the database belongs to a previous major.

`Schema::VERSION` (the integer in `db_version`) is the schema counter **within**
the current major and resets to `1` at each major bump. It cannot collide with
1.x on-disk values because the major gate fires first.

When 3.0 ships, the team decides whether to support 2.x → 3.x upgrade or repeat
this pattern (bump `CURRENT_TABSDATA_VERSION`, reset `db_version` to 1, refuse
2.x databases).

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
- Each `archive::vN::Schema` implements `SchemaVersion` (frozen snapshots).
- Each `deltas::vN_to_vM::Migration` implements `SchemaDelta`.

## How it works

| Path | Purpose |
|------|---------|
| `current::Schema::build(backend)` | Generates the **full schema** for a fresh install at `Schema::VERSION`. |
| `archive::build(version, backend)` | Dispatches to the archived `SchemaVersion` for a **previous** version. Panics if no archive exists for that version. |
| `archive::fingerprints(version)` | Returns the stored per-backend fingerprints for an archived version. |
| `deltas::get(target, backend)` | Dispatches to the `SchemaDelta` that upgrades from `target - 1` to `target`. Panics if no delta exists. |
| `deltas::fingerprints(target)` | Returns the stored per-backend fingerprints for a delta. |

**Fresh installs** run `current::Schema::build()`.

**Existing databases** within the same major run the delta ladder: `TdPool::upgrade_db_version`
iterates `(db_version + 1)..=CURRENT_VERSION` and calls `deltas::get(target, backend)` for
each step, executing the resulting SQL in its own transaction.

## Changing the schema within a major

Follow these steps **every time** you modify tables, views, indices, or seed
data in `current/`. The `test_schema_fingerprint` test will fail if you skip
any step.

### 1. Archive the current version

Copy the entire `current/` directory to `archive/v{Schema::VERSION}/`:

```bash
cp -r src/schema/current src/schema/archive/v1
```

Then clean up the archive copy:
- Freeze the version in the `SchemaVersion` impl: change `const VERSION` from
  `Self::VERSION` (dynamic) to a literal (e.g., `1`).
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
    const VERSION: usize = 1;
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
pub(crate) mod v1;

pub fn build(version: usize, backend: impl Into<Backend>) -> Vec<String> {
    let backend = backend.into();
    match version {
        1 => v1::Schema::build(backend),
        _ => panic!("No archived schema for version {version}"),
    }
}

pub fn fingerprints(version: usize) -> [&'static str; 3] {
    match version {
        1 => v1::Schema::FINGERPRINTS,
        _ => panic!("No archived schema for version {version}"),
    }
}

pub const MIN_ARCHIVE: usize = 1;
```

### 2. Create the delta

Create `deltas/v1_to_v2.rs`:

```rust
use crate::schema::SchemaDelta;
use crate::schema::backend::Backend;

pub struct Migration;

impl SchemaDelta for Migration {
    const FROM_VERSION: usize = 1;
    const TO_VERSION: usize = 2;
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
pub(crate) mod v1_to_v2;

pub fn get(target_version: usize, backend: impl Into<Backend>) -> Vec<String> {
    let backend = backend.into();
    match target_version {
        2 => v1_to_v2::Migration::up(backend),
        _ => panic!("No delta migration for version {target_version}"),
    }
}

pub fn fingerprints(target_version: usize) -> [&'static str; 3] {
    match target_version {
        2 => v1_to_v2::Migration::FINGERPRINTS,
        _ => panic!("No delta migration for version {target_version}"),
    }
}
```

### 3. Add validation tests

Add a `#[cfg(test)] mod tests` block to each new module. The shared helpers in
`crate::schema::tests` do the heavy lifting:

In `archive/v1/mod.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::Schema;
    use crate::schema::tests::validate_schema_version;

    #[test]
    fn test_validate_archive_v1() {
        validate_schema_version::<Schema>();
    }
}
```

In `deltas/v1_to_v2.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::Migration;
    use crate::schema::tests::validate_delta;

    #[test]
    fn test_validate_delta_v1_to_v2() {
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
    const VERSION: usize = 2;  // <- bumped
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
| `test_schema_fingerprint` | SQL output (archives + deltas + current) hasn't changed without a version bump. |
| `test_validate_current_schema` | Current schema produces non-empty, consistent output across all backends. |
| `test_validate_archive_vN` | Each archive produces non-empty, consistent output across all backends. |
| `test_validate_delta_vN_to_vM` | Each delta is single-step, non-empty, consistent, and bumps `db_version`. |
| `test_migration_guard_archives_exist` | An archive exists for every version from `MIN_ARCHIVE` to `Schema::VERSION - 1`. |
| `test_migration_guard_deltas_exist` | A delta exists for every version from `MIN_ARCHIVE + 1` to `Schema::VERSION`. |
| `test_migration_guard_backend_consistency` | Archives and deltas produce the same number of statements across all backends. |

## Why intra-major deltas were reset for 2.0

Pre-2.0 carried a chain of deltas (v5→v6 … v9→v10 → v10→v11) and matching
frozen archives. In-place renames during the 2.0 cut (floating → logical /
physical, function variant → function patch) propagated into the archived SQL
and broke the archive/delta equivalence the migration-invariant tests asserted.
Rather than reconstruct snapshots for a path that nobody will travel — 2.0 is
a fresh install — the archives and deltas were emptied. The machinery (the
trait, the dispatch functions, the guard tests, the upgrade loop in
`TdPool::upgrade_db_version`) stays in place, ready to be filled by the first
2.x in-major bump.

## Edition handling (`edition` row)

Unrelated to `db_version` and `tabsdata_version`. The `edition` row in
`tabsdata_system` tracks the Tabsdata edition (community / enterprise / etc.)
and has its own check/upgrade path in `td_database::pool`. See that module for
details.
