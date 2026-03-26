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
│   ├── mod.rs          # build(version, backend), MIN_ARCHIVE
│   └── v5/             # Schema (impl SchemaVersion) — frozen copy at v5
├── deltas/             # Incremental migration scripts
│   ├── mod.rs          # get(target_version, backend)
│   └── v5_to_v6.rs     # Migration (impl SchemaDelta) — v5 to v6
└── tests.rs            # Trait validation, fingerprint, migration guards
```

## Traits

Every version snapshot and every delta implements a trait that formalizes its
contract:

```rust
/// A complete schema snapshot for a specific database version.
pub trait SchemaVersion {
    const VERSION: usize;
    const FINGERPRINT: &'static str = "";  // only current overrides this
    fn build(backend: Backend) -> Vec<String>;
}

/// An incremental migration from one database version to the next.
pub trait SchemaDelta {
    const FROM_VERSION: usize;
    const TO_VERSION: usize;
    fn up(backend: Backend) -> Vec<String>;
}
```

- `current::Schema` implements `SchemaVersion` (the live version) and overrides
  `FINGERPRINT` with the SHA-256 hash of the SQL output across all backends.
- Each `archive::vN::Schema` implements `SchemaVersion` (frozen snapshots) —
  they use the default empty `FINGERPRINT` since their output is frozen.
- Each `deltas::vN_to_vM::Migration` implements `SchemaDelta`.

## How it works

| Path | Purpose |
|------|---------|
| `current::Schema::build(backend)` | Generates the **full schema** for a fresh install at `Schema::VERSION`. |
| `archive::build(version, backend)` | Dispatches to the archived `SchemaVersion` for a **previous** version. |
| `deltas::get(target, backend)` | Dispatches to the `SchemaDelta` that upgrades from `target - 1` to `target`. |

**Fresh installs** run `current::Schema::build()`.
**Existing databases** apply deltas **incrementally**: going from v2 to v5
runs deltas 3, 4, then 5 in order (see `TdPool::upgrade_db_version` in
td-database).

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
- Remove `FINGERPRINT` override and the delegating `pub fn build()` from the archive's `mod.rs`.
- Freeze the version in the `SchemaVersion` impl: change `const VERSION` from
  `Schema::VERSION` (dynamic) to a literal (e.g., `6`).
- In `system.rs`, replace the `super::Schema` / `SchemaVersion` imports with a
  local constant: `const ARCHIVE_VERSION: usize = 6;` and use it in the seed data.

The result should look like:

```rust
use crate::schema::SchemaVersion;
use crate::schema::backend::Backend;

pub struct Schema;

impl SchemaVersion for Schema {
    const VERSION: usize = 6;

    fn build(b: Backend) -> Vec<String> {
        // ... existing build body ...
    }
}
```

Wire it up in `archive/mod.rs`:
```rust
pub(crate) mod v6;

pub fn build(version: usize, backend: Backend) -> Vec<String> {
    match version {
        5 => v5::Schema::build(backend),
        6 => v6::Schema::build(backend),   // <- new
        _ => panic!("No archived schema for version {version}"),
    }
}
```

### 2. Create the delta

Create `deltas/v6_to_v7.rs`:

```rust
use crate::schema::backend::Backend;
use crate::schema::SchemaDelta;

pub struct Migration;

impl SchemaDelta for Migration {
    const FROM_VERSION: usize = 6;
    const TO_VERSION: usize = 7;

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
pub(crate) mod v6_to_v7;

pub fn get(target_version: usize, backend: Backend) -> Vec<String> {
    match target_version {
        6 => v5_to_v6::Migration::up(backend),
        7 => v6_to_v7::Migration::up(backend),  // <- new
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

### 6. Update the fingerprint

Run the tests -- the fingerprint test will fail and print the new hash:

```bash
cargo test -p td-schema
```

Copy the `Actual fingerprint` value from the error output and update
`FINGERPRINT` in the `SchemaVersion` impl in `current/mod.rs`.

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

## Example: the v5 to v6 showcase migration

The `v5_to_v6::Migration` delta is an empty migration (no schema changes,
only the mandatory `db_version` bump). It exists to demonstrate the migration
system end-to-end and to activate all the guard tests. Use it as a template
when creating your first real migration.
