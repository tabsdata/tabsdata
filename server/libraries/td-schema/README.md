<!--
Copyright 2026 Tabsdata Inc.
-->

# td-schema — Tabsdata SQL Schema

Cross-database schema definitions built with [sea-query](https://docs.rs/sea-query).
A single set of Rust builders produces correct SQL for **SQLite**, **Postgres**,
and **MySQL**.

## Directory layout

```
src/schema/
├── mod.rs              # module wiring + re-exports
├── source.rs           # SchemaSource trait + install/derived script builders
├── version.rs          # SchemaDelta trait
├── backend.rs          # Backend enum (Sqlite, Postgres, MySql) + helpers
├── current/            # Live schema — the source of truth
│   ├── mod.rs          # VERSION + Source (impl SchemaSource); gated by snapshots/
│   ├── snapshots/      # Committed rendered DDL per backend — the gate on current/
│   ├── system.rs       # tabsdata_system table + seed data
│   ├── users.rs        # users table, views, seed
│   ├── functions.rs    # functions, function_patches, current views, indices
│   ├── logical.rs      # logical_function_runs, LTDVs, pointer event logs
│   └── …               # one file per subsystem
├── archive/            # Frozen snapshots of previous versions (empty in 2.0)
│   └── mod.rs          # source(version) + build(version, backend)
├── deltas/             # Incremental migration scripts (empty in 2.0)
│   └── mod.rs          # get(target_version, backend)
└── tests.rs            # Trait validation, DDL snapshot, migration guards
```

`archive/` and `deltas/` are wired (constants, dispatch fns, guard tests) but
hold no archived versions yet — the 2.x line starts at `current::VERSION = 1` so
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

`current::VERSION` (the integer in `db_version`) is the schema counter **within**
the current major and resets to `1` at each major bump. It cannot collide with
1.x on-disk values because the major gate fires first.

When 3.0 ships, the team decides whether to support 2.x → 3.x upgrade or repeat
this pattern (bump `CURRENT_TABSDATA_VERSION`, reset `db_version` to 1, refuse
2.x databases).

## Traits

A schema exposes its content as **structured parts** through `SchemaSource`;
deltas implement `SchemaDelta`:

```rust
/// A derived object (view or trigger) as its create/drop statement pair,
/// both rendered from the same definition so they cannot disagree.
pub struct Derived {
    pub create: Vec<String>,
    pub drop: Vec<String>,
}

/// Structured schema content; the pool composes the install and the
/// derived-object lifecycle from these parts.
pub trait SchemaSource: Send + Sync + 'static {
    fn current_version(&self) -> usize;
    fn tables(&self, backend: Backend) -> Vec<String>;          // required
    fn triggers(&self, backend: Backend) -> Vec<Derived> { .. } // default: none
    fn views(&self, backend: Backend) -> Vec<Derived> { .. }    // default: none
    fn indexes(&self, backend: Backend) -> Vec<String> { .. }   // default: none
    fn seeds(&self, backend: Backend) -> Vec<String> { .. }     // default: none
    fn rls(&self, backend: Backend) -> Vec<String> { .. }       // default: none
    fn delta(&self, target: usize, backend: Backend) -> Vec<String> { .. }
}

/// An incremental migration from one database version to the next.
pub trait SchemaDelta {
    const FROM_VERSION: usize;
    const TO_VERSION: usize;
    fn up(backend: Backend) -> Vec<String>;
}
```

Because the pool only ever composes from the parts, a source cannot have a view
or trigger the pool does not know about — the same lists feed the fresh install
and the drop/re-create lifecycle around deltas.

- `current/mod.rs` implements the trait with its own `Source` (re-exported as
  `CurrentSource`), whose `delta` dispatches to `deltas/`. It is gated by the
  committed DDL snapshots under `current/snapshots/` (see
  `test_schema_ddl_snapshot`) — a schema edit surfaces as a reviewable SQL diff.
- Each `archive/vN/` keeps the same part functions frozen, wired through its own
  `Source` struct and gated the same way: the archive copy keeps its
  `snapshots/` dir, checked by `test_archive_ddl_snapshots`.
- Each `deltas::vN_to_vM::Migration` implements `SchemaDelta`, gated by committed
  snapshots under `deltas/snapshots/vN_to_vM/`, checked by
  `test_delta_ddl_snapshots`.

The snapshot guard itself (`crate::snapshot::assert_snapshot`, behind the
`test-utils` feature) is generic over any `Fn(Backend) -> Vec<String>`, so other
crates can gate their own schemas with it — `te-aiagent` does.

## How it works

| Path | Purpose |
|------|---------|
| `install_script(source, backend)` | Composes the **full install** from a source's parts: tables, triggers, views, indexes, seeds, RLS. Triggers precede seeds (so `_current` peers fill during seeding); RLS runs last (so `FORCE RLS` never rejects the seed inserts). |
| `derived_scripts(source, backend)` | The derived-object lifecycle as a (drop, create) pair from one render of the parts: views dropped in reverse creation order (no `CASCADE`), then triggers; re-created triggers-then-views. |
| `archive::source(version)` | The `SchemaSource` for a **previous** version. Panics if no archive exists for it. |
| `deltas::get(target, backend)` | Dispatches to the `SchemaDelta` that upgrades from `target - 1` to `target`. Panics if no delta exists. |

**Fresh installs** run `install_script(&CurrentSource, backend)` (also exposed
as `current::build`, which the DDL snapshots gate).

**Existing databases** within the same major run the delta ladder:
`UpgradeTx::upgrade_db_version` iterates `(db_version + 1)..=CURRENT_VERSION` and
calls `deltas::get(target, backend)` for each step. Each delta bumps `db_version`
itself as its last statement, so a delta reads as a complete step on its own.

The whole upgrade — every delta, the derived-object drop before them and the
rebuild after (`derived_scripts`), the version and edition rows, and the app-role
grants — runs as **one transaction**. A ladder that fails part-way up rolls all
of it back and the next run replays from the version it started at; it never
leaves a database at an intermediate version, nor without its views and triggers.
Deltas must therefore emit only transaction-safe SQL: no `CREATE INDEX
CONCURRENTLY`, no `VACUUM`.

The install script includes the derived objects — see
*Views and triggers are rebuilt for you* below.

## Changing the schema within a major

Follow these steps **every time** you modify tables, views, indices, or seed
data in `current/`. The `test_schema_ddl_snapshot` test will fail if you skip
the snapshot refresh (step 6); the migration-guard tests fail if you skip the
archive/delta wiring.

### 1. Archive the current version

Copy the entire `current/` directory to `archive/v{current::VERSION}/`:

```bash
cp -r src/schema/current src/schema/archive/v1
```

Then clean up the archive copy — the copied `Source` **is** the archive's
`SchemaSource`; it only sheds its live parts:
- Freeze the version: `pub const VERSION: usize = 1;` (already a literal; the
  copied `Source::current_version` reads it, so it needs no edit).
- Remove the delegating `pub fn build()`; only `current/mod.rs` needs it (the
  archive's snapshot gate renders through `archive::build` instead).
- Remove the `delta` override from the copied `Source`, plus the now-unused
  `deltas` and `install_script` imports — an archive is only ever installed as
  a starting point, so the trait's default (empty) `delta` is correct.
- Keep the copied `snapshots/` dir as-is: it is the frozen gate on the archive,
  checked by `test_archive_ddl_snapshots`. Archive **before** editing `current/`
  so the snapshots match the version being frozen.

Wire it up in `archive/mod.rs` — dropping the
`#[allow(clippy::match_single_binding)]` on `source` is part of the step:

```rust
pub(crate) mod v1;

pub fn source(version: usize) -> &'static dyn SchemaSource {
    match version {
        1 => &v1::Source,
        _ => panic!("No archived schema for version {version}"),
    }
}
```

### 2. Create the delta

Create `deltas/v1_to_v2.rs`. A delta is **frozen** once released, so it declares
its own `Iden`s rather than importing them from `current/` — an import would let
a later edit to `current/` silently rewrite already-shipped migration SQL:

```rust
use sea_query::{ColumnDef, Iden, Table};

use crate::schema::SchemaDelta;
use crate::schema::backend::Backend;

/// Frozen at v2: never re-point this at `current/`.
#[derive(Iden)]
enum Users {
    Table,
    Nickname,
}

pub struct Migration;

impl SchemaDelta for Migration {
    const FROM_VERSION: usize = 1;
    const TO_VERSION: usize = 2;

    fn up(b: Backend) -> Vec<String> {
        vec![
            b.build_schema(
                Table::alter()
                    .table(Users::Table)
                    .add_column(ColumnDef::new(Users::Nickname).text().null())
                    .to_owned(),
            ),
            // Nothing about the views over `users` — the pool re-creates those
            // from `current/` after the ladder. See below.

            // MANDATORY: always bump db_version as the last statement, so the
            // delta is a complete step on its own.
            b.update_db_version(Self::TO_VERSION),
        ]
    }
}
```

Wire it up in `deltas/mod.rs` — as with `archive/mod.rs`, import the trait and
drop the `#[allow(clippy::match_single_binding)]`:

```rust
use crate::schema::SchemaDelta;
use crate::schema::backend::Backend;

pub(crate) mod v1_to_v2;

pub fn get(target_version: usize, backend: impl Into<Backend>) -> Vec<String> {
    let backend = backend.into();
    match target_version {
        2 => v1_to_v2::Migration::up(backend),
        _ => panic!("No delta migration for version {target_version}"),
    }
}
```

#### Views and triggers are rebuilt for you

A delta describes the **table** change only. Before the ladder runs, the pool
drops every view and trigger; after it, they are re-created from `current/`'s
parts (the `derived_scripts` pair) — so a delta never
re-states the derived objects over a table it alters, and runs with none of
them standing. The pre-ladder drop is what lets a destructive delta
(`DROP COLUMN`, `ALTER COLUMN ... TYPE`) succeed on PostgreSQL, which records
per-column dependencies on the `SELECT *` it expanded at `CREATE VIEW`.

That step is not a nicety. PostgreSQL expands `SELECT *` at `CREATE VIEW` time
and freezes the resulting column list, while SQLite re-resolves it per query.
Nearly every view here (`*__with_names`, `*_active`, `*_resolved`,
`*__with_status`) is built from `Expr::col((Table, Asterisk))`, so without the
rebuild a delta that adds a column would leave those views serving the **old**
column list on PostgreSQL — and the sqlite tests would stay green. Views are
derived and stateless, so the rebuild is a pure re-render: it drops in reverse
creation order and re-creates in creation order, touches no rows, and runs on
every `db create`/`db upgrade` against an existing database (an up-to-date one
with drifted views self-heals; a fresh build renders them itself).

`maintain_current` / `versioned_current` triggers are the same story in a place
that is harder to see: their bodies enumerate the table's columns, and on
PostgreSQL those columns live in the trigger's supporting function, not in the
trigger itself — `pg_get_triggerdef` stops at `EXECUTE FUNCTION x_fn()`. A stale
function silently writes NULL into the `_current` peer for every new column, so
`migrated_schema_matches_fresh_install` fingerprints `pg_get_functiondef` too.

### 3. Add validation tests

Add a `#[cfg(test)] mod tests` block to each new module. The shared helpers in
`crate::schema::tests` do the heavy lifting:

In `archive/v1/mod.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::Source;
    use crate::schema::tests::validate_schema_source;

    #[test]
    fn test_validate_archive_v1() {
        validate_schema_source(&Source);
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

In `current/mod.rs`, update the version constant:

```rust
pub const VERSION: usize = 2; // <- bumped
```

### 6. Refresh the DDL snapshots

`current/` and the new delta are gated by committed DDL snapshots, so
regenerate them once the SQL is final:

```bash
TD_SNAPSHOT_UPDATE=1 cargo test -p td-schema --lib ddl_snapshot
```

The changes show up as reviewable SQL diffs under `current/snapshots/` and
`deltas/snapshots/v1_to_v2/`.

### 7. Verify

```bash
cargo test -p td-schema
cargo test -p td-database
```

Both halves of the equivalence gate run: td-database's `td-test` feature
(enabled by its self dev-dependency and by CI) carries the `sqlite` backend on
top of the postgres-only defaults.

Both should pass. The key tests are:

| Test | Crate | What it checks |
|------|-------|---------------|
| `test_schema_ddl_snapshot` | td-schema | `current/` SQL matches the committed `current/snapshots/` DDL (the gate on the editable baseline). |
| `test_archive_ddl_snapshots` | td-schema | Frozen archive SQL matches each archive's committed `snapshots/`. |
| `test_delta_ddl_snapshots` | td-schema | Frozen delta SQL matches the committed `deltas/snapshots/vN_to_vM/`. |
| `test_validate_current_schema` | td-schema | Current schema produces non-empty output on every backend. |
| `test_validate_archive_vN` | td-schema | Each archive produces non-empty output on every backend. |
| `test_validate_delta_vN_to_vM` | td-schema | Each delta is single-step, non-empty, and bumps `db_version`. |
| `test_migration_guard_archives_exist` | td-schema | An archive exists for every version in `1..current::VERSION`. |
| `test_migration_guard_deltas_exist` | td-schema | A delta exists for every version in `2..=current::VERSION`. |
| `test_migration_guard_deltas_bump_db_version` | td-schema | Every wired delta ends with the `db_version` bump — the ladder-resume contract, enforced even if a delta's module test forgets `validate_delta`. |
| `migrated_schema_matches_fresh_install_{sqlite,postgres}` | td-database | **The migration invariant**: `archive(N) + deltas ≡ current`, applied to a real database and compared object by object — columns, defaults, view bodies, trigger *and function* bodies, indexes and RLS policies. Run both: each backend is blind to a hazard the other exposes. |

The td-schema tests only prove an archive and a delta *exist* and render
non-empty SQL — the equivalence itself is proven only by
`migrated_schema_matches_fresh_install`, so treat that one as the gate.

## Why intra-major deltas were reset for 2.0

Pre-2.0 carried a chain of deltas (v5→v6 … v9→v10 → v10→v11) and matching
frozen archives. In-place renames during the 2.0 cut (floating → logical /
physical, function variant → function patch) propagated into the archived SQL
and broke the archive/delta equivalence the migration-invariant tests asserted.
Rather than reconstruct snapshots for a path that nobody will travel — 2.0 is
a fresh install — the archives and deltas were emptied. The machinery (the
trait, the dispatch functions, the guard tests, the upgrade loop in
`UpgradeTx::upgrade_db_version`) stays in place, ready to be filled by the first
2.x in-major bump.

## Edition handling (`edition` row)

Unrelated to `db_version` and `tabsdata_version`. The `edition` row in
`tabsdata_system` tracks the Tabsdata edition (community / enterprise / etc.)
and has its own check/upgrade path in `td_database::pool`. See that module for
details.
