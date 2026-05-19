//
// Copyright 2026 Tabs Data Inc.
//

//! Generation-only tests for the schema system.
//!
//! These tests validate SQL generation without touching a database. The tests
//! that execute SQL against a real database live in `td-database` where
//! `TdPool` / `DynamicPool` are available.

use super::backend::Backend;
use super::sql::TriggerSpec;
use super::{SchemaDelta, SchemaVersion, archive, current, deltas};
use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;

const ALL_BACKENDS: &[Backend] = &[Backend::Sqlite, Backend::Postgres, Backend::MySql];

/// Cached `current::build()` output per backend. The consistency tests
/// only read this — `Schema::build()` is deterministic (gated by
/// `test_schema_build_is_deterministic`) so a single build per backend
/// is reused across every test that just inspects the statement list.
static BUILD_SQLITE: LazyLock<Vec<String>> = LazyLock::new(|| current::build(Backend::Sqlite));
static BUILD_POSTGRES: LazyLock<Vec<String>> = LazyLock::new(|| current::build(Backend::Postgres));
static BUILD_MYSQL: LazyLock<Vec<String>> = LazyLock::new(|| current::build(Backend::MySql));

fn cached_build(b: Backend) -> &'static [String] {
    match b {
        Backend::Sqlite => &BUILD_SQLITE,
        Backend::Postgres => &BUILD_POSTGRES,
        Backend::MySql => &BUILD_MYSQL,
    }
}

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
        assert!(!stmts.is_empty(), "SchemaVersion v{} produced empty output on {b:?}", S::VERSION);
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
    assert_eq!(D::FROM_VERSION + 1, D::TO_VERSION, "Delta must be single-step: got {}->{}", D::FROM_VERSION, D::TO_VERSION,);

    for &b in ALL_BACKENDS {
        let stmts = D::up(b);
        assert!(!stmts.is_empty(), "SchemaDelta v{}->v{} produced empty output on {b:?}", D::FROM_VERSION, D::TO_VERSION,);
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
        println!("{b:?} (idx {}): {}", b.index(), compute_fingerprint(&current::build(b)),);
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
            assert!(!result.unwrap().is_empty(), "Archive v{v} produced empty output on {b:?}");
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
            assert!(!result.unwrap().is_empty(), "Delta v{v} produced empty output on {b:?}");
        }
    }
}

// ---------------------------------------------------------------------------
// Tier 1 consistency tests over the ENTITIES slice
// ---------------------------------------------------------------------------

/// T2: every table created by any entity has a unique name.
/// Built on top of `Schema::build` output rather than the entity API
/// directly because sea_query's `TableCreateStatement` does not expose
/// a stable name accessor — parsing the emitted CREATE TABLE is more
/// portable.
#[test]
fn test_table_names_are_unique() {
    let mut seen = HashSet::new();
    for sql in cached_build(Backend::Sqlite) {
        if let Some(name) = parse_create_table_name(sql) {
            assert!(seen.insert(name.clone()), "duplicate table: {name:?}");
        }
    }
}

/// T3: `ENTITIES` is topologically sorted by foreign-key dependency.
/// For each entity in order, every `REFERENCES <table>` in its
/// `CREATE TABLE` SQL targets a table declared by an earlier entity
/// (self-references allowed).
#[test]
fn test_entities_are_fk_topologically_sorted() {
    let b = Backend::Sqlite;
    let mut declared: HashSet<String> = HashSet::new();
    for (i, e) in current::ENTITIES.iter().enumerate() {
        let mut entity_tables: Vec<String> = Vec::new();
        for t in e.create_tables() {
            let sql = b.build_schema(t);
            let table = parse_create_table_name(&sql).unwrap_or_else(|| panic!("could not parse table name from: {sql}"));
            for fk in fk_references(&sql) {
                if entity_tables.contains(&fk) {
                    continue; // self-reference within the same entity
                }
                assert!(declared.contains(&fk), "ENTITIES[{i}]: table {table:?} references {fk:?} which is not yet declared in ENTITIES",);
            }
            entity_tables.push(table.clone());
            declared.insert(table);
        }
    }
}

/// T2b: every view created by any entity has a unique name.
/// Same rationale as `test_table_names_are_unique`: catches accidental
/// duplicates that SQLite would accept but Postgres/MySQL would reject
/// at the second `CREATE VIEW`.
#[test]
fn test_view_names_are_unique() {
    let mut seen = HashSet::new();
    for sql in cached_build(Backend::Sqlite) {
        if let Some(name) = parse_create_view_name(sql) {
            assert!(seen.insert(name.clone()), "duplicate view: {name:?}");
        }
    }
}

/// T2c: every index has a unique name across all entities.
#[test]
fn test_index_names_are_unique() {
    let mut seen = HashSet::new();
    for sql in cached_build(Backend::Sqlite) {
        if let Some(name) = parse_create_index_name(sql) {
            assert!(seen.insert(name.clone()), "duplicate index: {name:?}");
        }
    }
}

/// T2d: every DB trigger has a unique name across all entities. The
/// SQL trigger namespace is global on every backend, so collisions are
/// CREATE-time errors. SQLite encodes `_no_update`/`_no_delete` suffixes
/// for immutable triggers — those suffixed names also must be unique.
#[test]
fn test_db_trigger_names_are_unique() {
    let mut seen = HashSet::new();
    for sql in cached_build(Backend::Sqlite) {
        if let Some(name) = parse_create_trigger_name(sql) {
            assert!(seen.insert(name.clone()), "duplicate trigger: {name:?}");
        }
    }
}

/// T5: `Schema::build` is deterministic — two calls produce
/// byte-identical output. Catches HashMap-iteration leaks and other
/// hidden nondeterminism.
#[test]
fn test_schema_build_is_deterministic() {
    for &b in ALL_BACKENDS {
        let first = current::build(b);
        let second = current::build(b);
        assert_eq!(first, second, "current::build({b:?}) not deterministic");
    }
}

/// T6: `Schema::build` output is segmented by phase — CREATE TABLE,
/// then CREATE TRIGGER (and supporting CREATE FUNCTION on Postgres),
/// then CREATE VIEW, then CREATE INDEX, then INSERT. A regression that
/// reorders the phase loops in `Schema::build` fails here.
#[test]
fn test_schema_build_phase_ordering() {
    for &b in ALL_BACKENDS {
        let stmts = cached_build(b);
        let phases: Vec<Phase> = stmts.iter().map(|s| classify_stmt(s)).collect();
        // Permitted ordering: a strictly non-decreasing sequence of phase
        // indices. Within a phase, statements may interleave (e.g.
        // Postgres emits CREATE FUNCTION between CREATE TRIGGERs).
        let mut current_phase = Phase::Table as usize;
        for (i, p) in phases.iter().enumerate() {
            let pi = *p as usize;
            assert!(
                pi >= current_phase,
                "stmt {i} on {b:?} is phase {p:?} but later phases already started ({:?}); SQL: {}",
                phase_name(current_phase),
                stmts[i],
            );
            current_phase = pi;
        }
    }
}

/// T7: every `_current` table has a corresponding source table declared
/// earlier in the build output. Scans CREATE TABLE statements (in
/// emission order) for the `_current` suffix and asserts the base name
/// appeared first.
#[test]
fn test_current_peers_are_paired() {
    let mut tables_in_order: Vec<String> = Vec::new();
    for sql in cached_build(Backend::Sqlite) {
        if let Some(name) = parse_create_table_name(sql) {
            tables_in_order.push(name);
        }
    }
    for (i, name) in tables_in_order.iter().enumerate() {
        if let Some(base) = name.strip_suffix("_current") {
            let base_idx = tables_in_order.iter().position(|n| n == base).unwrap_or_else(|| panic!("{name} has no source table {base}"));
            assert!(base_idx < i, "{name} declared at index {i} but its source {base} is at {base_idx}");
        }
    }
}

// ---- helpers ----

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Table = 0,
    Trigger = 1,
    View = 2,
    Index = 3,
    Insert = 4,
}

fn phase_name(idx: usize) -> &'static str {
    match idx {
        0 => "Table",
        1 => "Trigger",
        2 => "View",
        3 => "Index",
        4 => "Insert",
        _ => "?",
    }
}

fn classify_stmt(sql: &str) -> Phase {
    let s = sql.trim_start();
    // Postgres emits `CREATE OR REPLACE FUNCTION ..._fn()` alongside
    // each CREATE TRIGGER. Group both under Trigger.
    if s.starts_with("CREATE TRIGGER") || s.starts_with("CREATE OR REPLACE FUNCTION") {
        Phase::Trigger
    } else if s.starts_with("CREATE TABLE") {
        Phase::Table
    } else if s.starts_with("CREATE VIEW") {
        Phase::View
    } else if s.starts_with("CREATE INDEX") || s.starts_with("CREATE UNIQUE INDEX") {
        Phase::Index
    } else if s.starts_with("INSERT") {
        Phase::Insert
    } else {
        panic!("unrecognized statement kind in Schema::build output: {sql}");
    }
}

/// Read one identifier starting at the beginning of `s`, accepting
/// optional `"…"` or `` `…` `` quoting. The unquoted form is terminated
/// by whitespace; on top of that, `extra_terminators` (e.g. `(`) end
/// the unquoted ident too. Returns `None` if no identifier is found.
fn read_ident(s: &str, extra_terminators: &[char]) -> Option<String> {
    let s = s.trim_start();
    let (quote_char, body) = match s.chars().next()? {
        '"' => (Some('"'), &s[1..]),
        '`' => (Some('`'), &s[1..]),
        _ => (None, s),
    };
    let end = match quote_char {
        Some(q) => body.find(q)?,
        None => body.find(|c: char| c.is_whitespace() || extra_terminators.contains(&c))?,
    };
    Some(body[..end].to_string())
}

/// Read the identifier following the first matching `prefix` in `sql`
/// (case-sensitive, full-string strip from start). Returns None if no
/// prefix matches.
fn parse_ident_after(sql: &str, prefixes: &[&str], extra_terminators: &[char]) -> Option<String> {
    let s = sql.trim_start();
    let rest = prefixes.iter().find_map(|p| s.strip_prefix(p))?;
    read_ident(rest, extra_terminators)
}

fn parse_create_table_name(sql: &str) -> Option<String> {
    parse_ident_after(sql, &["CREATE TABLE IF NOT EXISTS ", "CREATE TABLE "], &['('])
}

fn parse_create_view_name(sql: &str) -> Option<String> {
    parse_ident_after(sql, &["CREATE VIEW "], &[])
}

fn parse_create_index_name(sql: &str) -> Option<String> {
    parse_ident_after(sql, &["CREATE UNIQUE INDEX ", "CREATE INDEX "], &[])
}

/// Captures both `CREATE TRIGGER <name>` and the Postgres
/// `CREATE OR REPLACE FUNCTION <name>_fn()` so the trigger and its
/// supporting function share a uniqueness check.
fn parse_create_trigger_name(sql: &str) -> Option<String> {
    parse_ident_after(sql, &["CREATE TRIGGER ", "CREATE OR REPLACE FUNCTION "], &['('])
}

/// Extract every table name appearing as the target of `REFERENCES` in
/// the given SQL.
fn fk_references(sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = sql;
    while let Some(idx) = rest.find("REFERENCES ") {
        rest = &rest[idx + "REFERENCES ".len()..];
        if let Some(name) = read_ident(rest, &['(']) {
            out.push(name);
        }
    }
    out
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
            assert_eq!(first, *count, "{kind} v{v}: {first_b:?} produced {first} stmts but {b:?} produced {count}",);
        }
    };

    for v in 1..current::Schema::VERSION {
        let counts = ALL_BACKENDS.iter().map(|&b| (b, archive::build(v, b).len())).collect();
        assert_consistent("Archive", v, counts);
    }
    for v in 2..=current::Schema::VERSION {
        let counts = ALL_BACKENDS.iter().map(|&b| (b, deltas::get(v, b).len())).collect();
        assert_consistent("Delta", v, counts);
    }
}

// ---------------------------------------------------------------------------
// Cross-validation tests
// ---------------------------------------------------------------------------

/// Extract column names from a SQLite `CREATE TABLE` SQL.
///
/// A column declaration is `"name" <type-token> …`. References inside
/// `FOREIGN KEY ("col") REFERENCES "tab" ("col")` are followed by `)` or
/// `(`, so they don't qualify. The CREATE-TABLE table-name itself is
/// followed by ` (`, so it's also excluded.
fn parse_create_table_columns(sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = sql;
    while let Some(start) = rest.find('"') {
        rest = &rest[start + 1..];
        let Some(end) = rest.find('"') else { break };
        let ident = rest[..end].to_string();
        rest = &rest[end + 1..];
        let after = rest.trim_start();
        // A column def has an alphabetic type token next; REFERENCES targets
        // are followed by `(` or `)`.
        if after.chars().next().map(|c| c.is_ascii_alphabetic()).unwrap_or(false) {
            out.push(ident);
        }
    }
    out
}

/// Extract `(target_table, target_col)` for every `REFERENCES "T" ("C")` in
/// the SQL (SQLite dialect).
fn fk_target_table_columns(sql: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut rest = sql;
    while let Some(idx) = rest.find("REFERENCES ") {
        rest = &rest[idx + "REFERENCES ".len()..];
        let Some(table) = read_ident(rest, &['(']) else { continue };
        // Skip past the table ident — it may have been quoted.
        let after_table = rest.trim_start();
        let after_table = after_table.strip_prefix('"').unwrap_or(after_table);
        if let Some(close) = after_table.find('"') {
            rest = &after_table[close + 1..];
        }
        // Skip whitespace and the opening `(`.
        let after_open = rest.trim_start().strip_prefix('(').unwrap_or(rest);
        if let Some(col) = read_ident(after_open, &[')']) {
            out.push((table, col));
        }
    }
    out
}

/// Cache table_name → column_name list across one full build.
static COLS_BY_TABLE: LazyLock<HashMap<String, Vec<String>>> = LazyLock::new(|| {
    let mut map = HashMap::new();
    for sql in cached_build(Backend::Sqlite) {
        if let Some(t) = parse_create_table_name(sql) {
            map.insert(t, parse_create_table_columns(sql));
        }
    }
    map
});

/// T9: every `REFERENCES "T" ("C")` in any `CREATE TABLE` resolves to a
/// column `C` that `T` actually declares. Beyond
/// [`test_entities_are_fk_topologically_sorted`] (which checks the table
/// alone), this catches FK targets that name a column not present on the
/// referenced table — an Iden enum carrying a stale variant, for instance.
#[test]
fn test_fk_target_columns_resolve() {
    for sql in cached_build(Backend::Sqlite) {
        for (t, c) in fk_target_table_columns(sql) {
            let cols = COLS_BY_TABLE.get(&t).unwrap_or_else(|| panic!("FK in {sql} references unknown table {t:?}"));
            assert!(cols.iter().any(|x| x == &c), "FK target {t}({c}) does not appear in {t}'s declared columns {cols:?}",);
        }
    }
}

/// T10: every `maintain_current` / `versioned_current` trigger's column
/// list is a subset of *both* its source and target tables' columns.
/// Catches the case where a column gets added to the event log but not to
/// the `_current` peer (or vice versa).
#[test]
fn test_maintain_current_trigger_columns_match_tables() {
    for e in current::ENTITIES {
        for spec in e.create_db_triggers() {
            let (source, target, cols): (String, String, Vec<String>) = match &spec {
                TriggerSpec::MaintainCurrent(d) => {
                    (d.source_table.to_string(), d.target_table.to_string(), d.columns.iter().map(|c| c.to_string()).collect())
                }
                TriggerSpec::VersionedCurrent(d) => {
                    (d.source_table.to_string(), d.target_table.to_string(), d.columns.iter().map(|c| c.to_string()).collect())
                }
                _ => continue,
            };
            let source_cols = COLS_BY_TABLE.get(&source).unwrap_or_else(|| panic!("trigger source table {source:?} not declared"));
            let target_cols = COLS_BY_TABLE.get(&target).unwrap_or_else(|| panic!("trigger target table {target:?} not declared"));
            for c in &cols {
                assert!(source_cols.iter().any(|sc| sc == c), "{source}: maintain_current column {c:?} not present (have {source_cols:?})",);
                assert!(target_cols.iter().any(|tc| tc == c), "{target}: maintain_current column {c:?} not present (have {target_cols:?})",);
            }
        }
    }
}
