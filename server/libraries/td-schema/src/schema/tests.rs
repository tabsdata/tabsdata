//
// Copyright 2026 Tabsdata Inc.
//

//! Generation-only tests for the schema system.
//!
//! These tests validate SQL generation without touching a database. The tests
//! that execute SQL against a real database live in `td-database` where
//! `TdPool` is available.

use super::{CurrentSource, SchemaDelta, SchemaSource, archive, current, deltas, derived_scripts, install_script};
use crate::schema::render::{render_schema, update_db_version};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::LazyLock;

/// Cached `current::build()` output. The consistency tests only read this —
/// the install script is deterministic (gated by
/// `test_schema_build_is_deterministic`) so a single build is reused across
/// every test that just inspects the statement list.
static BUILD: LazyLock<Vec<String>> = LazyLock::new(current::build);

fn cached_build() -> &'static [String] {
    &BUILD
}

/// Validate that a [`SchemaSource`] produces a non-empty install script.
pub(crate) fn validate_schema_source(source: &dyn SchemaSource) {
    let stmts = install_script(source);
    assert!(!stmts.is_empty(), "schema v{} produced an empty install script", source.current_version());
}

#[test]
fn test_validate_current_schema() {
    validate_schema_source(&CurrentSource);
}

/// Validate that a [`SchemaDelta`] implementation:
/// - Is a single-step migration (`FROM_VERSION + 1 == TO_VERSION`)
/// - Produces non-empty output
/// - Last statement updates `db_version` to `TO_VERSION` (the bump guard)
///
/// Each delta module's own tests call this against its `Migration` impl.
#[allow(dead_code)]
pub(crate) fn validate_delta<D: SchemaDelta>() {
    assert_eq!(D::FROM_VERSION + 1, D::TO_VERSION, "Delta must be single-step: got {}->{}", D::FROM_VERSION, D::TO_VERSION,);

    let stmts = D::up();
    assert!(!stmts.is_empty(), "SchemaDelta v{}->v{} produced empty output", D::FROM_VERSION, D::TO_VERSION,);
    let last = stmts.last().unwrap();
    assert!(
        last.contains(&format!("'{}'", D::TO_VERSION)),
        "Last statement of delta v{}->v{} must update db_version to {}. Got: {last}",
        D::FROM_VERSION,
        D::TO_VERSION,
        D::TO_VERSION,
    );
}

// Synthetic deltas exercise the migration harness independently of the real
// ladder (its application half lives in td-database's `shared_schema` tests).
struct SyntheticDelta;

impl SchemaDelta for SyntheticDelta {
    const FROM_VERSION: usize = 1;
    const TO_VERSION: usize = 2;

    fn retired() -> Vec<String> {
        vec![]
    }

    fn up() -> Vec<String> {
        vec![
            "ALTER TABLE users ADD COLUMN nickname text".to_string(),
            update_db_version(Self::TO_VERSION),
        ]
    }
}

#[test]
fn test_synthetic_delta_satisfies_validate_delta() {
    validate_delta::<SyntheticDelta>();
}

struct MissingBumpDelta;

impl SchemaDelta for MissingBumpDelta {
    const FROM_VERSION: usize = 1;
    const TO_VERSION: usize = 2;

    fn retired() -> Vec<String> {
        vec![]
    }

    fn up() -> Vec<String> {
        vec!["ALTER TABLE users ADD COLUMN nickname text".to_string()]
    }
}

#[test]
#[should_panic(expected = "must update db_version")]
fn test_validate_delta_rejects_missing_version_bump() {
    validate_delta::<MissingBumpDelta>();
}

#[test]
fn test_update_db_version_sql_is_stable() {
    let last = SyntheticDelta::up().pop().unwrap();
    assert_eq!(last, "UPDATE tabsdata_system SET value = '2' WHERE name = 'db_version'");
}

fn snapshot_dir(sub: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/schema").join(sub)
}

/// The gate on `current/`: a schema edit surfaces as a reviewable SQL diff
/// against the committed snapshots.
#[test]
fn test_schema_ddl_snapshot() {
    crate::snapshot::assert_snapshot(&snapshot_dir("current/snapshots"), current::build);
}

/// Guards every frozen archive's committed snapshots.
#[allow(clippy::reversed_empty_ranges)]
#[test]
fn test_archive_ddl_snapshots() {
    for v in 1..current::VERSION {
        crate::snapshot::assert_snapshot(&snapshot_dir(&format!("archive/v{v}/snapshots")), || archive::build(v));
    }
}

/// Guards every frozen delta's committed snapshots.
#[allow(clippy::reversed_empty_ranges)]
#[test]
fn test_delta_ddl_snapshots() {
    for v in 2..=current::VERSION {
        crate::snapshot::assert_snapshot(&snapshot_dir(&format!("deltas/snapshots/v{}_to_v{v}", v - 1)), || deltas::get(v).up);
    }
}

/// On a version bump, archives must exist for every version in
/// `1..CURRENT_VERSION` and produce non-empty output. Guards against bumping
/// `CURRENT_VERSION` without creating `archive/vN/`.
#[allow(clippy::reversed_empty_ranges)]
#[test]
fn test_migration_guard_archives_exist() {
    for v in 1..current::VERSION {
        let result = std::panic::catch_unwind(|| archive::build(v));
        assert!(
            result.is_ok(),
            "Missing archive for version {v}. When bumping CURRENT_VERSION to {}, copy current/ to archive/v{v}/.",
            current::VERSION
        );
        assert!(!result.unwrap().is_empty(), "Archive v{v} produced empty output");
    }
}

/// On a version bump, deltas must exist for every version in
/// `2..=CURRENT_VERSION` and produce non-empty output. Guards against bumping
/// `CURRENT_VERSION` without creating `deltas/vN_to_vM.rs`.
#[allow(clippy::reversed_empty_ranges)]
#[test]
fn test_migration_guard_deltas_exist() {
    for v in 2..=current::VERSION {
        let result = std::panic::catch_unwind(|| deltas::get(v).up);
        assert!(
            result.is_ok(),
            "Missing delta for version {v}. When bumping CURRENT_VERSION to {}, create deltas/v{}_to_v{v}.rs.",
            current::VERSION,
            v - 1
        );
        assert!(!result.unwrap().is_empty(), "Delta v{v} produced empty output");
    }
}

/// Every wired delta must end with the `db_version` bump — without it a
/// mid-ladder failure replays the committed delta on retry. Unlike
/// `validate_delta`, this needs no per-module opt-in.
#[allow(clippy::reversed_empty_ranges)]
#[test]
fn test_migration_guard_deltas_bump_db_version() {
    for v in 2..=current::VERSION {
        let stmts = deltas::get(v).up;
        assert_eq!(stmts.last(), Some(&update_db_version(v)), "Delta v{}->v{v} must end with `update_db_version({v})`", v - 1,);
    }
}

fn derived_drops(source: &dyn SchemaSource) -> BTreeSet<String> {
    derived_scripts(source).0.into_iter().collect()
}

/// Every delta's declared [`SchemaDelta::retired`] set is exactly what the
/// archives prove disappeared across that rung: the derived objects standing at
/// `FROM_VERSION` that the target no longer declares. Without this the
/// declaration rests on the author's judgement; with it, a forgotten trigger or
/// index is named on the spot.
#[allow(clippy::reversed_empty_ranges)]
#[test]
fn test_retired_matches_archive_diff() {
    for v in 2..=current::VERSION {
        let from = archive::source(v - 1);
        let to: &dyn SchemaSource = if v == current::VERSION {
            &CurrentSource
        } else {
            archive::source(v)
        };
        let expected: BTreeSet<String> = derived_drops(from).difference(&derived_drops(to)).cloned().collect();
        let declared: BTreeSet<String> = deltas::get(v).retired.into_iter().collect();
        let missing: Vec<&String> = expected.difference(&declared).collect();
        let extra: Vec<&String> = declared.difference(&expected).collect();
        assert!(
            missing.is_empty() && extra.is_empty(),
            "delta v{}->v{v} `retired()` disagrees with the archives.\n\
             missing (standing at v{} and left behind by the ladder): {missing:#?}\n\
             extra (not standing at v{}): {extra:#?}",
            v - 1,
            v - 1,
            v - 1,
        );
    }
}

/// T2: every table created by any entity has a unique name.
/// Built on top of `Schema::build` output rather than the entity API
/// directly because sea_query's `TableCreateStatement` does not expose
/// a stable name accessor — parsing the emitted CREATE TABLE is more
/// portable.
#[test]
fn test_table_names_are_unique() {
    let mut seen = HashSet::new();
    for sql in cached_build() {
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
    let mut declared: HashSet<String> = HashSet::new();
    for (i, e) in current::ENTITIES.iter().enumerate() {
        let mut entity_tables: Vec<String> = Vec::new();
        for t in e.create_tables() {
            let sql = render_schema(t);
            let table = parse_create_table_name(&sql).unwrap_or_else(|| panic!("could not parse table name from: {sql}"));
            // Before its own FKs are checked: a table may reference itself
            // (or an earlier table of the same entity) within its own CREATE.
            entity_tables.push(table.clone());
            for fk in fk_references(&sql) {
                if entity_tables.contains(&fk) {
                    continue;
                }
                assert!(declared.contains(&fk), "ENTITIES[{i}]: table {table:?} references {fk:?} which is not yet declared in ENTITIES",);
            }
            declared.insert(table);
        }
    }
}

/// T2b: every view created by any entity has a unique name.
/// Same rationale as `test_table_names_are_unique`: catches accidental
/// duplicates that would be rejected at the second `CREATE VIEW`.
#[test]
fn test_view_names_are_unique() {
    let mut seen = HashSet::new();
    for sql in cached_build() {
        if let Some(name) = parse_create_view_name(sql) {
            assert!(seen.insert(name.clone()), "duplicate view: {name:?}");
        }
    }
}

/// T2c: every index has a unique name across all entities.
#[test]
fn test_index_names_are_unique() {
    let mut seen = HashSet::new();
    for sql in cached_build() {
        if let Some(name) = parse_create_index_name(sql) {
            assert!(seen.insert(name.clone()), "duplicate index: {name:?}");
        }
    }
}

/// T2d: every DB trigger has a unique name across all entities. Immutable
/// triggers carry `_no_update`/`_no_delete` suffixes — those suffixed names
/// must be unique too.
#[test]
fn test_db_trigger_names_are_unique() {
    let mut seen = HashSet::new();
    for sql in cached_build() {
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
    assert_eq!(current::build(), current::build(), "current::build() not deterministic");
}

/// T6: the install script is segmented by phase — CREATE TABLE, then
/// CREATE TRIGGER (and its supporting CREATE FUNCTION), then CREATE VIEW,
/// then CREATE INDEX, then INSERT, then RLS. A regression that reorders
/// `install_script`'s composition fails here.
#[test]
fn test_schema_build_phase_ordering() {
    let stmts = cached_build();
    let phases: Vec<Phase> = stmts.iter().map(|s| classify_stmt(s)).collect();
    // Permitted ordering: a strictly non-decreasing sequence of phase indices.
    // Within a phase, statements may interleave (CREATE FUNCTION between
    // CREATE TRIGGERs).
    let mut current_phase = Phase::Table as usize;
    for (i, p) in phases.iter().enumerate() {
        let pi = *p as usize;
        assert!(
            pi >= current_phase,
            "stmt {i} is phase {p:?} but later phases already started ({:?}); SQL: {}",
            phase_name(current_phase),
            stmts[i],
        );
        current_phase = pi;
    }
}

/// T11: every table with an SCD2 close-out trigger declares a `valid_to`
/// column and a unique `WHERE valid_to = 'infinity'` partial index — the
/// one-open-row invariant every interval read depends on.
#[test]
fn test_scd2_tables_have_valid_to_and_open_uniqueness() {
    let build = cached_build();
    let scd2_tables: Vec<String> = build
        .iter()
        .filter_map(|sql| {
            let rest = sql.strip_prefix("CREATE TRIGGER ")?;
            let name = rest.split_whitespace().next()?;
            let table = name.strip_suffix("__scd2_close")?;
            sql.contains("BEFORE INSERT").then(|| table.to_string())
        })
        .collect();
    assert!(!scd2_tables.is_empty(), "no SCD2 close-out triggers found");
    for table in &scd2_tables {
        let create = build
            .iter()
            .find(|sql| parse_create_table_name(sql).as_deref() == Some(table))
            .unwrap_or_else(|| panic!("no CREATE TABLE for {table}"));
        assert!(create.contains(r#""valid_to" timestamp with time zone NOT NULL"#), "{table} lacks valid_to: {create}");
        assert!(
            build.iter().any(|sql| sql.starts_with("CREATE UNIQUE INDEX")
                && sql.contains(&format!(r#"ON "{table}""#))
                && sql.contains(r#""valid_to" = ('infinity'::timestamptz)"#)),
            "{table} lacks the unique open-interval partial index",
        );
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Table = 0,
    Trigger = 1,
    View = 2,
    Index = 3,
    Insert = 4,
    Rls = 5,
}

fn phase_name(idx: usize) -> &'static str {
    match idx {
        0 => "Table",
        1 => "Trigger",
        2 => "View",
        3 => "Index",
        4 => "Insert",
        5 => "Rls",
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
    } else if s.starts_with("ALTER TABLE") || s.starts_with("CREATE POLICY") {
        // Row-level security (ALTER TABLE … RLS / SET DEFAULT, CREATE
        // POLICY), emitted after seeds.
        Phase::Rls
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

/// Extract column names from a `CREATE TABLE` statement.
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
/// the SQL.
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
    for sql in cached_build() {
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
    for sql in cached_build() {
        for (t, c) in fk_target_table_columns(sql) {
            let cols = COLS_BY_TABLE.get(&t).unwrap_or_else(|| panic!("FK in {sql} references unknown table {t:?}"));
            assert!(cols.iter().any(|x| x == &c), "FK target {t}({c}) does not appear in {t}'s declared columns {cols:?}",);
        }
    }
}

/// T11: the hand-maintained `project_scoped_tables` list is exactly the set of
/// tables that declare a `project_id` column. The schema build attaches an RLS
/// policy to every table in that list, so a table that grows a `project_id`
/// column but is omitted from the list gets NO row-level-security policy — a
/// silent cross-tenant leak. This makes that divergence (in either direction)
/// a build failure. `COLS_BY_TABLE` is authoritative for membership.
#[test]
fn test_project_scoped_tables_match_project_id_columns() {
    // `roles` carries a `project_id` column for per-project roles, but it is a
    // plain FK to `projects` — roles are managed GLOBALLY (a single namespace
    // shared across tenants), NOT tenant-RLS-scoped. So `roles` is deliberately
    // absent from project_scoped_tables and must be excluded from this
    // column-vs-list cross-check.
    let with_project_id: HashSet<String> = COLS_BY_TABLE
        .iter()
        .filter(|(t, cols)| t.as_str() != "roles" && cols.iter().any(|c| c == "project_id"))
        .map(|(t, _)| t.clone())
        .collect();
    let listed: HashSet<String> = current::project_scoped_tables().iter().map(|t| t.to_string()).collect();

    let missing: Vec<&String> = with_project_id.difference(&listed).collect();
    assert!(
        missing.is_empty(),
        "tables declare a `project_id` column but are absent from \
         project_scoped_tables, so they get NO RLS policy (silent cross-tenant \
         leak): {missing:?}. Add them to project_scoped_tables in current/mod.rs.",
    );

    let stale: Vec<&String> = listed.difference(&with_project_id).collect();
    assert!(
        stale.is_empty(),
        "project_scoped_tables lists tables that have no `project_id` column: \
         {stale:?}. Remove them from the list or add the column.",
    );
}

/// PostgreSQL truncates identifiers at `NAMEDATALEN - 1` = 63 bytes, silently:
/// a longer index name is created under its truncated form, so two names that
/// agree in their first 63 bytes collide and the second `CREATE` fails.
#[test]
fn test_index_names_fit_postgres_identifiers() {
    let build = cached_build();
    let long: Vec<String> = build.iter().filter_map(|sql| parse_create_index_name(sql)).filter(|name| name.len() > 63).collect();
    assert!(long.is_empty(), "index names past PostgreSQL's 63-byte limit: {long:?}");
}

/// An index whose columns are a leading prefix of another index's, under the
/// same partial predicate, is dead weight: every scan the shorter one can serve
/// the longer one serves too, and only the longer is maintained for free.
#[test]
fn test_no_index_is_a_prefix_of_another() {
    let build = cached_build();
    let mut declared: Vec<(String, String, Vec<String>, String)> = Vec::new();
    for sql in build.iter() {
        let Some(name) = parse_create_index_name(sql) else {
            continue;
        };
        let (table, rest) = sql.split_once(" ON \"").and_then(|(_, r)| r.split_once('"')).expect("index on a table");
        let cols = rest
            .trim_start()
            .strip_prefix('(')
            .and_then(|c| c.split_once(") WHERE ").or_else(|| c.rsplit_once(')')))
            .expect("index columns");
        let columns = cols.0.split(", ").map(|c| c.trim().to_string()).collect();
        declared.push((table.to_string(), name, columns, cols.1.to_string()));
    }
    for (table, name, columns, predicate) in &declared {
        let subsumer = declared.iter().find(|(t, n, c, p)| {
            t == table && n != name && p == predicate && c.len() > columns.len() && &c[..columns.len()] == columns.as_slice()
        });
        assert!(subsumer.is_none(), "{name} is a leading prefix of {}", subsumer.unwrap().1);
    }
}
