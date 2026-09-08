//
// Copyright 2026 Tabsdata Inc.
//

//! Deltas that migrate *data*, against a real database. `migration` proves the
//! ladder lands on the right schema; these prove the rows it rewrote hold the
//! right values, and that the upgrade seam leaves project scope open for them
//! — a data delta that silently matches nothing still reports success.

use crate::components::{AdminPool, ApiserverComponent};
use crate::config::{DatabaseConfig, PostgresConnect, RlsRoles, Schema};
use crate::error::DbError;
use crate::pool::TdPool;
use crate::pool::options::pg_connect_options;
use crate::test_utils::server;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use td_schema::schema::archive;
use td_schema::{ACROSS_PROJECTS_ROLE, PROJECT_ID_GUC};

use super::{execute, scalars};

async fn connect(identity: &PostgresConnect) -> PgPool {
    PgPoolOptions::new()
        .max_connections(1)
        .connect_with(pg_connect_options(identity))
        .await
        .unwrap()
}

/// Every FK-satisfying row a `physical_function_runs` insert needs, at the
/// point the v2 archive is installed. Three collections exercise the
/// backfill's COALESCE ladder: a function-version override, a collection's own
/// `base_path`, and the fall-through to the project's.
async fn seed_run_chain(db: &TdPool) {
    execute(
        db,
        "INSERT INTO projects (id, name, description, base_path, created_on, created_by_id, \
           modified_on, modified_by_id) \
         VALUES ('p0', 'p0', '', '/from-project', now(), 'u0', now(), 'u0')",
    )
    .await;
    execute(
        db,
        "INSERT INTO groups (id, project_id, name, description, created_on, created_by_id, \
           modified_on, modified_by_id, system) \
         VALUES ('g0', 'p0', 'g0', '', now(), 'u0', now(), 'u0', false)",
    )
    .await;
    for (id, base_path, fn_base_path) in [
        ("c-fn-override", "'/from-collection'", "'/from-function'"),
        ("c-own", "'/from-collection'", "NULL"),
        ("c-inherits", "NULL", "NULL"),
    ] {
        execute(
            db,
            &format!(
                "INSERT INTO collections (id, project_id, name, description, created_on, \
                   created_by_id, modified_on, modified_by_id, group_id, base_path, \
                   collection_type) \
                 VALUES ('{id}', 'p0', '{id}', '', now(), 'u0', now(), 'u0', 'g0', \
                   {base_path}, 'R')"
            ),
        )
        .await;
        execute(
            db,
            &format!(
                "INSERT INTO functions (id, project_id, collection_id, name, description, \
                   decorator, runtime_values, function_id, base_path, stage_id, \
                   stage_base_path, storage_version, bundle_id, snippet, defined_on, \
                   defined_by_id, status) \
                 VALUES ('fv-{id}', 'p0', '{id}', 'f0', '', 'P', '', 'f-{id}', \
                   {fn_base_path}, 's0', '/s', 'V1', 'b0', '', now(), 'u0', 'A')"
            ),
        )
        .await;
        execute(
            db,
            &format!(
                "INSERT INTO executions (id, project_id, collection_id, function_version_id, \
                   triggered_on, triggered_by_id) \
                 VALUES ('e-{id}', 'p0', '{id}', 'fv-{id}', now(), 'u0')"
            ),
        )
        .await;
        execute(
            db,
            &format!(
                "INSERT INTO transactions (id, project_id, collection_id, execution_id, \
                   transaction_by, transaction_key, triggered_on, triggered_by_id, group_id) \
                 VALUES ('t-{id}', 'p0', '{id}', 'e-{id}', 'F', 'k0', now(), 'u0', 'g0')"
            ),
        )
        .await;
        // v3 -> v4 puts the logical run on the attempt row, so every attempt
        // needs one — as every minted attempt has.
        execute(
            db,
            &format!(
                "INSERT INTO logical_function_runs (id, project_id, collection_id, \
                   execution_id, transaction_id, function_id, function_version_id, trigger, \
                   triggered_on, triggered_by_id, status, group_id) \
                 VALUES ('l-{id}', 'p0', '{id}', 'e-{id}', 't-{id}', 'f-{id}', 'fv-{id}', 'M', \
                   now(), 'u0', 'S', 'g0')"
            ),
        )
        .await;
        execute(
            db,
            &format!(
                "INSERT INTO physical_function_runs (id, project_id, collection_id, \
                   function_version_id, execution_id, transaction_id, triggered_on, \
                   triggered_by_id, trigger, function_id, status) \
                 VALUES ('r-{id}', 'p0', '{id}', 'fv-{id}', 'e-{id}', 't-{id}', now(), 'u0', \
                   'M', 'f-{id}', 'R')"
            ),
        )
        .await;
    }
}

/// v2 → v3 stamps every existing run with the prefix it resolves to today:
/// the collection's own `base_path`, falling back to the project's.
#[tokio::test]
async fn v2_to_v3_backfills_run_base_path_postgres() {
    let db = crate::test_utils::postgres::pool().await;
    db.admin
        .upgrade_source(archive::source(2))
        .await
        .expect("installing archive v2");
    seed_run_chain(&db.serving).await;

    db.admin
        .upgrade(&ApiserverComponent)
        .await
        .expect("upgrading v2 -> current");

    let stamped = scalars(
        &db.serving,
        "SELECT id || '=' || base_path FROM physical_function_runs ORDER BY id",
    )
    .await;
    assert_eq!(
        stamped,
        [
            "r-c-fn-override=/from-function",
            "r-c-inherits=/from-project",
            "r-c-own=/from-collection"
        ],
        "the backfill must resolve the function override, then the collection's \
         prefix, then the project's"
    );
}

/// Function-version rows against the v3 archive: partition `f-a` holds a
/// three-version chain plus a same-instant pair at its head (frozen-then-
/// deleted, ordered only by id); partition `f-b` holds a single version.
fn seed_v3_function_versions_sql() -> Vec<String> {
    let mut out = vec![
        "INSERT INTO projects (id, name, description, base_path, created_on, created_by_id, \
           modified_on, modified_by_id) \
         VALUES ('p0', 'p0', '', '/p', now(), 'u0', now(), 'u0')"
            .to_string(),
        "INSERT INTO groups (id, project_id, name, description, created_on, created_by_id, \
           modified_on, modified_by_id, system) \
         VALUES ('g0', 'p0', 'g0', '', now(), 'u0', now(), 'u0', false)"
            .to_string(),
        "INSERT INTO collections (id, project_id, name, description, created_on, \
           created_by_id, modified_on, modified_by_id, group_id, base_path, collection_type) \
         VALUES ('c0', 'p0', 'c0', '', now(), 'u0', now(), 'u0', 'g0', NULL, 'R')"
            .to_string(),
    ];
    for (id, function_id, defined_on) in [
        ("fv1", "f-a", "'2026-01-01T00:00:00Z'"),
        ("fv2", "f-a", "'2026-01-02T00:00:00Z'"),
        ("fv3", "f-a", "'2026-01-03T00:00:00Z'"),
        ("fv4", "f-a", "'2026-01-03T00:00:00Z'"),
        ("fv5", "f-b", "'2026-01-02T00:00:00Z'"),
    ] {
        out.push(format!(
            "INSERT INTO functions (id, project_id, collection_id, name, description, \
               decorator, runtime_values, function_id, stage_id, stage_base_path, \
               storage_version, bundle_id, snippet, defined_on, defined_by_id, status) \
             VALUES ('{id}', 'p0', 'c0', 'f0', '', 'P', '', '{function_id}', 's0', '/s', \
               'V1', 'b0', '', {defined_on}, 'u0', 'A')"
        ));
    }
    out
}

async fn seed_function_versions(db: &TdPool) {
    for stmt in seed_v3_function_versions_sql() {
        execute(db, &stmt).await;
    }
}

/// v3 → v4 backfills `valid_to = LEAD(defined_on)` ordered `(defined_on, id)`
/// — the same ordering the close-out trigger uses — leaving one open row per
/// partition and a zero-length interval for the loser of a same-instant pair,
/// and drops the `_current` snapshot tables.
#[tokio::test]
async fn v3_to_v4_backfills_valid_to_postgres() {
    let db = crate::test_utils::postgres::pool().await;
    db.admin
        .upgrade_source(archive::source(3))
        .await
        .expect("installing archive v3");
    seed_function_versions(&db.serving).await;

    db.admin
        .upgrade(&ApiserverComponent)
        .await
        .expect("upgrading v3 -> current");

    let chains = scalars(
        &db.serving,
        "SELECT id || '=' || CASE WHEN valid_to = 'infinity' THEN 'infinity' \
           ELSE to_char(valid_to AT TIME ZONE 'UTC', 'YYYY-MM-DD') END \
         FROM functions ORDER BY id",
    )
    .await;
    assert_eq!(
        chains,
        [
            "fv1=2026-01-02",
            "fv2=2026-01-03",
            "fv3=2026-01-03",
            "fv4=infinity",
            "fv5=infinity"
        ],
        "the backfill must chain intervals per partition, close the lower-id \
         row of a same-instant pair at zero length, and leave the latest open"
    );
    let leftovers = scalars(
        &db.serving,
        "SELECT tablename FROM pg_tables \
         WHERE schemaname = current_schema() AND tablename LIKE '%\\_current'",
    )
    .await;
    assert!(
        leftovers.is_empty(),
        "the _current snapshot tables must be dropped: {leftovers:?}"
    );
}

/// A function deleted under v3, as v3 wrote it: an Active version, then a `D`
/// tombstone version; its user table frozen under the tombstone; its system
/// table and system dependency re-registered Active under the tombstone.
fn seed_v3_deleted_function_sql() -> Vec<String> {
    let mut out = Vec::new();
    for (id, defined_on, status) in [
        ("fv6", "'2026-01-01T00:00:00Z'", "A"),
        ("fv7", "'2026-01-02T00:00:00Z'", "D"),
    ] {
        out.push(format!(
            "INSERT INTO functions (id, project_id, collection_id, name, description, \
               decorator, runtime_values, function_id, stage_id, stage_base_path, \
               storage_version, bundle_id, snippet, defined_on, defined_by_id, status) \
             VALUES ('{id}', 'p0', 'c0', 'fd', '', 'P', '', 'f-d', 's0', '/s', \
               'V1', 'b0', '', {defined_on}, 'u0', '{status}')"
        ));
    }
    for (id, table_id, name, fv, defined_on, status) in [
        ("tv1", "t-u", "u", "fv6", "'2026-01-01T00:00:00Z'", "A"),
        ("tv2", "t-u", "u", "fv7", "'2026-01-02T00:00:00Z'", "F"),
        (
            "tv3",
            "t-s",
            "td.fn_state__f-d",
            "fv6",
            "'2026-01-01T00:00:00Z'",
            "A",
        ),
        (
            "tv4",
            "t-s",
            "td.fn_state__f-d",
            "fv7",
            "'2026-01-02T00:00:00Z'",
            "A",
        ),
    ] {
        out.push(format!(
            "INSERT INTO tables (id, project_id, collection_id, table_id, name, function_id, \
               function_version_id, function_param_pos, private, partitioned, defined_on, \
               defined_by_id, status, table_type) \
             VALUES ('{id}', 'p0', 'c0', '{table_id}', '{name}', 'f-d', '{fv}', 0, false, \
               false, {defined_on}, 'u0', '{status}', 'R')"
        ));
    }
    for (id, defined_on) in [
        ("dv1", "'2026-01-01T00:00:00Z'"),
        ("dv2", "'2026-01-02T00:00:00Z'"),
    ] {
        out.push(format!(
            "INSERT INTO dependencies (id, project_id, collection_id, dependency_id, function_id, \
               table_collection_id, table_function_id, table_id, table_versions, dep_pos, status, \
               defined_on, defined_by_id, system) \
             VALUES ('{id}', 'p0', 'c0', 'd-s', 'f-d', 'c0', 'f-d', 't-s', 'HEAD', -1, 'A', \
               {defined_on}, 'u0', true)"
        ));
    }
    out
}

/// v3 → v4 closes what a v3 delete left open under the tombstone: the system
/// table and the system dependency end with the function, at the tombstone's
/// instant, while the frozen user table stays open and adoptable.
#[tokio::test]
async fn v3_to_v4_closes_the_leftovers_of_a_deleted_function_postgres() {
    let db = crate::test_utils::postgres::pool().await;
    db.admin
        .upgrade_source(archive::source(3))
        .await
        .expect("installing archive v3");
    seed_function_versions(&db.serving).await;
    for stmt in seed_v3_deleted_function_sql() {
        execute(&db.serving, &stmt).await;
    }

    db.admin
        .upgrade(&ApiserverComponent)
        .await
        .expect("upgrading v3 -> current");

    let open_functions = scalars(
        &db.serving,
        "SELECT id FROM functions WHERE function_id = 'f-d' AND valid_to = 'infinity'",
    )
    .await;
    assert!(
        open_functions.is_empty(),
        "the deleted function has no open version"
    );

    let tables = scalars(
        &db.serving,
        "SELECT id || '=' || status || '@' || CASE WHEN valid_to = 'infinity' THEN 'infinity' \
           ELSE to_char(valid_to AT TIME ZONE 'UTC', 'YYYY-MM-DD') END \
         FROM tables WHERE function_id = 'f-d' ORDER BY id",
    )
    .await;
    assert_eq!(
        tables,
        [
            "tv1=A@2026-01-02",
            "tv2=F@infinity",
            "tv3=A@2026-01-02",
            "tv4=A@2026-01-02"
        ],
        "the frozen user table stays open; the system table closes with the function"
    );

    let dependencies = scalars(
        &db.serving,
        "SELECT id || '@' || CASE WHEN valid_to = 'infinity' THEN 'infinity' \
           ELSE to_char(valid_to AT TIME ZONE 'UTC', 'YYYY-MM-DD') END \
         FROM dependencies WHERE function_id = 'f-d' ORDER BY id",
    )
    .await;
    assert_eq!(
        dependencies,
        ["dv1@2026-01-02", "dv2@2026-01-02"],
        "the re-registered system dependency closes with the function"
    );
}

/// Every log shape the attempts backfill reads: `lfr-a` retried twice (the
/// last attempt never in the log), `lfr-b` never dispatched, `lfr-c` bound
/// then voided; `ltdv-a`'s third materialization is reachable only through
/// its producer.
async fn seed_attempts(db: &TdPool) {
    for stmt in [
        "INSERT INTO projects (id, name, description, base_path, created_on, created_by_id, \
           modified_on, modified_by_id) \
         VALUES ('p0', 'p0', '', '/p', now(), 'u0', now(), 'u0')",
        "INSERT INTO groups (id, project_id, name, description, created_on, created_by_id, \
           modified_on, modified_by_id, system) \
         VALUES ('g0', 'p0', 'g0', '', now(), 'u0', now(), 'u0', false)",
        "INSERT INTO collections (id, project_id, name, description, created_on, \
           created_by_id, modified_on, modified_by_id, group_id, base_path, collection_type) \
         VALUES ('c0', 'p0', 'c0', '', now(), 'u0', now(), 'u0', 'g0', NULL, 'R')",
        "INSERT INTO functions (id, project_id, collection_id, name, description, decorator, \
           runtime_values, function_id, stage_id, stage_base_path, storage_version, bundle_id, \
           snippet, defined_on, defined_by_id, status) \
         VALUES ('fv1', 'p0', 'c0', 'f0', '', 'P', '', 'f-a', 's0', '/s', 'V1', 'b0', '', \
           '2026-01-01T00:00:00Z', 'u0', 'A')",
        "INSERT INTO executions (id, project_id, name, collection_id, function_version_id, \
           triggered_on, triggered_by_id, function_id) \
         VALUES ('e0', 'p0', 'e0', 'c0', 'fv1', '2026-01-01T00:00:00Z', 'u0', 'f-a')",
        "INSERT INTO transactions (id, project_id, collection_id, execution_id, transaction_by, \
           transaction_key, triggered_on, triggered_by_id, group_id) \
         VALUES ('t0', 'p0', 'c0', 'e0', 'F', 'k0', '2026-01-01T00:00:00Z', 'u0', 'g0')",
    ] {
        execute(db, stmt).await;
    }

    for lfr in ["lfr-a", "lfr-b", "lfr-c"] {
        execute(
            db,
            &format!(
                "INSERT INTO logical_function_runs (id, project_id, collection_id, execution_id, \
                   transaction_id, function_id, function_version_id, trigger, triggered_on, \
                   triggered_by_id, status, group_id) \
                 VALUES ('{lfr}', 'p0', 'c0', 'e0', 't0', '{lfr}', 'fv1', 'M', \
                   '2026-01-01T00:00:00Z', 'u0', 'C', 'g0')"
            ),
        )
        .await;
    }

    // `function_id` pairs an attempt with its logical run when the log does not.
    for (pfr, function_id, triggered_on) in [
        ("pfr1", "lfr-a", "2026-01-01T01:00:00Z"),
        ("pfr2", "lfr-a", "2026-01-01T02:00:00Z"),
        ("pfr3", "lfr-a", "2026-01-01T03:00:00Z"),
        ("pfr4", "lfr-c", "2026-01-01T01:00:00Z"),
    ] {
        execute(
            db,
            &format!(
                "INSERT INTO physical_function_runs (id, project_id, collection_id, \
                   function_version_id, execution_id, transaction_id, triggered_on, \
                   triggered_by_id, trigger, function_id, status, base_path) \
                 VALUES ('{pfr}', 'p0', 'c0', 'fv1', 'e0', 't0', '{triggered_on}', 'u0', \
                   'M', '{function_id}', 'D', '/p')"
            ),
        )
        .await;
    }

    execute(
        db,
        "INSERT INTO logical_table_data_versions (id, project_id, collection_id, execution_id, \
           transaction_id, table_id, producer_logical_function_run_id, function_param_pos, \
           private, triggered_on, triggered_by_id, group_id) \
         VALUES ('ltdv-a', 'p0', 'c0', 'e0', 't0', 'tb-a', 'lfr-a', 0, false, \
           '2026-01-01T00:00:00Z', 'u0', 'g0')",
    )
    .await;

    for (ptdv, pfr, triggered_on) in [
        ("ptdv1", "pfr1", "2026-01-01T01:00:00Z"),
        ("ptdv2", "pfr2", "2026-01-01T02:00:00Z"),
        ("ptdv3", "pfr3", "2026-01-01T03:00:00Z"),
    ] {
        execute(
            db,
            &format!(
                "INSERT INTO physical_table_data_versions (id, project_id, collection_id, \
                   table_id, name, table_version_id, function_version_id, execution_id, \
                   transaction_id, physical_function_run_id, function_param_pos, triggered_on, \
                   base_path) \
                 VALUES ('{ptdv}', 'p0', 'c0', 'tb-a', 'tb', 'tv0', 'fv1', 'e0', 't0', '{pfr}', \
                   0, '{triggered_on}', '/p')"
            ),
        )
        .await;
    }

    for (id, logical, status, concrete, set_on) in [
        ("lp1", "lfr-a", "P", "NULL", "2026-01-01T00:00:00Z"),
        ("lp2", "lfr-a", "B", "'pfr1'", "2026-01-01T01:00:00Z"),
        ("lp3", "lfr-a", "B", "'pfr2'", "2026-01-01T02:00:00Z"),
        ("lp4", "lfr-b", "P", "NULL", "2026-01-01T00:00:00Z"),
        ("lp5", "lfr-c", "P", "NULL", "2026-01-01T00:00:00Z"),
        ("lp6", "lfr-c", "B", "'pfr4'", "2026-01-01T01:00:00Z"),
        ("lp7", "lfr-c", "V", "NULL", "2026-01-01T03:00:00Z"),
    ] {
        execute(
            db,
            &format!(
                "INSERT INTO logical_function_run_pointers (id, project_id, \
                   logical_function_run_id, status, physical_function_run_id, set_on, set_by_id, \
                   reason) \
                 VALUES ('{id}', 'p0', '{logical}', '{status}', {concrete}, '{set_on}', 'u0', 'ID')"
            ),
        )
        .await;
    }

    for (id, status, concrete, set_on) in [
        ("tp1", "P", "NULL", "2026-01-01T00:00:00Z"),
        ("tp2", "B", "'ptdv1'", "2026-01-01T01:00:00Z"),
        ("tp3", "B", "'ptdv2'", "2026-01-01T02:00:00Z"),
    ] {
        execute(
            db,
            &format!(
                "INSERT INTO logical_table_data_version_pointers (id, project_id, \
                   logical_table_data_version_id, status, physical_table_data_version_id, set_on, \
                   set_by_id, reason) \
                 VALUES ('{id}', 'p0', 'ltdv-a', '{status}', {concrete}, '{set_on}', 'u0', 'ID')"
            ),
        )
        .await;
    }
}

/// v3 → v4 drops the pointer logs: every attempt gets an interval by
/// `triggered_on`, the newest is open, and `Pending`/`Voided` leave no trace.
#[tokio::test]
async fn v3_to_v4_backfills_attempts_postgres() {
    let db = crate::test_utils::postgres::pool().await;
    db.admin
        .upgrade_source(archive::source(3))
        .await
        .expect("installing archive v3");
    seed_attempts(&db.serving).await;

    db.admin
        .upgrade(&ApiserverComponent)
        .await
        .expect("upgrading v3 -> current");

    let stamp = |col: &str| {
        format!(
            "CASE WHEN {col} IS NULL THEN 'null' WHEN {col} = 'infinity' THEN 'infinity' \
               WHEN {col} = '-infinity' THEN '-infinity' \
               ELSE to_char({col} AT TIME ZONE 'UTC', 'HH24') END"
        )
    };

    let runs = scalars(
        &db.serving,
        &format!(
            "SELECT id || '=' || logical_function_run_id || '/' || {} \
             FROM physical_function_runs ORDER BY id",
            stamp("valid_to"),
        ),
    )
    .await;
    assert_eq!(
        runs,
        [
            "pfr1=lfr-a/02",
            "pfr2=lfr-a/03",
            "pfr3=lfr-a/infinity",
            "pfr4=lfr-c/infinity",
        ],
        "each attempt closes at its successor's triggered_on; the newest stays \
         open, whether or not the log ever bound it, and a void leaves no trace"
    );

    let versions = scalars(
        &db.serving,
        &format!(
            "SELECT id || '=' || logical_table_data_version_id || '/' || {} \
             FROM physical_table_data_versions ORDER BY id",
            stamp("valid_to"),
        ),
    )
    .await;
    assert_eq!(
        versions,
        [
            "ptdv1=ltdv-a/02",
            "ptdv2=ltdv-a/03",
            "ptdv3=ltdv-a/infinity",
        ],
        "the materialization the log never bound still resolves its logical \
         version, through its producer"
    );

    let leftovers = scalars(
        &db.serving,
        "SELECT tablename FROM pg_tables \
         WHERE schemaname = current_schema() AND tablename LIKE '%pointers%'",
    )
    .await;
    assert!(
        leftovers.is_empty(),
        "the pointer logs must be dropped: {leftovers:?}"
    );

    let functions = scalars(
        &db.serving,
        "SELECT proname FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace \
         WHERE n.nspname = current_schema() AND proname LIKE '%pointers%'",
    )
    .await;
    assert!(
        functions.is_empty(),
        "a dropped table's trigger functions must go with it: {functions:?}"
    );
}

/// The upgrade transaction runs outside project scope, so a data delta over a
/// project-scoped table sees every row without opting in. Asserted from inside
/// the transaction, the way a delta would observe it.
#[tokio::test]
async fn the_upgrade_transaction_is_not_project_scoped_postgres() {
    let db = crate::test_utils::postgres::pool().await;
    let mut tx = db.admin.begin_upgrade().await.unwrap();
    tx.execute_batch(&[format!(
        "DO $$ BEGIN \
           IF NOT pg_has_role(current_user, '{ACROSS_PROJECTS_ROLE}', 'USAGE') THEN \
             RAISE EXCEPTION 'upgrade transaction is project-scoped: current_user = %', \
               current_user; \
           END IF; \
         END $$"
    )])
    .await
    .expect("the upgrade transaction must open with project scope bypassed");
    tx.commit().await.unwrap();
}

/// `FORCE ROW LEVEL SECURITY` binds a non-superuser role, so an upgrade —
/// which connects as the schema owner — would be fail-closed on an unset
/// `app.project_id` and its data deltas would match nothing while still
/// reporting success. Provisioning therefore grants the owner the
/// `across_projects` membership. This pins both halves against a real
/// non-superuser owner, which the rest of the suite cannot show (its
/// factories keep the schema superuser-owned, and a superuser bypasses RLS
/// unconditionally): the membership admits an unscoped write, and a role
/// without it still sees nothing.
#[tokio::test]
async fn force_rls_binds_a_non_superuser_owner_postgres() {
    server::ensure_started().await;
    let su = connect(&server::admin_base()).await;
    let uid = td_common::id::id();
    let (owner, schema) = (format!("own_{uid}"), format!("sch_{uid}"));

    for stmt in [
        format!("CREATE ROLE \"{owner}\" LOGIN PASSWORD 'own_pw'"),
        format!("CREATE SCHEMA \"{schema}\" AUTHORIZATION \"{owner}\""),
    ] {
        sqlx::query(sqlx::AssertSqlSafe(stmt.clone()))
            .execute(&su)
            .await
            .unwrap();
    }

    let across = format!("ap_{uid}");
    let tenant = format!("ip_{uid}");
    for stmt in [
        format!("CREATE ROLE \"{across}\" NOBYPASSRLS"),
        format!("CREATE ROLE \"{tenant}\" NOBYPASSRLS"),
        format!("GRANT \"{ACROSS_PROJECTS_ROLE}\" TO \"{across}\""),
        format!("GRANT \"{across}\" TO \"{owner}\""),
        format!("GRANT \"{tenant}\" TO \"{owner}\""),
        format!("GRANT USAGE ON SCHEMA \"{schema}\" TO \"{across}\", \"{tenant}\""),
    ] {
        sqlx::query(sqlx::AssertSqlSafe(stmt.clone()))
            .execute(&su)
            .await
            .unwrap();
    }

    let mut identity = server::admin_base();
    identity.username = owner.clone();
    identity.password = "own_pw".to_owned();
    let as_owner = connect(&identity).await;
    let pred = format!("project_id = current_setting('{PROJECT_ID_GUC}', true)");
    for stmt in [
        format!("CREATE TABLE \"{schema}\".scoped (project_id text NOT NULL)"),
        format!("INSERT INTO \"{schema}\".scoped VALUES ('p0')"),
        format!("GRANT ALL ON \"{schema}\".scoped TO \"{across}\", \"{tenant}\""),
        format!("ALTER TABLE \"{schema}\".scoped ENABLE ROW LEVEL SECURITY"),
        format!("ALTER TABLE \"{schema}\".scoped FORCE ROW LEVEL SECURITY"),
        format!(
            "CREATE POLICY isolation ON \"{schema}\".scoped USING ({pred}) WITH CHECK ({pred})"
        ),
        format!(
            "CREATE POLICY across ON \"{schema}\".scoped TO \"{ACROSS_PROJECTS_ROLE}\" \
             USING (true) WITH CHECK (true)"
        ),
    ] {
        sqlx::query(sqlx::AssertSqlSafe(stmt.clone()))
            .execute(&as_owner)
            .await
            .unwrap();
    }

    let visible_rows = async |member: bool| -> u64 {
        let mut tx = as_owner.begin().await.unwrap();
        let role = match member {
            true => &across,
            false => &tenant,
        };
        sqlx::query(sqlx::AssertSqlSafe(format!("SET LOCAL ROLE \"{role}\"")))
            .execute(&mut *tx)
            .await
            .unwrap();
        let updated = sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE \"{schema}\".scoped SET project_id = project_id"
        )))
        .execute(&mut *tx)
        .await
        .unwrap()
        .rows_affected();
        tx.rollback().await.unwrap();
        updated
    };

    assert_eq!(
        visible_rows(false).await,
        0,
        "unscoped and without the membership, FORCE RLS hides every row"
    );
    assert_eq!(
        visible_rows(true).await,
        1,
        "the across_projects membership admits it unscoped"
    );

    as_owner.close().await;
    for stmt in [
        format!("DROP SCHEMA \"{schema}\" CASCADE"),
        format!("DROP OWNED BY \"{across}\""),
        format!("DROP ROLE \"{across}\""),
        format!("DROP OWNED BY \"{tenant}\""),
        format!("DROP ROLE \"{tenant}\""),
        format!("DROP ROLE \"{owner}\""),
    ] {
        sqlx::query(sqlx::AssertSqlSafe(stmt.clone()))
            .execute(&su)
            .await
            .unwrap();
    }
}

/// The ladder's data deltas against a **non-superuser-owned** schema — the
/// only shape where RLS actually binds the upgrader, and the one the rest of
/// this suite cannot reach (its factories keep schemas superuser-owned, and a
/// superuser bypasses RLS unconditionally).
///
/// It starts at v3, whose frozen policy admits cross-project work through
/// `LEGACY_BYPASS_RLS_GUC` rather than the `across_projects` role, so it pins
/// that the upgrade seam opens scope for *both* policy shapes a ladder spans.
/// Without it the backfills match nothing: the `valid_to` chain would be all
/// NULL and its `SET NOT NULL` would fail the ladder.
#[tokio::test]
async fn ladder_backfills_data_under_a_non_superuser_owner_postgres() {
    server::ensure_started().await;
    let su = connect(&server::admin_base()).await;
    let uid = td_common::id::id();
    let (owner, schema) = (format!("own_{uid}"), format!("sch_{uid}"));
    let roles = RlsRoles {
        in_project: format!("ip_{uid}"),
        across_projects: format!("ap_{uid}"),
    };
    let (tenant, across) = (&roles.in_project, &roles.across_projects);

    // Everything cluster-global is pre-provisioned by the superuser, the way a
    // DBA does it for an external database: the owner has no CREATEROLE, so
    // `ensure_rls_roles` must find each role and membership already in place.
    for stmt in [
        format!("CREATE ROLE \"{owner}\" LOGIN PASSWORD 'own_pw'"),
        format!("CREATE ROLE \"{tenant}\" NOBYPASSRLS"),
        format!("CREATE ROLE \"{across}\" NOBYPASSRLS"),
        format!(
            "DO $$ BEGIN \
               IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '{ACROSS_PROJECTS_ROLE}') THEN \
                 CREATE ROLE \"{ACROSS_PROJECTS_ROLE}\" NOBYPASSRLS NOLOGIN; \
               END IF; \
             END $$"
        ),
        format!("GRANT \"{ACROSS_PROJECTS_ROLE}\" TO \"{across}\""),
        format!("GRANT \"{ACROSS_PROJECTS_ROLE}\" TO \"{owner}\""),
        format!("GRANT \"{tenant}\" TO \"{owner}\""),
        format!("GRANT \"{across}\" TO \"{owner}\""),
        format!("CREATE SCHEMA \"{schema}\" AUTHORIZATION \"{owner}\""),
    ] {
        sqlx::query(sqlx::AssertSqlSafe(stmt.clone()))
            .execute(&su)
            .await
            .unwrap();
    }

    let mut identity = server::admin_base();
    identity.username = owner.clone();
    identity.password = "own_pw".to_owned();
    let config = DatabaseConfig {
        conn: identity.clone(),
        schema: Some(Schema::new(&schema)),
        roles: Some(roles.clone()),
        pool: crate::test_utils::test_pool_config(),
    };
    let admin = AdminPool::connect(&config).await.expect("owner admin pool");
    admin
        .upgrade_source(archive::source(3))
        .await
        .expect("installing archive v3 as the owner");

    // Seed as the owner under v3 semantics: its policy matches on the GUC, so
    // scoping to the seeded project satisfies every `WITH CHECK`.
    let as_owner = connect(&identity).await;
    let mut tx = as_owner.begin().await.unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "SET LOCAL search_path TO \"{schema}\""
    )))
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT set_config('{PROJECT_ID_GUC}', 'p0', true)"
    )))
    .execute(&mut *tx)
    .await
    .unwrap();
    for stmt in seed_v3_function_versions_sql() {
        sqlx::query(sqlx::AssertSqlSafe(stmt.clone()))
            .execute(&mut *tx)
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();

    admin
        .upgrade(&ApiserverComponent)
        .await
        .expect("upgrading v3 -> current as the owner");

    let chains: Vec<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT id || '=' || CASE WHEN valid_to = 'infinity' THEN 'infinity' \
           ELSE to_char(valid_to AT TIME ZONE 'UTC', 'YYYY-MM-DD') END \
         FROM \"{schema}\".functions ORDER BY id"
    )))
    .fetch_all(&as_owner)
    .await
    .unwrap();
    assert_eq!(
        chains,
        [
            "fv1=2026-01-02",
            "fv2=2026-01-03",
            "fv3=2026-01-03",
            "fv4=infinity",
            "fv5=infinity"
        ],
        "the ladder's backfill must reach every row of an RLS-bound owner's schema"
    );

    admin.close().await;
    as_owner.close().await;
    for stmt in [
        format!("DROP SCHEMA \"{schema}\" CASCADE"),
        format!("DROP OWNED BY \"{tenant}\""),
        format!("DROP OWNED BY \"{across}\""),
        format!("DROP OWNED BY \"{owner}\""),
        format!("DROP ROLE \"{tenant}\""),
        format!("DROP ROLE \"{across}\""),
        format!("DROP ROLE \"{owner}\""),
    ] {
        sqlx::query(sqlx::AssertSqlSafe(stmt.clone()))
            .execute(&su)
            .await
            .unwrap();
    }
}

/// The external-database shape that cannot self-heal: the owner may hold the
/// marker without `ADMIN` on it, so `ensure_rls_roles` warns instead of
/// granting it onward and the `across_projects` role ends up admitting
/// nobody. The upgrade must refuse before the ladder opens, naming the grant.
#[tokio::test]
async fn upgrade_refuses_a_missing_across_projects_membership_postgres() {
    server::ensure_started().await;
    let su = connect(&server::admin_base()).await;
    let uid = td_common::id::id();
    let (owner, schema) = (format!("own_{uid}"), format!("sch_{uid}"));
    let roles = RlsRoles {
        in_project: format!("ip_{uid}"),
        across_projects: format!("ap_{uid}"),
    };
    let (tenant, across) = (&roles.in_project, &roles.across_projects);

    for stmt in [
        format!("CREATE ROLE \"{owner}\" LOGIN PASSWORD 'own_pw'"),
        format!("CREATE ROLE \"{tenant}\" NOBYPASSRLS"),
        format!("CREATE ROLE \"{across}\" NOBYPASSRLS"),
        format!("GRANT \"{tenant}\" TO \"{owner}\""),
        format!("GRANT \"{across}\" TO \"{owner}\""),
        // The owner can switch into both roles, but nothing grants the marker
        // to the cross-project one and the owner cannot grant it itself.
        format!("CREATE SCHEMA \"{schema}\" AUTHORIZATION \"{owner}\""),
    ] {
        sqlx::query(sqlx::AssertSqlSafe(stmt.clone()))
            .execute(&su)
            .await
            .unwrap();
    }

    let mut identity = server::admin_base();
    identity.username = owner.clone();
    identity.password = "own_pw".to_owned();
    let admin = AdminPool::connect(&DatabaseConfig {
        conn: identity,
        schema: Some(Schema::new(&schema)),
        roles: Some(roles.clone()),
        pool: crate::test_utils::test_pool_config(),
    })
    .await
    .expect("owner admin pool");

    let error = admin
        .upgrade(&ApiserverComponent)
        .await
        .expect_err("the upgrade must refuse without the membership");
    let DbError::MissingAcrossProjectsMembership(sql) = &error else {
        panic!("expected MissingAcrossProjectsMembership, got {error:?}");
    };
    assert!(
        sql.contains(ACROSS_PROJECTS_ROLE) && sql.contains(across.as_str()),
        "the error must name the grant a DBA has to run: {sql}"
    );

    admin.close().await;
    for stmt in [
        format!("DROP SCHEMA \"{schema}\" CASCADE"),
        format!("DROP OWNED BY \"{tenant}\""),
        format!("DROP OWNED BY \"{across}\""),
        format!("DROP OWNED BY \"{owner}\""),
        format!("DROP ROLE \"{tenant}\""),
        format!("DROP ROLE \"{across}\""),
        format!("DROP ROLE \"{owner}\""),
    ] {
        sqlx::query(sqlx::AssertSqlSafe(stmt.clone()))
            .execute(&su)
            .await
            .unwrap();
    }
}
