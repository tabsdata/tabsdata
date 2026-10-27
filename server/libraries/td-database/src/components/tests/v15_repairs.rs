//
// Copyright 2026 Tabsdata Inc.
//

//! v14 → v15 repairs what earlier releases left behind: state tables,
//! dependencies and triggers that outlive their function go; a transaction
//! all of whose runs ended, none through an attempt that did, ends at its
//! latest known instant and its execution with it; a function worker still
//! live after its attempt ended fails at the attempt's end.

use crate::components::ApiserverComponent;
use crate::test_utils::domain::PostgresDomain;
use crate::test_utils::postgres::pool;
use td_schema::schema::archive;

use super::{execute, scalars};

const BASE: &[&str] = &[
    "INSERT INTO projects (id, name, description, base_path, created_on, created_by_id, \
       modified_on, modified_by_id) \
     VALUES ('p0', 'p0', '', '/p', '2026-01-01 06:00:00+00', 'u0', now(), 'u0')",
    "INSERT INTO groups (id, project_id, name, description, created_on, created_by_id, \
       modified_on, modified_by_id, system, kind) \
     VALUES ('g0', 'p0', 'g0', '', now(), 'u0', now(), 'u0', false, 'R')",
    "INSERT INTO collections (id, project_id, name, description, created_on, created_by_id, \
       modified_on, modified_by_id, group_id, collection_type) \
     VALUES ('c0', 'p0', 'c0', '', now(), 'u0', now(), 'u0', 'g0', 'R')",
    "INSERT INTO functions (id, project_id, collection_id, name, description, decorator, \
       runtime_values, function_id, stage_id, stage_base_path, storage_version, bundle_id, \
       snippet, valid_from, defined_by_id, status, enabled, valid_to, function_type) \
     VALUES (1, 'p0', 'c0', 'f', '', 'P', '', 'f0', 's0', '/s', 'V1', 'b0', '', \
       '2026-01-01 06:00:00+00', 'u0', 'A', true, 'infinity', 'R')",
];

fn execution(id: u32) -> String {
    format!(
        "INSERT INTO executions (id, project_id, collection_id, function_version_id, function_id, \
           triggered_on, triggered_by_id) \
         VALUES ({id}, 'p0', 'c0', 1, 'f0', '2026-01-01 07:00:00+00', 'u0')"
    )
}

fn transaction(id: u32, execution: u32, ended_on: &str) -> String {
    format!(
        "INSERT INTO transactions (id, project_id, collection_id, execution_id, transaction_by, \
           transaction_key, triggered_on, triggered_by_id, group_id, status, ended_on) \
         VALUES ({id}, 'p0', 'c0', {execution}, 'F', 'k{id}', '2026-01-01 07:00:00+00', 'u0', \
           'g0', 'X', {ended_on})"
    )
}

fn run(id: u32, execution: u32, transaction: u32, status: &str) -> String {
    format!(
        "INSERT INTO logical_function_runs (id, project_id, collection_id, execution_id, \
           transaction_id, function_id, function_version_id, trigger, triggered_on, \
           triggered_by_id, status, group_id) \
         VALUES ({id}, 'p0', 'c0', {execution}, {transaction}, 'f{id}', 1, 'M', \
           '2026-01-01 07:00:00+00', 'u0', '{status}', 'g0')"
    )
}

fn attempt(
    id: u32,
    execution: u32,
    transaction: u32,
    run: u32,
    status: &str,
    ended_on: &str,
) -> String {
    format!(
        "INSERT INTO physical_function_runs (id, project_id, collection_id, function_version_id, \
           execution_id, transaction_id, valid_from, triggered_by_id, trigger, function_id, \
           status, base_path, logical_function_run_id, valid_to, ended_on) \
         VALUES ({id}, 'p0', 'c0', 1, {execution}, {transaction}, '2026-01-01 07:30:00+00', \
           'u0', 'M', 'f0', '{status}', '/r', {run}, 'infinity', {ended_on})"
    )
}

fn worker(id: u32, worker_type: &str, attempt: &str) -> String {
    format!(
        "INSERT INTO workers (id, project_id, collection_id, function_version_id, \
           physical_function_run_id, started_on, status, worker_type, worker_enabled) \
         VALUES ({id}, 'p0', 'c0', 1, {attempt}, '2026-01-01 07:30:00+00', 'R', \
           '{worker_type}', true)"
    )
}

#[td_test::domain("apiserver_postgres_internals")]
#[tokio::test]
async fn v14_to_v15_settles_what_v13_left_unsettled_postgres(
    domain_apiserver_postgres_internals: PostgresDomain,
) {
    let db = pool(&domain_apiserver_postgres_internals).await;
    db.admin
        .upgrade_source(archive::source(14))
        .await
        .expect("installing archive v14");
    let seed = [
        execution(10),
        execution(11),
        transaction(20, 10, "NULL"),
        run(30, 10, 20, "X"),
        transaction(21, 10, "'2026-01-01 09:00:00+00'"),
        run(31, 10, 21, "C"),
        attempt(41, 10, 21, 31, "D", "'2026-01-01 09:00:00+00'"),
        transaction(22, 11, "NULL"),
        run(32, 11, 22, "R"),
        attempt(40, 11, 22, 32, "F", "'2026-01-01 08:30:00+00'"),
        attempt(42, 11, 22, 32, "R", "NULL"),
        worker(50, "F", "40"),
        worker(51, "F", "42"),
        worker(52, "CT", "NULL"),
    ];
    for stmt in BASE.iter().map(|stmt| stmt.to_string()).chain(seed) {
        execute(&db.serving, stmt).await;
    }

    db.admin
        .upgrade(&ApiserverComponent)
        .await
        .expect("upgrading v14 -> current");

    let ends = |table: &str| {
        format!(
            "SELECT id || '=' || COALESCE(to_char(ended_on AT TIME ZONE 'UTC', 'HH24:MI'), '-') \
             FROM {table} ORDER BY id"
        )
    };
    assert_eq!(
        scalars(&db.serving, ends("transactions")).await,
        ["20=07:00", "21=09:00", "22=-"]
    );
    assert_eq!(
        scalars(&db.serving, ends("executions")).await,
        ["10=09:00", "11=-"]
    );
    assert_eq!(
        scalars(
            &db.serving,
            "SELECT id || '=' || status || '@' || \
               COALESCE(to_char(ended_on AT TIME ZONE 'UTC', 'HH24:MI'), '-') \
             FROM workers ORDER BY id"
        )
        .await,
        ["50=F@08:30", "51=R@-", "52=R@-"]
    );
}

/// A function live from 08:00 to 12:00.
const CLOSED_FUNCTION: &[&str] = &[
    "INSERT INTO functions (id, project_id, collection_id, name, description, decorator, \
       runtime_values, function_id, stage_id, stage_base_path, storage_version, bundle_id, \
       snippet, valid_from, defined_by_id, status, enabled, valid_to, function_type) \
     VALUES (1, 'p0', 'c0', 'f', '', 'P', '', 'f0', 's0', '/s', 'V1', 'b0', '', \
       '2026-01-01 08:00:00+00', 'u0', 'A', true, 'infinity', 'R')",
    "UPDATE functions SET valid_to = '2026-01-01 12:00:00+00' WHERE id = 1",
];

const ORPHANS: &[&str] = &[
    "INSERT INTO tables (id, project_id, collection_id, table_id, name, function_id, \
       function_version_id, private, valid_from, defined_by_id, status, valid_to) \
     VALUES ('st0', 'p0', 'c0', 'state0', 'td.fn_state__f0', 'f0', 1, true, \
       '2026-01-01 08:00:00+00', 'u0', 'A', 'infinity')",
    "INSERT INTO tables (id, project_id, collection_id, table_id, name, function_id, \
       function_version_id, private, valid_from, defined_by_id, status, valid_to) \
     VALUES ('st1', 'p0', 'c0', 'state0', 'td.fn_state__f0', 'f0', 1, true, \
       '2026-01-01 12:00:00+00', 'u0', 'A', 'infinity')",
    "UPDATE tables SET valid_to = '2026-01-01 13:00:00+00' WHERE id = 'st1'",
    "INSERT INTO tables (id, project_id, collection_id, table_id, name, function_id, \
       function_version_id, private, valid_from, defined_by_id, status, valid_to) \
     VALUES ('ut0', 'p0', 'c0', 'user0', 'frozen', 'f0', 1, false, \
       '2026-01-01 12:00:00+00', 'u0', 'F', 'infinity')",
    "INSERT INTO dependencies (id, project_id, collection_id, dependency_id, function_id, \
       table_collection_id, table_function_id, table_id, table_versions, dep_pos, valid_from, \
       defined_by_id, system, valid_to) \
     VALUES ('d0', 'p0', 'c0', 'dep0', 'f0', 'c0', 'f0', 'state0', '', -1, \
       '2026-01-01 08:00:00+00', 'u0', true, 'infinity')",
    "INSERT INTO dependencies (id, project_id, collection_id, dependency_id, function_id, \
       table_collection_id, table_function_id, table_id, table_versions, dep_pos, valid_from, \
       defined_by_id, system, valid_to) \
     VALUES ('d1', 'p0', 'c0', 'dep0', 'f0', 'c0', 'f0', 'state0', '', -1, \
       '2026-01-01 12:00:00+00', 'u0', true, 'infinity')",
    "UPDATE dependencies SET valid_to = '2026-01-01 13:00:00+00' WHERE id = 'd1'",
    "INSERT INTO dependencies (id, project_id, collection_id, dependency_id, function_id, \
       table_collection_id, table_function_id, table_id, table_versions, dep_pos, valid_from, \
       defined_by_id, system, valid_to) \
     VALUES ('d2', 'p0', 'c0', 'dep1', 'f0', 'c0', 'f0', 'state0', '', -1, \
       '2026-01-01 12:00:00+00', 'u0', true, 'infinity')",
    "UPDATE dependencies SET valid_to = '2026-01-01 12:00:00+00' WHERE id = 'd2'",
    "INSERT INTO triggers (id, project_id, collection_id, trigger_id, function_id, \
       trigger_by_collection_id, trigger_by_function_id, trigger_by_table_id, status, \
       valid_from, defined_by_id, system, valid_to) \
     VALUES ('tr0', 'p0', 'c0', 'trig0', 'f0', 'c0', 'f0', 'state0', 'A', \
       '2026-01-01 08:00:00+00', 'u0', false, 'infinity')",
    "INSERT INTO triggers (id, project_id, collection_id, trigger_id, function_id, \
       trigger_by_collection_id, trigger_by_function_id, trigger_by_table_id, status, \
       valid_from, defined_by_id, system, valid_to) \
     VALUES ('tr1', 'p0', 'c0', 'trig0', 'f0', 'c0', 'f0', 'state0', 'A', \
       '2026-01-01 12:00:00+00', 'u0', false, 'infinity')",
    "UPDATE triggers SET valid_to = '2026-01-01 13:00:00+00' WHERE id = 'tr1'",
];

#[td_test::domain("apiserver_postgres_internals")]
#[tokio::test]
async fn v14_to_v15_drops_rows_outliving_their_function_postgres(
    domain_apiserver_postgres_internals: PostgresDomain,
) {
    let db = pool(&domain_apiserver_postgres_internals).await;
    db.admin
        .upgrade_source(archive::source(14))
        .await
        .expect("installing archive v14");
    for stmt in BASE.iter().take(3).chain(CLOSED_FUNCTION).chain(ORPHANS) {
        execute(&db.serving, *stmt).await;
    }

    db.admin
        .upgrade(&ApiserverComponent)
        .await
        .expect("upgrading v14 -> current");

    let kept = scalars(
        &db.serving,
        "SELECT id FROM tables UNION ALL SELECT id FROM dependencies \
         UNION ALL SELECT id FROM triggers ORDER BY 1",
    )
    .await;
    assert_eq!(kept, ["d0", "st0", "tr0", "ut0"]);
}
