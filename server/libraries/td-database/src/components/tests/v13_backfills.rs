//
// Copyright 2026 Tabsdata Inc.
//

//! v12 → v13 rewrites run data in place: materializations of attempts that
//! never reported Done become pending and the settled ones around them are
//! spliced back together, each run input is pinned to the table version its
//! plan read, and duplicate live names stop the upgrade.

use crate::components::ApiserverComponent;
use crate::test_utils::TestDb;
use td_schema::schema::archive;

use super::{execute, scalars};

const BASE: &[&str] = &[
    "INSERT INTO projects (id, name, description, base_path, created_on, created_by_id, \
       modified_on, modified_by_id) \
     VALUES ('p0', 'p0', '', '/p', now(), 'u0', now(), 'u0')",
    "INSERT INTO groups (id, project_id, name, description, created_on, created_by_id, \
       modified_on, modified_by_id, system, kind) \
     VALUES ('g0', 'p0', 'g0', '', now(), 'u0', now(), 'u0', false, 'R')",
    "INSERT INTO collections (id, project_id, name, description, created_on, created_by_id, \
       modified_on, modified_by_id, group_id, collection_type) \
     VALUES ('c0', 'p0', 'c0', '', now(), 'u0', now(), 'u0', 'g0', 'R')",
];

fn function(id: u32, function_id: &str, name: &str, valid_from: &str) -> String {
    format!(
        "INSERT INTO functions (id, project_id, collection_id, name, description, decorator, \
           runtime_values, function_id, stage_id, stage_base_path, storage_version, bundle_id, \
           snippet, valid_from, defined_by_id, status, enabled, valid_to, function_type) \
         VALUES ({id}, 'p0', 'c0', '{name}', '', 'P', '', '{function_id}', 's0', '/s', 'V1', \
           'b0', '', '{valid_from}', 'u0', 'A', true, 'infinity', 'R')"
    )
}

fn table(id: &str, table_id: &str, name: &str, valid_from: &str) -> String {
    format!(
        "INSERT INTO tables (id, project_id, collection_id, table_id, name, function_version_id, \
           private, valid_from, defined_by_id, status, valid_to) \
         VALUES ('{id}', 'p0', 'c0', '{table_id}', '{name}', 1, false, '{valid_from}', 'u0', 'A', \
           'infinity')"
    )
}

/// One run of function `f0` triggered at 10:00, reading table `tt0` through
/// its first dependency, with three attempts: Done at 10:01, Failed at
/// 10:02, and one still Running at 10:03.
const RUN: &[&str] = &[
    "INSERT INTO dependencies (id, project_id, collection_id, dependency_id, function_id, \
       table_collection_id, table_function_id, table_id, table_versions, dep_pos, valid_from, \
       defined_by_id, system, valid_to) \
     VALUES ('d0', 'p0', 'c0', 'd0', 'f0', 'c0', 'f0', 'tt0', 'HEAD', 0, \
       '2026-01-01 08:00:00+00', 'u0', false, 'infinity')",
    "INSERT INTO executions (id, project_id, collection_id, function_version_id, function_id, \
       triggered_on, triggered_by_id) \
     VALUES (10, 'p0', 'c0', 1, 'f0', '2026-01-01 10:00:00+00', 'u0')",
    "INSERT INTO transactions (id, project_id, collection_id, execution_id, transaction_by, \
       transaction_key, triggered_on, triggered_by_id, group_id) \
     VALUES (20, 'p0', 'c0', 10, 'F', 'k', '2026-01-01 10:00:00+00', 'u0', 'g0')",
    "INSERT INTO logical_function_runs (id, project_id, collection_id, execution_id, \
       transaction_id, function_id, function_version_id, trigger, triggered_on, triggered_by_id, \
       status, group_id) \
     VALUES (30, 'p0', 'c0', 10, 20, 'f0', 1, 'M', '2026-01-01 10:00:00+00', 'u0', 'R', 'g0')",
    "INSERT INTO logical_table_data_versions (id, project_id, collection_id, execution_id, \
       transaction_id, table_id, producer_logical_function_run_id, function_param_pos, private, \
       triggered_on, triggered_by_id, group_id) \
     VALUES (40, 'p0', 'c0', 10, 20, 'tt1', 30, 0, false, '2026-01-01 10:00:00+00', 'u0', 'g0')",
    "INSERT INTO logical_function_run_inputs (id, project_id, collection_id, execution_id, \
       transaction_id, logical_function_run_id, requirement_dependency_pos, \
       requirement_version_pos) \
     VALUES ('i0', 'p0', 'c0', 10, 20, 30, 0, 0)",
    "INSERT INTO logical_function_run_inputs (id, project_id, collection_id, execution_id, \
       transaction_id, logical_function_run_id, requirement_input_idx, requirement_version_pos) \
     VALUES ('i1', 'p0', 'c0', 10, 20, 30, 0, 0)",
];

fn attempt(id: u32, status: &str, at: &str) -> String {
    format!(
        "INSERT INTO physical_function_runs (id, project_id, collection_id, function_version_id, \
           execution_id, transaction_id, valid_from, triggered_by_id, trigger, function_id, \
           status, base_path, logical_function_run_id, valid_to) \
         VALUES ({id}, 'p0', 'c0', 1, 10, 20, '{at}', 'u0', 'M', 'f0', '{status}', '/r', 30, \
           'infinity')"
    )
}

fn materialization(id: u32, attempt: u32, has_data: &str, at: &str) -> String {
    format!(
        "INSERT INTO physical_table_data_versions (id, project_id, collection_id, table_id, name, \
           table_version_id, function_version_id, has_data, execution_id, transaction_id, \
           physical_function_run_id, function_param_pos, valid_from, base_path, \
           logical_table_data_version_id, valid_to) \
         VALUES ({id}, 'p0', 'c0', 'tt1', 't1', 't71', 1, {has_data}, 10, 20, {attempt}, 0, \
           '{at}', '/d', 40, 'infinity')"
    )
}

async fn v12() -> TestDb {
    let db = crate::test_utils::postgres::pool().await;
    db.admin
        .upgrade_source(archive::source(12))
        .await
        .expect("installing archive v12");
    for stmt in BASE {
        execute(&db.serving, *stmt).await;
    }
    db
}

#[tokio::test]
async fn v12_to_v13_parks_unsettled_materializations_and_splices_the_rest_postgres() {
    let db = v12().await;
    let seed = [
        function(1, "f0", "f", "2026-01-01 09:00:00+00"),
        table("t70", "tt0", "t", "2026-01-01 08:00:00+00"),
        table("t71", "tt1", "t1", "2026-01-01 08:00:00+00"),
    ];
    for stmt in seed.iter().map(String::as_str).chain(RUN.iter().copied()) {
        execute(&db.serving, stmt).await;
    }
    for stmt in [
        attempt(50, "D", "2026-01-01 10:01:00+00"),
        attempt(51, "F", "2026-01-01 10:02:00+00"),
        attempt(52, "R", "2026-01-01 10:03:00+00"),
        attempt(53, "D", "2026-01-01 10:04:00+00"),
        materialization(60, 50, "true", "2026-01-01 10:01:00+00"),
        materialization(61, 51, "NULL", "2026-01-01 10:02:00+00"),
        materialization(62, 52, "NULL", "2026-01-01 10:03:00+00"),
        materialization(63, 53, "NULL", "2026-01-01 10:04:00+00"),
    ] {
        execute(&db.serving, stmt).await;
    }

    db.admin
        .upgrade(&ApiserverComponent)
        .await
        .expect("upgrading v12 -> current");

    let chain = scalars(
        &db.serving,
        "SELECT id || ':' || to_char(valid_from AT TIME ZONE 'UTC', 'HH24:MI') || '-' || \
           CASE WHEN valid_to = 'infinity' THEN 'open' \
                ELSE to_char(valid_to AT TIME ZONE 'UTC', 'HH24:MI') END \
         FROM physical_table_data_versions ORDER BY id",
    )
    .await;
    assert_eq!(
        chain,
        [
            "60:10:01-10:04",
            "61:10:02-10:02",
            "62:10:03-10:03",
            "63:10:04-open"
        ],
        "the failed and the running attempt's versions are pending; the Done ones chain up"
    );
}

#[tokio::test]
async fn v12_to_v13_pins_each_input_to_the_table_version_its_run_read_postgres() {
    let db = v12().await;
    let seed = [
        function(1, "f0", "f", "2026-01-01 09:00:00+00"),
        table("t70", "tt0", "t", "2026-01-01 08:00:00+00"),
        table("t72", "tt0", "t", "2026-01-01 11:00:00+00"),
        table("t71", "tt1", "t1", "2026-01-01 08:00:00+00"),
    ];
    for stmt in seed.iter().map(String::as_str).chain(RUN.iter().copied()) {
        execute(&db.serving, stmt).await;
    }

    db.admin
        .upgrade(&ApiserverComponent)
        .await
        .expect("upgrading v12 -> current");

    let pins = scalars(
        &db.serving,
        "SELECT id || '=' || COALESCE(requirement_table_version_id, '-') \
         FROM logical_function_run_inputs ORDER BY id",
    )
    .await;
    assert_eq!(
        pins,
        ["i0=t70", "i1=-"],
        "a dependency input reads the version live at its trigger, not a later one; \
         a function input has no dependency to pin"
    );
}

#[tokio::test]
async fn v12_to_v13_refuses_duplicate_live_names_postgres() {
    let db = v12().await;
    for stmt in [
        function(1, "f0", "same", "2026-01-01 09:00:00+00"),
        function(2, "f1", "same", "2026-01-01 09:00:00+00"),
        function(3, "f2", "other", "2026-01-01 09:00:00+00"),
    ] {
        execute(&db.serving, stmt).await;
    }

    let failure = db
        .admin
        .upgrade(&ApiserverComponent)
        .await
        .expect_err("two live functions named alike must stop the upgrade")
        .to_string();
    assert!(
        failure.contains("function same in collection c0"),
        "the refusal names the duplicate: {failure}"
    );
    assert!(!failure.contains("other"), "{failure}");
}
