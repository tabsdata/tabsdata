//
// Copyright 2026 Tabsdata Inc.
//

//! v15 → v16 numbers each project's topology history: its creation and
//! every distinct instant its topology rows open or close at become a
//! version, in time order, and each execution takes the version current at
//! its trigger.

use crate::components::ApiserverComponent;
use crate::test_utils::postgres::pool;
use td_common::id::Id;
use td_schema::schema::archive;

use super::{execute, scalars};

const SEED: &[&str] = &[
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
       '2026-01-01 08:00:00+00', 'u0', 'A', true, 'infinity', 'R')",
    "UPDATE functions SET valid_to = '2026-01-01 12:00:00+00' WHERE id = 1",
    "INSERT INTO tables (id, project_id, collection_id, table_id, name, function_version_id, \
       private, valid_from, defined_by_id, status, valid_to) \
     VALUES ('t0', 'p0', 'c0', 'tt0', 't', 1, false, '2026-01-01 08:00:00+00', 'u0', 'A', \
       'infinity')",
    "INSERT INTO tables (id, project_id, collection_id, table_id, name, function_version_id, \
       private, valid_from, defined_by_id, status, valid_to) \
     VALUES ('t1', 'p0', 'c0', 'tt1', 't1', 1, false, '2026-01-01 10:00:00+00', 'u0', 'A', \
       'infinity')",
    "INSERT INTO executions (id, project_id, collection_id, function_version_id, function_id, \
       triggered_on, triggered_by_id) \
     VALUES (10, 'p0', 'c0', 1, 'f0', '2026-01-01 07:00:00+00', 'u0')",
    "INSERT INTO executions (id, project_id, collection_id, function_version_id, function_id, \
       triggered_on, triggered_by_id) \
     VALUES (11, 'p0', 'c0', 1, 'f0', '2026-01-01 11:00:00+00', 'u0')",
];

#[tokio::test]
async fn v15_to_v16_numbers_topology_history_and_executions_postgres() {
    let db = pool().await;
    db.admin
        .upgrade_source(archive::source(15))
        .await
        .expect("installing archive v15");
    for stmt in SEED {
        execute(&db.serving, *stmt).await;
    }

    db.admin
        .upgrade(&ApiserverComponent)
        .await
        .expect("upgrading v15 -> current");

    let versions = scalars(
        &db.serving,
        "SELECT to_char(valid_from AT TIME ZONE 'UTC', 'HH24:MI') || '-' || \
           CASE WHEN valid_to = 'infinity' THEN 'open' \
           ELSE to_char(valid_to AT TIME ZONE 'UTC', 'HH24:MI') END \
         FROM project_topologies ORDER BY valid_from",
    )
    .await;
    assert_eq!(
        versions,
        ["06:00-08:00", "08:00-10:00", "10:00-12:00", "12:00-open"]
    );

    for id in scalars(&db.serving, "SELECT id FROM project_topologies").await {
        Id::try_from(&id).unwrap_or_else(|e| panic!("{id} is not an Id: {e}"));
    }

    let executions = scalars(
        &db.serving,
        "SELECT e.id || '=' || to_char(t.valid_from AT TIME ZONE 'UTC', 'HH24:MI') \
         FROM executions e JOIN project_topologies t ON t.id = e.topology_version_id ORDER BY e.id",
    )
    .await;
    assert_eq!(executions, ["10=06:00", "11=10:00"]);
}
