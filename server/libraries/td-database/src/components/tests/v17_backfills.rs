//
// Copyright 2026 Tabsdata Inc.
//

//! v17 → v18 turns projects, collections and groups into version timelines:
//! a modified entity splits at its last modification, a deleted collection
//! closes at its tombstone stamp under its real name, whatever its rows or
//! the topology did later, and a deleted project closes with its groups.

use crate::components::ApiserverComponent;
use crate::test_utils::postgres::pool;
use td_common::id::Id;
use td_schema::schema::archive;

use super::{execute, scalars};

const SEED: &[&str] = &[
    "INSERT INTO projects (id, name, description, base_path, created_on, created_by_id, \
       modified_on, modified_by_id) \
     VALUES ('p_live', 'p_live', 'live', '/p', '2026-01-01 01:00:00+00', 'u_c', \
       '2026-01-01 02:00:00+00', 'u_m')",
    "INSERT INTO projects (id, name, description, base_path, created_on, created_by_id, \
       modified_on, modified_by_id, name_when_deleted) \
     VALUES ('p_gone', 'deleted_x', 'gone', '/p', '2026-01-01 01:00:00+00', 'u_c', \
       '2026-01-01 05:00:00+00', 'u_m', 'p_gone')",
    "INSERT INTO groups (id, project_id, name, description, created_on, created_by_id, \
       modified_on, modified_by_id, system, kind) \
     VALUES ('g_live', 'p_live', 'g_live', '', '2026-01-01 01:00:00+00', 'u_c', \
       '2026-01-01 03:00:00+00', 'u_m', false, 'custom')",
    "INSERT INTO groups (id, project_id, name, description, created_on, created_by_id, \
       modified_on, modified_by_id, system, kind) \
     VALUES ('g_gone', 'p_gone', 'g_gone', '', '2026-01-01 01:00:00+00', 'u_c', \
       '2026-01-01 01:00:00+00', 'u_c', false, 'custom')",
    "INSERT INTO collections (id, project_id, name, description, created_on, created_by_id, \
       modified_on, modified_by_id, group_id, base_path, collection_type) \
     VALUES ('c_live', 'p_live', 'c_live', '', '2026-01-01 01:00:00+00', 'u_c', \
       '2026-01-01 04:00:00+00', 'u_m', 'g_live', '/b', 'R')",
    "INSERT INTO collections (id, project_id, name, description, created_on, created_by_id, \
       modified_on, modified_by_id, group_id, collection_type, name_when_deleted) \
     VALUES ('c_gone', 'p_live', 'deleted_y', '', '2026-01-01 01:00:00+00', 'u_c', \
       '2026-01-01 06:00:00+00', 'u_m', 'g_live', 'R', 'c_gone')",
    "INSERT INTO collections (id, project_id, name, description, created_on, created_by_id, \
       modified_on, modified_by_id, group_id, collection_type, name_when_deleted) \
     VALUES ('c_empty', 'p_live', 'deleted_z', '', '2026-01-01 01:00:00+00', 'u_c', \
       '2026-01-01 07:00:00+00', 'u_m', 'g_live', 'R', 'c_empty')",
    "INSERT INTO functions (id, project_id, collection_id, name, description, decorator, \
       runtime_values, function_id, stage_id, stage_base_path, storage_version, bundle_id, \
       snippet, valid_from, defined_by_id, status, enabled, valid_to, function_type) \
     VALUES (1, 'p_live', 'c_gone', 'f', '', 'P', '', 'f0', 's0', '/s', 'V1', 'b0', '', \
       '2026-01-01 02:00:00+00', 'u0', 'A', true, 'infinity', 'R')",
    "UPDATE functions SET valid_to = '2026-01-01 06:00:00.5+00' WHERE id = 1",
    "INSERT INTO project_topologies (id, project_id, valid_from, valid_to) \
     VALUES ('t0', 'p_live', '2026-01-01 01:00:00+00', '2026-01-01 07:00:00.2+00')",
    "INSERT INTO project_topologies (id, project_id, valid_from, valid_to) \
     VALUES ('t1', 'p_live', '2026-01-01 07:00:00.2+00', 'infinity')",
];

const TIMELINE: &str = "to_char(valid_from AT TIME ZONE 'UTC', 'HH24:MI:SS.MS') || '-' || \
    CASE WHEN valid_to = 'infinity' THEN 'open' \
    ELSE to_char(valid_to AT TIME ZONE 'UTC', 'HH24:MI:SS.MS') END || '/' || defined_by_id";

#[tokio::test]
async fn v17_to_v18_backfills_the_entity_timelines_postgres() {
    let db = pool().await;
    db.admin
        .upgrade_source(archive::source(17))
        .await
        .expect("installing archive v17");
    for stmt in SEED {
        execute(&db.serving, *stmt).await;
    }

    db.admin
        .upgrade(&ApiserverComponent)
        .await
        .expect("upgrading v17 -> current");

    let projects = scalars(
        &db.serving,
        &format!(
            "SELECT project_id || '=' || name || ' ' || {TIMELINE} \
             FROM projects ORDER BY project_id, valid_from"
        ),
    )
    .await;
    assert_eq!(
        projects,
        [
            "p_gone=p_gone 01:00:00.000-05:00:00.000/u_c",
            "p_live=p_live 01:00:00.000-02:00:00.000/u_c",
            "p_live=p_live 02:00:00.000-open/u_m",
        ]
    );

    let groups = scalars(
        &db.serving,
        &format!(
            "SELECT group_id || '=' || name || ' ' || {TIMELINE} \
             FROM groups ORDER BY group_id, valid_from"
        ),
    )
    .await;
    assert_eq!(
        groups,
        [
            "g_gone=g_gone 01:00:00.000-05:00:00.000/u_c",
            "g_live=g_live 01:00:00.000-03:00:00.000/u_c",
            "g_live=g_live 03:00:00.000-open/u_m",
        ]
    );

    let collections = scalars(
        &db.serving,
        &format!(
            "SELECT collection_id || '=' || name || ' ' || COALESCE(base_path, '-') || ' ' || \
               {TIMELINE} \
             FROM collections ORDER BY collection_id, valid_from"
        ),
    )
    .await;
    assert_eq!(
        collections,
        [
            "c_empty=c_empty - 01:00:00.000-07:00:00.000/u_c",
            "c_gone=c_gone - 01:00:00.000-06:00:00.000/u_c",
            "c_live=c_live /b 01:00:00.000-04:00:00.000/u_c",
            "c_live=c_live /b 04:00:00.000-open/u_m",
        ]
    );

    for id in scalars(
        &db.serving,
        "SELECT id FROM projects UNION ALL SELECT id FROM groups \
         UNION ALL SELECT id FROM collections",
    )
    .await
    {
        Id::try_from(&id).unwrap_or_else(|e| panic!("{id} is not an Id: {e}"));
    }

    let live = scalars(
        &db.serving,
        "SELECT collection_id FROM collections WHERE valid_to = 'infinity' ORDER BY collection_id",
    )
    .await;
    assert_eq!(live, ["c_live"]);
}
