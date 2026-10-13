//
// Copyright 2026 Tabsdata Inc.
//

//! v12 → v13 ends the triggers of disabled versions left waiting for a stop
//! no tick sends: they never started, so there is nothing to stop.

use crate::components::ApiserverComponent;
use td_schema::schema::archive;

use super::{execute, scalars};

const SEED: &[&str] = &[
    "INSERT INTO projects (id, name, description, base_path, created_on, created_by_id, \
       modified_on, modified_by_id) \
     VALUES ('p0', 'p0', '', '/p', now(), 'u0', now(), 'u0')",
    "INSERT INTO groups (id, project_id, name, description, created_on, created_by_id, \
       modified_on, modified_by_id, system, kind) \
     VALUES ('g0', 'p0', 'g0', '', now(), 'u0', now(), 'u0', false, 'R')",
    "INSERT INTO collections (id, project_id, name, description, created_on, created_by_id, \
       modified_on, modified_by_id, group_id, collection_type) \
     VALUES ('c0', 'p0', 'c0', '', now(), 'u0', now(), 'u0', 'g0', 'R')",
    "INSERT INTO functions (id, project_id, collection_id, name, description, decorator, \
       runtime_values, function_id, stage_id, stage_base_path, storage_version, bundle_id, \
       snippet, valid_from, defined_by_id, status, enabled, valid_to, function_type) \
     VALUES (1, 'p0', 'c0', 'f', '', 'P', '', 'f0', 's0', '/s', 'V1', 'b0', '', now(), 'u0', \
       'A', false, 'infinity', 'R')",
];

fn worker(id: u32, status: &str, enabled: bool) -> String {
    format!(
        "INSERT INTO workers (id, project_id, collection_id, function_version_id, message_status, \
           status, worker_type, worker_enabled) \
         VALUES ({id}, 'p0', 'c0', 1, 'L', '{status}', 'CT', {enabled})"
    )
}

#[tokio::test]
async fn v12_to_v13_stops_the_unstarted_triggers_of_disabled_versions_postgres() {
    let db = crate::test_utils::postgres::pool().await;
    db.admin
        .upgrade_source(archive::source(12))
        .await
        .expect("installing archive v12");
    for stmt in SEED {
        execute(&db.serving, *stmt).await;
    }
    for stmt in [
        worker(1, "SPS", false),
        worker(2, "SPS", true),
        worker(3, "S", false),
    ] {
        execute(&db.serving, stmt).await;
    }

    db.admin
        .upgrade(&ApiserverComponent)
        .await
        .expect("upgrading v12 -> current");

    let workers = scalars(
        &db.serving,
        "SELECT id || '=' || status || '@' || (ended_on IS NOT NULL) FROM workers ORDER BY id",
    )
    .await;
    assert_eq!(
        workers,
        ["1=SP@true", "2=SPS@false", "3=S@false"],
        "only a disabled trigger waiting for a stop ends; an enabled one still gets its stop"
    );
}
