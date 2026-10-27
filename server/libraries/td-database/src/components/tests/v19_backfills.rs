//
// Copyright 2026 Tabsdata Inc.
//

//! v18 → v19 moves parents from columns to `_parent` topology tags: one tag
//! per run of contiguous row versions naming the same parent.

use crate::components::ApiserverComponent;
use crate::test_utils::domain::PostgresDomain;
use crate::test_utils::postgres::pool;
use td_common::id::Id;
use td_schema::schema::archive;

use super::{execute, scalars};

const SEED: &[&str] = &[
    "INSERT INTO projects (id, project_id, name, description, base_path, valid_from, \
       valid_to, defined_by_id) \
     VALUES ('pv0', 'p0', 'p0', '', '/p', '2026-01-01 06:00:00+00', 'infinity', 'u0')",
    "INSERT INTO groups (id, project_id, group_id, name, description, system, kind, \
       valid_from, valid_to, defined_by_id) \
     VALUES ('gv0', 'p0', 'g0', 'g0', '', false, 'R', '2026-01-01 06:00:00+00', 'infinity', \
       'u0')",
    "INSERT INTO collections (id, project_id, collection_id, name, description, group_id, \
       collection_type, valid_from, valid_to, defined_by_id) \
     VALUES ('cv0', 'p0', 'c0', 'c0', '', 'g0', 'R', '2026-01-01 06:00:00+00', 'infinity', \
       'u0')",
    "INSERT INTO functions (id, project_id, collection_id, name, description, decorator, \
       runtime_values, function_id, stage_id, stage_base_path, storage_version, bundle_id, \
       snippet, valid_from, defined_by_id, status, enabled, valid_to, function_type) \
     VALUES (1, 'p0', 'c0', 'owner', '', 'T', '', 'f0', 's0', '/s', 'V1', 'b0', '', \
       '2026-01-01 08:00:00+00', 'u0', 'A', true, 'infinity', 'R')",
    "INSERT INTO functions (id, project_id, collection_id, name, description, decorator, \
       runtime_values, function_id, stage_id, stage_base_path, storage_version, bundle_id, \
       snippet, valid_from, defined_by_id, status, enabled, valid_to, function_type, \
       parent_function_id) \
     VALUES (2, 'p0', 'c0', 'trend', '', 'T', '', 'f1', 's1', '/s', 'V1', 'b0', '', \
       '2026-01-01 08:00:00+00', 'u0', 'A', true, 'infinity', 'DQST', 'f0')",
    "INSERT INTO functions (id, project_id, collection_id, name, description, decorator, \
       runtime_values, function_id, stage_id, stage_base_path, storage_version, bundle_id, \
       snippet, valid_from, defined_by_id, status, enabled, valid_to, function_type, \
       parent_function_id) \
     VALUES (3, 'p0', 'c0', 'trend', '', 'T', '', 'f1', 's1', '/s', 'V1', 'b0', '', \
       '2026-01-01 10:00:00+00', 'u0', 'A', false, 'infinity', 'DQST', 'f0')",
    "INSERT INTO functions (id, project_id, collection_id, name, description, decorator, \
       runtime_values, function_id, stage_id, stage_base_path, storage_version, bundle_id, \
       snippet, valid_from, defined_by_id, status, enabled, valid_to, function_type, \
       parent_function_id) \
     VALUES (4, 'p0', 'c0', 'gap', '', 'T', '', 'f2', 's2', '/s', 'V1', 'b0', '', \
       '2026-01-01 08:00:00+00', 'u0', 'A', true, 'infinity', 'DQST', 'f0')",
    "INSERT INTO functions (id, project_id, collection_id, name, description, decorator, \
       runtime_values, function_id, stage_id, stage_base_path, storage_version, bundle_id, \
       snippet, valid_from, defined_by_id, status, enabled, valid_to, function_type) \
     VALUES (5, 'p0', 'c0', 'gap', '', 'T', '', 'f2', 's2', '/s', 'V1', 'b0', '', \
       '2026-01-01 09:00:00+00', 'u0', 'A', true, 'infinity', 'DQST')",
    "INSERT INTO functions (id, project_id, collection_id, name, description, decorator, \
       runtime_values, function_id, stage_id, stage_base_path, storage_version, bundle_id, \
       snippet, valid_from, defined_by_id, status, enabled, valid_to, function_type, \
       parent_function_id) \
     VALUES (6, 'p0', 'c0', 'gap', '', 'T', '', 'f2', 's2', '/s', 'V1', 'b0', '', \
       '2026-01-01 10:00:00+00', 'u0', 'A', true, 'infinity', 'DQST', 'f0')",
    "INSERT INTO tables (id, project_id, collection_id, table_id, name, function_version_id, \
       private, valid_from, defined_by_id, status, valid_to) \
     VALUES ('t0', 'p0', 'c0', 'tt0', 't', 1, false, '2026-01-01 08:00:00+00', 'u0', 'A', \
       'infinity')",
    "INSERT INTO tables (id, project_id, collection_id, table_id, name, function_version_id, \
       private, valid_from, defined_by_id, status, valid_to, table_type, parent_table_id) \
     VALUES ('t1', 'p0', 'c0', 'tt1', 't_summary', 1, false, '2026-01-01 08:00:00+00', 'u0', \
       'A', 'infinity', 'DQS', 'tt0')",
];

#[td_test::domain("apiserver_postgres_internals")]
#[tokio::test]
async fn v18_to_v19_moves_parents_to_tags_postgres(
    domain_apiserver_postgres_internals: PostgresDomain,
) {
    let db = pool(&domain_apiserver_postgres_internals).await;
    db.admin
        .upgrade_source(archive::source(18))
        .await
        .expect("installing archive v18");
    for stmt in SEED {
        execute(&db.serving, *stmt).await;
    }

    db.admin
        .upgrade(&ApiserverComponent)
        .await
        .expect("upgrading v18 -> current");

    let tags = scalars(
        &db.serving,
        "SELECT entity_type || ':' || entity_id || ':' || kind || ':' || key || '=' || value || ' ' || \
           to_char(valid_from AT TIME ZONE 'UTC', 'HH24:MI') || '-' || \
           CASE WHEN valid_to = 'infinity' THEN 'open' \
           ELSE to_char(valid_to AT TIME ZONE 'UTC', 'HH24:MI') END \
         FROM tags ORDER BY entity_type, entity_id, valid_from",
    )
    .await;
    assert_eq!(
        tags,
        [
            "F:f1:T:_parent=f0 08:00-open",
            "F:f2:T:_parent=f0 08:00-09:00",
            "F:f2:T:_parent=f0 10:00-open",
            "T:tt1:T:_parent=tt0 08:00-open",
        ]
    );

    let partitions = scalars(
        &db.serving,
        "SELECT entity_id || '=' || count(DISTINCT tag_id) FROM tags GROUP BY entity_id ORDER BY entity_id",
    )
    .await;
    assert_eq!(partitions, ["f1=1", "f2=2", "tt1=1"]);

    for id in scalars(
        &db.serving,
        "SELECT id FROM tags UNION ALL SELECT tag_id FROM tags",
    )
    .await
    {
        Id::try_from(&id).unwrap_or_else(|e| panic!("{id} is not an Id: {e}"));
    }
}
