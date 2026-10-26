//
// Copyright 2026 Tabsdata Inc.
//

//! The topology tables change only by opening and closing versions.

use crate::components::ApiserverComponent;
use crate::generated_sql::generated;
use crate::test_utils::TestDb;
use crate::test_utils::postgres::pool;

use super::{at_rest, execute};

async fn prelude() -> TestDb {
    let db = pool().await;
    db.admin.upgrade(&ApiserverComponent).await.unwrap();
    for stmt in [
        "INSERT INTO projects (id, project_id, name, description, base_path, valid_from, \
           defined_by_id) \
         VALUES ('pv0', 'p0', 'p0', '', '/p', now(), 'u0')",
        "INSERT INTO groups (id, project_id, group_id, name, description, system, kind, \
           valid_from, defined_by_id) \
         VALUES ('gv0', 'p0', 'g0', 'g0', '', false, 'custom', now(), 'u0')",
        "INSERT INTO collections (id, project_id, collection_id, name, description, base_path, \
           group_id, collection_type, valid_from, defined_by_id) \
         VALUES ('cv0', 'p0', 'c0', 'c0', '', NULL, 'g0', 'R', now(), 'u0')",
    ] {
        execute(&db.serving, stmt).await;
    }
    db
}

fn function(counter: usize, function_id: &str, valid_from: &str) -> String {
    let id = at_rest(counter);
    format!(
        "INSERT INTO functions (id, project_id, collection_id, name, description, decorator, \
           function_type, runtime_values, function_id, stage_id, stage_base_path, \
           storage_version, bundle_id, snippet, valid_from, defined_by_id, status) \
         VALUES ({id}, 'p0', 'c0', '{function_id}', '', 'P', 'R', '', '{function_id}', 's0', \
           '/s', 'V1', 'b0', '', '{valid_from}', 'u0', 'A')"
    )
}

#[tokio::test]
async fn topology_rows_change_only_by_opening_and_closing_postgres() {
    let db = prelude().await;
    execute(&db.serving, function(1, "f-a", "2026-01-01T00:00:00Z")).await;
    for stmt in [
        "UPDATE functions SET enabled = false WHERE function_id = 'f-a'",
        "DELETE FROM functions WHERE function_id = 'f-a'",
    ] {
        let err = sqlx::query(generated(stmt.to_string()))
            .execute(db.serving.as_ref())
            .await
            .expect_err("only a new version or a close changes topology")
            .to_string();
        assert!(err.contains("functions is insert-only"), "{err}");
    }
}
