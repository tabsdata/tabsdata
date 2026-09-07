//
// Copyright 2026 Tabsdata Inc.
//

//! The live schema's structure (table / view / index inventory — drift
//! detection) and the version / edition gates `check` enforces.

use super::{schema_db_version, schema_index_names, schema_table_names, schema_view_names};
use crate::components::{ApiserverComponent, SchemaComponent};
use crate::error::DbError;
use crate::pool::TdPool;
#[tokio::test]
async fn test_schema_tables_postgres() {
    let db = crate::test_utils::postgres::pool().await;
    db.upgrade().await.unwrap();
    assert_eq!(schema_table_names(&db.serving).await, expected_tables());
}

fn expected_tables() -> Vec<&'static str> {
    vec![
        "bundles",
        "collections",
        "connections",
        "dependencies",
        "dependencies_current",
        "executions",
        "function_patches",
        "function_patches_current",
        "functions",
        "functions_current",
        "groups",
        "logical_function_run_inputs",
        "logical_function_run_pointers",
        "logical_function_run_pointers_current",
        "logical_function_runs",
        "logical_table_data_version_pointers",
        "logical_table_data_version_pointers_current",
        "logical_table_data_versions",
        "permissions",
        "physical_function_runs",
        "physical_table_data_versions",
        "projects",
        "roles",
        "sessions",
        "table_partitions",
        "tables",
        "tables_current",
        "tabsdata_system",
        "transactions",
        "triggers",
        "triggers_current",
        "users",
        "users_roles",
        "workers",
    ]
}

#[tokio::test]
async fn test_schema_views_postgres() {
    let db = crate::test_utils::postgres::pool().await;
    db.upgrade().await.unwrap();
    assert_eq!(schema_view_names(&db.serving).await, expected_views());
}

fn expected_views() -> Vec<&'static str> {
    vec![
        "collections__with_names",
        "collections_active",
        "collections_resolved",
        "dependencies__with_names",
        "dependencies_current__with_names",
        "execution_status_summary",
        "executions__with_names",
        "executions__with_status",
        "function_runs__with_names",
        "function_versions__with_names",
        "functions__with_names",
        "functions_current__with_names",
        "global_status_summary",
        "groups__with_names",
        "logical_table_data_versions__active",
        "permissions__with_names",
        "physical_function_runs__with_names",
        "physical_table_data_versions__with_function",
        "physical_table_data_versions__with_names",
        "projects__with_names",
        "projects_active",
        "projects_resolved",
        "roles__with_names",
        "sessions__with_names",
        "table_data_versions__with_names",
        "tables__with_names",
        "tables_current__with_names",
        "transaction_status_summary",
        "transactions__with_names",
        "transactions__with_status",
        "triggers__read",
        "triggers__with_names",
        "triggers_current__with_names",
        "users__with_names",
        "users_roles__with_names",
        "workers__with_names",
    ]
}

#[tokio::test]
async fn test_schema_indices_postgres() {
    let db = crate::test_utils::postgres::pool().await;
    db.upgrade().await.unwrap();
    assert_indices(&db.serving).await;
}

async fn assert_indices(db: &TdPool) {
    // Postgres silently truncates identifiers at 63 characters (NAMEDATALEN-1) —
    // several of our index names exceed that. The expected list is the full
    let expected_full = vec![
        "bundles___collection_id___idx",
        "collections___group_id___idx",
        "collections___project_id__name___idx",
        "dependencies___dependency_id__defined_on__id___idx",
        "dependencies___function_id___idx",
        "dependencies___table_function_id___idx",
        "dependencies_current___function_id___idx",
        "dependencies_current___table_function_id___idx",
        "executions___collection_id___idx",
        "executions___function_id__external_uid___idx",
        "executions___function_version_id___idx",
        "executions___triggered_by_id___idx",
        "executions___triggered_on___idx",
        "function_patches___function_id__defined_on___idx",
        "function_patches___parent_function_version_id__defined_on__id___idx",
        "function_runs___collection_id___idx",
        "function_runs___ended_on___idx",
        "function_runs___execution_id__status___idx",
        "function_runs___function_version_id___idx",
        "function_runs___status___idx",
        "function_runs___transaction_id__status___idx",
        "functions___collection_id___idx",
        "functions___function_id__defined_on__id___idx",
        "functions_current___collection_id___idx",
        "functions_current___status___idx",
        "groups___project_id__name___idx",
        "logical_function_run_inputs___execution_id___idx",
        "logical_function_run_inputs___logical_function_run_id___idx",
        "logical_function_run_inputs___requirement_logical_table_data_version_id___idx",
        "lfr_pointers_current___physical_function_run_id___idx",
        "logical_function_run_pointers___function_run_id___idx",
        "logical_function_run_pointers___logical_function_run_id__set_on__id___idx",
        "logical_function_runs___execution_id__function_id___idx",
        "logical_function_runs___execution_id__status___idx",
        "logical_function_runs___function_id__triggered_on__id___idx",
        "logical_function_runs___group_id__collection_id___idx",
        "logical_function_runs___status___idx",
        "logical_function_runs___transaction_id__status___idx",
        "logical_table_data_version_pointers___logical_table_data_version_id__set_on__id___idx",
        "logical_table_data_version_pointers___table_data_version_id___idx",
        "logical_table_data_versions___execution_id___idx",
        "logical_table_data_versions___group_id___idx",
        "logical_table_data_versions___producer_logical_function_run_id___idx",
        "logical_table_data_versions___table_id__triggered_on__id___idx",
        "logical_table_data_versions___transaction_id___idx",
        "permissions___entity_id___idx",
        "permissions___role_id__permission_type__entity___idx",
        "physical_table_data_versions___collection_id___idx",
        "physical_table_data_versions___execution_id___idx",
        "physical_table_data_versions___physical_function_run_id___idx",
        "physical_table_data_versions___table_id___has_data___idx",
        "physical_table_data_versions___table_id__triggered_on__id___idx",
        "roles___project_id___idx",
        "roles___user_id__role_id___idx",
        "sessions___expires_on___idx",
        "sessions___user_id___idx",
        "tables___function_id__status___idx",
        "tables___status___idx",
        "tables___table_id__defined_on__id___idx",
        "tables_current___collection_id___idx",
        "tables_current___function_id___idx",
        "tables_current___status___idx",
        "transactions___collection_id___idx",
        "transactions___execution_id___idx",
        "transactions___group_id__collection_id___idx",
        "triggers___collection_id___idx",
        "triggers___function_id___idx",
        "triggers___trigger_by_function_id___idx",
        "triggers___trigger_id__defined_on__id___idx",
        "triggers_current___collection_id___idx",
        "triggers_current___function_id___idx",
        "triggers_current___trigger_by_function_id___idx",
        "users___enabled___idx",
        "users___system___idx",
        "workers___physical_function_run_id___idx",
    ];

    // PostgreSQL truncates identifiers at NAMEDATALEN - 1.
    let mut expected: Vec<String> = expected_full
        .into_iter()
        .map(|n| n.chars().take(63).collect::<String>())
        .collect();
    expected.sort();

    assert_eq!(schema_index_names(db).await, expected);
}

#[tokio::test]
async fn test_schema_version_postgres() {
    let db = crate::test_utils::postgres::pool().await;
    db.upgrade().await.unwrap();
    assert_eq!(
        schema_db_version(&db).await,
        td_schema::CURRENT_VERSION.to_string()
    );
}

#[tokio::test]
async fn test_schema_applies_postgres() {
    let db = crate::test_utils::postgres::pool().await;
    db.upgrade().await.expect("schema upgrade on postgres");
    ApiserverComponent
        .check(&db.serving)
        .await
        .expect("schema check on postgres");
}

/// A 1.x database lacks the `tabsdata_version` row and must be rejected by the
/// major-version gate before any db_version comparison runs.
#[tokio::test]
async fn test_pre_2_0_database_rejected() {
    let db = crate::test_utils::postgres::pool().await;
    db.upgrade().await.unwrap();

    super::execute(
        &db.serving,
        "DELETE FROM tabsdata_system WHERE name = 'tabsdata_version'",
    )
    .await;

    let res = ApiserverComponent.check(&db.serving).await;
    assert!(
        matches!(res, Err(DbError::IncompatibleMajorVersion(1, _))),
        "expected IncompatibleMajorVersion(1, _), got {res:?}"
    );
}
