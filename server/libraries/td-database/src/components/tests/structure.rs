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
        "executions",
        "function_patches",
        "functions",
        "groups",
        "logical_function_run_inputs",
        "logical_function_runs",
        "logical_table_data_versions",
        "permissions",
        "physical_function_runs",
        "physical_table_data_versions",
        "projects",
        "roles",
        "sessions",
        "table_partitions",
        "tables",
        "tabsdata_system",
        "transactions",
        "triggers",
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
        "execution_status_summary",
        "executions__with_names",
        "executions__with_status",
        "function_patches__with_names",
        "function_runs__with_names",
        "function_versions__with_names",
        "functions__with_names",
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
        "transaction_status_summary",
        "transactions__with_names",
        "transactions__with_status",
        "triggers__read",
        "triggers__with_names",
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
    // `td-schema`'s `test_index_names_fit_postgres_identifiers` keeps every
    // declared name inside PostgreSQL's 63-byte identifier limit, so these
    // compare verbatim.
    let mut expected: Vec<String> = [
        "collections___project_id__group_id___idx",
        "collections___project_id__id___idx",
        "collections___project_id__name___idx",
        "collections___project_id__resolved_name___idx",
        "dependencies___dependency_id___current_uidx",
        "dependencies___dependency_id__valid_from__id___idx",
        "dependencies___function_id___current_idx",
        "dependencies___function_id___idx",
        "dependencies___project_id__id___idx",
        "dependencies___project_id___current_idx",
        "dependencies___table_id___current_idx",
        "dependencies___table_id___idx",
        "executions___function_id__external_uid___idx",
        "executions___fv_id__trigger_type__triggered_on___idx",
        "executions___project_id__id___idx",
        "executions___project_id__triggered_on__id___idx",
        "function_patches___function_id__valid_from___idx",
        "function_patches___parent_function_version_id___current_uidx",
        "function_patches___parent_fv_id__valid_from__id___idx",
        "functions___collection_id__name___current_idx",
        "functions___collection_id__name___idx",
        "functions___function_id___current_uidx",
        "functions___function_id__valid_from__id___idx",
        "functions___project_id__id___idx",
        "functions___project_id__status___current_idx",
        "groups___id___idx",
        "groups___name___idx",
        "groups___project_id__name___idx",
        "lfri___requirement_logical_table_data_version_id___idx",
        "logical_function_run_inputs___execution_id___idx",
        "logical_function_run_inputs___logical_function_run_id___idx",
        "logical_function_runs___execution_id__function_id___idx",
        "logical_function_runs___execution_id__status___idx",
        "logical_function_runs___function_id__triggered_on__id___idx",
        "logical_function_runs___function_version_id___idx",
        "logical_function_runs___project_id__group_id___idx",
        "logical_function_runs___project_id__id___idx",
        "logical_function_runs___status__project_id__triggered_on___idx",
        "logical_function_runs___transaction_id__status___idx",
        "logical_table_data_versions___execution_id__table_id___idx",
        "logical_table_data_versions___project_id__group_id___idx",
        "logical_table_data_versions___project_id__id___idx",
        "logical_table_data_versions___table_id__triggered_on__id___idx",
        "ltdv___producer_logical_function_run_id___idx",
        "permissions___role_id__permission_type__entity___idx",
        "pfr___function_version_id___idx",
        "pfr___function_version_patch_id___idx",
        "pfr___logical_function_run_id___current_uidx",
        "pfr___logical_function_run_id__valid_from__id___idx",
        "pfr___project_id__ended_on___current_idx",
        "pfr___project_id__execution_id__started_on__ended_on___idx",
        "pfr___project_id__id___idx",
        "pfr___project_id__transaction_id__started_on__ended_on___idx",
        "pfr___status__project_id__dispatched_on___idx",
        "pfr___status__project_id__valid_from___idx",
        "projects___resolved_name___idx",
        "ptdv___logical_table_data_version_id___current_uidx",
        "ptdv___logical_table_data_version_id__valid_from__id___idx",
        "ptdv___physical_function_run_id__table_id___uidx",
        "ptdv___project_id__id___idx",
        "roles___project_id___idx",
        "roles___user_id__role_id___idx",
        "sessions___expires_on___idx",
        "sessions___status___idx",
        "sessions___user_id___idx",
        "tables___collection_id__name___current_idx",
        "tables___collection_id__name___idx",
        "tables___function_id___current_idx",
        "tables___function_id__status___idx",
        "tables___function_version_id___idx",
        "tables___project_id__id___idx",
        "tables___project_id__status___current_idx",
        "tables___table_id___current_uidx",
        "tables___table_id__valid_from__id___idx",
        "transactions___execution_id___idx",
        "transactions___project_id__group_id___idx",
        "transactions___project_id__id___idx",
        "transactions___project_id__triggered_on__id___idx",
        "triggers___function_id___current_idx",
        "triggers___function_id___idx",
        "triggers___project_id__id___idx",
        "triggers___project_id__status___current_idx",
        "triggers___trigger_by_table_id___current_idx",
        "triggers___trigger_by_table_id___idx",
        "triggers___trigger_id___current_uidx",
        "triggers___trigger_id__valid_from__id___idx",
        "users_roles___role_id__id___idx",
        "workers___collection_id___idx",
        "workers___enabled__worker_type__status__project_id__id___idx",
        "workers___function_version_id__worker_type___idx",
        "workers___physical_function_run_id___idx",
        "workers___project_id__id___idx",
    ]
    .map(str::to_string)
    .to_vec();
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
