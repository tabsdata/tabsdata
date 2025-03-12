//
// Copyright 2025 Tabs Data Inc.
//

use td_common::dataset::DatasetRef;
use td_common::uri::TdUri;
use td_error::TdError;
use td_execution::dataset::DatasetWithUris;
use td_execution::execution_planner::{ExecutionPlan, ExecutionPlanWithNames};
use td_objects::crudl::handle_select_error;
use td_objects::datasets::dao::DatasetWithNames;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

pub async fn execution_plan_with_names(
    Connection(connection): Connection,
    Input(execution_plan): Input<ExecutionPlan>,
) -> Result<ExecutionPlanWithNames, TdError> {
    let execution_plan_with_uris = execution_plan
        .named(|d| select_dataset_with_uris(Connection(connection.clone()), d.dataset()))
        .await?;
    Ok(execution_plan_with_uris)
}

async fn select_dataset_with_uris(
    Connection(connection): Connection,
    dataset_id: &str,
) -> Result<DatasetWithUris, TdError> {
    const SELECT_DATASET: &str = r#"
            SELECT
                id,
                name,
                description,
                collection_id,
                collection,

                created_on,
                created_by_id,
                created_by,
                modified_on,
                modified_by_id,
                modified_by,

                current_function_id,
                current_data_id,
                last_run_on,
                data_versions,
                data_location,
                bundle_avail,
                function_snippet
            FROM datasets_with_names
            WHERE id = ?1
        "#;

    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    let dataset_with_names: DatasetWithNames = sqlx::query_as(SELECT_DATASET)
        .bind(dataset_id.to_string())
        .fetch_one(conn)
        .await
        .map_err(handle_select_error)?;

    let uri_with_ids = TdUri::new(
        dataset_with_names.collection_id(),
        dataset_with_names.id(),
        None,
        None,
    )?;

    let uri_with_names = TdUri::new(
        dataset_with_names.collection(),
        dataset_with_names.name(),
        None,
        None,
    )?;

    Ok(DatasetWithUris::new(uri_with_ids, uri_with_names))
}
