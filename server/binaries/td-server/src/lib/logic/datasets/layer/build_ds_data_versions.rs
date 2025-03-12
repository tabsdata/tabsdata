//
//   Copyright 2024 Tabs Data Inc.
//

use chrono::{DateTime, Utc};
use td_common::dataset::DatasetRef;
use td_common::execution_status::DataVersionStatus;
use td_common::id::{id, Id};
use td_error::TdError;
use td_execution::execution_planner::ExecutionTemplate;
use td_objects::crudl::handle_select_error;
use td_objects::datasets::dao::{DsDataVersion, DsDataVersionBuilder, DsFunction};
use td_objects::dlo::ExecutionPlanId;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};
use td_transaction::TransactionMap;

pub async fn build_ds_data_versions(
    Connection(connection): Connection,
    Input(execution_plan_id): Input<ExecutionPlanId>,
    Input(trigger_time): Input<DateTime<Utc>>,
    Input(transaction_ids): Input<TransactionMap<Id>>,
    Input(execution_template): Input<ExecutionTemplate>,
) -> Result<Vec<DsDataVersion>, TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    let mut ds_data_versions = Vec::new();

    const SELECT_DS_FUNCTION: &str = r#"
        SELECT
            id,
            name,
            description,
            collection_id,
            dataset_id,
            data_location,
            storage_location_version,
            bundle_hash,
            bundle_avail,
            function_snippet,
            execution_template,
            execution_template_created_on,
            created_on,
            created_by_id
        FROM ds_current_functions
        WHERE dataset_id = ?1
    "#;

    let (trigger_dataset, _) = execution_template.manual_trigger();
    let function: DsFunction = sqlx::query_as(SELECT_DS_FUNCTION)
        .bind(trigger_dataset.dataset())
        .fetch_one(&mut *conn)
        .await
        .map_err(handle_select_error)?;

    let data_version = DsDataVersionBuilder::default()
        .id(id().to_string())
        .collection_id(trigger_dataset.collection())
        .dataset_id(trigger_dataset.dataset())
        .function_id(function.id())
        .transaction_id(transaction_ids.get(trigger_dataset)?.to_string())
        .execution_plan_id(execution_plan_id.as_str())
        .trigger("M")
        .triggered_on(*trigger_time)
        .started_on(None)
        .ended_on(None)
        .commit_id(None)
        .commited_on(None)
        .status(DataVersionStatus::Scheduled)
        .build()
        .unwrap();
    ds_data_versions.push(data_version);

    for (planned_dataset, _) in execution_template.dependency_triggers() {
        let function: DsFunction = sqlx::query_as(SELECT_DS_FUNCTION)
            .bind(planned_dataset.dataset())
            .fetch_one(&mut *conn)
            .await
            .map_err(handle_select_error)?;

        let data_version = DsDataVersionBuilder::default()
            .id(id().to_string())
            .collection_id(planned_dataset.collection())
            .dataset_id(planned_dataset.dataset())
            .function_id(function.id())
            .transaction_id(transaction_ids.get(planned_dataset)?.to_string())
            .execution_plan_id(execution_plan_id.as_str())
            .trigger("D")
            .triggered_on(*trigger_time)
            .started_on(None)
            .ended_on(None)
            .commit_id(None)
            .commited_on(None)
            .status(DataVersionStatus::Scheduled)
            .build()
            .unwrap();
        ds_data_versions.push(data_version);
    }

    Ok(ds_data_versions)
}
