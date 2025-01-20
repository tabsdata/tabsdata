//
// Copyright 2025 Tabs Data Inc.
//

use td_common::error::TdError;
use td_objects::crudl::{handle_sql_err, list_result, ListRequest, ListResult};
use td_objects::datasets::dao::DsWorkerMessageWithNames;
use td_objects::rest_urls::{By, WorkerMessageListParam};
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

pub async fn list_ds_worker_messages(
    Connection(connection): Connection,
    Input(request): Input<ListRequest<WorkerMessageListParam>>,
    Input(by): Input<By>,
) -> Result<ListResult<DsWorkerMessageWithNames>, TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    let (query, id) = match &*by {
        By::FunctionId(function_id) => {
            const SELECT_MESSAGE: &str = r#"
                SELECT
                    id,
                    collection,
                    collection_id,
                    dataset,
                    dataset_id,
                    function,
                    function_id,
                    transaction_id,
                    execution_plan,
                    execution_plan_id,
                    data_version_id,
                    started_on,
                    status
                FROM ds_worker_messages_with_names
                WHERE
                    function_id = ?1
            "#;
            (SELECT_MESSAGE, function_id)
        }
        By::TransactionId(transaction_id) => {
            const SELECT_MESSAGE: &str = r#"
                SELECT
                    id,
                    collection,
                    collection_id,
                    dataset,
                    dataset_id,
                    function,
                    function_id,
                    transaction_id,
                    execution_plan,
                    execution_plan_id,
                    data_version_id,
                    started_on,
                    status
                FROM ds_worker_messages_with_names
                WHERE
                    transaction_id = ?1
            "#;
            (SELECT_MESSAGE, transaction_id)
        }
        By::ExecutionPlanId(execution_plan_id) => {
            const SELECT_MESSAGE: &str = r#"
                SELECT
                    id,
                    collection,
                    collection_id,
                    dataset,
                    dataset_id,
                    function,
                    function_id,
                    transaction_id,
                    execution_plan,
                    execution_plan_id,
                    data_version_id,
                    started_on,
                    status
                FROM ds_worker_messages_with_names
                WHERE
                    execution_plan_id = ?1
            "#;
            (SELECT_MESSAGE, execution_plan_id)
        }
        By::DataVersionId(data_version_id) => {
            const SELECT_MESSAGE: &str = r#"
                SELECT
                    id,
                    collection,
                    collection_id,
                    dataset,
                    dataset_id,
                    function,
                    function_id,
                    transaction_id,
                    execution_plan,
                    execution_plan_id,
                    data_version_id,
                    started_on,
                    status
                FROM ds_worker_messages_with_names
                WHERE
                    data_version_id = ?1
            "#;
            (SELECT_MESSAGE, data_version_id)
        }
    };

    let messages = sqlx::query_as(query)
        .bind(id.to_string())
        .fetch_all(conn)
        .await
        .map_err(handle_sql_err)?;

    Ok(list_result(request.list_params().clone(), messages))
}
