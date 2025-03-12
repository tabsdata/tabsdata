//
// Copyright 2025 Tabs Data Inc.
//

use td_error::TdError;
use td_objects::crudl::handle_sql_err;
use td_objects::datasets::dao::DsWorkerMessageWithNames;
use td_objects::dlo::{Value, WorkerMessageId};
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

pub async fn select_ds_worker_message(
    Connection(connection): Connection,
    Input(message_id): Input<WorkerMessageId>,
) -> Result<DsWorkerMessageWithNames, TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

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
            id = ?1
    "#;

    let message = sqlx::query_as(SELECT_MESSAGE)
        .bind(message_id.value())
        .fetch_one(conn)
        .await
        .map_err(handle_sql_err)?;

    Ok(message)
}
