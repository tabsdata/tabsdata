//
// Copyright 2025 Tabs Data Inc.
//

use td_error::TdError;
use td_objects::crudl::handle_sql_err;
use td_objects::datasets::dao::DsWorkerMessage;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

pub async fn insert_ds_worker_message(
    Connection(connection): Connection,
    Input(message): Input<DsWorkerMessage>,
) -> Result<(), TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    const INSERT_MESSAGE: &str = r#"
        INSERT INTO ds_worker_messages (
            id,
            collection_id,
            dataset_id,
            function_id,
            transaction_id,
            execution_plan_id,
            data_version_id
        )
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
    "#;

    sqlx::query(INSERT_MESSAGE)
        .bind(message.id())
        .bind(message.collection_id())
        .bind(message.dataset_id())
        .bind(message.function_id())
        .bind(message.transaction_id())
        .bind(message.execution_plan_id())
        .bind(message.data_version_id())
        .execute(conn)
        .await
        .map_err(handle_sql_err)?;

    Ok(())
}
