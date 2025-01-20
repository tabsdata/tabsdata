//
// Copyright 2024 Tabs Data Inc.
//

use td_common::error::TdError;
use td_objects::crudl::{handle_list_error, list_result, list_select, ListRequest, ListResult};
use td_objects::datasets::dao::DsTransaction;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

pub async fn list_transactions_sql(
    Connection(connection): Connection,
    Input(request): Input<ListRequest<()>>,
) -> Result<ListResult<DsTransaction>, TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    const SELECT: &str = r#"
            SELECT
                id,
                execution_plan_id,
                transaction_by,
                transaction_key,
                triggered_by_id,
                triggered_on,
                started_on,
                ended_on,
                commit_id,
                commited_on,
                status
            FROM ds_transactions
            ORDER BY triggered_on DESC
        "#;

    let db_data: Vec<DsTransaction> = sqlx::query_as(&list_select(request.list_params(), SELECT))
        .persistent(true)
        .fetch_all(conn)
        .await
        .map_err(handle_list_error)?;
    Ok(list_result(request.list_params().clone(), db_data))
}
