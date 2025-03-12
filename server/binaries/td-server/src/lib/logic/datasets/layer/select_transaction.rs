//
//  Copyright 2024 Tabs Data Inc.
//

use td_error::TdError;
use td_objects::crudl::handle_select_error;
use td_objects::datasets::dao::DsTransaction;
use td_objects::dlo::TransactionId;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

pub async fn select_transaction(
    Connection(connection): Connection,
    Input(transaction_id): Input<TransactionId>,
) -> Result<DsTransaction, TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    const SELECT_SQL: &str = r#"
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
        WHERE
            id = ?1
    "#;

    let transaction: DsTransaction = sqlx::query_as(SELECT_SQL)
        .bind(transaction_id.as_str())
        .fetch_one(&mut *conn)
        .await
        .map_err(handle_select_error)?;

    Ok(transaction)
}
