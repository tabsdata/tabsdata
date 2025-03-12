//
// Copyright 2024 Tabs Data Inc.
//

use td_error::TdError;
use td_execution::error::ExecutionPlannerError;
use td_objects::datasets::dao::DsTransaction;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

pub async fn insert_transactions(
    Connection(connection): Connection,
    Input(ds_transactions): Input<Vec<DsTransaction>>,
) -> Result<(), TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    const INSERT_SQL: &str = r#"
        INSERT INTO ds_transactions (
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
        )
        VALUES
            (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
    "#;

    for transaction in ds_transactions.iter() {
        sqlx::query(INSERT_SQL)
            .bind(transaction.id())
            .bind(transaction.execution_plan_id())
            .bind(transaction.transaction_by().to_string())
            .bind(transaction.transaction_key())
            .bind(transaction.triggered_by_id())
            .bind(transaction.triggered_on())
            .bind(transaction.started_on())
            .bind(transaction.ended_on())
            .bind(transaction.commit_id())
            .bind(transaction.commited_on())
            .bind(transaction.status().to_string())
            .execute(&mut *conn)
            .await
            .map_err(ExecutionPlannerError::CouldNotInsertExecutionPlan)?;
    }

    Ok(())
}
