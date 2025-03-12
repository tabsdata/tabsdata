//
// Copyright 2024 Tabs Data Inc.
//

use td_error::TdError;
use td_execution::error::ExecutionPlannerError;
use td_objects::datasets::dao::DsExecutionPlan;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

pub async fn insert_execution_plan(
    Connection(connection): Connection,
    Input(ds_execution_plan): Input<DsExecutionPlan>,
) -> Result<(), TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    const INSERT_SQL: &str = r#"
        INSERT INTO ds_execution_plans (
            id,
            name,
            collection_id,
            dataset_id,
            function_id,
            plan,
            triggered_by_id,
            triggered_on
        )
        VALUES
            (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
    "#;

    sqlx::query(INSERT_SQL)
        .bind(ds_execution_plan.id())
        .bind(ds_execution_plan.name())
        .bind(ds_execution_plan.collection_id())
        .bind(ds_execution_plan.dataset_id())
        .bind(ds_execution_plan.function_id())
        .bind(ds_execution_plan.plan())
        .bind(ds_execution_plan.triggered_by_id())
        .bind(ds_execution_plan.triggered_on())
        .execute(&mut *conn)
        .await
        .map_err(ExecutionPlannerError::CouldNotInsertExecutionPlan)?;

    Ok(())
}
