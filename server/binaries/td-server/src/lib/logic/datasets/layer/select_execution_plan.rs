//
// Copyright 2025 Tabs Data Inc.
//

use crate::logic::datasets::error::DatasetError;
use td_common::error::TdError;
use td_database::sql::DbError;
use td_objects::crudl::handle_select_one_err;
use td_objects::datasets::dao::DsExecutionPlan;
use td_objects::dlo::ExecutionPlanId;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

pub async fn select_execution_plan(
    Connection(connection): Connection,
    Input(execution_plan_id): Input<ExecutionPlanId>,
) -> Result<DsExecutionPlan, TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    const SELECT_SQL: &str = r#"
        SELECT
            id,
            name,
            collection_id,
            dataset_id,
            function_id,
            plan,
            triggered_by_id,
            triggered_on
        FROM ds_execution_plans
        WHERE id = ?1
    "#;

    let execution_plan: DsExecutionPlan = sqlx::query_as(SELECT_SQL)
        .bind(execution_plan_id.as_str())
        .fetch_one(&mut *conn)
        .await
        .map_err(handle_select_one_err(
            DatasetError::ExecutionPlanNotFound,
            DbError::SqlError,
        ))?;

    Ok(execution_plan)
}
