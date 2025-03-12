//
//  Copyright 2024 Tabs Data Inc.
//

use td_error::TdError;
use td_objects::crudl::handle_select_error;
use td_objects::datasets::dao::DsExecutionPlanWithNames;
use td_objects::dlo::ExecutionPlanId;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

pub async fn select_execution_plan_with_names(
    Connection(connection): Connection,
    Input(execution_plan_id): Input<ExecutionPlanId>,
) -> Result<DsExecutionPlanWithNames, TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    const SELECT_SQL: &str = r#"
        SELECT
            id,
            name,
            collection_id,
            collection,
            dataset_id,
            dataset,
            triggered_by_id,
            triggered_by,
            triggered_on,
            started_on,
            ended_on,
            status
        FROM ds_execution_plans_with_names
        WHERE
            id = ?1
    "#;

    let execution_plan: DsExecutionPlanWithNames = sqlx::query_as(SELECT_SQL)
        .bind(execution_plan_id.as_str())
        .fetch_one(&mut *conn)
        .await
        .map_err(handle_select_error)?;

    Ok(execution_plan)
}
