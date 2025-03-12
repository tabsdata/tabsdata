//
// Copyright 2024 Tabs Data Inc.
//

use td_common::time::UniqueUtc;
use td_error::TdError;
use td_execution::execution_planner::ExecutionTemplate;
use td_objects::crudl::handle_update_error;
use td_objects::dlo::DatasetId;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

pub async fn update_execution_template(
    Connection(connection): Connection,
    Input(dataset_id): Input<DatasetId>,
    Input(execution_template): Input<ExecutionTemplate>,
) -> Result<(), TdError> {
    let serialized = serde_json::to_string(&execution_template).unwrap();
    let now = UniqueUtc::now_millis().await;

    const UPDATE_SQL: &str = r#"
        UPDATE ds_functions SET
            execution_template = ?1,
            execution_template_created_on = ?2
        WHERE
            dataset_id = ?3
    "#;

    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    sqlx::query(UPDATE_SQL)
        .bind(serialized)
        .bind(now)
        .bind(dataset_id.as_str())
        .execute(conn)
        .await
        .map_err(handle_update_error)?;
    Ok(())
}
