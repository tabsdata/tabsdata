//
//  Copyright 2024 Tabs Data Inc.
//

use td_error::TdError;
use td_objects::crudl::handle_select_error;
use td_objects::datasets::dao::DsReadyToExecute;
use td_objects::dlo::Limit;
use td_tower::extractors::{Connection, IntoMutSqlConnection, SrvCtx};

pub async fn poll_execution_requirements(
    Connection(connection): Connection,
    SrvCtx(limit): SrvCtx<Limit>,
) -> Result<Vec<DsReadyToExecute>, TdError> {
    const SELECT_REQUIREMENTS: &str = r#"
        SELECT
            transaction_id,
            execution_plan_id,

            collection_id,
            collection_name,
            dataset_id,
            dataset_name,
            function_id,
            data_version,

            data_location,
            storage_location_version
        FROM ds_datasets_ready_to_execute
        LIMIT ?1
    "#;

    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    let limit: &i32 = &limit;
    let ds: Vec<DsReadyToExecute> = sqlx::query_as(SELECT_REQUIREMENTS)
        .bind(limit)
        .fetch_all(&mut *conn)
        .await
        .map_err(handle_select_error)?;

    Ok(ds)
}
