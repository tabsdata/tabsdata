//
//  Copyright 2024 Tabs Data Inc.
//

use td_common::error::TdError;
use td_objects::crudl::handle_select_error;
use td_objects::datasets::dao::*;
use td_objects::dlo::DataVersionId;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

pub async fn select_data_version(
    Connection(connection): Connection,
    Input(data_version_id): Input<DataVersionId>,
) -> Result<DsDataVersion, TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    const SELECT_DATA_VERSION: &str = r#"
        SELECT
            id,
            collection_id,
            dataset_id,
            function_id,
            transaction_id,
            execution_plan_id,
            trigger,
            triggered_on,
            started_on,
            ended_on,
            commit_id,
            commited_on,
            status
        FROM ds_data_versions
        WHERE
            id = ?1
    "#;

    let data_version: DsDataVersion = sqlx::query_as(SELECT_DATA_VERSION)
        .bind(data_version_id.as_str())
        .fetch_one(&mut *conn)
        .await
        .map_err(handle_select_error)?;

    Ok(data_version)
}
