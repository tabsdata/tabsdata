//
//  Copyright 2024 Tabs Data Inc.
//

use td_common::execution_status::DataVersionStatus;
use td_error::TdError;
use td_objects::datasets::dao::DsDataVersion;
use td_objects::dlo::DataVersionId;
use td_tower::default_services::Condition;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

pub async fn check_data_version_run_requested_status(
    Connection(connection): Connection,
    Input(data_version): Input<DataVersionId>,
) -> Result<Condition, TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    const SELECT_DATA_VERSION_STATUS: &str = r#"
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

    let data_version = sqlx::query_as(SELECT_DATA_VERSION_STATUS)
        .bind(data_version.to_string())
        .fetch_optional(&mut *conn)
        .await;

    let is_valid = match data_version {
        Ok(Some(dv)) => {
            let dv: DsDataVersion = dv;
            matches!(dv.status(), DataVersionStatus::RunRequested)
        }
        _ => false,
    };

    Ok(Condition(is_valid))
}
