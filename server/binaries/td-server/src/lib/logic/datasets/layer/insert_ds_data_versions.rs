//
//   Copyright 2024 Tabs Data Inc.
//

use td_common::error::TdError;
use td_objects::crudl::handle_create_error;
use td_objects::datasets::dao::DsDataVersion;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

pub async fn insert_ds_data_versions(
    Connection(connection): Connection,
    Input(data_versions): Input<Vec<DsDataVersion>>,
) -> Result<(), TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    const INSERT_DS_DATA_VERSION: &str = r#"
        INSERT INTO ds_data_versions (
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
        )
        VALUES
            (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
    "#;

    for data_version in data_versions.iter() {
        sqlx::query(INSERT_DS_DATA_VERSION)
            .bind(data_version.id())
            .bind(data_version.collection_id())
            .bind(data_version.dataset_id())
            .bind(data_version.function_id())
            .bind(data_version.transaction_id())
            .bind(data_version.execution_plan_id())
            .bind(data_version.trigger())
            .bind(data_version.triggered_on())
            .bind(data_version.started_on())
            .bind(data_version.ended_on())
            .bind(data_version.commit_id())
            .bind(data_version.commited_on())
            .bind(data_version.status().to_string())
            .execute(&mut *conn)
            .await
            .map_err(handle_create_error)?;
    }

    Ok(())
}
