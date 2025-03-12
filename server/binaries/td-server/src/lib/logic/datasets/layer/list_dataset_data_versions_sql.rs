//
//  Copyright 2024 Tabs Data Inc.
//

use crate::logic::datasets::error::DatasetError;
use td_error::TdError;
use td_objects::crudl::{list_result, list_select, ListRequest, ListResult};
use td_objects::datasets::dao::DsDataVersion;
use td_objects::dlo::{DatasetId, Value};
use td_objects::rest_urls::FunctionParam;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

pub async fn list_dataset_data_versions_sql(
    Connection(connection): Connection,
    Input(dataset_id): Input<DatasetId>,
    Input(request): Input<ListRequest<FunctionParam>>,
) -> Result<ListResult<DsDataVersion>, TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    const SELECT_FUNCTIONS: &str = r#"
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
        FROM ds_data_versions_with_names
        WHERE
             dataset_id = ?1
        ORDER BY triggered_on DESC
    "#;

    let daos: Vec<DsDataVersion> =
        sqlx::query_as(&list_select(request.list_params(), SELECT_FUNCTIONS))
            .persistent(true)
            .bind(dataset_id.value())
            .fetch_all(&mut *conn)
            .await
            .map_err(DatasetError::SqlError)?;

    Ok(list_result(request.list_params().clone(), daos))
}
