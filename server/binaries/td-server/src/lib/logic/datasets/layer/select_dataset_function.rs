//
// Copyright 2024 Tabs Data Inc.
//

use td_error::TdError;
use td_objects::crudl::select_by;
use td_objects::datasets::dao::DsFunction;
use td_objects::dlo::DatasetId;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

pub async fn select_dataset_function(
    Connection(connection): Connection,
    Input(dataset_id): Input<DatasetId>,
) -> Result<DsFunction, TdError> {
    const SELECT_DS_FUNCTION_SQL: &str = r#"
            SELECT
                id,
                name,
                description,
                collection_id,
                dataset_id,
                data_location,
                storage_location_version,
                bundle_hash,
                bundle_avail,
                function_snippet,
                execution_template,
                execution_template_created_on,
                created_on,
                created_by_id
            FROM ds_functions
            WHERE dataset_id = ?1
        "#;

    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    let ds_function: DsFunction = select_by(conn, SELECT_DS_FUNCTION_SQL, &dataset_id).await?;
    Ok(ds_function)
}
