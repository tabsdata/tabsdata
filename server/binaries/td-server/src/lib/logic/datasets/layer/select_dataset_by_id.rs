//
//  Copyright 2024 Tabs Data Inc.
//

use td_common::error::TdError;
use td_database::sql::DbError;
use td_objects::crudl::handle_select_one_err;
use td_objects::datasets::dao::*;
use td_objects::dlo::CollectionId;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

use crate::logic::datasets::error::DatasetError;
use td_objects::datasets::dto::*;

pub async fn select_function_by_id(
    Connection(connection): Connection,
    Input(upload_function): Input<UploadFunction>,
    Input(collection_id): Input<CollectionId>,
) -> Result<DsFunction, TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    let collection_id = collection_id.as_str();
    let dataset = upload_function.dataset();
    let function_id = upload_function.function_id();

    const SELECT: &str = r#"
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
        WHERE
            collection_id = ?1
            AND
            name = ?2
            AND
            id = ?3
    "#;

    let function: DsFunction = sqlx::query_as(SELECT)
        .bind(collection_id.to_string())
        .bind(dataset.to_string())
        .bind(function_id.to_string())
        .fetch_one(conn)
        .await
        .map_err(handle_select_one_err(
            DatasetError::FunctionNotFound,
            DbError::SqlError,
        ))?;
    Ok(function)
}
