//
// Copyright 2024 Tabs Data Inc.
//

use td_common::error::TdError;
use td_objects::crudl::{handle_list_error, list_result, list_select, ListRequest, ListResult};
use td_objects::datasets::dao::DatasetWithNames;
use td_objects::dlo::{CollectionId, CollectionName};
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

pub async fn list_datasets_sql(
    Connection(connection): Connection,
    Input(request): Input<ListRequest<CollectionName>>,
    Input(collection_id): Input<CollectionId>,
) -> Result<ListResult<DatasetWithNames>, TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    const LIST_WITH_NAMES_SQL: &str = r#"
            SELECT
                id,
                name,
                description,
                collection_id,
                collection,
                created_on,
                created_by_id,
                created_by,
                modified_on,
                modified_by_id,
                modified_by,
                current_function_id,
                current_data_id,
                last_run_on,
                data_versions,
                data_location,
                bundle_avail,
                function_snippet
            FROM datasets_with_names
            WHERE collection_id = $1
        "#;

    let db_data: Vec<DatasetWithNames> =
        sqlx::query_as(&list_select(request.list_params(), LIST_WITH_NAMES_SQL))
            .persistent(true)
            .bind(collection_id.to_string())
            .fetch_all(conn)
            .await
            .map_err(handle_list_error)?;
    Ok(list_result(request.list_params().clone(), db_data))
}
