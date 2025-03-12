//
// Copyright 2024 Tabs Data Inc.
//

use td_error::TdError;
use td_objects::crudl::{handle_sql_err, list_result, ListRequest, ListResult};
use td_objects::datasets::dao::DsTableList;
use td_objects::dlo::CollectionId;
use td_objects::rest_urls::CollectionParam;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

pub async fn list_tables_sql(
    Connection(connection): Connection,
    Input(request): Input<ListRequest<CollectionParam>>,
    Input(collection_id): Input<CollectionId>,
) -> Result<ListResult<DsTableList>, TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    const SELECT_TABLES: &str = r#"
        SELECT
            id,
            name,
            dataset_id,
            dataset as function,
            function_id
        FROM ds_current_tables_with_names
        WHERE collection_id = ?1
          AND name IS NOT 'td-initial-values'
    "#;
    let mut query_as = sqlx::query_as(SELECT_TABLES);
    query_as = query_as.bind(collection_id.as_str());
    let db_data = query_as.fetch_all(conn).await.map_err(handle_sql_err)?;
    Ok(list_result(request.list_params().clone(), db_data))
}
