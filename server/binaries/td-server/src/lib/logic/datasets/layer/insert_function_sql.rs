//
//  Copyright 2024 Tabs Data Inc.
//
//

use td_common::error::TdError;
use td_objects::crudl::handle_sql_err;
use td_objects::datasets::dao::*;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

pub async fn insert_function_sql(
    Connection(connection): Connection,
    Input(function): Input<DsFunction>,
) -> Result<(), TdError> {
    const INSERT_SQL: &str = r#"
              INSERT INTO ds_functions (
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
              )
              VALUES
                    (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, false, ?9, NULL, NULL, ?10, ?11)
        "#;

    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    sqlx::query(INSERT_SQL)
        .bind(function.id())
        .bind(function.name())
        .bind(function.description())
        .bind(function.collection_id())
        .bind(function.dataset_id())
        .bind(function.data_location())
        .bind(function.storage_location_version().to_string())
        .bind(function.bundle_hash())
        .bind(function.function_snippet())
        .bind(function.created_on())
        .bind(function.created_by_id())
        .execute(conn)
        .await
        .map_err(handle_sql_err)?;
    Ok(())
}
