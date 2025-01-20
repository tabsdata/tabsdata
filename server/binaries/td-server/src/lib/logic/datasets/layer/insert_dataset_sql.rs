//
// Copyright 2024 Tabs Data Inc.
//

use td_common::error::TdError;
use td_objects::crudl::handle_sql_err;
use td_objects::datasets::dao::*;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

pub async fn insert_dataset_sql(
    Connection(connection): Connection,
    Input(dataset): Input<Dataset>,
) -> Result<(), TdError> {
    const INSERT_SQL: &str = r#"
              INSERT INTO datasets (
                    id,
                    name,
                    collection_id,
                    created_on,
                    created_by_id,
                    modified_on,
                    modified_by_id,
                    current_function_id,
                    current_data_id,
                    last_run_on,
                    data_versions
              )
              VALUES
                    (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, NULL, NULL, 0)
        "#;

    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    sqlx::query(INSERT_SQL)
        .bind(dataset.id())
        .bind(dataset.name())
        .bind(dataset.collection_id())
        .bind(dataset.created_on())
        .bind(dataset.created_by_id())
        .bind(dataset.modified_on())
        .bind(dataset.modified_by_id())
        .bind(dataset.current_function_id())
        .execute(conn)
        .await
        .map_err(handle_sql_err)?;
    Ok(())
}
