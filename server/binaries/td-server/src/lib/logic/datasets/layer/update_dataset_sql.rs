//
// Copyright 2024 Tabs Data Inc.
//

use td_common::error::TdError;
use td_objects::crudl::handle_sql_err;
use td_objects::datasets::dao::*;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

pub async fn update_dataset_sql(
    Connection(connection): Connection,
    Input(dataset): Input<Dataset>,
) -> Result<(), TdError> {
    const UPDATE_SQL: &str = r#"
              UPDATE datasets
                 SET name = ?1,
                     modified_on = ?2,
                     modified_by_id = ?3,
                     current_function_id = ?4
               WHERE id = ?5
        "#;

    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    sqlx::query(UPDATE_SQL)
        .bind(dataset.name())
        .bind(dataset.modified_on())
        .bind(dataset.modified_by_id())
        .bind(dataset.current_function_id())
        .bind(dataset.id())
        .execute(conn)
        .await
        .map_err(handle_sql_err)?;
    Ok(())
}
