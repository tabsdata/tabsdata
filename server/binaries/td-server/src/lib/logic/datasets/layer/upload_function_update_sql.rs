//
// Copyright 2024 Tabs Data Inc.
//

use td_error::TdError;
use td_objects::crudl::handle_sql_err;
use td_objects::datasets::dao::DsFunction;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

pub async fn upload_function_update_sql(
    Connection(connection): Connection,
    Input(function): Input<DsFunction>,
) -> Result<(), TdError> {
    const UPDATE_SQL: &str = r#"
              UPDATE ds_functions
              SET bundle_avail = true
              WHERE id = ?1
        "#;

    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    sqlx::query(UPDATE_SQL)
        .bind(function.id())
        .execute(conn)
        .await
        .map_err(handle_sql_err)?;
    Ok(())
}
