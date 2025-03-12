//
// Copyright 2024 Tabs Data Inc.
//

use td_error::TdError;
use td_objects::crudl::handle_sql_err;
use td_objects::datasets::dao::*;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

pub async fn insert_tables_sql(
    Connection(connection): Connection,
    Input(tables): Input<Vec<DsTable>>,
) -> Result<(), TdError> {
    const INSERT_SQL: &str = r#"
              INSERT INTO ds_tables (
                    id,
                    name,
                    collection_id,
                    dataset_id,
                    function_id,
                    pos
              )
              VALUES
                    (?1, ?2, ?3, ?4, ?5, ?6)
        "#;

    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    for dep in tables.iter() {
        sqlx::query(INSERT_SQL)
            .bind(dep.id())
            .bind(dep.name())
            .bind(dep.collection_id())
            .bind(dep.dataset_id())
            .bind(dep.function_id())
            .bind(dep.pos())
            .execute(&mut *conn)
            .await
            .map_err(handle_sql_err)?;
    }
    Ok(())
}
