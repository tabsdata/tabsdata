//
// Copyright 2024 Tabs Data Inc.
//

use td_common::error::TdError;
use td_objects::crudl::handle_sql_err;
use td_objects::datasets::dao::*;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

pub async fn insert_dependencies_sql(
    Connection(connection): Connection,
    Input(deps): Input<Vec<DsDependency>>,
) -> Result<(), TdError> {
    const INSERT_SQL: &str = r#"
        INSERT INTO ds_dependencies (
            id,
            collection_id,
            dataset_id,
            function_id,

            table_collection_id,
            table_dataset_id,
            table_name,
            table_versions,
            pos
        )
        VALUES
            (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
    "#;

    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    for dep in deps.iter() {
        sqlx::query(INSERT_SQL)
            .bind(dep.id())
            .bind(dep.collection_id())
            .bind(dep.dataset_id())
            .bind(dep.function_id())
            .bind(dep.table_collection_id())
            .bind(dep.table_dataset_id())
            .bind(dep.table_name())
            .bind(dep.table_versions())
            .bind(dep.pos())
            .execute(&mut *conn)
            .await
            .map_err(handle_sql_err)?;
    }
    Ok(())
}
