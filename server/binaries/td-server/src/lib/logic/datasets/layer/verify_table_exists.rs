//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::datasets::error::DatasetError;
use td_common::error::TdError;
use td_database::sql::DbError;
use td_objects::crudl::handle_select_one_err;
use td_objects::datasets::dao::VersionInfo;
use td_objects::dlo::{TableName, Value};
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

pub async fn verify_table_exists(
    Connection(connection): Connection,
    Input(table): Input<TableName>,
    Input(version_info): Input<VersionInfo>,
) -> Result<(), TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    const SELECT_TABLE_EXISTS: &str = r#"
                SELECT 1
                FROM ds_tables
                WHERE collection_id = ?1
                  AND dataset_id = ?2
                  AND function_id = ?3
                  AND name = ?4
            "#;
    let _ = sqlx::query(SELECT_TABLE_EXISTS)
        .bind(version_info.collection_id())
        .bind(version_info.dataset_id())
        .bind(version_info.function_id())
        .bind(table.value())
        .fetch_one(conn)
        .await
        .map_err(handle_select_one_err(
            DatasetError::TableNotFound,
            DbError::SqlError,
        ))?;
    Ok(())
}
