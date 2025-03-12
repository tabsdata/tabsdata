//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::datasets::error::DatasetError;
use sqlx::FromRow;
use td_database::sql::DbError;
use td_error::TdError;
use td_objects::crudl::handle_select_one_err;
use td_objects::dlo::{CollectionName, DatasetId, TableName, Value};
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

pub async fn find_table_dataset_id(
    Connection(connection): Connection,
    Input(collection_name): Input<CollectionName>,
    Input(table_name): Input<TableName>,
) -> Result<DatasetId, TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;
    const SQL: &str = r#"
                SELECT dataset_id as id
                FROM ds_current_tables_with_names
                WHERE collection = ?1
                    AND name = ?2
            "#;

    #[derive(Debug, FromRow)]
    struct Id {
        id: String,
    }

    let dataset_id: Id = sqlx::query_as(SQL)
        .bind(collection_name.value())
        .bind(table_name.value())
        .fetch_one(conn)
        .await
        .map_err(handle_select_one_err(
            DatasetError::TableNotFound,
            DbError::SqlError,
        ))?;
    Ok(DatasetId::new(dataset_id.id))
}
