//
//  Copyright 2024 Tabs Data Inc.
//

use td_common::error::TdError;
use td_common::id::id;
use td_execution::parameters::{Location, OutputTable};
use td_objects::crudl::{handle_select_error, handle_sql_err};
use td_objects::datasets::dao::{DsFunction, DsTable, DsTableData};
use td_objects::dlo::{DataVersionId, FunctionId};
use td_storage::{SPath, Storage};
use td_tower::extractors::{Connection, Context, Input, IntoMutSqlConnection};

pub async fn build_worker_output_tables(
    Connection(connection): Connection,
    Context(storage): Context<Storage>,
    Input(data_version_id): Input<DataVersionId>,
    Input(function_id): Input<FunctionId>,
) -> Result<Vec<OutputTable>, TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    const SELECT_DS_TABLE_SQL: &str = r#"
            SELECT
                id,
                name,
                collection_id,
                dataset_id,
                function_id,
                pos
            FROM ds_tables
            WHERE function_id = ?1
            -- we are sorting by positive pos (0,1,2...), then negative pos (-1,-2,-3,...)
            ORDER BY
                CASE WHEN pos >= 0 THEN 1 ELSE 2 END,
                    ABS(pos);       
            "#;

    let mut output_tables = vec![];
    let tables: Vec<DsTable> = sqlx::query_as(SELECT_DS_TABLE_SQL)
        .bind(function_id.as_str())
        .fetch_all(&mut *conn)
        .await
        .map_err(handle_select_error)?;

    const SELECT_FUNCTION_SQL: &str = r#"
            SELECT
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
            FROM ds_functions
            WHERE id = ?1
        "#;

    let function: DsFunction = sqlx::query_as(SELECT_FUNCTION_SQL)
        .bind(function_id.as_str())
        .fetch_one(&mut *conn)
        .await
        .map_err(handle_select_error)?;

    for table in tables {
        let table_data = DsTableData::builder()
            .id(id().to_string())
            .collection_id(table.collection_id())
            .dataset_id(table.dataset_id())
            .function_id(table.function_id())
            .data_version_id(data_version_id.as_str())
            .table_id(table.id())
            .partition("") // TODO
            .schema_id("") // TODO
            .data_location(function.data_location())
            .storage_location_version(function.storage_location_version().clone())
            .build()
            .unwrap();

        const INSERT_DS_TABLE_DATA_SQL: &str = r#"
            INSERT INTO ds_table_data (
                id,
                collection_id,
                dataset_id,
                function_id,
                data_version_id,
                table_id,
                partition,
                schema_id,
                data_location,
                storage_location_version
            )
            VALUES
                (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
        "#;

        sqlx::query(INSERT_DS_TABLE_DATA_SQL)
            .bind(table_data.id())
            .bind(table_data.collection_id())
            .bind(table_data.dataset_id())
            .bind(table_data.function_id())
            .bind(table_data.data_version_id())
            .bind(table_data.table_id())
            .bind(table_data.partition())
            .bind(table_data.schema_id())
            .bind(table_data.data_location())
            .bind(table_data.storage_location_version().to_string())
            .execute(&mut *conn)
            .await
            .map_err(handle_sql_err)?;

        let (path, _) = table_data
            .storage_location_version()
            .builder(SPath::parse(table_data.data_location())?)
            .collection(table.collection_id())
            .dataset(table.dataset_id())
            .function(table.function_id())
            .version(table_data.data_version_id())
            .table(table.name())
            .build();

        let external_path = storage.to_external_uri(&path)?;
        let location = Location::builder()
            .uri(external_path)
            .env_prefix(None)
            .build()
            .unwrap();

        let output_table =
            OutputTable::from_table(table.name().to_string(), location, *table.pos());
        output_tables.push(output_table);
    }
    Ok(output_tables)
}
