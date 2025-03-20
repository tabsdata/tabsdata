//
//  Copyright 2024 Tabs Data Inc.
//

use indexmap::IndexMap;
use td_common::uri::TdUri;
use td_error::TdError;
use td_execution::parameters::{InputTable, InputTableVersion, Location};
use td_objects::crudl::handle_select_error;
use td_objects::datasets::dao::DsExecutionRequirementDependency;
use td_objects::dlo::DataVersionId;
use td_storage::{SPath, Storage};
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection, SrvCtx};

pub async fn build_worker_input_tables(
    Connection(connection): Connection,
    SrvCtx(storage): SrvCtx<Storage>,
    Input(data_version_id): Input<DataVersionId>,
) -> Result<Vec<InputTable>, TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    const SELECT_REQUIREMENT_DEPENDENCIES: &str = r#"
            SELECT
                collection_id,
                collection_name,
                dataset_id,
                dataset_name,
                function_id,
                table_name,
                pos,
                data_version,
                formal_data_version,
                data_version_pos,
                data_location,
                storage_location_version
            FROM ds_execution_requirement_dependencies
            WHERE target_data_version = ?1
        "#;

    let requirements: Vec<DsExecutionRequirementDependency> =
        sqlx::query_as(SELECT_REQUIREMENT_DEPENDENCIES)
            .bind(data_version_id.as_str())
            .fetch_all(&mut *conn)
            .await
            .map_err(handle_select_error)?;

    // Index map to maintain insertion order.
    let mut input_tables_map = IndexMap::new();
    for req in requirements {
        let td_uri_with_names = TdUri::new(
            req.collection_name(),
            req.dataset_name(),
            Some(req.table_name()),
            req.formal_data_version().as_deref(),
        )?;

        let td_uri_with_ids = TdUri::new(
            req.collection_id(),
            req.dataset_id(),
            Some(req.table_name()),
            req.data_version().as_deref(),
        )?;

        let location = match req.data_version() {
            Some(data_version) => {
                let (path, _) = req
                    .storage_location_version()
                    .builder(SPath::parse(req.data_location())?)
                    .collection(req.collection_id())
                    .dataset(req.dataset_id())
                    .function(req.function_id())
                    .version(data_version)
                    .table(req.table_name())
                    .build();
                let (external_path, mount_def) = storage.to_external_uri(&path)?;
                let location = Location::builder()
                    .uri(external_path)
                    .env_prefix(mount_def.id_as_prefix())
                    .build()
                    .unwrap();
                Some(location)
            }
            None => None,
        };

        let mut builder = InputTableVersion::builder();

        builder
            .name(req.table_name())
            .table(td_uri_with_names.to_string())
            .table_id(td_uri_with_ids.to_string())
            .table_pos(*req.pos())
            .version_pos(*req.data_version_pos());

        if let Some(location) = location {
            builder.location(location);
        }
        let input_table = builder.build().unwrap();

        input_tables_map
            .entry(*req.pos())
            .or_insert_with(Vec::new)
            .push(input_table);
    }

    let input_tables = input_tables_map
        .into_values()
        .map(InputTable::new)
        .collect();
    Ok(input_tables)
}
