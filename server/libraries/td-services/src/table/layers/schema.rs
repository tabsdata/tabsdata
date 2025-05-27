//
// Copyright 2025 Tabs Data Inc.
//

use polars::prelude::cloud::CloudOptions;
use polars::prelude::{Field, LazyFrame, PolarsError, ScanArgsParquet, SchemaExt};
use td_error::{td_error, TdError};
use td_objects::types::execution::TableDataVersionDB;
use td_objects::types::function::FunctionDB;
use td_objects::types::table::SchemaField;
use td_storage::location::StorageLocation;
use td_storage::{SPath, Storage};
use td_tower::extractors::{Input, SrvCtx};

#[td_error]
#[allow(clippy::enum_variant_names)]
enum SchemaError {
    #[error("Could not create storage configs: {0}")]
    CouldNotCreateStorageConfig(#[source] PolarsError) = 5005,
    #[error("Could not create lazy frame to get schema: {0}")]
    CouldNoCreateLazyFrameToGetSchema(#[source] PolarsError) = 5006,
    #[error("Could not get schema: {0}")]
    CouldNotGetSchema(#[source] PolarsError) = 5007,
}

pub async fn resolve_table_location(
    Input(function): Input<FunctionDB>,
    Input(table): Input<TableDataVersionDB>,
) -> Result<SPath, TdError> {
    let storage_location = function.storage_version();
    let (path, _) = StorageLocation::try_from(storage_location)
        .unwrap()
        .builder(function.data_location())
        .collection(table.collection_id())
        .data(table.id())
        .build();
    Ok(path)
}

pub async fn get_table_schema(
    SrvCtx(storage): SrvCtx<Storage>,
    Input(table_path): Input<SPath>,
) -> Result<Vec<SchemaField>, TdError> {
    let (url, mount_def) = storage.to_external_uri(&table_path)?;
    let url_str = url.to_string();
    let cloud_config = CloudOptions::from_untyped_config(&url_str, mount_def.configs())
        .map_err(SchemaError::CouldNotCreateStorageConfig)?;
    let parquet_config = ScanArgsParquet {
        cloud_options: Some(cloud_config),
        ..ScanArgsParquet::default()
    };
    let schema_res: Result<_, TdError> = tokio::task::block_in_place(move || {
        let mut lazy_frame = LazyFrame::scan_parquet(&url_str, parquet_config)
            .map_err(SchemaError::CouldNoCreateLazyFrameToGetSchema)?;
        let schema = lazy_frame
            .collect_schema()
            .map_err(SchemaError::CouldNotGetSchema)?;
        let schema = schema
            .iter_fields()
            .map(Field::try_into)
            .collect::<Result<_, _>>()?;
        Ok(schema)
    });
    schema_res
}
