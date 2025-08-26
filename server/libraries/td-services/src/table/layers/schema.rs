//
// Copyright 2025 Tabs Data Inc.
//

use crate::table::layers::storage::StorageServiceError;
use polars::prelude::cloud::CloudOptions;
use polars::prelude::{Field, LazyFrame, PlPath, PolarsError, ScanArgsParquet, SchemaExt};
use td_error::{TdError, td_error};
use td_objects::types::table::{SchemaField, TableSchema};
use td_storage::{SPath, Storage};
use td_tableframe::common::drop_system_columns;
use td_tower::extractors::{Input, SrvCtx};

#[td_error]
enum SchemaError {
    #[error("Could not get schema: {0}")]
    CouldNotGetSchema(#[source] PolarsError) = 5000,
}

pub async fn get_table_schema(
    SrvCtx(storage): SrvCtx<Storage>,
    Input(table_path): Input<Option<SPath>>,
) -> Result<TableSchema, TdError> {
    fn get_schema(mut lazy_frame: LazyFrame) -> Result<TableSchema, TdError> {
        let schema = lazy_frame
            .collect_schema()
            .map_err(SchemaError::CouldNotGetSchema)?;
        let schema: Vec<SchemaField> = schema
            .iter_fields()
            .map(Field::try_into)
            .collect::<Result<_, _>>()?;
        Ok(TableSchema::builder().fields(schema).build()?)
    }

    if let Some(table_path) = &*table_path {
        let (url, mount_def) = storage.to_external_uri(table_path)?;
        let url_str = url.to_string();
        let cloud_config = CloudOptions::from_untyped_config(&url_str, mount_def.options())
            .map_err(StorageServiceError::CouldNotCreateStorageConfig)?;
        let parquet_config = ScanArgsParquet {
            cloud_options: Some(cloud_config),
            ..ScanArgsParquet::default()
        };

        tokio::task::block_in_place(move || {
            let lazy_frame = LazyFrame::scan_parquet(PlPath::new(url_str.as_str()), parquet_config)
                .map_err(StorageServiceError::CouldNoCreateLazyFrameToGetSchema)?;

            let lazy_frame = drop_system_columns(lazy_frame)
                .map_err(StorageServiceError::CouldNoCreateLazyFrameToGetSchema)?;

            get_schema(lazy_frame)
        })
    } else {
        tokio::task::block_in_place(move || {
            let lazy_frame = LazyFrame::default();
            get_schema(lazy_frame)
        })
    }
}
