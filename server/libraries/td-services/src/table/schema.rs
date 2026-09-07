//
// Copyright 2025 Tabsdata Inc.
//

use crate::table::columns::drop_extra_columns;
use crate::table::storage::StorageServiceError;
use polars::prelude::cloud::CloudOptions;
use polars::prelude::{Field, LazyFrame, PlPath, PolarsError, ScanArgsParquet, SchemaExt};
use td_error::{TdError, td_error};
use td_objects::dxo::table::{ExtraCols, SchemaField, TableSchema};
use td_storage::{SPath, Storage};

#[td_error]
enum SchemaError {
    #[error("Could not get schema: {0}")]
    CouldNotGetSchema(#[source] PolarsError) = 5000,
}

pub async fn get_table_schema(
    storage: &Storage,
    extra_cols: &ExtraCols,
    table_path: &Option<SPath>,
) -> Result<TableSchema, TdError> {
    fn get_schema(mut lazy_frame: LazyFrame) -> Result<TableSchema, TdError> {
        let schema = lazy_frame
            .collect_schema()
            .map_err(SchemaError::CouldNotGetSchema)?;
        let schema: Vec<SchemaField> = schema
            .iter_fields()
            .map(Field::try_into)
            .collect::<Result<_, _>>()?;
        Ok(TableSchema { fields: schema })
    }

    if let Some(table_path) = table_path {
        let (url, mount_def) = storage.to_external_uri(table_path)?;
        let url_str = url.to_string();
        let pl_path = PlPath::new(url_str.as_str());
        let cloud_config =
            CloudOptions::from_untyped_config(pl_path.cloud_scheme().as_ref(), mount_def.options())
                .map_err(StorageServiceError::CouldNotCreateStorageConfig)?;
        let parquet_config = ScanArgsParquet {
            cloud_options: Some(cloud_config),
            ..ScanArgsParquet::default()
        };

        tokio::task::block_in_place(move || {
            let lazy_frame = LazyFrame::scan_parquet(PlPath::new(url_str.as_str()), parquet_config)
                .map_err(StorageServiceError::CouldNoCreateLazyFrameToGetSchema)?;

            let lazy_frame = drop_extra_columns(lazy_frame, extra_cols)
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
