//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::datasets::error::DatasetError;
use polars::prelude::cloud::CloudOptions;
use polars::prelude::{Field, LazyFrame, ScanArgsParquet, SchemaExt};
use std::collections::HashMap;
use td_common::error::TdError;
use td_objects::datasets::dto::SchemaField;
use td_storage::{SPath, Storage};
use td_tower::extractors::{Context, Input};

pub async fn get_table_schema(
    Context(storage): Context<Storage>,
    Input(table_path): Input<SPath>,
) -> Result<Vec<SchemaField>, TdError> {
    let url = storage.to_external_uri(&table_path)?;
    let url_str = url.to_string();
    let cloud_config =
        CloudOptions::from_untyped_config(&url_str, HashMap::<String, String>::new())
            .map_err(DatasetError::CouldNotCreateStorageConfig)?;
    let parquet_config = ScanArgsParquet {
        cloud_options: Some(cloud_config),
        ..ScanArgsParquet::default()
    };
    let schema_res: Result<_, TdError> = tokio::task::block_in_place(move || {
        let mut lazy_frame = LazyFrame::scan_parquet(&url_str, parquet_config)
            .map_err(DatasetError::CouldNoCreateLazyFrameToGetSchema)?;
        let schema = lazy_frame
            .collect_schema()
            .map_err(DatasetError::CouldNotGetSchema)?;
        let schema = schema
            .iter_fields()
            .map(Field::into)
            .collect::<Vec<SchemaField>>();
        Ok(schema)
    });
    schema_res
}
