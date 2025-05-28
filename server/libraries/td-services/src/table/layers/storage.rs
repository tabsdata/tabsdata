//
// Copyright 2025 Tabs Data Inc.
//

use polars::prelude::PolarsError;
use td_error::{td_error, TdError};
use td_objects::types::basic::TableName;
use td_objects::types::execution::TableDataVersionDBRead;
use td_storage::location::StorageLocation;
use td_storage::SPath;
use td_tower::extractors::Input;

#[td_error]
pub enum StorageServiceError {
    #[error("Table {0} has no data")]
    NoDataFound(TableName) = 0,

    #[error("Could not create storage configs: {0}")]
    CouldNotCreateStorageConfig(#[source] PolarsError) = 5000,
    #[error("Could not create lazy frame to get schema: {0}")]
    CouldNoCreateLazyFrameToGetSchema(#[source] PolarsError) = 5001,
    #[error("Could not get schema: {0}")]
    CouldNotGetSchema(#[source] PolarsError) = 5002,
}

pub async fn resolve_table_location(
    Input(data_version): Input<TableDataVersionDBRead>,
) -> Result<SPath, TdError> {
    let with_data_table_data_version_id = data_version
        .with_data_table_data_version_id()
        .ok_or_else(|| StorageServiceError::NoDataFound(data_version.table_name().clone()))?;

    let storage_location = data_version.storage_version();
    let (path, _) = StorageLocation::try_from(storage_location)
        .unwrap()
        .builder(data_version.data_location())
        .collection(data_version.collection_id())
        .data(&with_data_table_data_version_id)
        .table(data_version.table_id(), data_version.table_version_id())
        .build();
    Ok(path)
}
