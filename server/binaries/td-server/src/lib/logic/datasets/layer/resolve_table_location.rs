//
// Copyright 2024 Tabs Data Inc.
//

use td_common::error::TdError;
use td_objects::datasets::dao::VersionInfo;
use td_objects::dlo::{TableName, Value};
use td_storage::SPath;
use td_tower::extractors::Input;

pub async fn resolve_table_location(
    Input(table): Input<TableName>,
    Input(version_info): Input<VersionInfo>,
) -> Result<SPath, TdError> {
    let storage_location = version_info.storage_location_version();
    let (path, _) = storage_location
        .builder(SPath::parse(version_info.data_location())?)
        .collection(version_info.collection_id())
        .dataset(version_info.dataset_id())
        .function(version_info.function_id())
        .version(version_info.version_id())
        .table(table.value())
        .build();
    Ok(path)
}
