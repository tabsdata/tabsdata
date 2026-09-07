//
// Copyright 2026 Tabsdata Inc.
//

//! Storage addressing for a publisher run's raw data: what its source plugin
//! ingested, stamped with the framework's system columns, before the user
//! function saw it. Dispatch hands the directory to the worker and the read
//! path addresses a single file in it, both from here so they cannot drift.

use futures_util::TryStreamExt;
use sync_wrapper::SyncStream;
use td_error::TdError;
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDBWithNames;
use td_objects::types::basic::InputIdx;
use td_objects::types::stream::BoxedSyncStream;
use td_storage::location::StorageLocation;
use td_storage::{SPath, Storage};

pub fn raw_data_dir(run: &PhysicalFunctionRunDBWithNames) -> Result<SPath, TdError> {
    let (path, _) = StorageLocation::from(&run.storage_version)
        .builder(&run.base.base_path)
        .collection(&run.base.collection_id)
        .function_run(&run.base.id)
        .raw()
        .build()?;
    Ok(path)
}

pub fn raw_data_file(
    run: &PhysicalFunctionRunDBWithNames,
    input_idx: &InputIdx,
) -> Result<SPath, TdError> {
    let (path, _) = StorageLocation::from(&run.storage_version)
        .builder(&run.base.base_path)
        .collection(&run.base.collection_id)
        .function_run(&run.base.id)
        .raw_input(input_idx)
        .build()?;
    Ok(path)
}

pub async fn read_raw_data(storage: &Storage, path: &SPath) -> Result<BoxedSyncStream, TdError> {
    let stream = storage.read_stream(path).await?.map_err(TdError::from);
    Ok(BoxedSyncStream::new(SyncStream::new(stream)))
}
