//
// Copyright 2026 Tabsdata Inc.
//

//! Storage addressing for a run's scratch area: temporary or work files a run
//! keeps in storage instead of the pod's disk, keyed by the physical run so each
//! attempt has its own.

use td_error::TdError;
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDBWithNames;
use td_storage::SPath;
use td_storage::location::StorageLocation;

pub fn scratch_dir(run: &PhysicalFunctionRunDBWithNames) -> Result<SPath, TdError> {
    let (path, _) = StorageLocation::from(&run.storage_version)
        .builder(&run.base_path)
        .collection(&run.collection_id)
        .function_run(&run.id)
        .scratch()
        .build()?;
    Ok(path)
}
