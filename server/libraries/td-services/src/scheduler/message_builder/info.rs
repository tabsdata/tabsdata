//
// Copyright 2026 Tabs Data Inc.
//

use super::build_external_location;
use td_error::TdError;
use td_objects::Buildable;
use td_objects::dxo::physical_function_run::PhysicalFunctionRunToExecute;
use td_objects::dxo::yaml::function_runner::v2::FunctionInfo;
use td_storage::Storage;
use td_storage::location::StorageLocation;

/// Build the `FunctionInfo` block of a function-runner message: the
/// bundle / stage / function-data storage locations plus identity columns.
/// Uses the patched function-version id when one is resolved so reads and
/// writes go under the patched identity.
pub(crate) fn build_function_info(
    storage: &Storage,
    f: &PhysicalFunctionRunToExecute,
) -> Result<FunctionInfo, TdError> {
    let storage_location = StorageLocation::from(&f.storage_version);

    // Bundle location — the effective bundle's stamped base_path (from the to_execute join).
    let (path, _) = storage_location
        .builder(&f.bundle_base_path)
        .collection(&f.collection_id)
        .function(&f.bundle_id)
        .build()?;
    let (bundle_location, _) = build_external_location(storage, &path)?;

    // Staging location — the function's stamped stage_base_path + stage_id, so it does
    // not relocate when the collection base_path later changes.
    let (path, _) = storage_location
        .builder(&f.stage_base_path)
        .collection(&f.collection_id)
        .stage(&f.stage_id)
        .build()?;
    let (stage_location, _) = build_external_location(storage, &path)?;

    // Patch if resolved, otherwise head — drives the worker's data uri
    // and the yaml `function_version_id` so reads/writes go under the
    // patched identity.
    let effective_fvid = f
        .function_version_patch_id
        .as_ref()
        .unwrap_or(&f.function_version_id);

    // Function run content location — the collection's (live) base_path.
    let (path, _) = storage_location
        .builder(&f.collection_base_path)
        .collection(&f.collection_id)
        .transaction(&f.transaction_id)
        .function_version(effective_fvid)
        .build()?;
    let (function_run_location, _) = build_external_location(storage, &path)?;

    let info = FunctionInfo::builder()
        .collection_id(f.collection_id)
        .collection(f.collection.clone())
        .function_version_id(effective_fvid)
        .function_defined_on(f.defined_on)
        .function(f.name.clone())
        .function_run_id(f.id.clone())
        .function_bundle(bundle_location)
        .function_stage(stage_location)
        .triggered_on(f.triggered_on)
        .transaction_id(&f.transaction_id)
        .execution_id(&f.execution_id)
        .execution_name(f.execution.clone())
        .function_data(function_run_location)
        .build()?;
    Ok(info)
}
