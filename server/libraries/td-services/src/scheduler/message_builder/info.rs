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
        .project_id(f.project_id)
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

#[cfg(test)]
mod tests {
    use super::*;
    use td_objects::types::basic::{
        AtTime, BasePath, BundleId, CollectionId, CollectionName, Decorator, ExecutionId,
        FunctionId, FunctionName, FunctionVersionId, PhysicalFunctionRunId,
        PhysicalFunctionRunStatus, ProjectId, StageId, StorageVersion, TransactionId, Trigger,
        TriggeredOn, UserId, UserName,
    };

    /// Proves each stamped base_path on `PhysicalFunctionRunToExecute` is routed
    /// to the CORRECT storage location (no cross-wiring): bundle → `bundle_base_path`,
    /// staging → `stage_base_path`, function-run content → `collection_base_path`.
    /// With distinct prefixes `/b`, `/s`, `/c` we assert each produced URI carries
    /// its own prefix and not another's.
    #[test]
    fn test_function_info_uses_stamped_base_paths() {
        // Storage mounted at "/" over a temp dir, so a path "/b/..." renders to
        // a file URI whose path is "<tempdir>/b/...". We assert on the path segments.
        let storage = Storage::default();

        let f = PhysicalFunctionRunToExecute {
            id: PhysicalFunctionRunId::new(),
            project_id: ProjectId::default(),
            collection_id: CollectionId::default(),
            function_id: FunctionId::default(),
            function_version_id: FunctionVersionId::new(),
            execution_id: ExecutionId::new(),
            transaction_id: TransactionId::new(),
            triggered_on: TriggeredOn::now(),
            triggered_by_id: UserId::admin(),
            trigger: Trigger::Manual,
            started_on: None,
            ended_on: None,
            status: PhysicalFunctionRunStatus::Requested,
            function_version_patch_id: None,
            ended_on_hour_ms: None,
            ended_on_day_ms: None,
            ended_on_week_ms: None,
            ended_on_month_ms: None,
            metric_table_versions: None,
            metric_rows: None,
            metric_data_bytes: None,
            name: FunctionName::try_from("fn0").unwrap(),
            collection: CollectionName::try_from("c0").unwrap(),
            execution: None,
            execution_function_version_id: FunctionVersionId::new(),
            triggered_by: UserName::try_from("u0").unwrap(),
            storage_version: StorageVersion::V1,
            bundle_id: BundleId::default(),
            defined_on: AtTime::now(),
            decorator: Decorator::Transformer,
            bundle_base_path: BasePath::try_from("/b").unwrap(),
            stage_id: StageId::default(),
            stage_base_path: BasePath::try_from("/s").unwrap(),
            collection_base_path: BasePath::try_from("/c").unwrap(),
        };

        let info = build_function_info(&storage, &f).unwrap();

        let bundle = info.function_bundle.uri.path();
        let stage = info.function_stage.uri.path();
        let data = info.function_data.uri.path();

        // Each base_path lands under its own location and nowhere else.
        assert!(
            bundle.contains("/b/bundles/"),
            "bundle path should carry bundle_base_path '/b': {bundle}"
        );
        assert!(
            stage.contains("/s/stages/"),
            "stage path should carry stage_base_path '/s': {stage}"
        );
        assert!(
            data.contains("/c/c/"),
            "function-run content path should carry collection_base_path '/c': {data}"
        );

        // No cross-wiring: the bundle/stage prefixes must not leak into other paths.
        assert!(
            !bundle.contains("/s/"),
            "bundle path leaked stage prefix: {bundle}"
        );
        assert!(
            !bundle.contains("/c/c/"),
            "bundle path leaked collection prefix: {bundle}"
        );
        assert!(
            !stage.contains("/b/"),
            "stage path leaked bundle prefix: {stage}"
        );
        assert!(
            !stage.contains("/c/c/"),
            "stage path leaked collection prefix: {stage}"
        );
        assert!(
            !data.contains("/b/"),
            "data path leaked bundle prefix: {data}"
        );
        assert!(
            !data.contains("/s/"),
            "data path leaked stage prefix: {data}"
        );
    }
}
