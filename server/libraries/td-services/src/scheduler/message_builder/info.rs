//
// Copyright 2026 Tabsdata Inc.
//

use super::build_external_location;
use crate::function_run::landing::landing_dir;
use crate::function_run::scratch::scratch_dir;
use td_error::{ApiError, TdError, api_error};
use td_objects::dxo::physical_function_run::PhysicalFunctionRunToExecute;
use td_objects::dxo::worker_input::function::FunctionInfo;
use td_objects::types::basic::{AtTime, Decorator};
use td_storage::Storage;
use td_storage::location::StorageLocation;

/// Build the `FunctionInfo` block of a function-runner message: the
/// bundle / stage / function-data / landing / scratch storage locations plus
/// identity columns.
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
    let effective_fvid = match &f.function_version_patch_id {
        Some(patch) => (**patch).clone(),
        None => f.function_version_id.clone(),
    };

    // Function run content location — the prefix stamped on the run at mint.
    let (path, _) = storage_location
        .builder(&f.base_path)
        .collection(&f.collection_id)
        .transaction(&f.transaction_id)
        .function_version(&effective_fvid)
        .build()?;
    let (function_run_location, _) = build_external_location(storage, &path)?;

    // Landing-data location — only publishers ingest, so only they get one. Keyed by
    // the physical run so each attempt keeps its own copy and any data version
    // resolves back to the ingested data through its physical_function_run_id.
    let function_landing = match f.decorator {
        Decorator::Publisher => {
            let (location, _) = build_external_location(storage, &landing_dir(f)?)?;
            Some(location)
        }
        Decorator::Transformer | Decorator::Subscriber => None,
    };

    let (function_scratch, _) = build_external_location(storage, &scratch_dir(f)?)?;

    let project = f
        .base
        .project
        .clone()
        .ok_or_else(|| {
            api_error!(
                ApiError::InternalError,
                "function run [{}] references unknown project {}",
                f.id,
                f.project_id
            )
        })?
        .name()?;

    let info = FunctionInfo {
        project_id: f.project_id,
        project,
        collection_id: f.collection_id,
        collection: f.collection.name()?,
        function_id: f.function_id,
        function: f.name.clone(),
        function_version_id: effective_fvid.clone(),
        function_defined_on: f.defined_on,
        function_bundle: bundle_location,
        function_stage: stage_location,
        execution_id: f.execution_id.clone(),
        execution: f.execution.clone(),
        transaction_id: f.transaction_id.clone(),
        logical_function_run_id: f.logical_function_run_id.clone(),
        physical_function_run_id: f.id.clone(),
        triggered_on: AtTime::from(f.interval.valid_from),
        function_data: function_run_location,
        function_landing,
        function_scratch,
        scheduled_on: AtTime::default(),
    };
    Ok(info)
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_objects::dxo::physical_function_run::{
        PhysicalFunctionRunDB, PhysicalFunctionRunDBWithNames, PhysicalFunctionRunInsertDB,
    };
    use td_objects::dxo::scd2_interval::Scd2Interval;
    use td_objects::types::basic::Scd2ValidFrom;
    use td_objects::types::basic::{
        AtTime, BasePath, BundleId, CollectionId, CollectionName, Decorator, ExecutionId,
        FunctionId, FunctionName, FunctionVersionId, LogicalFunctionRunId, PhysicalFunctionRunId,
        PhysicalFunctionRunStatus, ProjectId, ProjectName, StageId, StorageVersion, TransactionId,
        Trigger, UserId, UserName,
    };
    use td_objects::types::basic::{CollectionLabel, ProjectLabel, UserLabel};

    /// Proves each stamped base_path on `PhysicalFunctionRunToExecute` is routed
    /// to the CORRECT storage location (no cross-wiring): bundle → `bundle_base_path`,
    /// staging → `stage_base_path`, function-run content and landing data → the
    /// prefix stamped on the run itself. With distinct prefixes `/b`, `/s`, `/r`
    /// we assert each produced URI carries its own prefix and not another's.
    #[test]
    fn test_function_info_uses_stamped_base_paths() {
        // Storage mounted at "/" over a temp dir, so a path "/b/..." renders to
        // a file URI whose path is "<tempdir>/b/...". We assert on the path segments.
        let storage = Storage::default();

        let f = PhysicalFunctionRunToExecute {
            base: PhysicalFunctionRunDBWithNames {
                base: PhysicalFunctionRunDB {
                    base: PhysicalFunctionRunInsertDB {
                        id: PhysicalFunctionRunId::new(),
                        collection_id: CollectionId::default(),
                        function_id: FunctionId::default(),
                        function_version_id: FunctionVersionId::new(),
                        execution_id: ExecutionId::new(),
                        transaction_id: TransactionId::new(),
                        triggered_by_id: UserId::admin(),
                        trigger: Trigger::Manual,
                        base_path: BasePath::try_from("/r").unwrap(),
                        dispatched_on: None,
                        started_on: None,
                        ended_on: None,
                        status: PhysicalFunctionRunStatus::Requested,
                        function_version_patch_id: None,
                        logical_function_run_id: LogicalFunctionRunId::new(),
                    },
                    project_id: ProjectId::default(),
                    interval: Scd2Interval::open(Scd2ValidFrom::asserted(*AtTime::now())),
                },
                name: FunctionName::try_from("fn0").unwrap(),
                project: Some(ProjectLabel::new(ProjectName::try_from("p0").unwrap())),
                collection: CollectionLabel::new(CollectionName::try_from("c0").unwrap()),
                execution: None,
                execution_function_version_id: FunctionVersionId::new().into(),
                storage_version: StorageVersion::V1,
                triggered_by: UserLabel::new(UserName::try_from("u0").unwrap()),
                connection: None,
            },
            bundle_id: BundleId::default(),
            defined_on: AtTime::now(),
            decorator: Decorator::Transformer,
            connection_id: None,
            bundle_base_path: BasePath::try_from("/b").unwrap(),
            stage_id: StageId::default(),
            stage_base_path: BasePath::try_from("/s").unwrap(),
        };

        let info = build_function_info(&storage, &f).unwrap();

        let bundle = info.function_bundle.uri.path();
        let stage = info.function_stage.uri.path();
        let data = info.function_data.uri.path();
        let scratch = info.function_scratch.uri.path();

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
            data.contains("/r/c/"),
            "function-run content path should carry the run's stamped prefix '/r': {data}"
        );

        assert!(
            scratch.contains(&format!("/r/c/{}/w/{}/s", f.collection_id, f.id)),
            "every run keeps its scratch area under its own physical run: {scratch}"
        );

        // Only publishers ingest, so only they are given a landing-data location.
        assert!(
            info.function_landing.is_none(),
            "a transformer must not be given a landing-data location"
        );
        let publisher = PhysicalFunctionRunToExecute {
            decorator: Decorator::Publisher,
            ..f.clone()
        };
        let landing = build_function_info(&storage, &publisher)
            .unwrap()
            .function_landing
            .expect("a publisher is given a landing-data location");
        let landing = landing.uri.path();
        assert!(
            landing.contains(&format!("/r/c/{}/w/{}/r", f.collection_id, f.id)),
            "landing data must hang off the physical run that ingested it, under the \
             prefix stamped on that run: {landing}"
        );

        // No cross-wiring: the bundle/stage prefixes must not leak into other paths.
        assert!(
            !bundle.contains("/s/"),
            "bundle path leaked stage prefix: {bundle}"
        );
        assert!(
            !bundle.contains("/r/c/"),
            "bundle path leaked the run prefix: {bundle}"
        );
        assert!(
            !stage.contains("/b/"),
            "stage path leaked bundle prefix: {stage}"
        );
        assert!(
            !stage.contains("/r/c/"),
            "stage path leaked the run prefix: {stage}"
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
