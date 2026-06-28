//
// Copyright 2026 Tabs Data Inc.
//

use super::build_external_location;
use td_database::dynamic::DynamicConnection;
use td_error::{ApiError, TdError, api_error};
use td_objects::dxo::worker::WorkerDBWithNames;
use td_objects::dxo::yaml::stream::{StreamerInfo, StreamerInput};
use td_objects::types::basic::AtTime;
use td_queries::LfrEntry;
use td_storage::Storage;
use td_storage::location::StorageLocation;

/// Build the lean k8s streamer `request.yaml` from a stage-trigger worker:
/// identity + bundle/stage storage locations, callback-free (the streamer
/// reads its callbacks from env). The streamer counterpart of
/// `build_function_v2` — needs only `conn` + `storage`, no jwt / server
/// addresses, so the resolve endpoint can call it from its request-serving
/// context.
pub(crate) async fn build_streamer_input(
    conn: &mut DynamicConnection<'_>,
    storage: &Storage,
    worker: &WorkerDBWithNames,
) -> Result<StreamerInput, TdError> {
    let f = conn
        .lfr()
        .with_bundle_base_path(std::slice::from_ref(&worker.function_version_id))
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| {
            api_error!(
                ApiError::InternalError,
                "stage trigger worker [{}] references unknown function_version_id {}",
                worker.id,
                worker.function_version_id
            )
        })?;

    let storage_location = StorageLocation::from(&f.storage_version);

    // Function bundle (stamped bundle_base_path + bundle_id).
    let (path, _) = storage_location
        .builder(&f.bundle_base_path)
        .collection(&f.collection_id)
        .function(&f.bundle_id)
        .build()?;
    let (function_bundle, _) = build_external_location(storage, &path)?;

    // Function stage (stamped stage_base_path + stage_id).
    let (path, _) = storage_location
        .builder(&f.stage_base_path)
        .collection(&f.collection_id)
        .stage(&f.stage_id)
        .build()?;
    let (function_stage, _) = build_external_location(storage, &path)?;

    let info = StreamerInfo {
        project_id: worker.project_id,
        collection_id: worker.collection_id,
        collection: worker.collection.clone(),
        function_version_id: worker.function_version_id.clone(),
        function_defined_on: f.defined_on,
        function: worker.function.clone(),
        function_bundle,
        function_stage,
        scheduled_on: AtTime::now(),
        worker_id: worker.id,
    };
    Ok(StreamerInput { info })
}
