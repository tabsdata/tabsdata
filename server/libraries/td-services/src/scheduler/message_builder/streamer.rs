//
// Copyright 2026 Tabsdata Inc.
//

use super::build_external_location;
use td_database::access::{AccessMode, DbScope};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::{ApiError, TdError, api_error};
use td_objects::dxo::worker::{WorkerDB, WorkerDBWithNames, WorkerInsertDB};
use td_objects::dxo::worker_input::stream::{StreamerInfo, StreamerInput};
use td_objects::types::basic::AtTime;
use td_queries::Queries;
use td_storage::Storage;
use td_storage::location::StorageLocation;

/// Build the lean k8s streamer `request.yaml` from a stage-trigger worker:
/// identity + bundle/stage storage locations, callback-free (the streamer
/// reads its callbacks from env). The streamer counterpart of
/// `build_function_v2` — needs only `conn` + `storage`, no jwt / server
/// addresses, so the resolve endpoint can call it from its request-serving
/// context.
pub(crate) async fn build_streamer_input<S: DbScope, M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    storage: &Storage,
    worker: WorkerDBWithNames,
) -> Result<StreamerInput, TdError> {
    let WorkerDBWithNames {
        base:
            WorkerDB {
                base:
                    WorkerInsertDB {
                        id,
                        collection_id,
                        function_version_id,
                        ..
                    },
                project_id,
                ..
            },
        collection,
        function_id,
        function,
        project,
        ..
    } = worker;
    let f = conn
        .logical_runs()
        .with_bundle_base_path(std::slice::from_ref(&function_version_id))
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| {
            api_error!(
                ApiError::InternalError,
                "stage trigger worker [{}] references unknown function_version_id {}",
                id,
                function_version_id
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

    let project = project
        .ok_or_else(|| {
            api_error!(
                ApiError::InternalError,
                "stage trigger worker [{}] references unknown project {}",
                id,
                project_id
            )
        })?
        .name()?;

    let info = StreamerInfo {
        project_id,
        project,
        collection_id,
        collection: collection.name()?,
        function_id,
        function,
        function_version_id,
        function_defined_on: f.interval.valid_from.into(),
        function_bundle,
        function_stage,
        worker_id: id,
        scheduled_on: AtTime::now(),
    };
    Ok(StreamerInput { info })
}
