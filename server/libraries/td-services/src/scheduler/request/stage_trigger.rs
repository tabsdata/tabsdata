//
// Copyright 2025 Tabsdata Inc.
//

use crate::internal_api::InternalApi;
use crate::scheduler::message_builder::build_external_location;
use crate::scheduler::request::function_runner::WorkerMessages;
use std::collections::HashMap;
use td_bridge::messaging::{
    OrchestratorMessagePayload, StageTriggerRequest, StageTriggerStartRequest,
    StageTriggerStopRequest,
};
use td_bridge::{BridgeProviderHandle, Orchestrator};
use td_common::id;
use td_common::worker::{Callback, MessageAction, RequestMessagePayload, WorkerClass, WorkerName};
use td_database::access::{AccessMode, DbScope};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::{ApiError, TdError, api_error};
use td_objects::dxo::function::FunctionDBWithBundleBasePath;
use td_objects::dxo::worker::WorkerDBWithNames;
use td_objects::dxo::yaml::stage_trigger::StageTriggerInput;
use td_objects::dxo::yaml::{WorkerInput, stage_trigger};
use td_objects::types::basic::{AtTime, FunctionVersionId, WorkerConfig};
use td_queries::Queries;
use td_storage::Storage;
use td_storage::location::StorageLocation;
use tracing::{instrument, warn};

#[instrument(
    name = "scheduler.create_stage_trigger_workers_start",
    skip_all,
    level = "debug",
    err(Debug)
)]
pub async fn create_locked_stage_trigger_workers_start_request<S: DbScope, M: AccessMode>(
    orchestrator: Orchestrator,
    storage: &Storage,
    internal_api: &InternalApi,
    conn: &mut DomainConnection<Apiserver, S, M>,
    stage_trigger_workers: Vec<WorkerDBWithNames>,
    messages: &mut WorkerMessages,
) -> Result<Vec<WorkerDBWithNames>, TdError> {
    // Both orchestrators need the function's external storage locations
    // (bundle / stage / checkpoint), so pre-fetch every function record —
    // joined with its effective bundle's stamped base_path — in one chunked
    // query instead of per-worker round-trips inside the loop. The bundle
    // base_path is frozen on the bundle row; stage/checkpoint use the function's
    // own stamped stage_base_path + stage_id (carried on FunctionDB), so they
    // don't relocate when the collection base_path later changes. The supervisor
    // resolves the full message here; the kubernetes path ships a lean request
    // carrying just the callbacks, the locations, and the worker-row identity.
    let fvids: Vec<FunctionVersionId> = stage_trigger_workers
        .iter()
        .map(|w| w.base.base.function_version_id.clone())
        .collect();
    let function_by_fvid: HashMap<FunctionVersionId, FunctionDBWithBundleBasePath> = conn
        .logical_runs()
        .with_bundle_base_path(&fvids)
        .await?
        .into_iter()
        .map(|f| (f.base.base.id.clone(), f))
        .collect();

    let kubernetes = matches!(orchestrator, Orchestrator::Kubernetes);

    // One worker's failure must not abort the tick: skip it (it stays
    // `Scheduled`, re-picked next tick) and keep the rest.
    let mut kept = Vec::with_capacity(stage_trigger_workers.len());
    for stage_trigger_worker in stage_trigger_workers {
        let built = stage_trigger_start_message(
            storage,
            internal_api,
            &function_by_fvid,
            kubernetes,
            &stage_trigger_worker,
        );
        match built {
            Ok((id, payload)) => {
                messages.push((id, payload));
                kept.push(stage_trigger_worker);
            }
            Err(e) => warn!(
                worker_id = %stage_trigger_worker.base.base.id,
                reason = ?e,
                "request: skipping stage-trigger start; will retry next tick"
            ),
        }
    }
    Ok(kept)
}

pub(crate) fn stage_trigger_start_message(
    storage: &Storage,
    internal_api: &InternalApi,
    function_by_fvid: &HashMap<FunctionVersionId, FunctionDBWithBundleBasePath>,
    kubernetes: bool,
    stage_trigger_worker: &WorkerDBWithNames,
) -> Result<(String, OrchestratorMessagePayload<serde_yaml::Value>), TdError> {
    let worker_id = stage_trigger_worker.base.base.id.to_string();
    let callback = internal_api.worker_responder(&stage_trigger_worker.base.base.id)?;

    let function_version_id = &stage_trigger_worker.base.base.function_version_id;

    // POST endpoint to trigger the function (tokenized). Needed by both
    // orchestrators (supervisor embeds it in the resolved message; the
    // kubernetes lean request carries it directly).
    let fn_trigger = internal_api.function_version_execute(function_version_id, HashMap::new())?;

    let f = function_by_fvid
        .get(&stage_trigger_worker.base.base.function_version_id)
        .ok_or_else(|| {
            api_error!(
                ApiError::InternalError,
                "stage trigger worker references unknown function_version_id {}",
                stage_trigger_worker.base.base.function_version_id
            )
        })?;

    let payload = if kubernetes {
        // Kubernetes: lean request carrying the worker-lifecycle
        // callbacks + worker-row identity. The pod fetches its
        // request.yaml from the worker-scoped resolver endpoint.
        let resolver = internal_api.worker_resolver(&stage_trigger_worker.base.base.id)?;
        OrchestratorMessagePayload::OrchestratorStageTriggerRequestMessagePayload(
            StageTriggerRequest::Start(StageTriggerStartRequest {
                worker: WorkerName::StageTrigger,
                worker_id: stage_trigger_worker.base.base.id.clone(),
                decorator: f.base.base.decorator.to_message(),
                resolver,
                trigger: Callback::Http(fn_trigger),
                responder: callback,
                project_id: stage_trigger_worker.base.project_id,
                project: stage_trigger_worker.project.clone(),
                collection_id: stage_trigger_worker.base.base.collection_id,
                collection: stage_trigger_worker.collection.clone(),
                connection_id: stage_trigger_worker.connection_id,
                connection: stage_trigger_worker.connection.clone(),
                function_version_id: stage_trigger_worker.base.base.function_version_id.clone(),
                function: stage_trigger_worker.function.clone(),
            }),
        )
    } else {
        // Supervisor: resolve the full message here -- including the
        // external locations the k8s path leaves to the resolve endpoint.
        // fn_state / fn_status are only embedded in the eagerly-resolved
        // message.
        let storage_location = StorageLocation::from(&f.base.base.storage_version);
        // Bundle: stamped base_path on the durable bundle row, joined onto
        // the function row in `with_bundle_base_path`.
        let bundle_base_path = f.bundle_base_path.clone();

        let (path, _) = storage_location
            .builder(&bundle_base_path)
            .collection(&f.base.base.collection_id)
            .function(&f.base.base.bundle_id)
            .build()?;
        let (function_bundle, _) = build_external_location(storage, &path)?;

        let (path, _) = storage_location
            .builder(&f.base.base.stage_base_path)
            .collection(&f.base.base.collection_id)
            .stage(&f.base.base.stage_id)
            .build()?;
        let (function_stage, _) = build_external_location(storage, &path)?;

        let (path, _) = storage_location
            .builder(&f.base.base.stage_base_path)
            .collection(&f.base.base.collection_id)
            .checkpoint(&f.base.base.stage_id)
            .build()?;
        let (function_checkpoint, _) = build_external_location(storage, &path)?;

        let fn_state = internal_api.download_function_state(function_version_id)?;
        let fn_status = internal_api.function_version_execution_status(function_version_id)?;

        // TODO Urls are wrong as of now
        let info = stage_trigger::Info {
            worker_id: stage_trigger_worker.base.base.id.clone(),
            project_id: stage_trigger_worker.base.project_id,
            collection_id: stage_trigger_worker.base.base.collection_id,
            collection: stage_trigger_worker.collection.clone(),
            function_version_id: stage_trigger_worker.base.base.function_version_id.clone(),
            function: stage_trigger_worker.function.clone(),
            function_defined_on: f.base.interval.valid_from.into(),
            function_bundle,
            function_stage,
            function_checkpoint,
            scheduled_on: AtTime::now(),
            fn_trigger,
            fn_state,
            config: WorkerConfig::serialize(&stage_trigger_worker.base.base.worker_config)?,
            fn_status,
        };
        let stage_trigger_input = StageTriggerInput { info };
        let context = WorkerInput::StageTrigger(Box::new(stage_trigger_input));

        // TODO ADD get_states to states
        let message_payload = RequestMessagePayload::<serde_yaml::Value> {
            class: WorkerClass::Ephemeral,
            worker: WorkerName::StageTrigger.as_ref().to_string(),
            worker_id: worker_id.clone(),
            action: MessageAction::Start,
            arguments: Vec::new(),
            callback: Some(callback),
            context: Some(serde_yaml::to_value(&context).map_err(|e| {
                api_error!(
                    ApiError::InternalError,
                    "Failed to serialize stage trigger input: {e}"
                )
            })?),
        };
        OrchestratorMessagePayload::OrchestratorRequestMessagePayload(message_payload)
    };

    Ok((worker_id, payload))
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn put_stage_trigger_start(
    bridge: &BridgeProviderHandle,
    storage: &Storage,
    internal_api: &InternalApi,
    function_by_fvid: &HashMap<FunctionVersionId, FunctionDBWithBundleBasePath>,
    stage_trigger_worker: &WorkerDBWithNames,
) -> Result<(), TdError> {
    let kubernetes = matches!(bridge.orchestrator(), Orchestrator::Kubernetes);
    let (id, payload) = stage_trigger_start_message(
        storage,
        internal_api,
        function_by_fvid,
        kubernetes,
        stage_trigger_worker,
    )?;
    bridge.message_queue().put(id, payload).await?;
    Ok(())
}

#[instrument(
    name = "scheduler.create_stage_trigger_workers_stop",
    skip_all,
    level = "debug",
    err(Debug)
)]
pub fn create_locked_stage_trigger_workers_stop_request(
    orchestrator: Orchestrator,
    internal_api: &InternalApi,
    stage_trigger_workers: Vec<WorkerDBWithNames>,
    messages: &mut WorkerMessages,
) -> Result<Vec<WorkerDBWithNames>, TdError> {
    let kubernetes = matches!(orchestrator, Orchestrator::Kubernetes);

    // A worker whose message cannot be built must not abort the tick: skip it
    // (it stays `StopScheduled`, re-picked next tick) and keep the rest.
    let mut kept = Vec::with_capacity(stage_trigger_workers.len());
    for stage_trigger_worker in stage_trigger_workers {
        let built = (|| {
            let worker_id = stage_trigger_worker.base.base.id.to_string();
            let callback = internal_api.worker_responder(&stage_trigger_worker.base.base.id)?;

            // Kubernetes ships a lean Stop keyed by the worker id (so the
            // gateway can derive the pod to delete); supervisor ships a flat
            // Stop on the file queue under a distinct message id.
            let (message_id, payload) = if kubernetes {
                (
                    worker_id,
                    OrchestratorMessagePayload::OrchestratorStageTriggerRequestMessagePayload(
                        StageTriggerRequest::Stop(StageTriggerStopRequest {
                            worker: WorkerName::StageTrigger,
                            worker_id: stage_trigger_worker.base.base.id.clone(),
                            responder: callback,
                        }),
                    ),
                )
            } else {
                let message_payload = RequestMessagePayload::<serde_yaml::Value> {
                    class: WorkerClass::Ephemeral,
                    worker: WorkerName::StageTrigger.as_ref().to_string(),
                    worker_id,
                    action: MessageAction::Stop,
                    arguments: Vec::new(),
                    callback: Some(callback),
                    context: None,
                };
                (
                    id::id().to_string(),
                    OrchestratorMessagePayload::OrchestratorRequestMessagePayload(message_payload),
                )
            };

            Ok::<_, TdError>((message_id, payload))
        })();
        match built {
            Ok((id, payload)) => {
                messages.push((id, payload));
                kept.push(stage_trigger_worker);
            }
            Err(e) => warn!(
                worker_id = %stage_trigger_worker.base.base.id,
                reason = ?e,
                "request: skipping stage-trigger stop; will retry next tick"
            ),
        }
    }
    Ok(kept)
}
