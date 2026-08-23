//
// Copyright 2025 Tabsdata Inc.
//

use super::shared::{internal_get_callback, internal_post_callback, internal_request};
use crate::scheduler::message_builder::build_external_location;
use futures::future::join_all;
use http::Method;
use std::collections::HashMap;
use td_bridge::messaging::{
    OrchestratorMessagePayload, StageTriggerRequest, StageTriggerStartRequest,
    StageTriggerStopRequest,
};
use td_bridge::{BridgeProviderHandle, Orchestrator};
use td_common::id;
use td_common::jwt::JwtConfig;
use td_common::worker::{Callback, MessageAction, RequestMessagePayload, WorkerClass, WorkerName};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::{ApiError, TdError, api_error};
use td_objects::dxo::function::FunctionDBWithBundleBasePath;
use td_objects::dxo::worker::WorkerDBWithNames;
use td_objects::dxo::yaml::stage_trigger::StageTriggerInput;
use td_objects::dxo::yaml::{WorkerInput, stage_trigger};
use td_objects::rest_urls::internal::{
    DOWNLOAD_FUNCTION_STATE, FUNCTION_VERSION_EXECUTE, FUNCTION_VERSION_EXECUTION_STATUS,
    RESOLVE_WORKER, UPDATE_WORKER,
};
use td_objects::types::addresses::InternalServerEndpoint;
use td_objects::types::basic::{AtTime, FunctionVersionId, WorkerConfig};
use td_queries::LfrEntry;
use td_storage::Storage;
use td_storage::location::StorageLocation;
use td_tower::extractors::{MutRef, Ref};
use tracing::{instrument, warn};

#[instrument(
    name = "scheduler.create_stage_trigger_workers_start",
    skip_all,
    level = "debug",
    err(Debug)
)]
pub async fn create_locked_stage_trigger_workers_start_request(
    bridge: Ref<BridgeProviderHandle>,
    storage: Ref<Storage>,
    server_endpoint: Ref<InternalServerEndpoint>,
    jwt: Ref<JwtConfig>,
    mut conn: MutRef<DomainConnection<Apiserver>>,
    stage_trigger_workers: Ref<Vec<WorkerDBWithNames>>,
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
        .map(|w| w.function_version_id.clone())
        .collect();
    let function_by_fvid: HashMap<FunctionVersionId, FunctionDBWithBundleBasePath> = conn
        .lfr()
        .with_bundle_base_path(&fvids)
        .await?
        .into_iter()
        .map(|f| (f.id.clone(), f))
        .collect();

    let kubernetes = matches!(bridge.orchestrator(), Orchestrator::Kubernetes);

    // One worker's failure must not abort the tick: skip it (it stays
    // `Scheduled`, re-picked next tick) and keep the rest.
    let mut kept = Vec::with_capacity(stage_trigger_workers.len());
    for stage_trigger_worker in stage_trigger_workers.iter() {
        let result = put_stage_trigger_start(
            &bridge,
            &storage,
            &server_endpoint,
            &jwt,
            &function_by_fvid,
            kubernetes,
            stage_trigger_worker,
        )
        .await;
        match result {
            Ok(()) => kept.push(stage_trigger_worker.clone()),
            Err(e) => warn!(
                worker_id = %stage_trigger_worker.id,
                reason = ?e,
                "request: skipping stage-trigger start; will retry next tick"
            ),
        }
    }
    Ok(kept)
}

pub(crate) async fn put_stage_trigger_start(
    bridge: &BridgeProviderHandle,
    storage: &Storage,
    server_endpoint: &InternalServerEndpoint,
    jwt: &JwtConfig,
    function_by_fvid: &HashMap<FunctionVersionId, FunctionDBWithBundleBasePath>,
    kubernetes: bool,
    stage_trigger_worker: &WorkerDBWithNames,
) -> Result<(), TdError> {
    let worker_id = stage_trigger_worker.id.to_string();
    let server_address = server_endpoint.authority();
    let callback = internal_post_callback(
        jwt,
        server_address,
        UPDATE_WORKER,
        &[("worker_id", &worker_id)],
    )?;

    let function_version_id = stage_trigger_worker.function_version_id.to_string();
    let fv_binding = [("function_version_id", function_version_id.as_str())];

    // POST endpoint to trigger the function (tokenized). Needed by both
    // orchestrators (supervisor embeds it in the resolved message; the
    // kubernetes lean request carries it directly).
    let fn_trigger = internal_request(
        jwt,
        server_address,
        Method::POST,
        FUNCTION_VERSION_EXECUTE,
        &fv_binding,
        HashMap::new(),
        true,
    )?;

    let f = function_by_fvid
        .get(&stage_trigger_worker.function_version_id)
        .ok_or_else(|| {
            api_error!(
                ApiError::InternalError,
                "stage trigger worker references unknown function_version_id {}",
                stage_trigger_worker.function_version_id
            )
        })?;

    let payload = if kubernetes {
        // Kubernetes: lean request carrying the worker-lifecycle
        // callbacks + worker-row identity. The pod fetches its
        // request.yaml from the worker-scoped resolver endpoint.
        let resolver = internal_get_callback(
            jwt,
            server_address,
            RESOLVE_WORKER,
            &[("worker_id", &worker_id)],
        )?;
        OrchestratorMessagePayload::OrchestratorStageTriggerRequestMessagePayload(
            StageTriggerRequest::Start(StageTriggerStartRequest {
                worker: WorkerName::StageTrigger,
                worker_id: stage_trigger_worker.id.clone(),
                decorator: f.decorator.to_message(),
                resolver,
                trigger: Callback::Http(fn_trigger),
                responder: callback,
                project_id: stage_trigger_worker.project_id,
                project: stage_trigger_worker.project.clone(),
                collection_id: stage_trigger_worker.collection_id,
                collection: stage_trigger_worker.collection.clone(),
                connection_id: stage_trigger_worker.connection_id,
                connection: stage_trigger_worker.connection.clone(),
                function_version_id: stage_trigger_worker.function_version_id.clone(),
                function: stage_trigger_worker.function.clone(),
            }),
        )
    } else {
        // Supervisor: resolve the full message here -- including the
        // external locations the k8s path leaves to the resolve endpoint.
        // fn_state / fn_status are only embedded in the eagerly-resolved
        // message.
        let storage_location = StorageLocation::from(&f.storage_version);
        // Bundle: stamped base_path on the durable bundle row, joined onto
        // the function row in `with_bundle_base_path`.
        let bundle_base_path = f.bundle_base_path.clone();

        // Function bundle
        let (path, _) = storage_location
            .builder(&bundle_base_path)
            .collection(&f.collection_id)
            .function(&f.bundle_id)
            .build()?;
        let (function_bundle, _) = build_external_location(storage, &path)?;

        // Function stage (stamped stage_base_path + stage_id)
        let (path, _) = storage_location
            .builder(&f.stage_base_path)
            .collection(&f.collection_id)
            .stage(&f.stage_id)
            .build()?;
        let (function_stage, _) = build_external_location(storage, &path)?;

        // Function checkpoint (stamped stage_base_path + stage_id)
        let (path, _) = storage_location
            .builder(&f.stage_base_path)
            .collection(&f.collection_id)
            .checkpoint(&f.stage_id)
            .build()?;
        let (function_checkpoint, _) = build_external_location(storage, &path)?;

        let fn_state = internal_request(
            jwt,
            server_address,
            Method::GET,
            DOWNLOAD_FUNCTION_STATE,
            &fv_binding,
            HashMap::new(),
            false,
        )?;
        let fn_status = internal_request(
            jwt,
            server_address,
            Method::GET,
            FUNCTION_VERSION_EXECUTION_STATUS,
            &fv_binding,
            HashMap::new(),
            true,
        )?;

        // Build info
        // TODO Urls are wrong as of now
        let info = stage_trigger::Info {
            worker_id: stage_trigger_worker.id.clone(),
            project_id: stage_trigger_worker.project_id,
            collection_id: stage_trigger_worker.collection_id,
            collection: stage_trigger_worker.collection.clone(),
            function_version_id: stage_trigger_worker.function_version_id.clone(),
            function: stage_trigger_worker.function.clone(),
            function_defined_on: f.defined_on,
            function_bundle,
            function_stage,
            function_checkpoint,
            scheduled_on: AtTime::now(),
            fn_trigger,
            fn_state,
            config: WorkerConfig::serialize(&stage_trigger_worker.worker_config)?,
            fn_status,
        };
        // Build message context
        let stage_trigger_input = StageTriggerInput { info };
        let context = WorkerInput::StageTrigger(Box::new(stage_trigger_input));

        // Build message payload
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

    // Add it to the queue
    bridge.message_queue().put(worker_id, payload).await?;
    Ok(())
}

#[instrument(
    name = "scheduler.create_stage_trigger_workers_stop",
    skip_all,
    level = "debug",
    err(Debug)
)]
pub async fn create_locked_stage_trigger_workers_stop_request(
    bridge: Ref<BridgeProviderHandle>,
    server_endpoint: Ref<InternalServerEndpoint>,
    jwt: Ref<JwtConfig>,
    stage_trigger_workers: Ref<Vec<WorkerDBWithNames>>,
) -> Result<Vec<WorkerDBWithNames>, TdError> {
    let kubernetes = matches!(bridge.orchestrator(), Orchestrator::Kubernetes);

    let futures: Vec<_> = stage_trigger_workers
        .iter()
        .map(|stage_trigger_worker| {
            let bridge = bridge.clone();
            let server_endpoint = server_endpoint.clone();
            let jwt = jwt.clone();
            async move {
                let worker_id = stage_trigger_worker.id.to_string();
                let server_address = server_endpoint.authority();
                let callback = internal_post_callback(
                    &jwt,
                    server_address,
                    UPDATE_WORKER,
                    &[("worker_id", &worker_id)],
                )?;

                // Kubernetes ships a lean Stop keyed by the worker id (so the
                // gateway can derive the pod to delete); supervisor ships a flat
                // Stop on the file queue under a distinct message id.
                let (message_id, payload) = if kubernetes {
                    (
                        worker_id,
                        OrchestratorMessagePayload::OrchestratorStageTriggerRequestMessagePayload(
                            StageTriggerRequest::Stop(StageTriggerStopRequest {
                                worker: WorkerName::StageTrigger,
                                worker_id: stage_trigger_worker.id.clone(),
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
                        OrchestratorMessagePayload::OrchestratorRequestMessagePayload(
                            message_payload,
                        ),
                    )
                };

                bridge.message_queue().put(message_id, payload).await?;
                Ok::<_, TdError>(())
            }
        })
        .collect();
    // One worker's failure must not abort the tick: skip it (it stays
    // `StopScheduled`, re-picked next tick) and keep the rest.
    let results = join_all(futures).await;
    let mut kept = Vec::with_capacity(stage_trigger_workers.len());
    for (worker, result) in stage_trigger_workers.iter().zip(results) {
        match result {
            Ok(()) => kept.push(worker.clone()),
            Err(e) => warn!(
                worker_id = %worker.id,
                reason = ?e,
                "request: skipping stage-trigger stop; will retry next tick"
            ),
        }
    }
    Ok(kept)
}
