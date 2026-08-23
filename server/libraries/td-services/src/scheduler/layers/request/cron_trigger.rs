//
// Copyright 2025 Tabsdata Inc.
//

use super::shared::{internal_get_callback, internal_post_callback, internal_request};
use futures::future::join_all;
use http::Method;
use std::collections::HashMap;
use td_bridge::messaging::{
    CronRequest, CronStartRequest, CronStopRequest, OrchestratorMessagePayload,
};
use td_bridge::{BridgeProviderHandle, Orchestrator};
use td_common::id;
use td_common::jwt::JwtConfig;
use td_common::worker::{Callback, MessageAction, RequestMessagePayload, WorkerClass, WorkerName};
use td_error::{ApiError, TdError, api_error};
use td_objects::dxo::worker::{CronTriggerConfig, WorkerDBWithNames};
use td_objects::dxo::yaml::cron_trigger::CronTriggerInput;
use td_objects::dxo::yaml::{WorkerInput, cron_trigger};
use td_objects::rest_urls::internal::{FUNCTION_VERSION_EXECUTE, RESOLVE_WORKER, UPDATE_WORKER};
use td_objects::types::addresses::InternalServerEndpoint;
use td_objects::types::basic::{AtTime, WorkerConfig};
use td_tower::extractors::Ref;
use tracing::{instrument, warn};

#[instrument(
    name = "scheduler.create_cron_trigger_workers_start",
    skip_all,
    level = "debug",
    err(Debug)
)]
pub async fn create_locked_cron_trigger_workers_start_request(
    bridge: Ref<BridgeProviderHandle>,
    server_endpoint: Ref<InternalServerEndpoint>,
    jwt: Ref<JwtConfig>,
    cron_trigger_workers: Ref<Vec<WorkerDBWithNames>>,
) -> Result<Vec<WorkerDBWithNames>, TdError> {
    // Supervisor resolves the full cron-trigger message eagerly here; the
    // kubernetes path defers resolution to the gateway/pod and ships a lean
    // request built entirely from the worker row. Both shapes are assembled
    // from `WorkerDBWithNames` alone — no extra fetch.
    let kubernetes = matches!(bridge.orchestrator(), Orchestrator::Kubernetes);

    // One worker's failure must not abort the tick: skip it (it stays
    // `Scheduled`, re-picked next tick) and keep the rest.
    let mut kept = Vec::with_capacity(cron_trigger_workers.len());
    for cron_trigger_worker in cron_trigger_workers.iter() {
        let result = put_cron_trigger_start(
            &bridge,
            &server_endpoint,
            &jwt,
            kubernetes,
            cron_trigger_worker,
        )
        .await;
        match result {
            Ok(()) => kept.push(cron_trigger_worker.clone()),
            Err(e) => warn!(
                worker_id = %cron_trigger_worker.id,
                reason = ?e,
                "request: skipping cron-trigger start; will retry next tick"
            ),
        }
    }
    Ok(kept)
}

pub(crate) async fn put_cron_trigger_start(
    bridge: &BridgeProviderHandle,
    server_endpoint: &InternalServerEndpoint,
    jwt: &JwtConfig,
    kubernetes: bool,
    cron_trigger_worker: &WorkerDBWithNames,
) -> Result<(), TdError> {
    let worker_id = cron_trigger_worker.id.to_string();
    let server_address = server_endpoint.authority();
    let callback = internal_post_callback(
        jwt,
        server_address,
        UPDATE_WORKER,
        &[("worker_id", &worker_id)],
    )?;

    let function_version_id = cron_trigger_worker.function_version_id.to_string();

    // Build trigger task (tokenized POST to the execute endpoint).
    let task = internal_request(
        jwt,
        server_address,
        Method::POST,
        FUNCTION_VERSION_EXECUTE,
        &[("function_version_id", &function_version_id)],
        HashMap::from([("content-type".to_string(), "application/json".to_string())]),
        true,
    )?;

    let payload = if kubernetes {
        // Kubernetes: lean request built entirely from the worker
        // row. The pod resolves its message from the resolver.
        let resolver = internal_get_callback(
            jwt,
            server_address,
            RESOLVE_WORKER,
            &[("worker_id", &worker_id)],
        )?;
        // The CronJob's schedule must be known when the gateway
        // creates it, so the mask travels in the start request
        // rather than being resolved in-pod later. It is not a
        // worker row field: it is nested in the worker's serialized
        // `CronTriggerConfig`, so deserialize it to reach the mask.
        let cron_config: CronTriggerConfig = cron_trigger_worker
            .worker_config
            .as_ref()
            .ok_or_else(|| {
                api_error!(
                    ApiError::InternalError,
                    "Cron trigger worker '{worker_id}' is missing its worker config"
                )
            })
            .and_then(|config| {
                serde_json::from_str(config.as_str()).map_err(|e| {
                    api_error!(
                        ApiError::InternalError,
                        "Could not parse cron trigger worker config for '{worker_id}': {e}"
                    )
                })
            })?;
        OrchestratorMessagePayload::OrchestratorCronRequestMessagePayload(CronRequest::Start(
            CronStartRequest {
                worker: WorkerName::CronTrigger,
                worker_id: cron_trigger_worker.id.clone(),
                resolver,
                trigger: Callback::Http(task),
                responder: callback,
                project_id: cron_trigger_worker.project_id,
                project: cron_trigger_worker.project.clone(),
                collection_id: cron_trigger_worker.collection_id,
                collection: cron_trigger_worker.collection.clone(),
                connection_id: cron_trigger_worker.connection_id,
                connection: cron_trigger_worker.connection.clone(),
                function_version_id: cron_trigger_worker.function_version_id.clone(),
                function: cron_trigger_worker.function.clone(),
                schedule: cron_config.mask,
            },
        ))
    } else {
        // Supervisor: resolve the full message here.
        // TODO ADD get_states to states
        let info = cron_trigger::Info {
            worker_id: cron_trigger_worker.id.clone(),
            collection_id: cron_trigger_worker.collection_id,
            collection: cron_trigger_worker.collection.clone(),
            function_version_id: cron_trigger_worker.function_version_id.clone(),
            function: cron_trigger_worker.function.clone(),
            scheduled_on: AtTime::now(),
            task,
            config: WorkerConfig::serialize(&cron_trigger_worker.worker_config)?,
        };
        let cron_trigger_input = CronTriggerInput { info };
        let context = WorkerInput::CronTrigger(Box::new(cron_trigger_input));

        let message_payload = RequestMessagePayload::<serde_yaml::Value> {
            class: WorkerClass::Ephemeral,
            worker: WorkerName::CronTrigger.as_ref().to_string(),
            worker_id: worker_id.clone(),
            action: MessageAction::Start,
            arguments: Vec::new(),
            callback: Some(callback),
            context: Some(serde_yaml::to_value(&context).map_err(|e| {
                api_error!(
                    ApiError::InternalError,
                    "Failed to serialize cron trigger input: {e}"
                )
            })?),
        };
        OrchestratorMessagePayload::OrchestratorRequestMessagePayload(message_payload)
    };

    bridge.message_queue().put(worker_id, payload).await?;
    Ok(())
}

#[instrument(
    name = "scheduler.create_cron_trigger_workers_stop",
    skip_all,
    level = "debug",
    err(Debug)
)]
pub async fn create_locked_cron_trigger_workers_stop_request(
    bridge: Ref<BridgeProviderHandle>,
    server_endpoint: Ref<InternalServerEndpoint>,
    jwt: Ref<JwtConfig>,
    cron_trigger_workers: Ref<Vec<WorkerDBWithNames>>,
) -> Result<Vec<WorkerDBWithNames>, TdError> {
    let kubernetes = matches!(bridge.orchestrator(), Orchestrator::Kubernetes);

    let futures: Vec<_> = cron_trigger_workers
        .iter()
        .map(|cron_trigger_worker| {
            let bridge = bridge.clone();
            let server_endpoint = server_endpoint.clone();
            let jwt = jwt.clone();
            async move {
                let worker_id = cron_trigger_worker.id.to_string();
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
                        OrchestratorMessagePayload::OrchestratorCronRequestMessagePayload(
                            CronRequest::Stop(CronStopRequest {
                                worker: WorkerName::CronTrigger,
                                worker_id: cron_trigger_worker.id.clone(),
                                responder: callback,
                            }),
                        ),
                    )
                } else {
                    let message_payload = RequestMessagePayload::<serde_yaml::Value> {
                        class: WorkerClass::Ephemeral,
                        worker: WorkerName::CronTrigger.as_ref().to_string(),
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

                // Add it to the queue
                bridge.message_queue().put(message_id, payload).await?;
                Ok::<_, TdError>(())
            }
        })
        .collect();
    // One worker's failure must not abort the tick: skip it (it stays
    // `StopScheduled`, re-picked next tick) and keep the rest.
    let results = join_all(futures).await;
    let mut kept = Vec::with_capacity(cron_trigger_workers.len());
    for (worker, result) in cron_trigger_workers.iter().zip(results) {
        match result {
            Ok(()) => kept.push(worker.clone()),
            Err(e) => warn!(
                worker_id = %worker.id,
                reason = ?e,
                "request: skipping cron-trigger stop; will retry next tick"
            ),
        }
    }
    Ok(kept)
}
