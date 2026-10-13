//
// Copyright 2025 Tabsdata Inc.
//

use crate::internal_api::InternalApi;
use crate::scheduler::request::function_runner::WorkerMessages;
use std::collections::HashMap;
use td_bridge::messaging::{
    CronRequest, CronStartRequest, CronStopRequest, OrchestratorMessagePayload,
};
use td_bridge::{BridgeProviderHandle, Orchestrator};
use td_common::id;
use td_common::worker::{Callback, MessageAction, RequestMessagePayload, WorkerClass, WorkerName};
use td_error::{ApiError, TdError, api_error};
use td_objects::dxo::worker::{CronTriggerConfig, WorkerDBWithNames};
use td_objects::dxo::worker_input::cron_trigger::CronTriggerInput;
use td_objects::dxo::worker_input::{WorkerInput, cron_trigger};
use td_objects::types::basic::{AtTime, WorkerConfig};
use tracing::{instrument, warn};

#[instrument(
    name = "scheduler.create_cron_trigger_workers_start",
    skip_all,
    level = "debug",
    err(Debug, level = "debug")
)]
pub fn create_locked_cron_trigger_workers_start_request(
    orchestrator: Orchestrator,
    internal_api: &InternalApi,
    cron_trigger_workers: Vec<WorkerDBWithNames>,
    messages: &mut WorkerMessages,
) -> Result<Vec<WorkerDBWithNames>, TdError> {
    // Supervisor resolves the full cron-trigger message eagerly here; the
    // kubernetes path defers resolution to the gateway/pod and ships a lean
    // request built entirely from the worker row. Both shapes are assembled
    // from `WorkerDBWithNames` alone — no extra fetch.
    let kubernetes = matches!(orchestrator, Orchestrator::Kubernetes);

    // One worker's failure must not abort the tick: skip it (it stays
    // `Scheduled`, re-picked next tick) and keep the rest.
    let mut kept = Vec::with_capacity(cron_trigger_workers.len());
    for cron_trigger_worker in cron_trigger_workers {
        match cron_trigger_start_message(internal_api, kubernetes, &cron_trigger_worker) {
            Ok((id, payload)) => {
                messages.push((id, payload));
                kept.push(cron_trigger_worker);
            }
            Err(e) => warn!(
                worker_id = %cron_trigger_worker.base.base.id,
                reason = ?e,
                "request: skipping cron-trigger start; will retry next tick"
            ),
        }
    }
    Ok(kept)
}

pub(crate) fn cron_trigger_start_message(
    internal_api: &InternalApi,
    kubernetes: bool,
    cron_trigger_worker: &WorkerDBWithNames,
) -> Result<(String, OrchestratorMessagePayload<serde_yaml::Value>), TdError> {
    let worker_id = cron_trigger_worker.base.base.id.to_string();
    let callback = internal_api.worker_responder(&cron_trigger_worker.base.base.id)?;

    // Build trigger task (tokenized POST to the execute endpoint).
    let task = internal_api.function_version_execute(
        &cron_trigger_worker.base.base.function_version_id,
        HashMap::from([("content-type".to_string(), "application/json".to_string())]),
    )?;

    let payload = if kubernetes {
        // Kubernetes: lean request built entirely from the worker
        // row. The pod resolves its message from the resolver.
        let resolver = internal_api.worker_resolver(&cron_trigger_worker.base.base.id)?;
        // The CronJob's schedule must be known when the gateway
        // creates it, so the mask travels in the start request
        // rather than being resolved in-pod later. It is not a
        // worker row field: it is nested in the worker's serialized
        // `CronTriggerConfig`, so deserialize it to reach the mask.
        let cron_config: CronTriggerConfig = cron_trigger_worker
            .base
            .base
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
                worker_id: cron_trigger_worker.base.base.id.clone(),
                resolver,
                trigger: Callback::Http(task),
                responder: callback,
                project_id: cron_trigger_worker.base.project_id,
                project: cron_trigger_worker.project.clone(),
                collection_id: cron_trigger_worker.base.base.collection_id,
                collection: cron_trigger_worker.collection.clone(),
                connection_id: cron_trigger_worker.connection_id,
                connection: cron_trigger_worker.connection.clone(),
                function_id: cron_trigger_worker.function_id,
                function_version_id: cron_trigger_worker.base.base.function_version_id.clone(),
                function: cron_trigger_worker.function.clone(),
                schedule: cron_config.mask,
            },
        ))
    } else {
        // Supervisor: resolve the full message here.
        // TODO ADD get_states to states
        let info = cron_trigger::Info {
            worker_id: cron_trigger_worker.base.base.id.clone(),
            collection_id: cron_trigger_worker.base.base.collection_id,
            collection: cron_trigger_worker.collection.clone(),
            function_version_id: cron_trigger_worker.base.base.function_version_id.clone(),
            function: cron_trigger_worker.function.clone(),
            scheduled_on: AtTime::now(),
            task,
            config: WorkerConfig::serialize(&cron_trigger_worker.base.base.worker_config)?,
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

    Ok((worker_id, payload))
}

pub(crate) async fn put_cron_trigger_start(
    bridge: &BridgeProviderHandle,
    internal_api: &InternalApi,
    cron_trigger_worker: &WorkerDBWithNames,
) -> Result<(), TdError> {
    let kubernetes = matches!(bridge.orchestrator(), Orchestrator::Kubernetes);
    let (id, payload) = cron_trigger_start_message(internal_api, kubernetes, cron_trigger_worker)?;
    bridge.message_queue().put(id, payload).await?;
    Ok(())
}

#[instrument(
    name = "scheduler.create_cron_trigger_workers_stop",
    skip_all,
    level = "debug",
    err(Debug, level = "debug")
)]
pub fn create_locked_cron_trigger_workers_stop_request(
    orchestrator: Orchestrator,
    cron_trigger_workers: Vec<WorkerDBWithNames>,
    messages: &mut WorkerMessages,
) -> Result<Vec<WorkerDBWithNames>, TdError> {
    let kubernetes = matches!(orchestrator, Orchestrator::Kubernetes);

    // A worker whose message cannot be built must not abort the tick: skip it
    // (it stays `StopScheduled`, re-picked next tick) and keep the rest.
    let mut kept = Vec::with_capacity(cron_trigger_workers.len());
    for cron_trigger_worker in cron_trigger_workers {
        let built = (|| {
            let worker_id = cron_trigger_worker.base.base.id.to_string();

            // Kubernetes ships a lean Stop keyed by the worker id (so the
            // gateway can derive the pod to delete); supervisor ships a flat
            // Stop on the file queue under a distinct message id.
            let (message_id, payload) = if kubernetes {
                (
                    worker_id,
                    OrchestratorMessagePayload::OrchestratorCronRequestMessagePayload(
                        CronRequest::Stop(CronStopRequest {
                            worker: WorkerName::CronTrigger,
                            worker_id: cron_trigger_worker.base.base.id.clone(),
                            function_id: cron_trigger_worker.function_id,
                            function: cron_trigger_worker.function.clone(),
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
                    callback: None,
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
                kept.push(cron_trigger_worker);
            }
            Err(e) => warn!(
                worker_id = %cron_trigger_worker.base.base.id,
                reason = ?e,
                "request: skipping cron-trigger stop; will retry next tick"
            ),
        }
    }
    Ok(kept)
}
