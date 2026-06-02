//
// Copyright 2025 Tabs Data Inc.
//

use super::shared::{WorkerRequestError, loopback_post_callback};
use crate::scheduler::message_builder::build_external_location;
use http::Method;
use std::collections::HashMap;
use td_common::id;
use td_common::server::{
    HttpRequest, MessageAction, OrchestratorMessageQueueHandle, RequestMessagePayload, WorkerClass,
    WorkerName,
};
use td_database::dynamic::DynamicConnection;
use td_error::{ApiError, TdError, api_error};
use td_objects::dxo::function::FunctionDB;
use td_objects::dxo::worker::WorkerDBWithNames;
use td_objects::dxo::yaml::stage_trigger::v1::StageTriggerInput;
use td_objects::dxo::yaml::{WorkerInput, stage_trigger};
use td_objects::rest_urls::BASE_URL;
use td_objects::rest_urls::internal::{
    DOWNLOAD_FUNCTION_STATE, FUNCTION_VERSION_EXECUTE, FUNCTION_VERSION_EXECUTION_STATUS,
    UPDATE_WORKER,
};
use td_objects::types::addresses::InternalServerAddresses;
use td_objects::types::basic::{AtTime, FunctionVersionId, WorkerConfig};
use td_queries::ChunkedQueries;
use td_storage::Storage;
use td_storage::location::StorageLocation;
use td_tower::extractors::{MutRef, Ref};
use tracing::instrument;
use url::Url;

#[instrument(
    name = "scheduler.create_stage_trigger_workers_start",
    skip_all,
    level = "debug",
    err(Debug)
)]
pub async fn create_locked_stage_trigger_workers_start_request(
    message_queue: Ref<OrchestratorMessageQueueHandle>,
    storage: Ref<Storage>,
    server_addresses: Ref<InternalServerAddresses>,
    mut conn: MutRef<DynamicConnection<'static>>,
    stage_trigger_workers: Ref<Vec<WorkerDBWithNames>>,
) -> Result<(), TdError> {
    // Pre-fetch every function record in one chunked query instead of
    // a per-worker round-trip inside the loop.
    let fvids: Vec<FunctionVersionId> = stage_trigger_workers
        .iter()
        .map(|w| w.function_version_id.clone())
        .collect();
    let functions: Vec<FunctionDB> = if fvids.is_empty() {
        Vec::new()
    } else {
        conn.fetch_all_chunked::<FunctionDB, FunctionVersionId>(&fvids)
            .await?
    };
    let function_by_fvid: HashMap<FunctionVersionId, FunctionDB> =
        functions.into_iter().map(|f| (f.id.clone(), f)).collect();

    for stage_trigger_worker in stage_trigger_workers.iter() {
        let worker_id = stage_trigger_worker.id.to_string();
        let server_address = server_addresses.first();
        let endpoint = UPDATE_WORKER.replace("{worker_id}", &worker_id);
        let callback = loopback_post_callback(server_address, &endpoint)?;

        let function_version_id = stage_trigger_worker.function_version_id.to_string();

        // Build fn_trigger request
        let fn_trigger = HttpRequest {
            url: {
                let endpoint =
                    FUNCTION_VERSION_EXECUTE.replace("{function_version_id}", &function_version_id);
                let trigger_url = format!("http://{server_address}{BASE_URL}{endpoint}");
                Url::parse(&trigger_url).map_err(WorkerRequestError::UrlParse)?
            },
            method: Method::POST,
            headers: Default::default(),
            body: false,
        };

        // Build fn_state request
        let fn_state = HttpRequest {
            url: {
                let endpoint =
                    DOWNLOAD_FUNCTION_STATE.replace("{function_version_id}", &function_version_id);
                let fn_state = format!("http://{server_address}{BASE_URL}{endpoint}");
                Url::parse(&fn_state).map_err(WorkerRequestError::UrlParse)?
            },
            method: Method::GET,
            headers: Default::default(),
            body: false,
        };

        // Build fn_status request
        let fn_status = HttpRequest {
            url: {
                let endpoint = FUNCTION_VERSION_EXECUTION_STATUS
                    .replace("{function_version_id}", &function_version_id);
                let fn_status = format!("http://{server_address}{BASE_URL}{endpoint}");
                Url::parse(&fn_status).map_err(WorkerRequestError::UrlParse)?
            },
            method: Method::GET,
            headers: Default::default(),
            body: true,
        };

        let f = function_by_fvid
            .get(&stage_trigger_worker.function_version_id)
            .ok_or_else(|| {
                api_error!(
                    ApiError::InternalError,
                    "stage trigger worker references unknown function_version_id {}",
                    stage_trigger_worker.function_version_id
                )
            })?;

        // Function bundle
        let storage_location = StorageLocation::from(&f.storage_version);
        let (path, _) = storage_location
            .builder(&f.data_location)
            .collection(&f.collection_id)
            .function(&f.bundle_id)
            .build()?;
        let (function_bundle, _) = build_external_location(&storage, &path)?;

        // Function stage
        let (path, _) = storage_location
            .builder(&f.data_location)
            .collection(&f.collection_id)
            .stage(&f.function_id)
            .build()?;
        let (function_stage, _) = build_external_location(&storage, &path)?;

        // Function checkpoint
        let (path, _) = storage_location
            .builder(&f.data_location)
            .collection(&f.collection_id)
            .checkpoint(&f.function_id)
            .build()?;
        let (function_checkpoint, _) = build_external_location(&storage, &path)?;

        // Build info
        // TODO Urls are wrong as of now
        let info = stage_trigger::v1::Info {
            worker_id: stage_trigger_worker.id,
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
        let context = WorkerInput::StageTriggerV1(Box::new(stage_trigger_input));

        // Build message payload
        // TODO ADD get_states to states
        let message_payload = RequestMessagePayload::<serde_yaml::Value> {
            class: WorkerClass::Ephemeral,
            worker: WorkerName::StageTrigger.as_ref().to_string(),
            run_id: worker_id.clone(),
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

        // Add it to the queue
        message_queue.put(worker_id, message_payload).await?;
    }
    Ok(())
}

#[instrument(
    name = "scheduler.create_stage_trigger_workers_stop",
    skip_all,
    level = "debug",
    err(Debug)
)]
pub async fn create_locked_stage_trigger_workers_stop_request(
    message_queue: Ref<OrchestratorMessageQueueHandle>,
    server_addresses: Ref<InternalServerAddresses>,
    stage_trigger_workers: Ref<Vec<WorkerDBWithNames>>,
) -> Result<(), TdError> {
    let futures: Vec<_> = stage_trigger_workers
        .iter()
        .map(|stage_trigger_worker| {
            let message_queue = message_queue.clone();
            let server_addresses = server_addresses.clone();
            async move {
                let server_address = server_addresses.first();
                let worker_id = stage_trigger_worker.id.to_string();
                let endpoint = UPDATE_WORKER.replace("{worker_id}", &worker_id);
                let callback = loopback_post_callback(server_address, &endpoint)?;

                // Different message ID than the worker ID for STOP — current
                // messaging impl needs a distinct file name.
                let message_id = id::id().to_string();

                // Build message payload
                let message_payload = RequestMessagePayload::<serde_yaml::Value> {
                    class: WorkerClass::Ephemeral,
                    worker: WorkerName::StageTrigger.as_ref().to_string(),
                    run_id: worker_id.clone(),
                    action: MessageAction::Stop,
                    arguments: Vec::new(),
                    callback: Some(callback),
                    context: None,
                };

                // Add it to the queue
                message_queue.put(message_id, message_payload).await?;
                Ok::<_, TdError>(())
            }
        })
        .collect();
    futures::future::try_join_all(futures).await?;
    Ok(())
}
