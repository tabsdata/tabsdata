//
// Copyright 2025 Tabs Data Inc.
//

use super::shared::{WorkerRequestError, loopback_post_callback};
use http::Method;
use std::collections::HashMap;
use td_common::id;
use td_common::server::{
    HttpRequest, MessageAction, OrchestratorMessageQueueHandle, RequestMessagePayload, WorkerClass,
    WorkerName,
};
use td_error::{ApiError, TdError, api_error};
use td_objects::dxo::worker::WorkerDBWithNames;
use td_objects::dxo::yaml::cron_trigger::v1::CronTriggerInput;
use td_objects::dxo::yaml::{WorkerInput, cron_trigger};
use td_objects::rest_urls::BASE_URL;
use td_objects::rest_urls::internal::{FUNCTION_VERSION_EXECUTE, UPDATE_WORKER};
use td_objects::types::addresses::InternalServerAddresses;
use td_objects::types::basic::{AtTime, WorkerConfig};
use td_tower::extractors::Ref;
use tracing::instrument;
use url::Url;

#[instrument(
    name = "scheduler.create_cron_trigger_workers_start",
    skip_all,
    level = "debug",
    err(Debug)
)]
pub async fn create_locked_cron_trigger_workers_start_request(
    message_queue: Ref<OrchestratorMessageQueueHandle>,
    server_addresses: Ref<InternalServerAddresses>,
    cron_trigger_workers: Ref<Vec<WorkerDBWithNames>>,
) -> Result<(), TdError> {
    let futures: Vec<_> = cron_trigger_workers
        .iter()
        .map(|cron_trigger_worker| {
            let message_queue = message_queue.clone();
            let server_addresses = server_addresses.clone();
            async move {
                let worker_id = cron_trigger_worker.id.to_string();
                let server_address = server_addresses.first();
                let endpoint = UPDATE_WORKER.replace("{worker_id}", &worker_id);
                let callback = loopback_post_callback(server_address, &endpoint)?;

                let function_version_id = cron_trigger_worker.function_version_id.to_string();

                // Build trigger task
                let task = HttpRequest {
                    url: {
                        let endpoint = FUNCTION_VERSION_EXECUTE
                            .replace("{function_version_id}", &function_version_id);
                        let trigger_url = format!("http://{server_address}{BASE_URL}{endpoint}");
                        Url::parse(&trigger_url).map_err(WorkerRequestError::UrlParse)?
                    },
                    method: Method::POST,
                    headers: HashMap::from([(
                        "content-type".to_string(),
                        "application/json".to_string(),
                    )]),
                    body: true,
                };

                // Build info
                let info = cron_trigger::v1::Info {
                    worker_id: cron_trigger_worker.id,
                    collection_id: cron_trigger_worker.collection_id,
                    collection: cron_trigger_worker.collection.clone(),
                    function_version_id: cron_trigger_worker.function_version_id.clone(),
                    function: cron_trigger_worker.function.clone(),
                    scheduled_on: AtTime::now(),
                    task,
                    config: WorkerConfig::serialize(&cron_trigger_worker.worker_config)?,
                };
                // Build message context
                let cron_trigger_input = CronTriggerInput { info };
                let context = WorkerInput::CronTriggerV1(Box::new(cron_trigger_input));

                // Build message payload
                // TODO ADD get_states to states
                let message_payload = RequestMessagePayload::<serde_yaml::Value> {
                    class: WorkerClass::Ephemeral,
                    worker: WorkerName::CronTrigger.as_ref().to_string(),
                    run_id: worker_id.clone(),
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

                // Add it to the queue
                message_queue.put(worker_id, message_payload).await?;
                Ok::<_, TdError>(())
            }
        })
        .collect();
    futures::future::try_join_all(futures).await?;
    Ok(())
}

#[instrument(
    name = "scheduler.create_cron_trigger_workers_stop",
    skip_all,
    level = "debug",
    err(Debug)
)]
pub async fn create_locked_cron_trigger_workers_stop_request(
    message_queue: Ref<OrchestratorMessageQueueHandle>,
    server_addresses: Ref<InternalServerAddresses>,
    cron_trigger_workers: Ref<Vec<WorkerDBWithNames>>,
) -> Result<(), TdError> {
    let futures: Vec<_> = cron_trigger_workers
        .iter()
        .map(|cron_trigger_worker| {
            let message_queue = message_queue.clone();
            let server_addresses = server_addresses.clone();
            async move {
                let worker_id = cron_trigger_worker.id.to_string();
                let server_address = server_addresses.first();
                let endpoint = UPDATE_WORKER.replace("{worker_id}", &worker_id);
                let callback = loopback_post_callback(server_address, &endpoint)?;

                // Different message ID than the worker ID for STOP — current
                // messaging impl needs a distinct file name.
                let message_id = id::id().to_string();

                // Build message payload
                let message_payload = RequestMessagePayload::<serde_yaml::Value> {
                    class: WorkerClass::Ephemeral,
                    worker: WorkerName::CronTrigger.as_ref().to_string(),
                    run_id: worker_id,
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
