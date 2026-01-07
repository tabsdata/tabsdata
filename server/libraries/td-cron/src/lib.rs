//
// Copyright 2025 Tabs Data Inc.
//

use crate::scheduler::runner::CronScheduler;
use crate::scheduler::tasks::http_request::HttpRequestTask;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::fs::File;
use td_common::logging;
use td_common::server::REQUEST_FILE;
use td_common::server_runner::ServerRunner;
use td_common::status::ExitStatus;
use td_objects::dxo::execution::ExecutionRequest;
use td_objects::dxo::worker::CronConfig;
use td_objects::dxo::yaml::WorkerInput;
use td_objects::types::basic::ExecutionName;
use td_process::launcher::cli::{Cli, NoConfig};
use td_process::launcher::params::ArgumentParameters;
use tracing::{Level, error, info};

mod scheduler;

const CONFIG_NAME: &str = "cron";

/// Run the cron scheduler. This function is the entry point for the `cron` binary.
/// This binary is able to post to the given endpoint (trigger endpoint) at a cron mask rate.
pub fn run() {
    Cli::<NoConfig, ArgumentParameters>::exec_async(
        CONFIG_NAME,
        |_config, params| async move {
            let ArgumentParameters { request_folder, .. } = params;

            // Initialize logging
            logging::start(Level::INFO, None, false);

            // parse request.yaml
            let request_file = request_folder.join(REQUEST_FILE);
            let file = File::open(&request_file).map_err(|e| {
                error!("Failed to open request_file: {e}");
                ExitStatus::GeneralError
            })?;

            let request = serde_yaml::from_reader::<_, WorkerInput>(file).map_err(|e| {
                error!("Failed to parse worker input YAML: {e}");
                ExitStatus::GeneralError
            })?;

            // extract cron input
            let cron = match request {
                WorkerInput::CronV1(cron) => cron,
                input => {
                    error!("Unsupported WorkerInput type. Request is {input:?}.");
                    Err(ExitStatus::GeneralError)?
                }
            };

            // get cron info
            let cron_config = deserialize_json_string::<CronConfig>(cron.info.config.as_str())
                .map_err(|e| {
                    error!("Error deserializing cron config: {e}");
                    ExitStatus::GeneralError
                })?;
            let mask = cron_config.mask.into_cron().map_err(|e| {
                error!("Error parsing cron mask: {e}");
                ExitStatus::GeneralError
            })?;
            let start = cron_config.start.map(|t| t.timestamp_millis());
            let end = cron_config.end.map(|t| t.timestamp_millis());

            // prepare trigger task
            let trigger_name = format!("by cron mask '{}'", mask);
            let execution_request = ExecutionName::try_from(trigger_name)
                .map(|name| ExecutionRequest {
                    name: Some(name),
                    function_only: Default::default(),
                })
                .map(|r| serde_json::to_string_pretty(&r).unwrap_or_default())
                .unwrap_or_default();
            info!("Execution request to be sent: \n{execution_request}");
            // Note that this request can be created inside the closure instead of moving it,
            // so things like time of the request could be included there if needed.
            let body = move || execution_request.clone();

            let mut task = HttpRequestTask::new(cron.info.task);
            task.with_body(body);
            let task = Box::new(task);

            // start scheduler
            let cron_scheduler = CronScheduler::new(mask, start, end, task).map_err(|e| {
                error!("Error creating cron scheduler: {e}");
                ExitStatus::GeneralError
            })?;
            ServerRunner::default().server(cron_scheduler).run().await
        },
        None,
        None,
    );
}

pub fn deserialize_json_string<T>(s: &str) -> Result<T, serde_json::Error>
where
    T: DeserializeOwned,
{
    let value: Value = serde_json::from_str(s)?;

    match value {
        // case: "\"{...}\""
        Value::String(inner) => serde_json::from_str(&inner),

        // case: "{...}"
        other => serde_json::from_value(other),
    }
}
