//
// Copyright 2026. Tabs Data Inc.
//

use crate::{CronTask, HttpTask, TaskRunner, TouchFileTask};
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::fs::File;
use std::time::Duration;
use td_common::env::get_current_dir;
use td_common::logging;
use td_common::server::{REQUEST_FILE, WORKER_PID_FILE};
use td_common::server_runner::ServerRunner;
use td_common::status::ExitStatus;
use td_objects::dxo::execution::ExecutionRequestInternal;
use td_objects::dxo::worker::CronTriggerConfig;
use td_objects::dxo::yaml::WorkerInput;
use td_objects::types::basic::{ExecutionName, FunctionOnly, SkipIfRunning, TriggerType};
use td_process::launcher::cli::{Cli, NoConfig};
use td_process::launcher::params::ArgumentParameters;
use tracing::{Level, error};

const CONFIG_NAME: &str = "crontrigger";

/// Run the crontrigger scheduler. This function is the entry point for the `crontrigger` binary.
/// This binary is able to post to the given endpoint (trigger endpoint) at a cron mask rate.
pub fn run() {
    Cli::<NoConfig, ArgumentParameters>::exec_async(
        CONFIG_NAME,
        |_config, params| async move {
            let ArgumentParameters { request_folder, .. } = params;

            // Initialize logging
            logging::start(Some(Level::INFO), None, false, true);

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

            // extract cron trigger input
            let cron_trigger = match request {
                WorkerInput::CronTriggerV1(cron_trigger) => cron_trigger,
                input => {
                    error!("Unsupported WorkerInput type. Request is {input:?}.");
                    Err(ExitStatus::GeneralError)?
                }
            };

            // get cron trigger info
            let cron_trigger_config =
                deserialize_json_string::<CronTriggerConfig>(cron_trigger.info.config.as_str())
                    .map_err(|e| {
                        error!("Error deserializing cron trigger config: {e}");
                        ExitStatus::GeneralError
                    })?;

            let mask = cron_trigger_config
                .mask
                .into_cron()
                .map_err(|e| {
                    error!("Error parsing cron mask: {e}");
                    ExitStatus::GeneralError
                })?
                .to_string();

            // prepare trigger task
            let execution_name = format!("by cron mask '{}'", mask);

            let execution_request = ExecutionName::try_from(execution_name)
                .map(|name| ExecutionRequestInternal {
                    name: Some(name),
                    function_only: FunctionOnly::FALSE,
                    skip_if_running: SkipIfRunning::FALSE,
                    trigger_type: TriggerType::CronTrigger,
                })
                .map(|r| serde_json::to_string_pretty(&r).unwrap_or_default())
                .unwrap_or_default();
            // Note that this request can be created inside the closure instead of moving it,
            // so things like time of the request could be included there if needed.
            let body = move || execution_request.clone();

            // function trigger
            let trigger_task = HttpTask::new(cron_trigger.info.task).with_body(body);

            // task that periodically executes the trigger based on the cron mask
            let cron_task = CronTask::new(&mask, trigger_task).map_err(|e| {
                error!("Error creating cron scheduler: {e}");
                ExitStatus::GeneralError
            })?;

            // heartbeat task that touches the PID file of the cron worker.
            let touch_file = get_current_dir().join(WORKER_PID_FILE);
            let heartbeat_task = TouchFileTask::new(touch_file);

            // runs the cron_task and the heartbeat_task
            let task_runner = TaskRunner::new_with_heartbeat(
                cron_task,
                cron_trigger_config.start,
                cron_trigger_config.end,
                heartbeat_task,
                Duration::from_secs(30),
            );

            ServerRunner::default().server(task_runner).run().await
        },
        None,
        None,
    );
}

fn deserialize_json_string<T>(s: &str) -> Result<T, serde_json::Error>
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
