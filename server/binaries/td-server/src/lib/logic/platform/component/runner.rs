//
// Copyright 2024 Tabs Data Inc.
//

//! Module that runs a worker under Tabsdata system.

use crate::logic::platform::component::describer::DescriberError;
use crate::logic::platform::component::runner::RunnerError::*;
use crate::logic::platform::component::supplier::SupplierError;
use crate::logic::platform::component::tracker::{TrackerError, UNKNOWN_WORKER_PID};
use crate::logic::platform::launch::worker::Worker;
use crate::logic::platform::resource::instance::{InstanceError, WORKER_ERR_FILE, WORKER_OUT_FILE};
use http::header::{InvalidHeaderName, InvalidHeaderValue};
use http::StatusCode;
use reqwest::Error;
use std::fmt::{Debug, Formatter};
use std::fs::OpenOptions;
use std::{env, fmt};
use td_common::env::get_current_dir;
use td_common::logging::LOG_LOCATION;
use td_common::monitor::check_show_env;
use td_common::server::ResponseMessagePayloadBuilderError;
use thiserror::Error;
use tokio::process::{Child, Command};
use tracing::{debug, error, info};

/// Runs a worker under the Tabsdata system.
pub trait WorkerRunner: Debug {
    fn run(&self, worker: &dyn Worker) -> Result<Child, RunnerError>;
}

// Default runner.
#[derive(Default)]
pub struct TabsDataWorkerRunner;

impl TabsDataWorkerRunner {
    pub fn new() -> Self {
        Self {}
    }
}

impl Debug for TabsDataWorkerRunner {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("TabsDataWorkerRunner").finish()
    }
}

impl WorkerRunner for TabsDataWorkerRunner {
    fn run(&self, worker: &dyn Worker) -> Result<Child, RunnerError> {
        let current_dir = get_current_dir();
        debug!(
            "Starting new worker from current directory: '{:?}'",
            current_dir
        );
        debug!(
            "Worker current directory will be: '{:?}'",
            worker.describer().work()
        );
        debug!(
            "Worker config folder is: '{:?}'",
            worker.describer().config()
        );
        debug!("Worker work folder is: '{:?}'", worker.describer().work());

        worker.supplier().supply(worker)?;

        let out = OpenOptions::new().create(true).append(true).open(
            worker
                .describer()
                .work()
                .join(LOG_LOCATION)
                .join(WORKER_OUT_FILE),
        )?;
        let err = OpenOptions::new().create(true).append(true).open(
            worker
                .describer()
                .work()
                .join(LOG_LOCATION)
                .join(WORKER_ERR_FILE),
        )?;
        let mut command = Command::new(worker.describer().program());
        command
            .current_dir(worker.describer().work())
            .envs(obtain_env_vars())
            .stdout(out)
            .stderr(err)
            .args(worker.describer().arguments());

        debug!(
            "Starting worker with command: '{:?}' and arguments '{:?}'",
            worker.describer().program(),
            worker.describer().arguments()
        );

        let child = match command.spawn() {
            Ok(child) => child,
            Err(e) => {
                return Err(LaunchError {
                    describer: worker.describer().to_string(),
                    cause: e,
                })
            }
        };
        info!(
            "Worker '{}' started with pid '{}'",
            &worker.describer().name(),
            &child.id().unwrap_or(UNKNOWN_WORKER_PID)
        );
        Ok(child)
    }
}

// Adjusts environment variables of new worker:
// - PATH is enriched with directory of current running program.
fn obtain_env_vars() -> Vec<(String, String)> {
    let mut env_vars: Vec<(String, String)> = env::vars().collect();
    if let Ok(program_path) = env::current_exe() {
        if let Some(program_folder) = program_path.parent() {
            if let Ok(former_env_var_path) = env::var("PATH") {
                let separator = if cfg!(windows) { ";" } else { ":" };
                let current_env_var_path = format!(
                    "{}{}{}",
                    former_env_var_path,
                    separator,
                    program_folder.display()
                );
                env_vars.push(("PATH".to_string(), current_env_var_path));
            }
        }
    }
    if check_show_env() {
        debug!("Using environment variables");
        for env in &env_vars {
            info!("   - '{:?}': '{:?}", env.0, env.1);
        }
    }
    env_vars
}

// Runner for unctions.
#[derive(Default)]
pub struct FunctionWorkerRunner;

impl FunctionWorkerRunner {
    pub fn new() -> Self {
        Self {}
    }
}

impl Debug for FunctionWorkerRunner {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("FunctionWorkerRunner").finish()
    }
}

// ToDo: Dimas: Pending implementation. It will need to add dedicated logic to this kind of workers.
impl WorkerRunner for FunctionWorkerRunner {
    fn run(&self, _worker: &dyn Worker) -> Result<Child, RunnerError> {
        unimplemented!()
    }
}

#[derive(Debug, Error)]
pub enum RunnerError {
    #[error("Failed to describe worker '{worker}': {cause}")]
    DescriberFailure {
        worker: String,
        cause: DescriberError,
    },
    #[error("Failed to start Tabs Data main worker '{describer}': {cause}")]
    LaunchError {
        describer: String,
        cause: std::io::Error,
    },
    #[error("Launched worker has no process id")]
    MissingProcessId,
    #[error("An error occurred running file system operations: {0}")]
    DescribeFailure(#[from] DescriberError),
    #[error("An error occurred tracking a worker: {0}")]
    TrackerFailure(#[from] TrackerError),
    #[error("An error occurred running an I/O operation: {0}")]
    SupplierFailure(#[from] SupplierError),
    #[error("An error occurred supplying resources: {0}")]
    IOError(#[from] std::io::Error),
    #[error("Worker exited: {message}")]
    WorkerExited { message: String },
    #[error("An error occurred running instance operations: {0}")]
    InstanceFailure(#[from] InstanceError),
    #[error("Received void message for ephemeral controller. Ignoring request.")]
    VoidEphemeralMessage,
    #[error("Unexpected response message received.")]
    InvalidMessageType,
    #[error("Error receiving response body content: {cause}")]
    BrokenContent { cause: Error },
    #[error("Bad HTTP status: {status}")]
    BadStatus { status: StatusCode },
    #[error("Error generating HTTP request: {cause}")]
    BadRequest { cause: Error },
    #[error("Unable to build the response message payload: {cause}")]
    WrongPayload {
        cause: ResponseMessagePayloadBuilderError,
    },
    #[error("Invalid header name.")]
    InvalidHeaderNameError(#[from] InvalidHeaderName),
    #[error("Invalid header value.")]
    InvalidHeaderValueError(#[from] InvalidHeaderValue),
    #[error("An error occurred building a response message: {0}")]
    ResponseBuilderError(#[from] ResponseMessagePayloadBuilderError),
    #[error("An error occurred serializing an struct to yaml: {0}")]
    YamlError(#[from] serde_yaml::Error),
    #[error("An error occurred serializing an struct to json: {0}")]
    JsonError(#[from] serde_json::Error),
    #[error("No start date for running worker")]
    MissingStartDate,
    #[error("worker start notification error")]
    StartNotificationError,
}
