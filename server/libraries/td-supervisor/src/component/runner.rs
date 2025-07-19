//
// Copyright 2024 Tabs Data Inc.
//

//! Module that runs a worker under Tabsdata system.

use crate::component::describer::DescriberError;
use crate::component::runner::RunnerError::*;
use crate::component::supplier::SupplierError;
use crate::component::tracker::{TrackerError, UNKNOWN_WORKER_PID};
use crate::launch::worker::Worker;
use crate::resource::instance::InstanceError;
use crate::resource::state::StateError;
use http::header::{InvalidHeaderName, InvalidHeaderValue};
use http::StatusCode;
use reqwest::Error;
use std::fmt::{Debug, Formatter};
use std::fs::OpenOptions;
use std::path::PathBuf;
use std::process::Stdio;
use std::{env, fmt};
use td_common::env::{check_flag_env, get_current_dir};
use td_common::logging::LOG_LOCATION;
use td_common::server::WorkerName::FUNCTION;
use td_common::server::{
    ResponseMessagePayloadBuilderError, WorkerClass, WORKER_ERR_FILE, WORKER_OUT_FILE,
};
use td_python::venv::{
    ENV_CONDA_PREFIX, ENV_PYENV_VERSION, ENV_PYTHONHOME, ENV_PYTHONPATH, ENV_UV_VENV,
    ENV_VIRTUAL_ENV, ENV_VIRTUAL_ENV_PROMPT,
};
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::process::{Child, Command};
use tracing::{debug, error, info};

/// Runs a worker under the Tabsdata system.
pub trait WorkerRunner: Debug {
    fn run(
        &self,
        worker: &dyn Worker,
        state: Option<String>,
        detached: bool,
    ) -> Result<(Child, Option<PathBuf>, Option<PathBuf>), RunnerError>;
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
    fn run(
        &self,
        worker: &dyn Worker,
        state: Option<String>,
        detached: bool,
    ) -> Result<(Child, Option<PathBuf>, Option<PathBuf>), RunnerError> {
        let current_dir = get_current_dir();
        debug!(
            "Starting new worker from current directory: '{:?}' (detached: '{}')",
            current_dir, detached
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

        let mut option_out_path: Option<PathBuf> = None;

        let err_path = worker
            .describer()
            .work()
            .join(LOG_LOCATION)
            .join(WORKER_ERR_FILE);
        let option_err_path = Some(err_path.clone());
        let err = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&err_path)?;

        let mut args = worker.describer().arguments().to_vec();
        if worker.describer().class() == &WorkerClass::EPHEMERAL {
            args.extend(worker.describer().markers().iter().cloned())
        }

        let mut command = Command::new(worker.describer().program());

        #[cfg(windows)]
        {
            use td_common::server::TD_DETACHED_SUBPROCESSES;
            if check_flag_env(TD_DETACHED_SUBPROCESSES) || detached {
                use windows_sys::Win32::System::Threading::CREATE_NO_WINDOW;

                command.creation_flags(CREATE_NO_WINDOW);
            }
        }

        command
            .current_dir(worker.describer().work())
            .envs(obtain_env_vars(worker.describer().name()))
            .args(args)
            .stdin(Stdio::piped())
            .stderr(err);

        if worker.describer().set_state().is_some() {
            command.stdout(Stdio::piped());
        } else {
            let out_path = worker
                .describer()
                .work()
                .join(LOG_LOCATION)
                .join(WORKER_OUT_FILE);
            option_out_path = Some(out_path.clone());
            let out = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&out_path)?;

            command.stdout(out);
        }

        debug!(
            "Starting worker with command: '{:?}' and arguments '{:?}'",
            worker.describer().program(),
            worker.describer().arguments()
        );

        let mut child = match command.spawn() {
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

        if state.is_some() {
            info!(
                "Feeding stdin of worker '{}' with pid '{}'",
                &worker.describer().name(),
                &child.id().unwrap_or(UNKNOWN_WORKER_PID)
            );

            let mut stdin = match child.stdin.take() {
                None => Err(MissingStdIn)?,
                Some(stdin) => stdin,
            };

            tokio::spawn(async move {
                // Here we are sure Option has some content...
                let _ = stdin.write_all(state.unwrap().as_bytes()).await;
                let _ = stdin.shutdown().await;
            });
        }

        Ok((child, option_out_path, option_err_path))
    }
}

// Adjusts environment variables of new worker:
// - PATH is enriched with directory of current running program.
fn obtain_env_vars(worker: &str) -> Vec<(String, String)> {
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

    if worker == FUNCTION.as_ref() {
        let python_envs = [
            ENV_CONDA_PREFIX,
            ENV_PYENV_VERSION,
            ENV_PYTHONHOME,
            ENV_PYTHONPATH,
            ENV_UV_VENV,
            ENV_VIRTUAL_ENV,
            ENV_VIRTUAL_ENV_PROMPT,
        ];
        env_vars.retain(|(key, _)| !python_envs.contains(&key.as_str()));
    }

    if check_show_env() {
        debug!("Using environment variables");
        for env in &env_vars {
            info!("   - '{:?}': '{:?}", env.0, env.1);
        }
    }
    env_vars
}

// Runner for functions.
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
    fn run(
        &self,
        _worker: &dyn Worker,
        _state: Option<String>,
        _detached: bool,
    ) -> Result<(Child, Option<PathBuf>, Option<PathBuf>), RunnerError> {
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
    #[error("Process has no standard output")]
    MissingStdOutError,
    #[error("An error occurred reading process standard output: {0}")]
    ReadStdOutError(std::io::Error),
    #[error("Missing required stdin")]
    MissingStdIn,
    #[error("Unexpected error processing supervisor states: {0}")]
    GetSetStateError(#[from] StateError),
}

pub fn check_show_env() -> bool {
    const TD_SHOW_ENV: &str = "TD_SHOW_ENV";
    check_flag_env(TD_SHOW_ENV)
}
