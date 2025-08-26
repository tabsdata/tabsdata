//
// Copyright 2025 Tabs Data Inc.
//

use crate::component::describer::TabsDataWorkerDescriberBuilder;
use crate::component::notifier::execution;
use crate::component::parameters::render;
use crate::component::runner::RunnerError;
use crate::component::runner::RunnerError::{
    DescriberFailure, IOError, InvalidMessageType, MissingStartDate, MissingStdOutError,
    ReadStdOutError, StartNotificationError, VoidEphemeralMessage, WorkerExited,
};
use crate::component::tracker::{WorkerStatus, check_status, get_pid_path};
use crate::launch::worker::{TabsDataWorker, Worker, notify};
use crate::resource::instance::{
    copy_mold_tree, get_instance_path_for_instance, get_repository_path_for_instance,
    get_workspace_path_for_instance,
};
use crate::resource::messaging::SupervisorMessageQueue;
use crate::resource::scripting::{ArgumentPrefix, CommandBuilder, ScriptBuilder};
use crate::resource::settings::extract_profile_config;
use crate::resource::state::{
    EncodedMap, State, StateDataKind, StateDataValue, StateError, SupervisorState,
    extract_state_data_from_string,
};
use crate::runtime::error::RuntimeError;
use crate::services::supervisor::ControllerState::{KO, NA, OK};
use crate::services::supervisor::WorkerKind::SUPERVISOR;
use atomic_enum::atomic_enum;
use chrono::Utc;
use clap::Parser;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use getset::Getters;
use indexmap::IndexMap;
use path_slash::PathBufExt;
use regex::Regex;
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;
use std::env::{set_current_dir, set_var};
use std::ffi::OsString;
use std::fmt::{Display, Formatter};
use std::fs::{create_dir_all, read_dir};
#[cfg(not(target_os = "windows"))]
use std::os::unix::process::ExitStatusExt;
use std::path::PathBuf;
use std::process::exit;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;
use std::time::Duration;
use std::{fmt, panic, process};
use strum::{AsRefStr, EnumString};
use sysinfo::Signal::Kill;
use sysinfo::{Pid, Signal};
use td_common::attach::check_nowait_env;
use td_common::env::to_absolute;
use td_common::execution_status::WorkerCallbackStatus;
use td_common::os::terminate_process;
use td_common::server::SupervisorMessagePayload::{
    SupervisorExceptionMessagePayload, SupervisorRequestMessagePayload,
    SupervisorResponseMessagePayload,
};
use td_common::server::WorkerClass::{EPHEMERAL, INIT, REGULAR};
use td_common::server::{
    CAST_FOLDER, CONFIG_FILE, CONFIG_FOLDER, CONFIG_NAMESPACE, CONFIG_PATH_ENV, CONFIG_URI_ENV,
    ETC_FOLDER, INSTANCE_PATH_ENV, INSTANCE_URI_ENV, MOLD_FOLDER, MSG_FOLDER, PARENT_FOLDER,
    PROC_FOLDER, REPOSITORY_PATH_ENV, REPOSITORY_URI_ENV, REQUEST_MESSAGE_FILE_PATTERN,
    RETRIES_DELIMITER, SupervisorMessage, WORK_FOLDER, WORK_PATH_ENV, WORK_URI_ENV,
    WORKSPACE_FOLDER, WORKSPACE_PATH_ENV, WORKSPACE_URI_ENV, WorkerClass,
};
use td_common::signal::terminate;
use td_common::status::ExitStatus::{GeneralError, Success, TabsDataError};
use td_process::launcher::arg::InheritedArgumentKey::*;
use td_process::launcher::arg::{ArgumentKey, InheritedArgumentKey, MarkerKey};
use td_process::launcher::cli::{Cli, TRAILING_ARGUMENTS_PREFIX, parse_extra_arguments};
use td_process::launcher::config::Config;
use td_python::venv::prepare;
use tempfile::tempdir;
use tokio::io::AsyncReadExt;
use tokio::runtime::Handle;
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{Mutex, mpsc};
use tokio::task::{JoinHandle, block_in_place};
use tokio::time::sleep;
use tracing::{debug, error, info, trace, warn};

pub const TD_ARGUMENT_KEY: &str = "::td";

pub const POLL_DISPATCH_CHANNEL_SIZE: usize = 256;
pub const DISPATCH_INIT_CHANNEL_SIZE: usize = 1;
pub const DISPATCH_REGULAR_CHANNEL_SIZE: usize = 1;
pub const DISPATCH_EPHEMERAL_CHANNEL_SIZE: usize = 256;

pub const MONITOR_WAIT_MILLISECONDS: u64 = 1000;
pub const POLLING_WAIT_MILLISECONDS: u64 = 1000;
pub const CONTROLLER_WAIT_MILLISECONDS: u64 = 100;

pub const WAIT_FOR_INIT_MILLISECONDS: u64 = 1000;
pub const WAIT_FOR_REGULAR_MILLISECONDS: u64 = 1000;
pub const WAIT_FOR_EPHEMERAL_MILLISECONDS: u64 = 1000;

pub const CONTROLLERS_ALIVE_CHECK_MILLISECONDS: u64 = 30000;

pub const EPHEMERAL_WAIT_MILLISECONDS: u64 = 500;

#[allow(unused_qualifications)]
#[atomic_enum]
enum ControllerState {
    NA,
    OK,
    KO,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize, EnumString, AsRefStr)]
#[strum(serialize_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum WorkerLocation {
    #[default]
    Relative,
    System,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize, EnumString, AsRefStr)]
#[strum(serialize_all = "lowercase")]
#[serde(rename_all = "lowercase")]
pub enum WorkerKind {
    SUPERVISOR,
    #[default]
    PROCESSOR,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize, Getters)]
#[getset(get = "pub")]
pub struct Configuration {
    name: String,
    #[serde(default)]
    controllers: ControllersConfig,
}

#[derive(Default, Debug, Clone, Serialize, Getters)]
#[getset(get = "pub")]
struct ControllersConfig {
    #[serde(default)]
    init: ControllerConfig,
    #[serde(default)]
    regular: ControllerConfig,
    #[serde(default)]
    ephemeral: ControllerConfig,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize, Getters)]
#[getset(get = "pub")]
struct ControllerConfig {
    name: String,
    concurrency: u16,
    #[serde(default, deserialize_with = "workers_to_map")]
    workers: IndexMap<String, WorkerConfig>,
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Getters)]
pub struct SetState {
    #[serde(rename = "state-type")]
    state_type: StateDataKind,
    #[serde(rename = "state-key")]
    state_key: String,
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Getters)]
pub struct GetState {
    #[serde(rename = "state-type")]
    state_type: StateDataKind,
    #[serde(rename = "state-key")]
    state_key: String,
    #[serde(rename = "state-prefixes", default)]
    state_prefixes: Vec<String>,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize, Getters)]
#[getset(get = "pub")]
pub struct WorkerConfig {
    name: String,
    #[serde(default)]
    kind: WorkerKind,
    #[serde(default)]
    location: WorkerLocation,
    program: String,
    #[serde(default, deserialize_with = "parameters_to_map")]
    parameters: HashMap<String, String>,
    #[serde(default)]
    inherit: Vec<String>,
    #[serde(default)]
    arguments: Vec<String>,
    #[serde(default)]
    markers: Vec<String>,
    #[serde(rename = "set-state")]
    set_state: Option<SetState>,
    #[serde(default, rename = "get-states")]
    get_states: Vec<GetState>,
    #[serde(default = "default_concurrency")]
    concurrency: u16,
    #[serde(default = "default_retries")]
    retries: u16,
}

impl Display for WorkerConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "WorkerConfig {{ name: {}, \
                             kind: {:?}, \
                             location: {:?}, \
                             program: {}, \
                             parameters: {:?}, \
                             inherit: {:?}, \
                             arguments: {:?}, \
                             markers: {:?}, \
                             set_state: {:?}, \
                             get_states: {:?}, \
                             concurrency: {} }}",
            self.name,
            self.kind,
            self.location,
            self.program,
            self.parameters,
            self.inherit,
            self.arguments,
            self.markers,
            self.set_state,
            self.get_states,
            self.concurrency
        )
    }
}

impl Config for Configuration {}

fn default_concurrency() -> u16 {
    0
}

fn default_retries() -> u16 {
    2
}

fn workers_to_map<'de, D>(deserializer: D) -> Result<IndexMap<String, WorkerConfig>, D::Error>
where
    D: Deserializer<'de>,
{
    let workers_list: Vec<WorkerConfig> = Vec::deserialize(deserializer)?;
    let mut workers_map = IndexMap::new();
    for worker in workers_list {
        workers_map.insert(worker.name.clone(), worker);
    }
    Ok(workers_map)
}

fn parameters_to_map<'de, D>(deserializer: D) -> Result<HashMap<String, String>, D::Error>
where
    D: Deserializer<'de>,
{
    let parameters_list: Vec<HashMap<String, String>> = Vec::deserialize(deserializer)?;
    let mut parameters_map = HashMap::new();
    for entry in parameters_list {
        parameters_map.extend(entry);
    }
    Ok(parameters_map)
}

impl<'de> Deserialize<'de> for ControllersConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let mut controllers_map: IndexMap<String, ControllerConfig> =
            IndexMap::deserialize(deserializer)?;
        Ok(ControllersConfig {
            init: ControllerConfig {
                name: INIT.as_ref().to_string(),
                ..controllers_map
                    .shift_remove(INIT.as_ref())
                    .ok_or_else(|| serde::de::Error::missing_field(INIT.as_ref()))?
            },
            regular: ControllerConfig {
                name: REGULAR.as_ref().to_string(),
                ..controllers_map
                    .shift_remove(REGULAR.as_ref())
                    .ok_or_else(|| serde::de::Error::missing_field(REGULAR.as_ref()))?
            },
            ephemeral: ControllerConfig {
                name: EPHEMERAL.as_ref().to_string(),
                ..controllers_map
                    .shift_remove(EPHEMERAL.as_ref())
                    .ok_or_else(|| serde::de::Error::missing_field(EPHEMERAL.as_ref()))?
            },
        })
    }
}

#[derive(Debug, Clone, clap_derive::Parser)]
#[command(
    name = "Tabsdata Supervisor",
    version = "1.3.0",
    about = "Tabsdata Supervisor",
    long_about = "Tabsdata supervisor that can manage workers using a configuration descriptor."
)]
pub struct Arguments {
    /// Name/Location of the Tabsdata instance.
    #[arg(
        long,
        name = "instance",
        required = false,
        value_parser = clap::value_parser!(PathBuf),
        long_help = "Name/Location of the Tabsdata instance. \
                     The instance is stored as a subfolder of the user's home folder, when a relative path. \
                     If unspecified, instance ~/.tabsdata/instances/tabsdata will be used."
    )]
    instance: Option<PathBuf>,

    /// Folder containing the instance's persistent data.
    #[arg(
        long,
        name = "repository",
        required = false,
        value_parser = clap::value_parser!(PathBuf),
        long_help = "Folder containing the instance's persistent data. \
                     If unspecified, the subfolder 'repository' inside the instance's folder will be used."
    )]
    repository: Option<PathBuf>,

    /// Folder containing the instance's transient data.
    #[arg(
        long,
        name = "workspace",
        required = false,
        value_parser = clap::value_parser!(PathBuf),
        long_help = "Folder containing the instance's transient data. \
                     If unspecified, the subfolder 'workspace' inside the instance's folder will be used."
    )]
    workspace: Option<PathBuf>,

    /// Folder containing the instance's profile.
    #[arg(
        long,
        name = "profile",
        required = false,
        value_parser = clap::value_parser!(PathBuf),
        long_help = "Folder containing the instance's profile. \
                    The default Tabsdata profile will we used if unspecified."
    )]
    profile: Option<PathBuf>,

    /// Additional arguments for spawned workers.
    #[arg(
        trailing_var_arg = true,
        allow_hyphen_values = true,
        value_name = "-- <arguments>",
        long_help = "Additional arguments to pass to all the supervisor workers. \
                     Use any additional argument supported by these workers. \
                     Use the syntax '-- worker1_1 --arg_1_1_k arg1_1_v ... -- worker_2 --arg_2_1_k arg2_1_v ...'."
    )]
    arguments: Vec<String>,
}

impl Arguments {
    fn name(&self) -> String {
        CONFIG_NAMESPACE.to_string()
    }

    fn instance(&self) -> PathBuf {
        to_absolute(&get_instance_path_for_instance(&self.instance.clone())).unwrap()
    }

    fn repository(&self) -> PathBuf {
        to_absolute(&get_repository_path_for_instance(&self.instance)).unwrap()
    }

    fn workspace(&self) -> PathBuf {
        get_workspace_path_for_instance(&self.instance)
    }

    fn config(&self) -> PathBuf {
        to_absolute(&get_workspace_path_for_instance(&self.instance).join(CONFIG_FOLDER)).unwrap()
    }

    fn work(&self) -> PathBuf {
        to_absolute(&get_workspace_path_for_instance(&self.instance).join(WORK_FOLDER)).unwrap()
    }

    pub fn instance_path(&self) -> PathBuf {
        to_absolute(&get_instance_path_for_instance(&Some(self.instance()))).unwrap()
    }
}

#[derive(Clone)]
pub struct Supervisor {
    config: Configuration,
    params: Arguments,
    monitor: Arc<AtomicBool>,
    semaphore: Arc<Mutex<()>>,
    init_mark: Arc<AtomicControllerState>,
    regular_mark: Arc<AtomicControllerState>,
    ephemeral_mark: Arc<AtomicControllerState>,
    mutex: Arc<std::sync::Mutex<()>>,
    dropping: Arc<AtomicBool>,
    state: SupervisorState,
}

type WorkerCommandLine = (String, Vec<String>, Vec<String>);

impl Drop for Supervisor {
    fn drop(&mut self) {
        let mut dropping: bool = false;
        match self.mutex.lock() {
            Ok(_lock) => {
                if !self.dropping.load(SeqCst) {
                    self.dropping.store(true, SeqCst);
                    dropping = true;
                }
            }
            _ => {
                error!("Supervisor already dropped or dropping. Skipping drop lock acquisition.");
                return;
            }
        }
        if dropping {
            info!("Supervisor is being dropped...");
            self.drop_workers();
            info!("Supervisor dropped!");
        }
    }
}

impl Supervisor {
    fn drop_workers(&mut self) {
        info!("Dropping the Supervisor workers...");
        let mut system = sysinfo::System::new_all();
        system.refresh_all();
        let pid = process::id();
        if pid > 0 {
            let workers: Vec<_> = system
                .processes()
                .values()
                .filter(|process| process.parent() == Some(Pid::from_u32(pid)))
                .collect();
            for worker in &workers {
                self.drop_worker(worker.pid().as_u32() as i32, Kill);
            }
        } else {
            warn!(
                "Current process does not have a positive pid ({}). Skipping process termination request.",
                pid
            );
        }
    }

    fn drop_worker(&self, pid: i32, signal: Signal) {
        if pid > 0 {
            info!("Sending drop signal to process with pid: {}", pid);
            match terminate_process(pid, signal) {
                Ok(()) => info!("Successfully sent drop signal to worker with pid: {}", pid),
                Err(e) => error!(
                    "Failed to send drop signal to process with pid: {}: {}",
                    pid, e
                ),
            }
        } else {
            warn!(
                "Process does not have a positive pid ({}). Skipping process termination request.",
                pid
            );
        };
    }
}

impl Supervisor {
    pub fn new(config: Configuration, params: Arguments) -> Self {
        Self {
            config,
            params,
            monitor: Arc::new(AtomicBool::new(false)),
            semaphore: Arc::new(Mutex::new(())),
            init_mark: Arc::new(AtomicControllerState::new(NA)),
            regular_mark: Arc::new(AtomicControllerState::new(NA)),
            ephemeral_mark: Arc::new(AtomicControllerState::new(NA)),
            mutex: Arc::new(std::sync::Mutex::new(())),
            dropping: Arc::new(AtomicBool::new(false)),
            state: State::new(),
        }
    }

    pub fn exit(&self, code: i32) {
        let handle = Handle::current();
        block_in_place(move || {
            handle.block_on(async {
                self.cleanup().await;
            });
            exit(code);
        });
        exit(code);
    }

    async fn cleanup(&self) {
        let _lock = self.semaphore.lock().await;
        info!("Supervisor is performing cleanup...");
        match self.stop_workers(Kill).await {
            Ok(_) => info!("Supervisor cleanup completed successfully."),
            Err(e) => error!("Supervisor cleanup failed: {}", e),
        }
    }

    async fn run(&self) -> Result<(), RuntimeError> {
        if let Err(e) = set_current_dir(self.params.work()) {
            return Err(RuntimeError::new(format!(
                "Failed to set the current directory to the work folder {}: {}",
                self.params.work().display(),
                e
            )));
        }

        let mut monitor_handle = Box::pin(self.monitor());
        let mut manager_handle = Box::pin(self.manage());

        loop {
            select! {
                result = manager_handle.as_mut() => {
                    info!("Supervisor task completed: '{:?}'", result);
                    let _ = self.stop_workers(Signal::Term).await;
                    return result
                },
                result = monitor_handle.as_mut() => {
                    info!("Received STOP request. Initiating graceful stop...: '{:?}'", result);
                    return self.stop_workers(Signal::Term).await;
                },
                signal = terminate() => {
                    if let Some(signal) = signal {
                        return self.stop_workers(signal).await;
                    }
                },
            }
        }
    }

    async fn monitor(&self) -> Result<(), RuntimeError> {
        loop {
            if self.monitor.load(SeqCst) {
                info!("Received monitor state to stop Supervisor.");
                return Ok(());
            }
            sleep(Duration::from_millis(MONITOR_WAIT_MILLISECONDS)).await;
        }
    }

    async fn manage(&self) -> Result<(), RuntimeError> {
        let (sender, receiver) = mpsc::channel(POLL_DISPATCH_CHANNEL_SIZE);
        let poller_handle = self.poll(sender);
        let dispatcher_handle = self.dispatch(receiver);
        select! {
            result = poller_handle => {
                info!("Poller completed: '{:?}'", result);
                result
            },
            result = dispatcher_handle => {
                info!("Dispatcher completed: '{:?}'", result);
                result
            },
        }
    }

    async fn poll(&self, sender: Sender<SupervisorMessage>) -> Result<(), RuntimeError> {
        let queue = SupervisorMessageQueue::new(self.params.clone().work().join(MSG_FOLDER));
        info!("Created message queue: {:?}", queue);
        loop {
            self.poll_error(&queue).await?;
            self.poll_planned(&queue, &sender).await?;
            sleep(Duration::from_millis(POLLING_WAIT_MILLISECONDS)).await;
        }
    }

    async fn poll_error(&self, queue: &SupervisorMessageQueue) -> Result<(), RuntimeError> {
        let messages = queue.error_messages();
        trace!(
            "Polling for error messages. Got {} new messages.",
            messages.len()
        );
        if !messages.is_empty() {
            trace!("Received new error message: {}.", messages.len());
        }
        for message in messages {
            self.retry(message)?;
        }
        Ok(())
    }

    fn retry(&self, message: SupervisorMessage) -> Result<(), RuntimeError> {
        let regex = Regex::new(REQUEST_MESSAGE_FILE_PATTERN).unwrap();
        if let Some(file_name) = message.file().file_name().and_then(|f| f.to_str())
            && let Some(captures) = regex.captures(file_name)
        {
            let id = captures.get(1).map(|m| m.as_str()).unwrap();
            let run = captures.get(2).map(|m| m.as_str()).unwrap();
            let ext = captures.get(3).map(|m| m.as_str()).unwrap();
            if let Ok(retry) = run.parse::<u16>() {
                let payload = match message.payload() {
                    SupervisorRequestMessagePayload(payload) => payload,
                    SupervisorResponseMessagePayload(_) | SupervisorExceptionMessagePayload(_) => {
                        return Err(RuntimeError::new(
                            "Unexpected response message received".to_string(),
                        ));
                    }
                };
                let worker = self
                    .config
                    .controllers
                    .ephemeral
                    .workers
                    .get(payload.worker());
                if let Some(worker) = worker
                    && retry <= *worker.retries()
                {
                    let retry = retry + 1;
                    let name = format!("{id}{RETRIES_DELIMITER}{retry}{ext}");
                    return if let Err(e) = SupervisorMessageQueue::planned(message.clone(), name) {
                        let error = format!("Error retrying message '{message:?}': {e}");
                        error!("{}", error);
                        Err(RuntimeError::new(error))
                    } else {
                        info!("Sent retry message to planned queue: {:?}", message);
                        Ok(())
                    };
                }
            }
        }
        if let Err(e) = SupervisorMessageQueue::fail(message.clone()) {
            let error = format!("Error failing message '{message:?}': {e}");
            error!("{}", error);
            return Err(RuntimeError::new(error));
        } else {
            info!("Sent error message to fail vault: {:?}", message);
        }
        Ok(())
    }

    async fn poll_planned(
        &self,
        queue: &SupervisorMessageQueue,
        sender: &Sender<SupervisorMessage>,
    ) -> Result<(), RuntimeError> {
        let messages = queue.planned_messages();
        trace!(
            "Polling for planned messages. Got {} new messages.",
            messages.len()
        );
        if !messages.is_empty() {
            trace!("Received new planned message: {}.", messages.len());
        }
        for message in messages {
            if let Err(e) = SupervisorMessageQueue::queued(message.clone()) {
                let error = format!("Error queuing message '{message:?}': {e}");
                error!("{}", error);
                return Err(RuntimeError::new(error));
            } else {
                info!("Sent message to controllers queue: {:?}", message);
            }
            if let Err(e) = sender.send(message.clone()).await {
                let error = format!("Error dispatching message '{message:?}': {e}");
                error!("{}", error);
                return Err(RuntimeError::new(error));
            } else {
                info!("Sent message to controllers queue: {:?}", message);
            }
        }
        Ok(())
    }

    async fn dispatch(
        &self,
        mut receiver: Receiver<SupervisorMessage>,
    ) -> Result<(), RuntimeError> {
        let (sender_init, receiver_init): (Sender<SupervisorMessage>, Receiver<SupervisorMessage>) =
            mpsc::channel(DISPATCH_INIT_CHANNEL_SIZE);
        let (sender_regular, receiver_regular): (
            Sender<SupervisorMessage>,
            Receiver<SupervisorMessage>,
        ) = mpsc::channel(DISPATCH_REGULAR_CHANNEL_SIZE);
        let (sender_ephemeral, receiver_ephemeral): (
            Sender<SupervisorMessage>,
            Receiver<SupervisorMessage>,
        ) = mpsc::channel(DISPATCH_EPHEMERAL_CHANNEL_SIZE);

        let mut controllers_handle = tokio::spawn({
            let self_arc = Arc::new(self.clone());
            async move {
                self_arc
                    .start_controllers(receiver_init, receiver_regular, receiver_ephemeral)
                    .await
            }
        });

        let mut timer =
            tokio::time::interval(Duration::from_millis(CONTROLLERS_ALIVE_CHECK_MILLISECONDS));
        loop {
            select! {
                result = &mut controllers_handle => {
                    match result {
                        Ok(outcome) => match outcome {
                            Ok(()) => {
                                info!("Controller task completed successfully!");
                            },
                            Err(e) => {
                                error!("Controller task completed unsuccessfully: {:?}", e);
                                return Err(e);
                            }
                        },
                        Err(e) => {
                            let error = format!("Controller task completed unexpectedly: {e:?}");
                            error!(error);
                            return Err(RuntimeError::new(error));
                        }
                    }
                },
                Some(message) = receiver.recv() => {
                    debug!("Received message '{:?}", message);
                    let payload = match message.payload() {
                        SupervisorRequestMessagePayload(payload) => {payload},
                        SupervisorResponseMessagePayload(_) |
                        SupervisorExceptionMessagePayload(_) => {
                            return Err(RuntimeError::new("Unexpected response message received".to_string()));
                        }
                    };
                    let send_result = match payload.class() {
                        INIT => sender_init.send(message.clone()),
                        REGULAR => sender_regular.send(message.clone()),
                        EPHEMERAL => sender_ephemeral.send(message.clone()),
                    };
                    match send_result.await {
                        Ok(_) => {
                            debug!("Message '{:?}' sent successfully!", message);
                        }
                        Err(e) => {
                            let error = format!("Failed to send message '{message:?}': {e:?}");
                            error!(error);
                            return Err(RuntimeError::new(error));
                        }
                    }
                },
                _ = timer.tick() => {
                    debug!("Periodic check: controllers are still running...");
                }
            }
        }
    }

    async fn start_controllers(
        &self,
        receiver_init: Receiver<SupervisorMessage>,
        receiver_regular: Receiver<SupervisorMessage>,
        receiver_ephemeral: Receiver<SupervisorMessage>,
    ) -> Result<(), RuntimeError> {
        let self_arc = Arc::new(self.clone());

        let init_self_arc = self_arc.clone();
        let init_controller_handle = tokio::spawn(async move {
            init_self_arc
                .clone()
                .start_init_controller(receiver_init)
                .await
        });
        while matches!(self.init_mark.load(SeqCst), NA) {
            debug!("Init workers not yet ready...");
            sleep(Duration::from_millis(WAIT_FOR_INIT_MILLISECONDS)).await;
        }
        if matches!(self.init_mark.load(SeqCst), KO) {
            error!("Init workers failed. Exiting...");
            self.exit(GeneralError.code());
        } else {
            info!("Init workers ready!");
        }

        let regular_self_arc = self_arc.clone();
        let regular_controller_handle = tokio::spawn(async move {
            regular_self_arc
                .clone()
                .start_regular_controller(receiver_regular)
                .await
        });
        while matches!(self.regular_mark.load(SeqCst), NA) {
            debug!("Regular workers not yet ready...");
            sleep(Duration::from_millis(WAIT_FOR_INIT_MILLISECONDS)).await;
        }
        if matches!(self.regular_mark.load(SeqCst), KO) {
            error!("Regular workers failed. Exiting...");
            self.exit(GeneralError.code());
        } else {
            info!("Regular workers ready!");
        }

        let ephemeral_self_arc = self_arc.clone();
        let ephemeral_controller_handle = tokio::spawn(async move {
            ephemeral_self_arc
                .clone()
                .start_ephemeral_controller(receiver_ephemeral)
                .await
        });
        while matches!(self.ephemeral_mark.load(SeqCst), NA) {
            debug!("Ephemeral workers not yet ready...");
            sleep(Duration::from_millis(WAIT_FOR_INIT_MILLISECONDS)).await;
        }
        if matches!(self.ephemeral_mark.load(SeqCst), KO) {
            error!("Ephemeral workers failed. Exiting...");
            self.exit(GeneralError.code());
        } else {
            info!("Ephemeral workers ready!");
        }

        select! {
            result = init_controller_handle => {
                info!("Init controller task completed. Leaving...: '{:?}'", result);
                match result {
                    Ok(_) => Ok(()),
                    Err(e) => Err(RuntimeError::new(format!("Init controller error: '{e:?}'")))
                }
            },
            result = regular_controller_handle => {
                info!("Regular controller task completed. Leaving...: '{:?}'", result);
                match result {
                    Ok(_) => Ok(()),
                    Err(e) => Err(RuntimeError::new(format!("Regular controller error: '{e:?}'")))
                }
            },
            result = ephemeral_controller_handle => {
                info!("Ephemeral controller task completed. Leaving...: '{:?}'", result);
                match result {
                    Ok(_) => Ok(()),
                    Err(e) => Err(RuntimeError::new(format!("Ephemeral controller error: '{e:?}'")))
                }
            },
        }
    }

    async fn start_init_controller(
        &self,
        receiver: Receiver<SupervisorMessage>,
    ) -> Result<(), RuntimeError> {
        let result = self
            .start_controller(
                move || Arc::new(self.clone()).start_init_workers(receiver),
                "Init workers completed successfully!",
                "Init workers completed unsuccessfully",
            )
            .await;
        match result {
            Ok(_) => {
                if !matches!(self.init_mark.load(SeqCst), KO) {
                    self.init_mark.store(OK, SeqCst);
                }
            }
            Err(_) => {
                self.init_mark.store(KO, SeqCst);
            }
        };
        result
    }

    async fn start_regular_controller(
        &self,
        receiver: Receiver<SupervisorMessage>,
    ) -> Result<(), RuntimeError> {
        let result = self
            .start_controller(
                move || Arc::new(self.clone()).start_regular_workers(receiver),
                "Regular workers completed successfully!",
                "Regular workers completed unsuccessfully",
            )
            .await;
        match result {
            Ok(_) => {
                if !matches!(self.regular_mark.load(SeqCst), KO) {
                    self.regular_mark.store(OK, SeqCst);
                }
            }
            Err(_) => {
                self.regular_mark.store(KO, SeqCst);
            }
        };
        result
    }

    async fn start_ephemeral_controller(
        &self,
        receiver: Receiver<SupervisorMessage>,
    ) -> Result<(), RuntimeError> {
        let result = self
            .start_controller(
                move || Arc::new(self.clone()).start_ephemeral_workers(receiver),
                "Ephemeral workers completed successfully!",
                "Ephemeral workers completed unsuccessfully",
            )
            .await;
        match result {
            Ok(_) => {
                if !matches!(self.ephemeral_mark.load(SeqCst), KO) {
                    self.ephemeral_mark.store(OK, SeqCst);
                }
            }
            Err(_) => {
                self.ephemeral_mark.store(KO, SeqCst);
            }
        };
        result
    }

    async fn start_controller<Fut>(
        &self,
        start_workers_function: impl FnOnce() -> Fut,
        success_message: &'static str,
        failure_message: &'static str,
    ) -> Result<(), RuntimeError>
    where
        Fut: std::future::Future<Output = Result<(), RuntimeError>>,
    {
        let workers_task = start_workers_function();
        let workers_result = workers_task.await;
        match workers_result {
            Ok(_) => info!("{}", success_message),
            Err(e) => {
                info!("{}: '{}'", failure_message, e);
                return Err(e);
            }
        };
        Ok(())
    }

    async fn start_init_workers(
        self: Arc<Self>,
        receiver: Receiver<SupervisorMessage>,
    ) -> Result<(), RuntimeError> {
        let controller = &self.config.controllers.init.clone();
        let mark = self.init_mark.clone();
        self.start_workers(
            controller,
            receiver,
            mark,
            |arc_self, worker, message| async move {
                arc_self
                    .start_init_worker(worker, message)
                    .await
                    .map_err(|e| RuntimeError::new(format!("Init worker error: {e:?}")))
            },
            "Init".to_string(),
        )
        .await
    }

    async fn start_regular_workers(
        self: Arc<Self>,
        receiver: Receiver<SupervisorMessage>,
    ) -> Result<(), RuntimeError> {
        let controller = &self.config.controllers.regular.clone();
        let mark = self.regular_mark.clone();
        self.start_workers(
            controller,
            receiver,
            mark,
            |arc_self, worker, message| async move {
                arc_self
                    .start_regular_worker(worker, message)
                    .await
                    .map_err(|e| RuntimeError::new(format!("Regular worker error: {e:?}")))
            },
            "Regular".to_string(),
        )
        .await
    }

    async fn start_ephemeral_workers(
        self: Arc<Self>,
        receiver: Receiver<SupervisorMessage>,
    ) -> Result<(), RuntimeError> {
        let controller = &self.config.controllers.ephemeral.clone();
        let mark = self.ephemeral_mark.clone();
        self.start_workers(
            controller,
            receiver,
            mark,
            |arc_self, worker, message| async move {
                arc_self
                    .start_ephemeral_worker(worker, message)
                    .await
                    .map_err(|e| RuntimeError::new(format!("Ephemeral worker error: {e:?}")))
            },
            "Ephemeral".to_string(),
        )
        .await
    }

    async fn start_workers<F, Fut>(
        self: Arc<Self>,
        controller: &ControllerConfig,
        mut receiver: Receiver<SupervisorMessage>,
        mark: Arc<AtomicControllerState>,
        start_worker_function: F,
        controller_class: String,
    ) -> Result<(), RuntimeError>
    where
        F: Fn(Arc<Self>, WorkerConfig, Option<SupervisorMessage>) -> Fut
            + Copy
            + Send
            + Sync
            + 'static,
        Fut: std::future::Future<Output = Result<(), RuntimeError>> + Send + 'static,
    {
        type RunningTasksSet = FuturesUnordered<
            JoinHandle<Result<(WorkerConfig, Result<(), RuntimeError>), tokio::task::JoinError>>,
        >;

        let concurrency_limit = controller.concurrency as usize;
        let workers_map: IndexMap<String, WorkerConfig> = if controller.name == EPHEMERAL.as_ref() {
            IndexMap::new()
        } else {
            controller.workers().clone()
        };
        let mut workers_iter = workers_map.iter();
        let mut running_tasks: RunningTasksSet = FuturesUnordered::new();

        loop {
            let can_spawn_worker = workers_iter.len() > 0;
            let can_spawn_task = running_tasks.len() < concurrency_limit || concurrency_limit == 0;
            let can_poll_task = !running_tasks.is_empty();
            select! {
                biased;
                Some(result) = running_tasks.next(), if can_poll_task => {
                    match result {
                        Ok(Ok((worker, outcome))) => {
                            match outcome {
                                Ok(()) => {
                                    info!("{} task for worker '{:?}' finished successfully", controller_class, worker);
                                }
                                Err(e) => {
                                    error!("{} task for worker '{:?}' finished unsuccessfully: {:?}", controller_class, worker, e);
                                    return Err(e);
                                }
                            }
                        }
                        Ok(Err(e)) => {
                            let message = format!("{controller_class} task finished unexpectedly with error: {e:?}");
                            error!(message);
                            return Err(RuntimeError::new(message));
                        }
                        Err(e) => {
                            let message = format!("{controller_class} task finished abruptly with error: {e:?}");
                            error!(message);
                            return Err(RuntimeError::new(message));
                        }
                    }
                }
                Some((_name, worker)) = async { workers_iter.next() }, if can_spawn_worker && can_spawn_task => {
                    let worker_clone = worker.clone();
                    let self_arc = Arc::clone(&self);
                    let log_prefix_clone = controller_class.clone();
                    let handle = tokio::spawn(async move {
                        let outcome = start_worker_function(self_arc, worker_clone.clone(), None).await
                            .map_err(|e| RuntimeError::new(format!("{log_prefix_clone} RunnerError: {e:?} - {worker_clone:?}")));
                        Ok((worker_clone, outcome))
                    });
                    running_tasks.push(handle);
                }
                _ = async { }, if !can_spawn_worker && matches!(mark.load(SeqCst), NA) => {
                    let done = if controller.name == INIT.as_ref() {
                        if running_tasks.is_empty() {
                            info!("All scheduled Init tasks completed!");
                            true
                        } else {
                            info!("Some scheduled Init tasks pending: {}", running_tasks.len());
                            false
                        }
                    } else {
                        true
                    };
                    if done {
                        if !matches!(mark.load(SeqCst), KO) {
                            mark.store(OK, SeqCst);
                        }
                        info!("All {} workers have been launched.", controller_class);
                    }
                }
                Some(message) = receiver.recv(), if !can_spawn_worker && can_spawn_task => {
                    let payload = match message.payload() {
                        SupervisorRequestMessagePayload(payload) => {payload},
                        SupervisorResponseMessagePayload(_) |
                        SupervisorExceptionMessagePayload(_) => {
                            return Err(RuntimeError::new("Unexpected response message received".to_string()));
                        }
                    };
                    let worker = controller.workers.get(payload.worker());
                    match worker {
                        Some(worker) => {
                            let worker_clone = worker.clone();
                            let self_arc = Arc::clone(&self);
                            let log_prefix_clone = controller_class.clone();
                            let handle = tokio::spawn(async move {
                                let outcome = start_worker_function(self_arc, worker_clone.clone(), Some(message)).await
                                    .map_err(|e| RuntimeError::new(format!("{log_prefix_clone} RunnerError: {e:?} - {worker_clone:?}")));
                                Ok((worker_clone, outcome))
                            });
                            running_tasks.push(handle);
                        }
                        None => {
                            let message = format!("Unrecognized worker name in message. Discarding it. - '{message:?}'");
                            error!(message);
                            return Err(RuntimeError::new(message));
                        }
                    }
                }
                else => {
                    if running_tasks.is_empty() && receiver.is_closed() {
                        info!("No pending {} worker in initial list, or in queue, or in execution stack.", controller_class);
                        break;
                    }
                }
            }
            sleep(Duration::from_millis(CONTROLLER_WAIT_MILLISECONDS)).await;
        }
        Ok(())
    }

    async fn start_init_worker(
        &self,
        worker: WorkerConfig,
        message: Option<SupervisorMessage>,
    ) -> Result<(), RunnerError> {
        let (_, result) = self.start_worker(worker, message, None, INIT).await;
        result.map(|_| ())?;
        Ok(())
    }

    async fn start_regular_worker(
        &self,
        worker: WorkerConfig,
        message: Option<SupervisorMessage>,
    ) -> Result<(), RunnerError> {
        let (_, result) = self.start_worker(worker, message, None, REGULAR).await;
        result.map(|_| ())?;
        Ok(())
    }

    async fn start_ephemeral_worker(
        &self,
        worker: WorkerConfig,
        message: Option<SupervisorMessage>,
    ) -> Result<(), RunnerError> {
        let message = message.ok_or_else(|| {
            let error = "Received void message for ephemeral controller. Ignoring request.";
            error!(error);
            VoidEphemeralMessage
        })?;

        let start = Utc::now().timestamp_millis();

        let (worker_run, result) = self
            .start_worker(
                worker.clone(),
                Some(message.clone()),
                Some(start),
                EPHEMERAL,
            )
            .await;

        self.notify_end(worker, message, start, worker_run.as_ref(), result)
            .await?;
        Ok(())
    }

    async fn notify_start(
        &self,
        message: SupervisorMessage,
        start: i64,
        worker_run: Option<&TabsDataWorker>,
    ) -> Result<(), RunnerError> {
        let execution = execution(&message);
        let status = WorkerCallbackStatus::Running;
        match notify(
            worker_run,
            message.clone(),
            start,
            None,
            status,
            execution,
            None,
            None,
        )
        .await
        {
            Ok(_) => {
                info!(
                    "Successful notification of worker start: {}:\n",
                    serde_json::to_string_pretty(&message)?
                );
                Ok(())
            }
            Err(error) => {
                info!(
                    "Failed notification of worker start: {}\n{}:\n",
                    error,
                    serde_json::to_string_pretty(&message)?
                );
                Err(StartNotificationError)
            }
        }
    }

    async fn notify_end(
        &self,
        worker: WorkerConfig,
        message: SupervisorMessage,
        start: i64,
        worker_run: Option<&TabsDataWorker>,
        result: Result<(), RunnerError>,
    ) -> Result<(), RunnerError> {
        let end = Utc::now().timestamp_millis();
        let execution = execution(&message);
        let limit = worker.retries;
        let (status, error) = match &result {
            Ok(_) => (WorkerCallbackStatus::Done, None),
            Err(e) => {
                if execution <= limit {
                    (WorkerCallbackStatus::Error, Some(format!("{e:?}")))
                } else {
                    (WorkerCallbackStatus::Failed, Some(format!("{e:?}")))
                }
            }
        };

        let notify_answer = notify(
            worker_run,
            message.clone(),
            start,
            Some(end),
            status.clone(),
            execution,
            Some(limit),
            error.clone(),
        )
        .await;

        let failed = match notify_answer {
            Ok(answer) => answer,
            Err(_) => {
                SupervisorMessageQueue::error(message)?;
                return Ok(());
            }
        };
        if failed {
            info!(
                "Failing worker as function returned a failure exit status:\n\
                - Worker:\n{}\n\
                - Request Message: {}\n\
                - Start Time: {}\n\
                - End Time: {}\n\
                - Status: {:?}\n\
                - Execution: {}\n\
                - Executions Limit: {}\n\
                - Error: {:?}",
                match &worker_run {
                    Some(worker) => worker.describer().to_string(),
                    None => "No worker...".to_string(),
                },
                serde_yaml::to_string(&message)?,
                start,
                end,
                status,
                execution,
                limit,
                error
            );
            SupervisorMessageQueue::fail(message)?;
        } else {
            match result {
                Ok(_) => {
                    SupervisorMessageQueue::complete(message)?;
                }
                Err(_) => {
                    SupervisorMessageQueue::error(message)?;
                }
            }
        }
        Ok(())
    }

    async fn start_worker(
        &self,
        worker: WorkerConfig,
        message: Option<SupervisorMessage>,
        start: Option<i64>,
        class: WorkerClass,
    ) -> (Option<TabsDataWorker>, Result<(), RunnerError>) {
        info!(
            "Entering {} worker... '{}'",
            class.as_ref().to_string(),
            worker.name()
        );

        let (config_folder, work_folder) =
            match self.obtain_worker_folders(worker.clone(), message.clone(), class.clone()) {
                Ok(folders) => folders,
                Err(err) => return (None, Err(err)),
            };

        let (program, arguments, markers) = match self.obtain_worker_command(
            worker.clone(),
            message.clone(),
            class.clone(),
            config_folder.clone(),
            work_folder.clone(),
        ) {
            Ok(command) => command,
            Err(err) => return (None, Err(err)),
        };

        let describer = match TabsDataWorkerDescriberBuilder::default()
            .class(class.clone())
            .name(worker.name())
            .location(worker.location().clone())
            .program(program)
            .set_state(worker.set_state().clone())
            .get_states(worker.get_states().clone())
            .arguments(arguments)
            .markers(markers)
            .config(config_folder)
            .work(work_folder)
            .queue(self.params.clone().work().join(MSG_FOLDER))
            .etc(self.params.clone().work().join(ETC_FOLDER))
            .message(message.clone())
            .build()
            .map_err(|e| {
                error!(
                    "Class {} worker '{}' failed to be described: {:?}",
                    class.as_ref().to_string(),
                    worker.name(),
                    e
                );
                DescriberFailure {
                    worker: worker.clone().name,
                    cause: e,
                }
            }) {
            Ok(describer) => describer,
            Err(err) => return (None, Err(err)),
        };

        let mut work_get_state = None;

        if !worker.get_states.is_empty() {
            debug!(
                "Class {} worker '{}' requested get-states: {:?}",
                class.as_ref().to_string(),
                worker.name(),
                worker.get_states
            );
            if worker.get_states.len() > 1 {
                warn!(
                    "Class {} worker '{}' requested more than 1 get-states. This is not currently supported: {:?}",
                    class.as_ref().to_string(),
                    worker.name(),
                    worker.get_states
                );
            }
            // We are sure there is at least 1 element...
            let get_state = worker.get_states.first().unwrap().clone();
            let set_state = SetState {
                state_type: get_state.state_type,
                state_key: get_state.state_key,
            };
            let state = match self.state.read().await.data().get(&set_state).cloned() {
                Some(value) => value,
                None => {
                    return (
                        None,
                        Err(StateError::MissingStateKey { key: set_state }.into()),
                    );
                }
            };

            let settings = match state {
                StateDataValue::Blob(encoded_blob) => match encoded_blob.decode() {
                    Ok(blob) => blob,
                    Err(error) => return (None, Err(error.into())),
                },
                StateDataValue::Map(encoded_map) => {
                    let map = match encoded_map.decode() {
                        Ok(mapping) => mapping,
                        Err(error) => return (None, Err(error.into())),
                    };
                    let prefixes = get_state.state_prefixes;
                    let mapping: HashMap<String, String> = if prefixes.is_empty() {
                        map
                    } else {
                        map.into_iter()
                            .filter(|(k, _)| prefixes.iter().any(|p| k.starts_with(p)))
                            .collect()
                    };
                    match EncodedMap::serialize(&mapping) {
                        Ok(yaml) => yaml,
                        Err(error) => return (None, Err(error.into())),
                    }
                }
            };
            work_get_state = Some(settings)
        }

        let td_worker = TabsDataWorker::new(describer.clone());
        match td_worker.work(work_get_state, false) {
            Ok((mut child, _out, _err)) => {
                if let Some(message) = message {
                    let start = match start {
                        None => return (Some(td_worker), Err(MissingStartDate)),
                        Some(start) => start,
                    };
                    if let Err(error) = self
                        .notify_start(message.clone(), start, Some(&td_worker))
                        .await
                    {
                        info!(
                            "Unexpected failure of notification of worker start: {}\n{:?}\n",
                            error,
                            serde_json::to_string_pretty(&message)
                        );
                        return (Some(td_worker), Err(error));
                    }
                    match SupervisorMessageQueue::ongoing(message) {
                        Ok(result) => result,
                        Err(err) => return (Some(td_worker), Err(RunnerError::from(err))),
                    };
                }
                if class == REGULAR && check_nowait_env() {
                    info!(
                        "Holding {} worker... '{}'",
                        class.as_ref().to_string(),
                        worker.name()
                    );
                    loop {
                        sleep(Duration::from_secs(5)).await;
                    }
                } else {
                    match child.wait().await {
                        Ok(exit_status) => {
                            if exit_status.success() {
                                info!(
                                    "Class {} worker '{}' completed successfully!",
                                    class.as_ref(),
                                    &describer.name()
                                );
                                if let Some(set_state) = worker.set_state().as_ref().cloned() {
                                    let mut output = String::new();
                                    match child.stdout.take() {
                                        Some(mut stdout) => {
                                            if let Err(e) = stdout.read_to_string(&mut output).await
                                            {
                                                return (Some(td_worker), Err(ReadStdOutError(e)));
                                            }
                                        }
                                        _ => {
                                            return (Some(td_worker), Err(MissingStdOutError));
                                        }
                                    }
                                    let state = extract_state_data_from_string(
                                        output,
                                        set_state.state_type,
                                    );
                                    self.state
                                        .write()
                                        .await
                                        .data_mut()
                                        .insert(set_state, state.unwrap());
                                }
                            } else {
                                return if let Some(code) = exit_status.code() {
                                    let message = format!(
                                        "Class {} worker '{}' failed with exit code: {:?}",
                                        class.as_ref(),
                                        &describer.name(),
                                        code
                                    );
                                    error!(message);
                                    (Some(td_worker), Err(WorkerExited { message }))
                                } else {
                                    #[cfg(not(target_os = "windows"))]
                                    {
                                        if let Some(signal) = exit_status.signal() {
                                            let message = format!(
                                                "Class {} worker '{}' failed with signal: {:?}",
                                                class.as_ref(),
                                                &describer.name(),
                                                signal
                                            );
                                            error!(message);
                                            return (
                                                Some(td_worker),
                                                Err(WorkerExited { message }),
                                            );
                                        }
                                    }
                                    let message =
                                        "Process exited with unknown exit code or signal."
                                            .to_string();
                                    error!(message);
                                    (Some(td_worker), Err(WorkerExited { message }))
                                };
                            }
                        }
                        Err(e) => {
                            error!(
                                "Class {} worker '{}' failed with error: {:?}",
                                class.as_ref().to_string(),
                                &describer.name(),
                                e
                            );
                            return (Some(td_worker), Err(IOError(e)));
                        }
                    };
                }
            }
            Err(e) => match class {
                INIT => {
                    error!(
                        "Init worker '{}' failed with error: {:?}",
                        &describer.name(),
                        e
                    );
                    self.exit(GeneralError.code());
                }
                REGULAR => return (Some(td_worker), Err(e)),
                EPHEMERAL => return (Some(td_worker), Err(e)),
            },
        };
        info!(
            "Exiting {} worker... '{}'",
            class.as_ref().to_string(),
            worker.name()
        );
        (Some(td_worker), Ok(()))
    }

    fn obtain_worker_folders(
        &self,
        worker: WorkerConfig,
        message: Option<SupervisorMessage>,
        class: WorkerClass,
    ) -> Result<(PathBuf, PathBuf), RunnerError> {
        let config_folder = match class {
            EPHEMERAL => self
                .params
                .clone()
                .work()
                .join(PROC_FOLDER)
                .join(class.as_ref())
                .join(worker.name())
                .join(WORK_FOLDER)
                .join(CAST_FOLDER)
                .join(message.clone().unwrap().work())
                .join(CONFIG_FOLDER),
            _ => self
                .params
                .clone()
                .config()
                .join(PROC_FOLDER)
                .join(class.as_ref())
                .join(worker.name())
                .join(CONFIG_FOLDER),
        };

        let work_folder = match class {
            EPHEMERAL => self
                .params
                .clone()
                .work()
                .join(PROC_FOLDER)
                .join(class.as_ref())
                .join(worker.name())
                .join(WORK_FOLDER)
                .join(CAST_FOLDER)
                .join(message.clone().unwrap().work())
                .join(WORK_FOLDER),
            _ => self
                .params
                .clone()
                .work()
                .join(PROC_FOLDER)
                .join(class.as_ref())
                .join(worker.name())
                .join(WORK_FOLDER),
        };

        if let EPHEMERAL = class {
            create_dir_all(&config_folder)?;
            create_dir_all(&work_folder)?;
            let mold_folder = self
                .params
                .clone()
                .work()
                .join(PROC_FOLDER)
                .join(class.as_ref())
                .join(worker.name())
                .join(MOLD_FOLDER);
            copy_mold_tree(&mold_folder, &work_folder)?;
        }

        Ok((config_folder, work_folder))
    }

    fn obtain_worker_command(
        &self,
        worker: WorkerConfig,
        message: Option<SupervisorMessage>,
        class: WorkerClass,
        _config_folder: PathBuf,
        work_folder: PathBuf,
    ) -> Result<WorkerCommandLine, RunnerError> {
        let parent_work_folder = self.params.clone().work();
        let child_work_folder = work_folder.clone();

        let mut markers = Vec::new();

        let forward_parameters = self
            .forward_parameters(
                worker.clone(),
                class.clone(),
                parent_work_folder,
                child_work_folder,
            )
            .unwrap_or_else(|err| {
                error!(
                    "Class {} worker '{}' failed to forward parameters: {:?}",
                    class.as_ref().to_string(),
                    worker.name(),
                    err
                );
                self.exit(TabsDataError.code());
                // This line is never executed, as exit will actually exit...
                Vec::new()
            });

        let program = match class {
            EPHEMERAL => ScriptBuilder::SHELL.to_string(),
            _ => PathBuf::from(worker.program())
                .as_os_str()
                .to_string_lossy()
                .to_string(),
        };

        let (parameters, markers) = match class {
            EPHEMERAL => {
                let script_path = work_folder.join(PARENT_FOLDER).join(worker.name());
                let mut command = CommandBuilder::new().binary(
                    PathBuf::from(worker.program())
                        .as_os_str()
                        .to_string_lossy()
                        .to_string(),
                );
                let message = message.unwrap();
                let payload = match message.payload() {
                    SupervisorRequestMessagePayload(payload) => payload,
                    SupervisorResponseMessagePayload(_) | SupervisorExceptionMessagePayload(_) => {
                        return Err(InvalidMessageType);
                    }
                };
                for argument in payload.arguments() {
                    command = command.argument(ArgumentPrefix::None, argument, None)
                }
                for argument in forward_parameters {
                    command = command.argument(ArgumentPrefix::None, &argument, None)
                }
                ScriptBuilder::new()
                    .background_statement(&command.build())
                    .build(script_path.clone())?;
                let mut parameters: Vec<_> = ScriptBuilder::SHELL_OPTIONS
                    .iter()
                    .map(|&s| s.to_string())
                    .collect();
                parameters.push(ScriptBuilder::script_to_platform(script_path));

                for key in worker.markers() {
                    match key.parse::<MarkerKey>() {
                        Ok(marker_key) => {
                            markers.push(format!("--{key}"));
                            markers.push(marker_key.produce(&message, payload).unwrap());
                        }
                        Err(e) => {
                            error!("Unrecognized marker '{}': {}", key, e);
                        }
                    }
                }

                (parameters, markers)
            }
            _ => (forward_parameters, markers),
        };

        Ok((program, parameters, markers))
    }

    async fn stop_workers(&self, signal: Signal) -> Result<(), RuntimeError> {
        self.stop_ephemeral_workers(signal).await?;
        self.stop_regular_workers(signal).await?;
        self.stop_init_workers(signal).await?;
        self.stop_processes(signal).await?;
        Ok(())
    }

    async fn stop_ephemeral_workers(&self, signal: Signal) -> Result<(), RuntimeError> {
        info!("Stopping ephemeral workers...");
        for (_name, worker) in self.config.controllers().ephemeral().workers().iter() {
            info!("Stopping ephemeral worker...: {:?}", worker);
            self.stop_ephemeral_worker(worker, signal).await?;
            info!("Stopped ephemeral worker...: {:?}", worker);
        }
        Ok(())
    }

    async fn stop_ephemeral_worker(
        &self,
        worker: &WorkerConfig,
        signal: Signal,
    ) -> Result<(), RuntimeError> {
        let cast_path = self
            .params
            .clone()
            .work()
            .join(PROC_FOLDER)
            .join(EPHEMERAL.as_ref())
            .join(worker.name())
            .join(WORK_FOLDER)
            .join(CAST_FOLDER);
        match read_dir(&cast_path) {
            Ok(nodes) => {
                for node in nodes {
                    match node {
                        Ok(entry) => {
                            let path = entry.path();
                            if path.is_dir() {
                                let instance = path.join(WORK_FOLDER);
                                self.stop_worker(instance, signal).await?;
                            }
                        }
                        Err(ref e) => {
                            warn!(
                                "Folder '{:?}' for ephemeral worker '{}' instance cannot be processed. Skipping processes termination request: '{}'",
                                node,
                                worker.name(),
                                e
                            );
                        }
                    }
                }
            }
            Err(e) => {
                warn!(
                    "Folder '{:?}' for ephemeral worker '{}' cannot be traversed. Skipping process termination request: {}",
                    cast_path,
                    worker.name(),
                    e
                );
            }
        }
        Ok(())
    }

    async fn stop_regular_workers(&self, signal: Signal) -> Result<(), RuntimeError> {
        info!("Stopping regular workers...");
        for (_name, worker) in self.config.controllers().regular().workers().iter() {
            info!("Stopping regular worker...: {:?}", worker);
            self.stop_regular_worker(worker, signal).await?;
            info!("Stopped regular worker...: {:?}", worker);
        }
        Ok(())
    }

    async fn stop_regular_worker(
        &self,
        worker: &WorkerConfig,
        signal: Signal,
    ) -> Result<(), RuntimeError> {
        self.stop_common_worker(worker, REGULAR, signal).await?;
        Ok(())
    }

    async fn stop_init_workers(&self, signal: Signal) -> Result<(), RuntimeError> {
        info!("Stopping init workers...");
        for (_name, worker) in self.config.controllers().init().workers().iter() {
            info!("Stopping init worker...: {:?}", worker);
            self.stop_init_worker(worker, signal).await?;
            info!("Stopped init worker...: {:?}", worker);
        }
        Ok(())
    }

    async fn stop_init_worker(
        &self,
        worker: &WorkerConfig,
        signal: Signal,
    ) -> Result<(), RuntimeError> {
        self.stop_common_worker(worker, INIT, signal).await?;
        Ok(())
    }

    async fn stop_common_worker(
        &self,
        worker: &WorkerConfig,
        class: WorkerClass,
        signal: Signal,
    ) -> Result<(), RuntimeError> {
        let work_path = self
            .params
            .clone()
            .work()
            .join(PROC_FOLDER)
            .join(class.as_ref())
            .join(worker.name())
            .join(WORK_FOLDER);
        self.stop_worker(work_path, signal).await?;
        Ok(())
    }

    async fn stop_processes(&self, signal: Signal) -> Result<(), RuntimeError> {
        let mut system = sysinfo::System::new_all();
        system.refresh_all();
        let pid = process::id();
        if pid > 0 {
            let workers: Vec<_> = system
                .processes()
                .values()
                .filter(|process| process.parent() == Some(Pid::from_u32(pid)))
                .collect();
            for worker in &workers {
                self.stop_process(worker.pid().as_u32() as i32, signal)
                    .await?;
            }
        } else {
            warn!(
                "Current process does not have a positive pid ({}). Skipping processes termination request.",
                pid
            );
        }
        Ok(())
    }

    async fn stop_worker(&self, work_path: PathBuf, signal: Signal) -> Result<(), RuntimeError> {
        match check_status(get_pid_path(work_path.clone())) {
            WorkerStatus::Running { pid } => {
                let _ = self.stop_process(pid, signal).await;
            }
            _ => {
                warn!(
                    "Process at work path '{}' not running. Skipping signaling request.",
                    work_path.display()
                );
            }
        };
        Ok(())
    }
    async fn stop_process(&self, pid: i32, signal: Signal) -> Result<(), RuntimeError> {
        if pid > 0 {
            info!("Sending termination signal to process with pid: {}", pid);
            match terminate_process(pid, signal) {
                Ok(()) => info!(
                    "Successfully sent termination signal to process with pid: {}",
                    pid
                ),
                Err(e) => error!(
                    "Failed to send termination signal to process with pid: {}: {}",
                    pid, e
                ),
            }
        } else {
            warn!(
                "Process does not have a positive pid ({}). Skipping processes termination request.",
                pid
            );
        };
        Ok(())
    }

    fn forward_parameters(
        &self,
        worker: WorkerConfig,
        class: WorkerClass,
        parent_work_folder: PathBuf,
        child_work_folder: PathBuf,
    ) -> Result<Vec<String>, RuntimeError> {
        let extra_arguments = parse_extra_arguments(self.params.arguments.clone()).unwrap();
        let common_arguments: &mut HashMap<String, String> =
            if let Some(arguments) = extra_arguments.get(TD_ARGUMENT_KEY) {
                &mut arguments.clone()
            } else {
                &mut HashMap::new()
            };

        for (key, value) in [
            (Instance.as_ref(), self.params.instance()),
            (Repository.as_ref(), self.params.repository()),
            (Workspace.as_ref(), self.params.workspace()),
        ] {
            common_arguments.insert(
                key.to_string(),
                value.as_os_str().to_string_lossy().to_string(),
            );
        }

        for (key, value, folder) in [
            (Conf.as_ref(), self.params.config(), CONFIG_FOLDER),
            (Work.as_ref(), self.params.work(), WORK_FOLDER),
        ] {
            common_arguments.insert(
                key.to_string(),
                value
                    .join(PROC_FOLDER)
                    .join(class.as_ref())
                    .join(worker.name())
                    .join(folder)
                    .as_os_str()
                    .to_string_lossy()
                    .to_string(),
            );
        }

        let program_arguments: &mut HashMap<String, String> =
            if let Some(arguments) = extra_arguments.get(worker.name()) {
                &mut arguments.clone()
            } else {
                &mut HashMap::new()
            };

        let mut parameters = Vec::new();

        for key in worker.inherit() {
            match key.parse::<InheritedArgumentKey>() {
                Ok(_) => {
                    if let Some(value) = common_arguments.get(key) {
                        parameters.push(format!("--{key}"));
                        parameters.push(value.to_string());
                    }
                }
                Err(e) => {
                    error!("Unrecognized inherited argument: {}", e);
                }
            }
        }

        for (key, value) in worker.parameters() {
            parameters.push(format!("--{key}"));
            parameters.push(
                render(value)
                    .map_err(|error| RuntimeError::new(error.to_string()))?
                    .to_string(),
            );
        }

        for key in worker.arguments() {
            match key.parse::<ArgumentKey>() {
                Ok(argument_key) => {
                    parameters.push(format!("--{key}"));
                    parameters.push(
                        argument_key
                            .produce(
                                get_instance_path_for_instance(&self.params.instance.clone()),
                                parent_work_folder.clone(),
                                child_work_folder.clone(),
                            )
                            .unwrap(),
                    );
                }
                Err(e) => {
                    error!("Unrecognized argument '{}': {}", key, e);
                }
            }
        }

        for (key, value) in program_arguments.iter() {
            parameters.push(format!("--{key}"));
            parameters.push(value.to_string());
        }

        if let SUPERVISOR = worker.kind() {
            parameters.push(TRAILING_ARGUMENTS_PREFIX.to_string());
            parameters.extend(self.params.clone().arguments);
        }
        Ok(parameters)
    }
}

fn setup(arguments: Arguments) -> (Option<PathBuf>, PathBuf) {
    let check_and_join = |option: Option<PathBuf>, profile: bool| {
        option.and_then(|mut p| {
            if profile {
                p = p.join(WORKSPACE_FOLDER).join(CONFIG_FOLDER);
            }
            let full_path = p.join(CONFIG_FILE);
            full_path.exists().then_some(full_path)
        })
    };

    let instance_dir = get_instance_path_for_instance(&Some(arguments.instance()));
    let instance_dir_absolute = to_absolute(&instance_dir.clone()).unwrap();

    let mut config = check_and_join(Some(instance_dir_absolute.clone()), true)
        .or_else(|| check_and_join(arguments.profile.clone(), true));

    if config.is_none() {
        let profile_folder = tempdir().unwrap_or_else(|e| {
            error!("Failed to create temporary profile config folder: {}", e);
            exit(GeneralError.code());
        });
        let persistent_profile_folder = profile_folder.keep();
        let profile_config = match extract_profile_config(persistent_profile_folder) {
            Ok(config) => match config {
                Some(config_file) => config_file,
                None => {
                    error!("No profile config yaml file");
                    exit(GeneralError.code());
                }
            },
            Err(e) => {
                error!("Failed to extract profile config yaml file: {}", e);
                exit(GeneralError.code());
            }
        };
        config = Some(profile_config);
    }

    let repository_dir = get_repository_path_for_instance(&Some(instance_dir_absolute.clone()));
    let repository_dir_absolute = to_absolute(&repository_dir.clone()).unwrap();

    let workspace_dir = get_workspace_path_for_instance(&Some(instance_dir_absolute.clone()));
    let workspace_dir_absolute = to_absolute(&workspace_dir.clone()).unwrap();

    let config_dir = &workspace_dir_absolute.join(CONFIG_FOLDER);
    let config_dir_absolute = to_absolute(&config_dir.clone()).unwrap();

    let work_dir = workspace_dir_absolute.join(WORK_FOLDER);
    let work_dir_absolute = to_absolute(&work_dir.clone()).unwrap();

    create_dir_all(instance_dir_absolute.clone()).expect("Failed to create instance folder '{}'");
    create_dir_all(repository_dir_absolute.clone())
        .expect("Failed to create repository folder '{}'");
    create_dir_all(workspace_dir_absolute.clone()).expect("Failed to create workspace folder '{}'");
    create_dir_all(config_dir_absolute.clone()).expect("Failed to create config folder '{}'");
    create_dir_all(work_dir_absolute.clone()).expect("Failed to create work folder '{}'");

    // These environment variables are meant to be used as URI locations. Therefore, in Windows they will have a
    // leading slash (/), resulting in, for example, '/c:\folder\file' instead of 'c:\folder\file'
    // Setting env vars is not thread-safe; use with care.
    unsafe {
        set_var(
            INSTANCE_URI_ENV,
            prepend_slash(instance_dir_absolute.clone()),
        );
    }
    // Setting env vars is not thread-safe; use with care.
    unsafe {
        set_var(
            REPOSITORY_URI_ENV,
            prepend_slash(repository_dir_absolute.clone()),
        );
    }
    // Setting env vars is not thread-safe; use with care.
    unsafe {
        set_var(
            WORKSPACE_URI_ENV,
            prepend_slash(workspace_dir_absolute.clone()),
        );
    }
    // Setting env vars is not thread-safe; use with care.
    unsafe {
        set_var(CONFIG_URI_ENV, prepend_slash(config_dir_absolute.clone()));
    }
    // Setting env vars is not thread-safe; use with care.
    unsafe {
        set_var(WORK_URI_ENV, prepend_slash(work_dir_absolute.clone()));
    }

    // These environment variables are meant to be used as regular PATH locations.
    // Setting env vars is not thread-safe; use with care.
    unsafe {
        set_var(
            INSTANCE_PATH_ENV,
            OsString::from(
                instance_dir_absolute
                    .clone()
                    .to_slash()
                    .unwrap_or_else(|| {
                        panic!("Invalid characters in instance path: {instance_dir_absolute:?}")
                    })
                    .into_owned(),
            ),
        );
    }
    // Setting env vars is not thread-safe; use with care.
    unsafe {
        set_var(
            REPOSITORY_PATH_ENV,
            OsString::from(
                repository_dir_absolute
                    .clone()
                    .to_slash()
                    .unwrap_or_else(|| {
                        panic!("Invalid characters in repository path: {repository_dir_absolute:?}")
                    })
                    .into_owned(),
            ),
        );
    }
    // Setting env vars is not thread-safe; use with care.
    unsafe {
        set_var(
            WORKSPACE_PATH_ENV,
            OsString::from(
                workspace_dir_absolute
                    .clone()
                    .to_slash()
                    .unwrap_or_else(|| {
                        panic!("Invalid characters in workspace path: {work_dir_absolute:?}")
                    })
                    .into_owned(),
            ),
        );
    }
    // Setting env vars is not thread-safe; use with care.
    unsafe {
        set_var(
            CONFIG_PATH_ENV,
            OsString::from(
                config_dir_absolute
                    .clone()
                    .to_slash()
                    .unwrap_or_else(|| {
                        panic!("Invalid characters in config path: {config_dir_absolute:?}")
                    })
                    .into_owned(),
            ),
        );
    }
    // Setting env vars is not thread-safe; use with care.
    unsafe {
        set_var(
            WORK_PATH_ENV,
            OsString::from(
                work_dir_absolute
                    .clone()
                    .to_slash()
                    .unwrap_or_else(|| {
                        panic!("Invalid characters in work path: {work_dir_absolute:?}")
                    })
                    .into_owned(),
            ),
        );
    }

    prepare(&instance_dir_absolute, true);

    (
        config.and_then(|file| file.parent().map(|folder| folder.to_path_buf())),
        instance_dir_absolute,
    )
}

// It is ok to unwrap as the Supervisor can fail abruptly if paths contain invalid characters.
pub fn prepend_slash(path: PathBuf) -> String {
    #[cfg(target_os = "windows")]
    {
        let mut new_path = String::new();
        new_path.push('/');
        new_path.push_str(path.to_str().unwrap());
        new_path
    }
    #[cfg(not(target_os = "windows"))]
    path.to_str().unwrap().to_string()
}

pub fn start() {
    let arguments = Arguments::parse();
    let (config_folder, instance_folder) = setup(arguments.clone());
    Cli::<Configuration, Arguments>::exec_async(
        arguments.name().as_str(),
        |config, params| async move {
            let result = Supervisor::new(config, params).run().await;
            if let Err(e) = result {
                error!("Supervisor worker failed: {}", e);
                error!("Leaving with exit code: {}", TabsDataError.code());
                return TabsDataError;
            }
            Success
        },
        config_folder,
        Some(instance_folder),
    );
}
