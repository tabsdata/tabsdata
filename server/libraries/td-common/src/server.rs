//
// Copyright 2024 Tabs Data Inc.
//

//! This module provides utilities that dedicated servers or workers can use to interact in several ways with their main
//! supervisor.

use crate::env::get_current_dir;
use crate::execution_status::WorkerCallbackStatus;
use crate::files::{LOCK_EXTENSION, YAML_EXTENSION, get_files_in_folder_sorted_by_name};
use crate::logging::LOG_LOCATION;
use crate::manifest::{Inf, WORKER_INF_FILE};
use crate::server::EtcError::EtcStoreLocationCreationError;
use crate::server::QueueError::{
    MessageAlreadyExisting, MessageNonExisting, QueuePlannedCreationError, QueueRootCreationError,
};
use crate::server::SupervisorMessagePayload::{
    SupervisorExceptionMessagePayload, SupervisorRequestMessagePayload,
    SupervisorResponseMessagePayload,
};
use crate::status::ExitStatus;
use async_trait::async_trait;
use chrono::Utc;
use const_format::concatcp;
use derive_builder::Builder;
use derive_new::new;
use getset::{Getters, Setters};
use http::Method;
use pico_args::Arguments;
use regex::Regex;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_yaml::{Mapping, Value};
use std::collections::HashMap;
use std::fmt;
use std::fmt::Formatter;
use std::fs::{File, create_dir_all, read_dir, remove_file, rename};
use std::io::{Error, Write};
use std::marker::PhantomData;
use std::option::Option;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use strum::{AsRefStr, Display, EnumString};
use td_error::td_error;
use tokio::{fs, io};
use tracing::{error, warn};
use url::Url;
use utoipa::ToSchema;

pub const AVAILABLE_ENVIRONMENTS_FOLDER: &str = "available_environments";
pub const ENVIRONMENTS_FOLDER: &str = "environments";

pub const DEFAULT_INSTANCE: &str = "tabsdata";

pub const CURRENT_FOLDER: &str = ".";
pub const PARENT_FOLDER: &str = "..";

pub const INSTANCES_FOLDER: &str = "instances";

pub const WORKSPACE_FOLDER: &str = "workspace";
pub const REPOSITORY_FOLDER: &str = "repository";

pub const MOLD_FOLDER: &str = "mold";
pub const LOG_FOLDER: &str = LOG_LOCATION;
pub const LOCK_FOLDER: &str = "lock";
pub const PROC_FOLDER: &str = "proc";
pub const INIT_FOLDER: &str = "init";
pub const REGULAR_FOLDER: &str = "regular";
pub const EPHEMERAL_FOLDER: &str = "ephemeral";
pub const CAST_FOLDER: &str = "cast";
pub const BIN_FOLDER: &str = "bin";
pub const REQUEST_FOLDER: &str = "request";
pub const RESPONSE_FOLDER: &str = "response";
pub const INPUT_FOLDER: &str = "input";
pub const OUTPUT_FOLDER: &str = "output";

pub const CONFIG_NAMESPACE: &str = "TD";

pub const INSTANCE_FOLDER: &str = "instance";

pub const CONFIG_FOLDER: &str = "config";
pub const WORK_FOLDER: &str = "work";

pub const DATABASE_FOLDER: &str = "database";
pub const DATABASE_FILE: &str = "tabsdata.db";

pub const STORAGE_FOLDER: &str = "storage";

pub const SSL_KEY_PEM_FILE: &str = "key.pem";
pub const SSL_CERT_PEM_FILE: &str = "cert.pem";

pub const CONFIG_FILE_STEM: &str = "config";
pub const CONFIG_FILE: &str = "config.yaml";

pub const REQUEST_FILE: &str = "request.yaml";
pub const RESPONSE_FILE: &str = "response.yaml";
pub const EXCEPTION_FILE: &str = "exception.yaml";

pub const WORKER_PID_FILE: &str = "pid";
pub const WORKER_OUT_FILE: &str = "out.log";
pub const WORKER_ERR_FILE: &str = "err.log";

pub const ERR_LOG_FILE_NAME: &str = "err";
pub const FN_LOG_FILE_NAME: &str = "fn";
pub const OUT_LOG_FILE_NAME: &str = "out";
pub const TD_LOG_FILE_NAME: &str = "td";

// These environment variables are meant to be used as URI locations. Therefore, in Windows they will have a
// leading slash (/), resulting in, for example, '/c:\folder\file' instead of 'c:\folder\file'
pub const INSTANCE_URI_ENV: &str = "TD_URI_INSTANCE";
pub const REPOSITORY_URI_ENV: &str = "TD_URI_REPOSITORY";
pub const WORKSPACE_URI_ENV: &str = "TD_URI_WORKSPACE";
pub const CONFIG_URI_ENV: &str = "TD_URI_CONFIG";
pub const WORK_URI_ENV: &str = "TD_URI_WORK";

// These environment variables are meant to be used as regular PATH locations.
pub const INSTANCE_PATH_ENV: &str = "TD_PATH_INSTANCE";
pub const REPOSITORY_PATH_ENV: &str = "TD_PATH_REPOSITORY";
pub const WORKSPACE_PATH_ENV: &str = "TD_PATH_WORKSPACE";
pub const CONFIG_PATH_ENV: &str = "TD_PATH_CONFIG";
pub const WORK_PATH_ENV: &str = "TD_PATH_WORK";

pub const QUEUE_PARAMETER: &str = "--msg";
pub const ETC_PARAMETER: &str = "--etc";

pub const EXCLUSION_PREFIX: char = '.';

pub const MSG_FOLDER: &str = "msg";

pub const PLANNED_FOLDER: &str = "planned";
pub const QUEUED_FOLDER: &str = "queued";
pub const ONGOING_FOLDER: &str = "ongoing";
pub const COMPLETE_FOLDER: &str = "complete";

pub const ERROR_FOLDER: &str = "error";
pub const FAIL_FOLDER: &str = "fail";

pub const UNKNOWN_RUN: u16 = 0;
pub const INITIAL_RUN: u16 = 1;

pub const RETRIES_DELIMITER: &str = "_";
pub const INITIAL_CALL: &str = concatcp!(RETRIES_DELIMITER, INITIAL_RUN);

pub const REQUEST_MESSAGE_FILE_PATTERN: &str =
    concatcp!(r"^(.*)", RETRIES_DELIMITER, r"([1-9][0-9]*)(\.yaml$)");
pub const REQUEST_MESSAGE_FORMAT: &str = concatcp!("{}", RETRIES_DELIMITER, "{}", "{}");

pub const ETC_FOLDER: &str = "etc";

pub const TD_DETACHED_SUBPROCESSES: &str = "TD_DETACHED_SUBPROCESSES";

#[td_error]
pub enum QueueError {
    #[error("Error creating the queue folder '{queue}': {cause}")]
    QueueRootCreationError { queue: PathBuf, cause: Error },
    #[error("Error creating the queue planned subfolder '{queue}': {cause}")]
    QueuePlannedCreationError { queue: PathBuf, cause: Error },
    #[error("Message already exists: {id}")]
    MessageAlreadyExisting { id: String },
    #[error("Message does no exist: {id}")]
    MessageNonExisting { id: String },
    #[error("An IO error occurred serializing the message file: {0}")]
    SerdeError(#[from] serde_yaml::Error),
    #[error("An IO error occurred generating the message file: {0}")]
    IOError(#[from] Error),
}

#[derive(
    ToSchema, Default, Debug, Clone, Eq, PartialEq, Serialize, Deserialize, EnumString, AsRefStr,
)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub enum WorkerClass {
    INIT,
    REGULAR,
    #[default]
    EPHEMERAL,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, EnumString, AsRefStr)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub enum WorkerName {
    FUNCTION,
}

#[derive(
    ToSchema,
    Default,
    Debug,
    Clone,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    EnumString,
    Display,
    AsRefStr,
)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub enum MessageAction {
    #[default]
    Start,
    Stop,
    Notify,
}

#[derive(Debug, Clone, Eq, PartialEq, new, Setters, Serialize, Deserialize)]
pub struct SupervisorMessage<T = Value>
where
    T: Clone,
{
    pub id: String,
    pub work: String,
    pub file: PathBuf,
    pub payload: SupervisorMessagePayload<T>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum SupervisorMessagePayload<T = Value>
where
    T: Clone,
{
    SupervisorRequestMessagePayload(RequestMessagePayload<T>),
    SupervisorResponseMessagePayload(ResponseMessagePayload<T>),
    SupervisorExceptionMessagePayload(ExceptionMessagePayload<T>),
}

#[derive(Debug, Clone, Eq, PartialEq, Getters, Setters, Builder, Serialize, Deserialize)]
#[getset(get = "pub", set = "pub")]
#[builder(setter(into))]
pub struct RequestMessagePayload<T = Value>
where
    T: Clone,
{
    #[serde(default)]
    class: WorkerClass,
    worker: String,
    action: MessageAction,
    arguments: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    callback: Option<Callback>,
    #[serde(skip_serializing_if = "Option::is_none")]
    context: Option<T>,
}

impl<T> RequestMessagePayload<T>
where
    T: Clone,
{
    pub fn builder() -> RequestMessagePayloadBuilder<T> {
        RequestMessagePayloadBuilder::default()
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Getters, Setters, Builder, Serialize, Deserialize)]
#[getset(get = "pub", set = "pub")]
#[serde(tag = "")]
pub struct ExceptionMessagePayload<T = Value>
where
    T: Clone,
{
    #[serde(skip)]
    _type: PhantomData<T>,
    kind: Option<String>,
    message: Option<String>,
    error_code: Option<String>,
    #[serde(default = "default_exit_status")]
    exit_status: i32,
}

fn default_exit_status() -> i32 {
    ExitStatus::Success.code()
}

impl ExceptionMessagePayload {
    pub fn builder() -> ExceptionMessagePayloadBuilder {
        ExceptionMessagePayloadBuilder::default()
    }
}

impl<T> Default for ExceptionMessagePayload<T>
where
    T: Clone + Default,
{
    fn default() -> Self {
        Self {
            _type: PhantomData::<T>,
            kind: None,
            message: None,
            error_code: None,
            exit_status: default_exit_status(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Builder, Serialize, Deserialize, ToSchema)]
pub struct ResponseMessagePayload<T = Value>
where
    T: Clone,
{
    #[serde(default = "default_id")]
    pub id: String,
    #[serde(default = "default_class")]
    pub class: WorkerClass,
    #[serde(default = "default_worker")]
    pub worker: String,
    #[serde(default = "default_action")]
    pub action: MessageAction,
    #[serde(default = "default_start")]
    pub start: i64,
    pub end: Option<i64>,
    #[serde(default = "default_status")]
    pub status: WorkerCallbackStatus,
    #[serde(default = "default_execution")]
    pub execution: i16,
    pub limit: Option<i16>,
    pub error: Option<String>,
    pub exception_kind: Option<String>,
    pub exception_message: Option<String>,
    pub exception_error_code: Option<String>,
    #[serde(default = "default_exit_status")]
    pub exit_status: i32,
    pub context: Option<T>,
}

fn default_id() -> String {
    String::new()
}

fn default_class() -> WorkerClass {
    WorkerClass::EPHEMERAL
}

fn default_worker() -> String {
    String::new()
}

fn default_action() -> MessageAction {
    MessageAction::Notify
}

fn default_start() -> i64 {
    Utc::now().timestamp()
}

fn default_status() -> WorkerCallbackStatus {
    WorkerCallbackStatus::Done
}

fn default_execution() -> i16 {
    INITIAL_RUN as i16
}

impl ResponseMessagePayload {
    pub fn builder() -> ResponseMessagePayloadBuilder {
        ResponseMessagePayloadBuilder::default()
    }
}

impl<T> Default for ResponseMessagePayload<T>
where
    T: Clone,
{
    fn default() -> Self {
        Self {
            id: default_id(),
            class: default_class(),
            worker: default_worker(),
            action: default_action(),
            start: default_start(),
            end: None,
            status: default_status(),
            execution: default_execution(),
            limit: None,
            error: None,
            exception_kind: None,
            exception_message: None,
            exception_error_code: None,
            exit_status: default_exit_status(),
            context: None,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum Callback {
    Http(HttpCallback),
}

#[derive(Debug, Clone, Eq, PartialEq, Getters, Setters, Builder, Serialize, Deserialize)]
#[getset(get = "pub")]
#[builder(setter(into))]
pub struct HttpCallback {
    url: Url,
    #[serde(serialize_with = "method_serialize")]
    #[serde(deserialize_with = "method_deserialize")]
    method: Method,
    headers: HashMap<String, String>,
    body: bool,
}

fn method_serialize<S>(method: &Method, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(method.as_str())
}

fn method_deserialize<'de, D>(deserializer: D) -> Result<Method, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    Method::from_str(&s).map_err(serde::de::Error::custom)
}

impl<T> RequestMessagePayload<T>
where
    T: Clone + Default,
{
    pub fn new(
        class: WorkerClass,
        worker: String,
        action: MessageAction,
        arguments: Vec<String>,
        callback: Option<Callback>,
        context: Option<T>,
    ) -> Self {
        RequestMessagePayload {
            class,
            worker,
            action,
            arguments,
            callback,
            context,
        }
    }
}

#[derive(Debug, Getters)]
#[getset(get = "pub")]
pub struct FileWorkerMessageQueue {
    location: PathBuf,
}

pub enum PayloadType {
    Request,
    Response,
    Exception,
}

impl<T> TryFrom<(PathBuf, PayloadType)> for SupervisorMessage<T>
where
    T: Clone + DeserializeOwned,
{
    type Error = Error;

    fn try_from(parameters: (PathBuf, PayloadType)) -> Result<Self, Self::Error> {
        fn strip_tags(value: Value) -> Value {
            match value {
                Value::Tagged(tagged) => {
                    let tag = tagged.tag.to_string().trim_start_matches('!').to_string();
                    let inner = strip_tags(tagged.value);
                    let mut map = Mapping::new();
                    map.insert(Value::String(tag), inner);
                    Value::Mapping(map)
                }
                Value::Sequence(seq) => Value::Sequence(seq.into_iter().map(strip_tags).collect()),
                Value::Mapping(mapping) => {
                    let map = mapping
                        .into_iter()
                        .map(|(k, v)| (strip_tags(k), strip_tags(v)))
                        .collect::<Mapping>();
                    Value::Mapping(map)
                }
                other => other,
            }
        }

        let (message, payload_type) = parameters;
        let file = File::open(&message)?;
        let payload = match payload_type {
            PayloadType::Request => {
                let request_payload: RequestMessagePayload<T> = serde_yaml::from_reader(&file)
                    .map_err(|e| {
                        Error::new(
                            io::ErrorKind::InvalidData,
                            format!("Error deserializing request message {message:?}: {e}"),
                        )
                    })?;
                SupervisorRequestMessagePayload(request_payload)
            }
            PayloadType::Response => {
                let response_context_value: Value =
                    serde_yaml::from_reader(&file).map_err(|e| {
                        Error::new(
                            io::ErrorKind::InvalidData,
                            format!("Error parsing response message {message:?}: {e}"),
                        )
                    })?;
                let response_context_value_cleaned = strip_tags(response_context_value);
                let response_context: T = serde_yaml::from_value(response_context_value_cleaned)
                    .map_err(|e| {
                        Error::new(
                            io::ErrorKind::InvalidData,
                            format!("Error deserializing response message {message:?}: {e}"),
                        )
                    })?;
                let response_payload = ResponseMessagePayload {
                    context: Some(response_context),
                    ..Default::default()
                };
                SupervisorResponseMessagePayload(response_payload)
            }
            PayloadType::Exception => {
                let exception_payload: ExceptionMessagePayload<T> = serde_yaml::from_reader(&file)
                    .map_err(|e| {
                        Error::new(
                            io::ErrorKind::InvalidData,
                            format!("Error deserializing exception message {message:?}: {e}"),
                        )
                    })?;
                SupervisorExceptionMessagePayload(exception_payload)
            }
        };
        let work = if let Some(file_stem) = message.file_stem() {
            file_stem.to_string_lossy().to_string()
        } else {
            return Err(Error::new(io::ErrorKind::InvalidInput, "Invalid file name"));
        };
        let id = base(&work);
        Ok(SupervisorMessage::new(id, work, message, payload))
    }
}

impl FileWorkerMessageQueue {
    /// Creates a queue instance to send processing message to the supervisor.
    /// This queue is thread-safe and reusable.
    pub async fn new() -> Result<Self, QueueError> {
        // Infers the queue base location.
        fn obtain_queue_location() -> PathBuf {
            if let Some(queue) = obtain_queue_location_from_info_file() {
                queue
            } else if let Some(queue) = obtain_queue_location_from_arguments() {
                queue
            } else {
                obtain_queue_location_from_current_dir().unwrap()
            }
        }

        // Gets base location form standard inf file.
        fn obtain_queue_location_from_info_file() -> Option<PathBuf> {
            let inf_path = get_current_dir().join(WORKER_INF_FILE);
            if inf_path.exists()
                && let Ok(inf_file) = File::open(&inf_path)
                && let Ok(inf) = serde_yaml::from_reader::<_, Inf>(inf_file)
            {
                return Some(inf.queue);
            }
            None
        }

        // Gets base location form passed arguments.
        pub fn obtain_queue_location_from_arguments() -> Option<PathBuf> {
            let mut arguments = Arguments::from_env();
            let queue: Option<PathBuf> = arguments
                .opt_value_from_str(QUEUE_PARAMETER)
                .unwrap_or(None);
            let _ = arguments.finish();
            queue
        }

        // Gets base location form current folder.
        pub fn obtain_queue_location_from_current_dir() -> Option<PathBuf> {
            Some(get_current_dir().join(MSG_FOLDER))
        }

        let root = obtain_queue_location();

        if let Err(e) = create_dir_all(root.clone()) {
            return Err(QueueRootCreationError {
                queue: root,
                cause: e,
            });
        };

        let location = root.clone().join(PLANNED_FOLDER);

        if let Err(e) = create_dir_all(location.clone()) {
            return Err(QueuePlannedCreationError {
                queue: location.clone(),
                cause: e,
            });
        };

        Ok(Self { location })
    }

    // Check if some message is already existing, in any of its possible modalities.
    fn check(&self, id: &str) -> bool {
        let pattern = format!(
            r"^{}{}([1-9][0-9]*)\.(yaml|lock)$",
            regex::escape(id),
            RETRIES_DELIMITER
        );
        let regex = Regex::new(&pattern).unwrap();
        if let Ok(entries) = read_dir(&self.location) {
            for entry in entries.flatten() {
                if let Some(file_name) = entry.file_name().to_str()
                    && regex.is_match(file_name)
                {
                    return true;
                }
            }
        }
        false
    }

    #[cfg(feature = "test-utils")]
    pub fn with_location(location: impl Into<PathBuf>) -> Result<Self, QueueError> {
        Ok(Self {
            location: location.into(),
        })
    }
}

#[cfg(feature = "test-utils")]
impl Default for FileWorkerMessageQueue {
    fn default() -> Self {
        let test_dir = testdir::testdir!();
        Self::with_location(test_dir).unwrap()
    }
}

#[async_trait]
pub trait WorkerMessageQueue: Send + Sync + Sized + 'static {
    /// Puts a message in the queue.
    async fn put<T: Serialize + Clone + Send + Sync>(
        &self,
        id: String,
        payload: RequestMessagePayload<T>,
    ) -> Result<SupervisorMessage<T>, QueueError>;

    /// Commits a message in the queue.
    async fn commit(&self, id: &str) -> Result<(), QueueError>;

    /// Rollbacks a message in the queue.
    async fn rollback(&self, id: &str) -> Result<(), QueueError>;

    async fn locked_messages<T: DeserializeOwned + Clone + Send + Sync>(
        &self,
    ) -> Vec<SupervisorMessage<T>>;
}

#[async_trait]
impl WorkerMessageQueue for FileWorkerMessageQueue {
    async fn put<T: Serialize + Clone + Send + Sync>(
        &self,
        id: String,
        payload: RequestMessagePayload<T>,
    ) -> Result<SupervisorMessage<T>, QueueError> {
        if self.check(&id) {
            return Err(MessageAlreadyExisting { id });
        };
        let work = format!("{id}{INITIAL_CALL}");
        let file = format!("{work}.{LOCK_EXTENSION}");
        let message_path = self.location.join(file);
        let mut message_file = File::create(message_path.clone())?;
        let message_yaml = serde_yaml::to_string(&payload)?;
        message_file.write_all(message_yaml.as_bytes())?;
        let message = SupervisorMessage::new(
            id,
            work,
            message_path,
            SupervisorRequestMessagePayload(payload),
        );
        Ok(message)
    }

    async fn commit(&self, id: &str) -> Result<(), QueueError> {
        if !self.check(id) {
            return Err(MessageNonExisting { id: id.to_string() });
        };
        let lock_message_path = self
            .location
            .join(format!("{id}{INITIAL_CALL}.{LOCK_EXTENSION}"));
        let yaml_message_path = self
            .location
            .join(format!("{id}{INITIAL_CALL}.{YAML_EXTENSION}"));
        rename(&lock_message_path, &yaml_message_path)?;
        Ok(())
    }

    async fn rollback(&self, id: &str) -> Result<(), QueueError> {
        if !self.check(id) {
            return Err(MessageNonExisting { id: id.to_string() });
        };
        let lock_message_path = self
            .location
            .join(format!("{id}{INITIAL_CALL}.{LOCK_EXTENSION}"));
        remove_file(&lock_message_path)?;
        Ok(())
    }

    async fn locked_messages<T: DeserializeOwned + Clone + Send + Sync>(
        &self,
    ) -> Vec<SupervisorMessage<T>> {
        get_files_in_folder_sorted_by_name(&self.location, Some(LOCK_EXTENSION))
            .unwrap_or_else(|_| Vec::new())
            .into_iter()
            .filter_map(|file| {
                match SupervisorMessage::<T>::try_from((file.clone(), PayloadType::Request)) {
                    Ok(msg) => Some(msg),
                    Err(e) => {
                        error!("Failed to extract message from file {:?}: {:?}", file, e);
                        None
                    }
                }
            })
            .collect()
    }
}

pub fn base(stem: &str) -> String {
    stem.split(RETRIES_DELIMITER)
        .next()
        .unwrap_or(stem)
        .to_string()
}

pub fn counter(path: &Path) -> String {
    if let Some(stem) = path.file_stem() {
        stem.to_string_lossy()
            .to_string()
            .split_once(RETRIES_DELIMITER)
            .and_then(|(_, counter)| counter.parse::<u32>().ok())
            .unwrap_or(0)
            .to_string()
    } else {
        0.to_string()
    }
}

#[cfg(test)]
mod tests_queue {
    use super::*;

    #[tokio::test]
    async fn test_put_and_commit_message() {
        let queue = FileWorkerMessageQueue::new().await.unwrap();

        let id = "test_message1";
        let payload = RequestMessagePayload::<Value> {
            class: WorkerClass::REGULAR,
            worker: String::from("worker1"),
            action: MessageAction::Start,
            arguments: vec![String::from("arg1")],
            callback: None,
            context: Some(Value::Null),
        };

        let lock_message_path = get_current_dir()
            .join(MSG_FOLDER)
            .join(format!("planned/{id}{INITIAL_CALL}.lock"));
        let yaml_message_path = get_current_dir()
            .join(MSG_FOLDER)
            .join(format!("planned/{id}{INITIAL_CALL}.yaml"));

        assert!(
            !lock_message_path.exists(),
            "File '.lock' exists and it shouldn't"
        );
        assert!(
            !yaml_message_path.exists(),
            "File '.yaml' exists and it shouldn't"
        );

        let message = queue.put(id.to_string(), payload.clone()).await.unwrap();

        assert!(
            lock_message_path.exists(),
            "File '.lock' does not exist and it should"
        );
        assert!(
            !yaml_message_path.exists(),
            "File '.yaml' exists and it shouldn't"
        );

        queue.commit(&message.id).await.unwrap();

        assert!(
            !lock_message_path.exists(),
            "File '.lock' exists and it shouldn't"
        );
        assert!(
            yaml_message_path.exists(),
            "File '.yaml' does not exist and it should"
        );
    }

    #[tokio::test]
    async fn test_put_and_rollback_message() {
        let queue = FileWorkerMessageQueue::new().await.unwrap();

        let id = "test_message2";
        let payload = RequestMessagePayload::<Value> {
            class: WorkerClass::REGULAR,
            worker: String::from("worker2"),
            action: MessageAction::Start,
            arguments: vec![String::from("arg2")],
            callback: None,
            context: Some(Value::Null),
        };

        let lock_message_path = get_current_dir()
            .join(MSG_FOLDER)
            .join(format!("planned/{id}{INITIAL_CALL}.lock"));
        let yaml_message_path = get_current_dir()
            .join(MSG_FOLDER)
            .join(format!("planned/{id}{INITIAL_CALL}.yaml"));

        assert!(
            !lock_message_path.exists(),
            "File '.lock' exists and it shouldn't"
        );
        assert!(
            !yaml_message_path.exists(),
            "File '.yaml' exists and it shouldn't"
        );

        let message = queue.put(id.to_string(), payload.clone()).await.unwrap();

        assert!(
            lock_message_path.exists(),
            "File '.lock' does not exist and it should"
        );
        assert!(
            !yaml_message_path.exists(),
            "File '.yaml' exists and it shouldn't"
        );

        queue.rollback(&message.id).await.unwrap();

        assert!(
            !lock_message_path.exists(),
            "File '.lock' exists and it shouldn't"
        );
        assert!(
            !yaml_message_path.exists(),
            "File '.yaml' exists and it shouldn't"
        );
    }

    #[tokio::test]
    async fn test_put_existing_message() {
        let queue = FileWorkerMessageQueue::new().await.unwrap();

        let id = "test_message3";
        let payload = RequestMessagePayload::<Value> {
            class: WorkerClass::REGULAR,
            worker: String::from("worker3"),
            action: MessageAction::Start,
            arguments: vec![String::from("arg3")],
            callback: None,
            context: None,
        };

        let _ = queue.put(id.to_string(), payload.clone()).await.unwrap();

        let result = queue.put(id.to_string(), payload.clone()).await;

        assert!(matches!(result, Err(MessageAlreadyExisting { .. })));
    }

    #[tokio::test]
    async fn test_commit_non_existing_message() {
        let queue = FileWorkerMessageQueue::new().await.unwrap();
        let message = "test_message4";
        let result = queue.commit(message).await;
        assert!(matches!(result, Err(MessageNonExisting { .. })));
    }

    #[tokio::test]
    async fn test_rollback_non_existing_message() {
        let queue = FileWorkerMessageQueue::new().await.unwrap();
        let message = "test_message5";
        let result = queue.commit(message).await;
        assert!(matches!(result, Err(MessageNonExisting { .. })));
    }

    #[test]
    fn test_valid_counter() {
        let path = PathBuf::from("/a/b/work_3");
        assert_eq!(counter(&path), "3");
    }

    #[test]
    fn test_no_counter() {
        let path = PathBuf::from("/a/b/work");
        assert_eq!(counter(&path), "0");
    }

    #[test]
    fn test_non_numeric_counter() {
        let path = PathBuf::from("/a/b/work_xyz");
        assert_eq!(counter(&path), "0");
    }

    #[test]
    fn test_empty_stem() {
        let path = PathBuf::from("");
        assert_eq!(counter(&path), "0");
    }

    #[test]
    fn test_multiple_underscores() {
        let path = PathBuf::from("/a/b/run_work_42");
        assert_eq!(counter(&path), "0");
    }

    #[test]
    fn test_valid_counter_extension() {
        let path = PathBuf::from("/a/b/work_3.t");
        assert_eq!(counter(&path), "3");
    }

    #[test]
    fn test_no_counter_extension() {
        let path = PathBuf::from("/a/b/work.t");
        assert_eq!(counter(&path), "0");
    }

    #[test]
    fn test_non_numeric_counter_extension() {
        let path = PathBuf::from("/a/b/work_xyz.t");
        assert_eq!(counter(&path), "0");
    }

    #[test]
    fn test_empty_stem_extension() {
        let path = PathBuf::from(".t");
        assert_eq!(counter(&path), "0");
    }

    #[test]
    fn test_multiple_underscores_extension() {
        let path = PathBuf::from("/a/b/run_work_42.t");
        assert_eq!(counter(&path), "0");
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(non_camel_case_types)]
pub enum EtcContent {
    ServerVersion_yaml,
    AllowedPythonVersions_yaml,
    AvailablePythonVersions_yaml,
    ValidPythonVersions_yaml,
    ServerBuildManifest_yaml,
}

impl EtcContent {
    fn file(&self) -> &'static str {
        match self {
            EtcContent::ServerVersion_yaml => "server-version.yaml",
            EtcContent::AllowedPythonVersions_yaml => "allowed-python-versions.yaml",
            EtcContent::AvailablePythonVersions_yaml => "available-python-versions.yaml",
            EtcContent::ValidPythonVersions_yaml => "valid-python-versions.yaml",
            EtcContent::ServerBuildManifest_yaml => "server-build-manifest.yaml",
        }
    }
}

impl fmt::Display for EtcContent {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let name = match self {
            EtcContent::ServerVersion_yaml => "Server Version",
            EtcContent::AllowedPythonVersions_yaml => "Allowed Python Versions",
            EtcContent::AvailablePythonVersions_yaml => "Available Python Versions",
            EtcContent::ValidPythonVersions_yaml => "Valid Python Versions",
            EtcContent::ServerBuildManifest_yaml => "Server Build Manifest",
        };
        write!(f, "{name}")
    }
}

#[td_error]
pub enum EtcError {
    #[error("Error creating the etc store instance '{location}': {cause}")]
    EtcStoreLocationCreationError { location: PathBuf, cause: Error },
    #[error("Error reading an etc store file '{location}'")]
    NotFoundError { location: PathBuf },
    #[error("Error reading an etc store file '{location}': {cause}")]
    ReadError { location: PathBuf, cause: Error },
}

#[derive(Debug, Getters)]
#[getset(get = "pub")]
pub struct FileEtcStore {
    location: PathBuf,
}

pub trait IntoEtcStoreBox {
    fn boxed(self) -> Box<dyn EtcStore + Send + Sync>;
}

impl<T> IntoEtcStoreBox for T
where
    T: EtcStore + Send + Sync + 'static,
{
    fn boxed(self) -> Box<dyn EtcStore + Send + Sync> {
        Box::new(self)
    }
}

impl FileEtcStore {
    /// Creates an etc store instance to read and write interchange files.
    pub async fn new() -> Result<Self, EtcError> {
        // Infers the etc store base location.
        fn obtain_etc_location() -> PathBuf {
            if let Some(location) = obtain_etc_location_from_info_file() {
                location
            } else if let Some(location) = obtain_etc_location_from_arguments() {
                location
            } else {
                obtain_etc_location_from_current_dir().unwrap()
            }
        }

        // Gets the base etc store base location form standard inf file.
        fn obtain_etc_location_from_info_file() -> Option<PathBuf> {
            let inf_path = get_current_dir().join(WORKER_INF_FILE);
            if inf_path.exists()
                && let Ok(inf_file) = File::open(&inf_path)
                && let Ok(inf) = serde_yaml::from_reader::<_, Inf>(inf_file)
            {
                return Some(inf.etc);
            }
            None
        }

        // Gets base etc store base location form passed arguments.
        pub fn obtain_etc_location_from_arguments() -> Option<PathBuf> {
            let mut arguments = Arguments::from_env();
            let location: Option<PathBuf> =
                arguments.opt_value_from_str(ETC_PARAMETER).unwrap_or(None);
            let _ = arguments.finish();
            location
        }

        // Gets base etc store base location form current folder.
        pub fn obtain_etc_location_from_current_dir() -> Option<PathBuf> {
            Some(get_current_dir().join(ETC_FOLDER))
        }

        let location = obtain_etc_location();

        if let Err(e) = create_dir_all(location.clone()) {
            return Err(EtcStoreLocationCreationError { location, cause: e });
        };

        Ok(Self { location })
    }

    #[cfg(feature = "td-test")]
    pub fn with_location(location: impl Into<PathBuf>) -> Result<Self, EtcError> {
        Ok(Self {
            location: location.into(),
        })
    }
}

pub async fn etc_service() -> Result<Box<dyn EtcStore + Send + Sync>, EtcError> {
    Ok(FileEtcStore::new().await?.boxed())
}

#[async_trait]
pub trait EtcStore {
    async fn read(&self, content: &EtcContent) -> Result<Option<Vec<u8>>, EtcError>;
}

#[async_trait]
impl EtcStore for FileEtcStore {
    async fn read(&self, content: &EtcContent) -> Result<Option<Vec<u8>>, EtcError> {
        let path = self.location().join(content.file());
        match fs::read(&path).await {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                warn!(
                    "File {} not found as an etc resource location: {}",
                    content.file(),
                    path.display()
                );
                Ok(None)
            }
            Err(e) => Err(EtcError::ReadError {
                location: path,
                cause: e,
            }),
        }
    }
}

#[cfg(test)]
mod tests_etc {
    use crate::server::{EtcContent, EtcStore, FileEtcStore};
    use std::path::Path;
    use testdir::testdir;

    #[tokio::test]
    async fn test_io_read() {
        let location = testdir!();
        let path = Path::new(&location).join(EtcContent::ServerBuildManifest_yaml.file());
        let content = "In a hole in the ground there lived a Hobbit.";
        std::fs::write(&path, content).expect("Failed to write the test file.");
        let etc_instance = FileEtcStore::with_location(&location);
        assert!(
            etc_instance.is_ok(),
            "Failed to create the etc store instance: {etc_instance:?}"
        );
        let result = etc_instance
            .unwrap()
            .read(&EtcContent::ServerBuildManifest_yaml)
            .await;
        assert!(
            result.is_ok(),
            "Failed to read from the etc store instance: {result:?}"
        );

        let result_bytes = result.unwrap();
        let result_string =
            String::from_utf8(result_bytes.unwrap()).expect("Failed to convert bytes to string");
        assert_eq!(result_string, content);
    }
}
