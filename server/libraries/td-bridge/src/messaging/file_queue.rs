//
// Copyright 2026 Tabsdata Inc.
//

//! The supervisor's file-based message queue: the writer side of the
//! planned/queued/ongoing/complete folder protocol the supervisor daemon
//! consumes, plus the message-file parsing both sides share.

use super::OrchestratorMessagePayload::{
    OrchestratorExceptionMessagePayload, OrchestratorRequestMessagePayload,
    OrchestratorResponseMessagePayload,
};
use super::QueueError::{
    MessageAlreadyExisting, MessageNonExisting, QueuePlannedCreationError, QueueRootCreationError,
};
use super::{
    OrchestratorMessage, OrchestratorMessagePayload, OrchestratorMessageQueue, QueueError,
};
use async_trait::async_trait;
use pico_args::Arguments;
use regex::Regex;
use serde::de::DeserializeOwned;
use serde_yaml::{Mapping, Value};
use std::fs::{File, create_dir_all, read_dir, remove_file, rename};
use std::io;
use std::io::{Error, Write};
use std::path::{Path, PathBuf};
use td_common::env::get_current_dir;
use td_common::files::{LOCK_EXTENSION, YAML_EXTENSION, get_files_in_folder_sorted_by_name};
use td_common::manifest::{Inf, WORKER_INF_FILE};
use td_common::worker::{
    ExceptionMessagePayload, INITIAL_CALL, RETRIES_DELIMITER, RequestMessagePayload,
    ResponseMessagePayload,
};
use tracing::{debug, error, trace};

/// The file queue's on-disk protocol: folder names shared by the writer (this
/// queue) and the reader (the supervisor daemon).
pub const MSG_FOLDER: &str = "msg";

pub const PLANNED_FOLDER: &str = "planned";
pub const QUEUED_FOLDER: &str = "queued";
pub const ONGOING_FOLDER: &str = "ongoing";
pub const COMPLETE_FOLDER: &str = "complete";

pub const STOP_FOLDER: &str = "stop";
pub const ERROR_FOLDER: &str = "error";
pub const FAIL_FOLDER: &str = "fail";

pub const QUEUE_PARAMETER: &str = "--msg";

#[derive(Debug)]
pub struct FileSupervisorMessageQueue {
    pub location: PathBuf,
}

pub enum PayloadType {
    Request,
    Response,
    Exception,
}

impl<T> TryFrom<(PathBuf, PayloadType)> for OrchestratorMessage<T>
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
                debug!(
                    variant = "Request",
                    class = ?request_payload.class,
                    worker = %request_payload.worker,
                    worker_id = %request_payload.worker_id,
                    action = %request_payload.action,
                    "queue.get"
                );
                OrchestratorRequestMessagePayload(request_payload)
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
                debug!(variant = "Response", "queue.get");
                OrchestratorResponseMessagePayload(response_payload)
            }
            PayloadType::Exception => {
                let exception_payload: ExceptionMessagePayload<T> = serde_yaml::from_reader(&file)
                    .map_err(|e| {
                        Error::new(
                            io::ErrorKind::InvalidData,
                            format!("Error deserializing exception message {message:?}: {e}"),
                        )
                    })?;
                debug!(
                    variant = "Exception",
                    exit_status = exception_payload.exit_status,
                    "queue.get"
                );
                OrchestratorExceptionMessagePayload(exception_payload)
            }
        };
        let work = if let Some(file_stem) = message.file_stem() {
            file_stem.to_string_lossy().to_string()
        } else {
            return Err(Error::new(io::ErrorKind::InvalidInput, "Invalid file name"));
        };
        let id = base(&work);
        Ok(OrchestratorMessage::new(id, work, message, payload))
    }
}

impl FileSupervisorMessageQueue {
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
        fn obtain_queue_location_from_arguments() -> Option<PathBuf> {
            let mut arguments = Arguments::from_env();
            let queue: Option<PathBuf> = arguments
                .opt_value_from_str(QUEUE_PARAMETER)
                .unwrap_or(None);
            let _ = arguments.finish();
            queue
        }

        // Gets base location form current folder.
        fn obtain_queue_location_from_current_dir() -> Option<PathBuf> {
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

    #[cfg(any(test, feature = "test-utils"))]
    pub fn with_location(location: impl Into<PathBuf>) -> Result<Self, QueueError> {
        Ok(Self {
            location: location.into(),
        })
    }
}

#[cfg(feature = "test-utils")]
impl Default for FileSupervisorMessageQueue {
    fn default() -> Self {
        let test_dir = td_common::tdtestdir!();
        Self::with_location(test_dir).unwrap()
    }
}

#[async_trait]
impl OrchestratorMessageQueue for FileSupervisorMessageQueue {
    async fn put(
        &self,
        id: String,
        payload: OrchestratorMessagePayload<Value>,
    ) -> Result<OrchestratorMessage<Value>, QueueError> {
        if self.check(&id) {
            return Err(MessageAlreadyExisting { id });
        };
        // The supervisor file queue only carries request messages; other kinds
        // (e.g. the kubernetes function request) are never written to files.
        let OrchestratorRequestMessagePayload(request) = &payload else {
            return Err(QueueError::BackendError {
                cause: format!(
                    "supervisor file queue only supports request messages, got: {payload:?}"
                ),
            });
        };
        let work = format!("{id}{INITIAL_CALL}");
        let file = format!("{work}.{LOCK_EXTENSION}");
        let message_path = self.location.join(file);
        let mut message_file = File::create(message_path.clone())?;
        let message_yaml = serde_yaml::to_string(request)?;
        message_file.write_all(message_yaml.as_bytes())?;
        debug!(
            id = %id,
            class = ?request.class,
            worker = %request.worker,
            worker_id = %request.worker_id,
            action = %request.action,
            payload_bytes = message_yaml.len(),
            "queue.put"
        );
        trace!(id = %id, body = %message_yaml, "queue.put: body");
        let message = OrchestratorMessage::new(id, work, message_path, payload);
        Ok(message)
    }

    async fn commit(&self, message: &OrchestratorMessage<Value>) -> Result<(), QueueError> {
        let id = message.id.as_str();
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

    async fn rollback(&self, message: &OrchestratorMessage<Value>) -> Result<(), QueueError> {
        let id = message.id.as_str();
        if !self.check(id) {
            return Err(MessageNonExisting { id: id.to_string() });
        };
        let lock_message_path = self
            .location
            .join(format!("{id}{INITIAL_CALL}.{LOCK_EXTENSION}"));
        remove_file(&lock_message_path)?;
        Ok(())
    }

    async fn locked_messages(&self) -> Vec<OrchestratorMessage<Value>> {
        get_files_in_folder_sorted_by_name(&self.location, Some(LOCK_EXTENSION))
            .unwrap_or_else(|_| Vec::new())
            .into_iter()
            .filter_map(|file| {
                match OrchestratorMessage::<Value>::try_from((file.clone(), PayloadType::Request)) {
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
    use td_common::id;
    use td_common::worker::{MessageAction, WorkerClass};

    #[tokio::test]
    async fn test_put_and_commit_message() {
        let queue = FileSupervisorMessageQueue::with_location(td_common::tdtestdir!()).unwrap();

        let id = "test_message1";
        let payload = RequestMessagePayload::<Value> {
            class: WorkerClass::Regular,
            worker: String::from("worker1"),
            worker_id: id::id().to_string(),
            action: MessageAction::Start,
            arguments: vec![String::from("arg1")],
            callback: None,
            context: Some(Value::Null),
        };

        let lock_message_path = queue.location.join(format!("{id}{INITIAL_CALL}.lock"));
        let yaml_message_path = queue.location.join(format!("{id}{INITIAL_CALL}.yaml"));

        assert!(
            !lock_message_path.exists(),
            "File '.lock' exists and it shouldn't"
        );
        assert!(
            !yaml_message_path.exists(),
            "File '.yaml' exists and it shouldn't"
        );

        let message = queue
            .put(
                id.to_string(),
                OrchestratorMessagePayload::OrchestratorRequestMessagePayload(payload.clone()),
            )
            .await
            .unwrap();

        assert!(
            lock_message_path.exists(),
            "File '.lock' does not exist and it should"
        );
        assert!(
            !yaml_message_path.exists(),
            "File '.yaml' exists and it shouldn't"
        );

        queue.commit(&message).await.unwrap();

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
        let queue = FileSupervisorMessageQueue::with_location(td_common::tdtestdir!()).unwrap();

        let id = "test_message2";
        let payload = RequestMessagePayload::<Value> {
            class: WorkerClass::Regular,
            worker: String::from("worker2"),
            worker_id: id::id().to_string(),
            action: MessageAction::Start,
            arguments: vec![String::from("arg2")],
            callback: None,
            context: Some(Value::Null),
        };

        let lock_message_path = queue.location.join(format!("{id}{INITIAL_CALL}.lock"));
        let yaml_message_path = queue.location.join(format!("{id}{INITIAL_CALL}.yaml"));

        assert!(
            !lock_message_path.exists(),
            "File '.lock' exists and it shouldn't"
        );
        assert!(
            !yaml_message_path.exists(),
            "File '.yaml' exists and it shouldn't"
        );

        let message = queue
            .put(
                id.to_string(),
                OrchestratorMessagePayload::OrchestratorRequestMessagePayload(payload.clone()),
            )
            .await
            .unwrap();

        assert!(
            lock_message_path.exists(),
            "File '.lock' does not exist and it should"
        );
        assert!(
            !yaml_message_path.exists(),
            "File '.yaml' exists and it shouldn't"
        );

        queue.rollback(&message).await.unwrap();

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
        let queue = FileSupervisorMessageQueue::with_location(td_common::tdtestdir!()).unwrap();

        let id = "test_message3";
        let payload = RequestMessagePayload::<Value> {
            class: WorkerClass::Regular,
            worker: String::from("worker3"),
            worker_id: id::id().to_string(),
            action: MessageAction::Start,
            arguments: vec![String::from("arg3")],
            callback: None,
            context: None,
        };

        let _ = queue
            .put(
                id.to_string(),
                OrchestratorMessagePayload::OrchestratorRequestMessagePayload(payload.clone()),
            )
            .await
            .unwrap();

        let result = queue
            .put(
                id.to_string(),
                OrchestratorMessagePayload::OrchestratorRequestMessagePayload(payload.clone()),
            )
            .await;

        assert!(matches!(result, Err(MessageAlreadyExisting { .. })));
    }

    #[tokio::test]
    async fn test_commit_non_existing_message() {
        let queue = FileSupervisorMessageQueue::with_location(td_common::tdtestdir!()).unwrap();
        let message = synthetic_request_message("test_message4");
        let result = queue.commit(&message).await;
        assert!(matches!(result, Err(MessageNonExisting { .. })));
    }

    #[tokio::test]
    async fn test_rollback_non_existing_message() {
        let queue = FileSupervisorMessageQueue::with_location(td_common::tdtestdir!()).unwrap();
        let message = synthetic_request_message("test_message5");
        let result = queue.commit(&message).await;
        assert!(matches!(result, Err(MessageNonExisting { .. })));
    }

    fn synthetic_request_message(id: &str) -> OrchestratorMessage<Value> {
        let payload = RequestMessagePayload::<Value> {
            class: WorkerClass::default(),
            worker: String::new(),
            worker_id: id.to_string(),
            action: MessageAction::default(),
            arguments: Vec::new(),
            callback: None,
            context: None,
        };
        OrchestratorMessage::new(
            id.to_string(),
            format!("{id}{INITIAL_CALL}"),
            PathBuf::new(),
            OrchestratorRequestMessagePayload(payload),
        )
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
