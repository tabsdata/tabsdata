//
// Copyright 2026 Tabsdata Inc.
//

//! Orchestrator messaging: the queue the server puts worker requests on for
//! the orchestrator to execute, plus the message envelope both sides share.
//! [`file_queue`] is the local file-based impl (the supervisor daemon consumes
//! it); the kubernetes queue lives in `td-kubernetes`.

pub mod file_queue;
#[cfg(feature = "test-utils")]
pub mod memory;
pub mod message;

pub use crate::messaging::message::cron::{CronRequest, CronStartRequest, CronStopRequest};
pub use crate::messaging::message::function::{
    FunctionRequest, FunctionStartRequest, FunctionStopRequest,
};
pub use crate::messaging::message::stream::{
    StageTriggerRequest, StageTriggerStartRequest, StageTriggerStopRequest,
};
pub use crate::messaging::message::{OrchestratorMessage, OrchestratorMessagePayload};
use async_trait::async_trait;
use serde_yaml::Value;
use std::io::Error;
use std::path::PathBuf;
use td_error::td_error;

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
    #[error("Orchestrator backend error: {cause}")]
    BackendError { cause: String },
}

#[async_trait]
pub trait OrchestratorMessageQueue: Send + Sync {
    /// Puts a message in the queue.
    async fn put(
        &self,
        id: String,
        payload: OrchestratorMessagePayload<Value>,
    ) -> Result<OrchestratorMessage<Value>, QueueError>;

    /// Commits a message in the queue.
    async fn commit(&self, message: &OrchestratorMessage<Value>) -> Result<(), QueueError>;

    /// Rollbacks a message in the queue.
    async fn rollback(&self, message: &OrchestratorMessage<Value>) -> Result<(), QueueError>;

    async fn locked_messages(&self) -> Vec<OrchestratorMessage<Value>>;
}
