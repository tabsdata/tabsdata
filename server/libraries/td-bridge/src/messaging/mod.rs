//
// Copyright 2026 Tabs Data Inc.
//

//! Orchestrator messaging: the queue the server puts worker requests on for
//! the orchestrator to execute, plus the message envelope both sides share.
//! [`file_queue`] is the local file-based impl (the supervisor daemon consumes
//! it); the kubernetes queue lives in `td-kubernetes`.

pub mod file_queue;
#[cfg(feature = "test-utils")]
pub mod memory;

use async_trait::async_trait;
use derive_new::new;
use serde::{Deserialize, Serialize};
use serde_yaml::Value;
use std::io::Error;
use std::path::PathBuf;
use td_common::worker::{
    Callback, Decorator, ExceptionMessagePayload, RequestMessagePayload, ResponseMessagePayload,
};
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

#[derive(Debug, Clone, Eq, PartialEq, new, Serialize, Deserialize)]
pub struct OrchestratorMessage<T = Value>
where
    T: Clone,
{
    pub id: String,
    pub work: String,
    pub file: PathBuf,
    pub payload: OrchestratorMessagePayload<T>,
}

// The FunctionRequest variant has its own allow on the inner enum (rare path,
// boxed allocation isn't worth it); we adopt the same stance at this outer
// level for the same reason.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum OrchestratorMessagePayload<T = Value>
where
    T: Clone,
{
    OrchestratorRequestMessagePayload(RequestMessagePayload<T>),
    OrchestratorResponseMessagePayload(ResponseMessagePayload<T>),
    OrchestratorExceptionMessagePayload(ExceptionMessagePayload<T>),
    /// Kubernetes function-runner request (per-action enum). Never written to
    /// the supervisor file queue — only the kubernetes gateway produces/accepts
    /// it.
    OrchestratorFunctionRequestMessagePayload(FunctionRequest),
}

/// A kubernetes function-runner request, modeled as a per-action enum (the
/// counterpart to the file-queue's flat `RequestMessagePayload`). Carried as
/// its own [`OrchestratorMessagePayload`] kind so the kubernetes gateway can
/// act on it before the worker resolves its full message. Mirrors the
/// gateway's internal `FunctionMessage` shape; lives here because the message
/// envelope references it.
// Start carries the full FunctionStartRequest payload while Stop and Notify
// are unit variants. Boxing Start would force an allocation on every message
// just to flatten the size of two variants we rarely build; the enum is small
// in absolute terms and lives behind a clone-on-send channel.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum FunctionRequest {
    Start(FunctionStartRequest),
    Stop,
    Notify,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct FunctionStartRequest {
    /// Worker name (template key), e.g. `function`.
    pub worker: String,
    /// Worker id.
    pub worker_id: String,
    /// Function decorator (publisher / transformer / subscriber).
    pub decorator: Decorator,
    /// GET endpoint the worker pulls its resolved message from.
    pub resolver: Callback,
    /// POST endpoint the worker reports status back on.
    pub responder: Callback,
    /// Collection id of the function being run.
    pub collection_id: String,
    /// Collection name of the function being run.
    pub collection: String,
    /// Function id of the function being run.
    pub function_id: String,
    /// Function name of the function being run.
    pub function: String,
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
