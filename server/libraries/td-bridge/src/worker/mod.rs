//
// Copyright 2026 Tabs Data Inc.
//

//! The `WorkerHandler` port: how the worker services read a worker's logs,
//! vended by [`BridgeProvider::worker_handler`](crate::BridgeProvider::worker_handler).
//! [`workspace::WorkspaceWorkerHandler`] reads the local workspace log files;
//! the Kubernetes one streams pod logs.

use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use std::pin::Pin;
use td_common::worker::WorkerName;
use td_error::td_error;

#[derive(Debug, Clone)]
pub struct WorkerLogTarget {
    pub worker_id: String,
    pub worker_kind: WorkerName,
    pub log_patterns: Vec<String>,
    pub casts: Vec<String>,
}

#[td_error]
pub enum LogBridgeError {
    #[error("Failed to resolve the workspace location: {cause}")]
    WorkspaceLocation { cause: String } = 5010,
    #[error("Error building the log path glob pattern")]
    EmptyPattern = 5011,
    #[error("Error evaluating the log path glob pattern: {cause}")]
    Glob { cause: String } = 5012,
    #[error("Failed to read the worker log: {cause}")]
    Read { cause: String } = 5013,
    #[error("Orchestrator backend error: {cause}")]
    Backend { cause: String } = 5014,
    #[error("Tailing worker logs is not supported by this orchestrator")]
    TailUnsupported = 6010,
}

pub type WorkerLogStream =
    Pin<Box<dyn Stream<Item = Result<Bytes, LogBridgeError>> + Send + 'static>>;

#[async_trait]
pub trait WorkerHandler: Send + Sync {
    /// Stream the worker's logs, complete files first to last.
    async fn logs(&self, target: &WorkerLogTarget) -> Result<WorkerLogStream, LogBridgeError>;

    /// Stream the worker's logs, following as they grow.
    async fn logs_tail(&self, target: &WorkerLogTarget) -> Result<WorkerLogStream, LogBridgeError>;
}

pub mod workspace;
