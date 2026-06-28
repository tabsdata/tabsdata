//
// Copyright 2026 Tabs Data Inc.
//

use serde::{Deserialize, Serialize};
use td_common::worker::{Callback, WorkerName};
use td_objects::types::basic::{
    AtTime, CollectionId, CollectionName, ConnectionId, FunctionName, FunctionVersionId, ProjectId,
    WorkerConfig, WorkerId,
};

/// A kubernetes cron-trigger request, modeled as a per-action enum (the
/// counterpart to the file-queue's flat `RequestMessagePayload`). Carried as
/// its own [`OrchestratorMessagePayload`] kind so the kubernetes gateway can
/// act on it before the worker resolves its full message. Lives here because
/// the message envelope references it.
///
/// [`OrchestratorMessagePayload`]: crate::messaging::message::OrchestratorMessagePayload
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum CronRequest {
    Start(CronStartRequest),
    Stop(CronStopRequest),
    Notify,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct CronStartRequest {
    /// Worker name (template key), e.g. `cron-trigger`.
    pub worker: WorkerName,
    /// Worker id.
    pub worker_id: WorkerId,
    /// POST endpoint to trigger the function.
    pub trigger: Callback,
    /// POST endpoint the worker reports status back on.
    pub responder: Callback,
    /// Project the function being run belongs to.
    pub project_id: ProjectId,
    /// Collection id of the function being run.
    pub collection_id: CollectionId,
    /// Collection name of the function being run.
    pub collection: CollectionName,
    /// The collection's connection (source/destination collections only).
    pub connection_id: Option<ConnectionId>,
    /// Function version id of the function being run.
    pub function_version_id: FunctionVersionId,
    /// Function name of the function being run.
    pub function: FunctionName,
    /// When this struct was created.
    pub scheduled_on: AtTime,
    /// Config of the cron trigger.
    pub config: WorkerConfig,
}

/// Routing identity for stopping a cron-trigger worker: enough for the
/// kubernetes gateway to derive the pod template (`worker`) and the pod name
/// (`worker`, `worker_id`) to delete.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct CronStopRequest {
    /// Worker name (template key), e.g. `cron-trigger`.
    pub worker: WorkerName,
    /// Worker id.
    pub worker_id: WorkerId,
    /// POST endpoint the worker reports status back on.
    pub responder: Callback,
}
