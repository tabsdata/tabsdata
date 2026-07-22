//
// Copyright 2026 Tabsdata Inc.
//

use serde::{Deserialize, Serialize};
use td_common::worker::{Callback, Decorator, WorkerName};
use td_objects::types::basic::{
    CollectionId, CollectionName, ConnectionId, FunctionName, FunctionVersionId, ProjectId,
    WorkerId,
};

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum StageTriggerRequest {
    Start(StageTriggerStartRequest),
    Stop(StageTriggerStopRequest),
    Notify,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct StageTriggerStartRequest {
    /// Worker name (template key), e.g. `stage-trigger`.
    pub worker: WorkerName,
    /// Worker id.
    pub worker_id: WorkerId,
    /// Function decorator (publisher / transformer / subscriber).
    pub decorator: Decorator,
    /// GET endpoint the worker resolves its message (`request.yaml`) from.
    pub resolver: Callback,
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
}

/// Routing identity for stopping a stage-trigger worker: enough for the
/// kubernetes gateway to derive the pod template (`worker`) and the pod name
/// (`worker`, `worker_id`) to delete.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct StageTriggerStopRequest {
    /// Worker name (template key), e.g. `stage-trigger`.
    pub worker: WorkerName,
    /// Worker id.
    pub worker_id: WorkerId,
    /// POST endpoint the worker reports status back on.
    pub responder: Callback,
}
