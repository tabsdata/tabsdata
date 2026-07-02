//
// Copyright 2026 Tabs Data Inc.
//

use serde::{Deserialize, Serialize};
use td_common::worker::{Callback, WorkerName};
use td_objects::types::basic::{
    CollectionId, CollectionName, ConnectionId, CronMask, FunctionName, FunctionVersionId,
    ProjectId, WorkerId,
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
    /// Cron schedule (5-field UTC mask) the kubernetes gateway stamps onto the
    /// `CronJob`'s `spec.schedule`. k8s owns the ticking, so the schedule must
    /// travel in the start request rather than being resolved by the worker.
    pub schedule: CronMask,
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
