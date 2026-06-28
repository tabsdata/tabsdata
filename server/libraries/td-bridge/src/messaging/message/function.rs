//
// Copyright 2026 Tabs Data Inc.
//

use serde::{Deserialize, Serialize};
use td_common::worker::{Callback, Decorator, WorkerName};
use td_objects::types::basic::{
    CollectionId, CollectionName, ConnectionId, FunctionId, FunctionName, ProjectId, WorkerId,
};

/// A kubernetes function-runner request, modeled as a per-action enum (the
/// counterpart to the file-queue's flat `RequestMessagePayload`). Carried as
/// its own [`OrchestratorMessagePayload`] kind so the kubernetes gateway can
/// act on it before the worker resolves its full message. Mirrors the
/// gateway's internal `FunctionMessage` shape; lives here because the message
/// envelope references it.
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
    pub worker: WorkerName,
    /// Worker id.
    pub worker_id: WorkerId,
    /// Function decorator (publisher / transformer / subscriber).
    pub decorator: Decorator,
    /// GET endpoint the worker pulls its resolved message from.
    pub resolver: Callback,
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
    /// Function id of the function being run.
    pub function_id: FunctionId,
    /// Function name of the function being run.
    pub function: FunctionName,
}
