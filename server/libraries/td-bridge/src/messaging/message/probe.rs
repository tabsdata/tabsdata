//
// Copyright 2026 Tabsdata Inc.
//

use serde::{Deserialize, Serialize};
use td_common::worker::Callback;
use td_objects::types::basic::{
    CollectionId, CollectionName, ConnectionId, ConnectionKind, ProbeId, ProjectId, ProjectName,
};

/// A kubernetes connection-probe request: one Explorer call against one
/// connection, run as a Job that receives the connection secret the same way a
/// function worker does. Never written to the supervisor file queue.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum ProbeRequest {
    Start(ProbeStartRequest),
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProbeStartRequest {
    pub probe_id: ProbeId,
    /// POST endpoint the probe reports its outcome on.
    pub responder: Callback,
    pub project_id: ProjectId,
    pub project: Option<ProjectName>,
    pub collection_id: CollectionId,
    pub collection: CollectionName,
    pub connection_id: ConnectionId,
    /// Type of the connection, as its Conn class.
    pub connection: ConnectionKind,
    /// The probe runner's `request.json`: kind, params, entity doc, caps.
    pub request: serde_json::Value,
}
