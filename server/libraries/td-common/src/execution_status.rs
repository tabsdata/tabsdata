//
// Copyright 2024 Tabsdata Inc.
//

use serde::{Deserialize, Serialize};

#[derive(utoipa::ToSchema, Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum WorkerCallbackStatus {
    Running,
    Done,
    Failed,
    Stopped,
}

/// What the server did with a worker's status report. A `Stale` report came
/// too late to change anything: its attempt was superseded, or its run or
/// worker had already moved past it. The worker stops reporting.
#[derive(utoipa::ToSchema, Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub enum CallbackOutcome {
    Applied,
    Stale,
}

#[derive(utoipa::ToSchema, Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct CallbackResponse {
    pub outcome: CallbackOutcome,
}

#[derive(utoipa::ToSchema, Debug, Clone, Serialize, Deserialize)]
pub enum RecoverStatus {
    Cancel,
    Reschedule,
}
