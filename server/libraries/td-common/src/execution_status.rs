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

#[derive(utoipa::ToSchema, Debug, Clone, Serialize, Deserialize)]
pub enum RecoverStatus {
    Cancel,
    Reschedule,
}
