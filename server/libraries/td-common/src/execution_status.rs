//
// Copyright 2024 Tabs Data Inc.
//

use serde::{Deserialize, Serialize};

#[derive(utoipa::ToSchema, Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum WorkerCallbackStatus {
    Running,
    Done,
    Error,
    Failed,
}

#[derive(utoipa::ToSchema, Debug, Clone, Serialize, Deserialize)]
pub enum RecoverStatus {
    Cancel,
    Reschedule,
}
