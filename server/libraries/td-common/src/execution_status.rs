//
// Copyright 2024 Tabs Data Inc.
//

use serde::{Deserialize, Serialize};
use td_apiforge::apiserver_schema;

#[apiserver_schema]
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum WorkerCallbackStatus {
    Running,
    Done,
    Error,
    Failed,
}

#[apiserver_schema]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RecoverStatus {
    Cancel,
    Reschedule,
}
