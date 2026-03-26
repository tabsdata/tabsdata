//
// Copyright 2025 Tabs Data Inc.
//

use crate::types::basic::{FunctionRunStatus, GlobalStatus, StatusCount};
use crate::types::json::Json;
use std::collections::HashMap;

#[ta_objects::Dao]
#[dao(sql_table = "global_status_summary")]
pub struct GlobalStatusSummaryDB {
    status: GlobalStatus,
    function_run_status_count: Json<HashMap<FunctionRunStatus, StatusCount>>,
}
