//
// Copyright 2025 Tabs Data Inc.
//

use crate::types::basic::{FunctionRunStatus, GlobalStatus, StatusCount};
use std::collections::HashMap;

#[td_type::Dao]
#[dao(sql_table = "global_status_summary")]
pub struct GlobalStatusSummaryDB {
    status: GlobalStatus,
    function_run_status_count: sqlx::types::Json<HashMap<FunctionRunStatus, StatusCount>>,
}
