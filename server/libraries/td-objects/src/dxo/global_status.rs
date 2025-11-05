//
// Copyright 2025 Tabs Data Inc.
//

use crate::types::i32::StatusCount;
use crate::types::typed_enum::{FunctionRunStatus, GlobalStatus};
use std::collections::HashMap;

#[td_type::Dao]
#[dao(sql_table = "global_status_summary")]
pub struct GlobalStatusSummaryDB {
    status: GlobalStatus,
    function_run_status_count: sqlx::types::Json<HashMap<FunctionRunStatus, StatusCount>>,
}
