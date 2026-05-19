//
// Copyright 2026 Tabs Data Inc.
//

pub mod dispatch_mint;
pub mod mint;
pub mod replay;

use std::collections::HashMap;
use td_objects::dxo::logical::LogicalFunctionRunDB;
use td_objects::types::basic::{ExecutionId, FunctionId};

/// Index LFRs by their natural composite key `(execution_id, function_id)`
/// into a nested map. `FunctionId` is `Copy`; the nested shape lets
/// per-PFR lookups borrow the outer `ExecutionId` (avoiding the clone
/// a tuple key would force).
pub fn index_lfrs_by_exec_fn(
    lfrs: Vec<LogicalFunctionRunDB>,
) -> HashMap<ExecutionId, HashMap<FunctionId, LogicalFunctionRunDB>> {
    let mut out: HashMap<ExecutionId, HashMap<FunctionId, LogicalFunctionRunDB>> = HashMap::new();
    for lfr in lfrs {
        out.entry(lfr.execution_id.clone())
            .or_default()
            .insert(lfr.function_id, lfr);
    }
    out
}
