//
// Copyright 2026 Tabsdata Inc.
//

use crate::types::basic::{ExecutionId, LogicalFunctionRunId, TransactionId};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// How a cancel reached a run: one it was asked to cancel, another run of a
/// transaction it ended, a run reading an output of a run it ended, or a
/// later run of the same function, which reads the state the earlier one
/// leaves.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum CancelReason {
    Requested,
    Transaction,
    Downstream,
    SameFunction,
}

#[ta_objects::Dto]
#[derive(Eq, PartialEq)]
pub struct CanceledRun {
    pub execution_id: ExecutionId,
    pub transaction_id: TransactionId,
    pub function_run_id: LogicalFunctionRunId,
    pub reason: CancelReason,
}

/// Every run a cancel ended, in any execution.
#[ta_objects::Dto]
#[derive(Eq, PartialEq)]
pub struct CancelResponse {
    pub canceled: Vec<CanceledRun>,
}
