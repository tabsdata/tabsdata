//
// Copyright 2026 Tabsdata Inc.
//

use crate::types::basic::{
    CollectionLabel, ExecutionId, FunctionName, LogicalFunctionRunId, TransactionId,
};
use ta_objects::{TypedEnum, TypedOpenApi, TypedSerde};

#[derive(TypedEnum, TypedSerde, TypedOpenApi, Eq, PartialEq, Ord, PartialOrd, Hash, Copy)]
pub enum CancelReason {
    #[typed_enum(rename = "requested")]
    Requested,
    #[typed_enum(rename = "transaction")]
    Transaction,
    #[typed_enum(rename = "downstream")]
    Downstream,
    #[typed_enum(rename = "same_function")]
    SameFunction,
}

#[ta_objects::Dto]
#[derive(Eq, PartialEq)]
pub struct CanceledRun {
    pub execution_id: ExecutionId,
    pub transaction_id: TransactionId,
    pub function_run_id: LogicalFunctionRunId,
    pub function: FunctionName,
    pub collection: CollectionLabel,
    pub reason: CancelReason,
}

/// Every run a cancel ended, in any execution.
#[ta_objects::Dto]
#[derive(Eq, PartialEq)]
pub struct CancelResponse {
    pub canceled: Vec<CanceledRun>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cancel_reason_wire_names() {
        for (reason, name) in [
            (CancelReason::Requested, "requested"),
            (CancelReason::Transaction, "transaction"),
            (CancelReason::Downstream, "downstream"),
            (CancelReason::SameFunction, "same_function"),
        ] {
            let json = serde_json::to_value(reason).unwrap();
            assert_eq!(json, serde_json::json!(name));
            assert_eq!(
                serde_json::from_value::<CancelReason>(json).unwrap(),
                reason
            );
        }
    }
}
