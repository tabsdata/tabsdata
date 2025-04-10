//
//   Copyright 2024 Tabs Data Inc.
//

use std::ops::Deref;
use td_common::id;
use td_error::TdError;
use td_execution::execution_planner::ExecutionTemplate;
use td_execution::transaction::TransactionMap;
use td_objects::types::basic::TransactionId;
use td_objects::types::execution::FunctionVersionNode;
use td_tower::extractors::{Input, SrvCtx};
use te_execution::transaction::TransactionBy;

pub async fn dataset(
    SrvCtx(transaction_by): SrvCtx<TransactionBy>,
    Input(execution_template): Input<ExecutionTemplate>,
) -> Result<TransactionMap<FunctionVersionNode, TransactionBy>, TdError> {
    let mut dataset_transactions = TransactionMap::new(transaction_by.deref());

    let (trigger_dataset, _) = execution_template.manual_trigger();
    dataset_transactions.add(trigger_dataset);

    for (planned_dataset, _) in execution_template.dependency_triggers() {
        dataset_transactions.add(planned_dataset);
    }

    Ok(dataset_transactions)
}

pub async fn id(
    Input(dataset_transactions): Input<TransactionMap<FunctionVersionNode, TransactionBy>>,
) -> Result<TransactionMap<TransactionId, TransactionBy>, TdError> {
    let transactions_ids = dataset_transactions.map(|_| id::id());
    Ok(transactions_ids)
}
