//
//   Copyright 2024 Tabs Data Inc.
//

use td_common::error::TdError;
use td_common::id;
use td_common::id::Id;
use td_execution::dataset::Dataset;
use td_execution::execution_planner::ExecutionTemplate;
use td_tower::extractors::{Input, SrvCtx};
use td_transaction::{TransactionBy, TransactionMap};

pub async fn dataset(
    SrvCtx(transaction_by): SrvCtx<TransactionBy>,
    Input(execution_template): Input<ExecutionTemplate>,
) -> Result<TransactionMap<Dataset>, TdError> {
    let mut dataset_transactions = TransactionMap::new(&transaction_by);

    let (trigger_dataset, _) = execution_template.manual_trigger();
    dataset_transactions.add(trigger_dataset);

    for (planned_dataset, _) in execution_template.dependency_triggers() {
        dataset_transactions.add(planned_dataset);
    }

    Ok(dataset_transactions)
}

pub async fn id(
    Input(dataset_transactions): Input<TransactionMap<Dataset>>,
) -> Result<TransactionMap<Id>, TdError> {
    let transactions_ids = dataset_transactions.map(|_| id::id());
    Ok(transactions_ids)
}
