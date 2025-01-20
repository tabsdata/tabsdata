//
// Copyright 2024 Tabs Data Inc.
//

use chrono::{DateTime, Utc};
use td_common::error::TdError;
use td_common::execution_status::TransactionStatus;
use td_common::id::Id;
use td_objects::datasets::dao::DsTransaction;
use td_objects::dlo::{ExecutionPlanId, RequestUserId};
use td_tower::extractors::{Context, Input};
use td_transaction::{TransactionBy, TransactionMap};

pub async fn build_transactions(
    Context(transaction_by): Context<TransactionBy>,
    Input(execution_plan_id): Input<ExecutionPlanId>,
    Input(transaction_ids): Input<TransactionMap<Id>>,
    Input(user_id): Input<RequestUserId>,
    Input(trigger_time): Input<DateTime<Utc>>,
) -> Result<Vec<DsTransaction>, TdError> {
    let mut transactions = Vec::new();

    for (key, transaction_id) in transaction_ids.iter() {
        let ds_execution_plan = DsTransaction::builder()
            .id(transaction_id.to_string())
            .execution_plan_id(execution_plan_id.as_str())
            .transaction_by((*transaction_by).clone())
            .transaction_key(key.to_string())
            .triggered_by_id(user_id.as_str())
            .triggered_on(*trigger_time)
            .started_on(None)
            .ended_on(None)
            .commit_id(None)
            .commited_on(None)
            .status(TransactionStatus::Scheduled)
            .build()
            .unwrap();
        transactions.push(ds_execution_plan);
    }

    Ok(transactions)
}
