//
// Copyright 2025 Tabsdata Inc.
//

use std::collections::HashMap;
use td_error::TdError;
use td_execution::{EXECUTION_GRAPH_OPTIONS, TransactionBy};
use td_objects::dxo::execution::ExecutionDB;
use td_objects::dxo::transaction::{TransactionDB, TransactionValue};
use td_objects::transaction::map::TransactionMap;
use td_objects::types::recursion::RecursionOptions;

pub async fn with_existing_recursion_options(
    execution: &ExecutionDB,
) -> Result<RecursionOptions, TdError> {
    // We are using the depth defined, if any, or defaulting to EXECUTION_GRAPH_DEPTH.
    let options = execution
        .recursion_options
        .as_deref()
        .cloned()
        .unwrap_or(EXECUTION_GRAPH_OPTIONS);
    Ok(options)
}

// Similar to build_transaction_map, but using already existing transactions instead of creating new ones.
pub fn build_existing_transaction_map(
    transaction_by: &TransactionBy,
    transactions: &Vec<TransactionDB>,
) -> Result<TransactionMap<TransactionBy>, TdError> {
    let mut mapped_transactions = HashMap::new();
    for transaction in transactions.iter() {
        if !mapped_transactions.contains_key(&transaction.transaction_key) {
            mapped_transactions.insert(
                transaction.transaction_key.clone(),
                TransactionValue::from(transaction),
            );
        }
    }

    // We should always have at least one transaction, and all transactions should have the same transaction_by.
    // In case it differs with the one configured in the system, we prioritize the one from the transaction.
    // This can happen when loading transactions from previous executions.
    let transaction_by_str = transactions.first().map(|t| t.transaction_by.clone());
    let transaction_by = if let Some(transaction_by_str) = transaction_by_str {
        transaction_by_str.parse().unwrap_or(transaction_by.clone())
    } else {
        transaction_by.clone()
    };
    let transaction_map = TransactionMap::from_map(mapped_transactions, transaction_by);
    Ok(transaction_map)
}
