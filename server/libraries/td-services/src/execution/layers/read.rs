//
// Copyright 2025 Tabs Data Inc.
//

use std::collections::HashMap;
use std::ops::Deref;
use ta_execution::transaction::TransactionMap;
use td_error::TdError;
use td_objects::dxo::transaction::{TransactionDB, TransactionValueBuilder};
use td_tower::extractors::{Input, SrvCtx};
use te_execution::transaction::TransactionBy;

// Similar to build_transaction_map, but using already existing transactions instead of creating new ones.
pub async fn build_existing_transaction_map(
    SrvCtx(transaction_by): SrvCtx<TransactionBy>,
    Input(transactions): Input<Vec<TransactionDB>>,
) -> Result<TransactionMap<TransactionBy>, TdError> {
    // Map transactions to their keys.
    let mut mapped_transactions = HashMap::new();
    for transaction in transactions.iter() {
        if !mapped_transactions.contains_key(&transaction.transaction_key) {
            mapped_transactions.insert(
                transaction.transaction_key.clone(),
                TransactionValueBuilder::try_from(transaction)?.build()?,
            );
        }
    }

    // We should always have at least one transaction, and all transactions should have the same transaction_by.
    // In case it differs with the one configured in the system, we prioritize the one from the transaction.
    // This can happen when loading transactions from previous executions.
    let transaction_by_str = transactions.first().map(|t| t.transaction_by.clone());
    let transaction_by = if let Some(transaction_by_str) = transaction_by_str {
        transaction_by_str
            .parse()
            .unwrap_or(transaction_by.deref().clone())
    } else {
        transaction_by.deref().clone()
    };
    let transaction_map = TransactionMap::from_map(mapped_transactions, transaction_by);
    Ok(transaction_map)
}
