//
// Copyright 2025 Tabs Data Inc.
//

use std::collections::HashMap;
use std::ops::Deref;
use ta_execution::transaction::TransactionMap;
use td_error::TdError;
use td_objects::dxo::transaction::defs::TransactionDB;
use td_tower::extractors::{Input, SrvCtx};
use te_execution::transaction::TransactionBy;

// Similar to build_transaction_map, but using already existing transactions instead of creating new ones.
pub async fn build_existing_transaction_map(
    SrvCtx(transaction_by): SrvCtx<TransactionBy>,
    Input(transactions): Input<Vec<TransactionDB>>,
) -> Result<TransactionMap<TransactionBy>, TdError> {
    let mapped_transactions = transactions
        .iter()
        .map(|t| (t.transaction_key.clone(), (t.id, t.collection_id)))
        .collect::<HashMap<_, _>>();

    let transaction_map =
        TransactionMap::from_map(mapped_transactions, transaction_by.deref().clone());

    Ok(transaction_map)
}
