//
// Copyright 2025 Tabs Data Inc.
//

use crate::transaction::TransactionMapper;
use strum_macros::{Display, EnumString};
use td_error::TdError;
use td_objects::types::basic::TransactionKey;
use td_objects::types::execution::FunctionVersionNode;

#[derive(Default, EnumString, Display)]
pub enum TestTransactionBy {
    #[default]
    #[strum(serialize = "F")]
    Name,
    #[strum(serialize = "S")]
    Single,
}

impl TransactionMapper for TestTransactionBy {
    fn key(&self, node: &FunctionVersionNode) -> Result<TransactionKey, TdError> {
        match self {
            TestTransactionBy::Name => Ok(node.name().to_string().try_into()?),
            TestTransactionBy::Single => Ok("S".to_string().try_into()?),
        }
    }
}
