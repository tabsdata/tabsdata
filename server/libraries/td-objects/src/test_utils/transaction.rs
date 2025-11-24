//
// Copyright 2025 Tabs Data Inc.
//

use crate::transaction::map::TransactionMapper;
use crate::types::basic::TransactionKey;
use crate::types::graph::FunctionNode;
use strum::{Display, EnumString};
use td_error::TdError;

#[derive(Default, EnumString, Display)]
pub enum TestTransactionBy {
    #[default]
    #[strum(serialize = "F")]
    Name,
    #[strum(serialize = "S")]
    Single,
}

impl TransactionMapper for TestTransactionBy {
    fn key(&self, node: &FunctionNode) -> Result<TransactionKey, TdError> {
        match self {
            TestTransactionBy::Name => Ok(node.name.to_string().try_into()?),
            TestTransactionBy::Single => Ok("S".to_string().try_into()?),
        }
    }
}
