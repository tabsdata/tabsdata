//
// Copyright 2025 Tabs Data Inc.
//

use serde::{Deserialize, Serialize};
use strum::{Display, EnumString};
use ta_execution::transaction::TransactionMapper;
use td_error::TdError;
use td_objects::execution::graph::FunctionNode;
use td_objects::types::basic::TransactionKey;

/// `TransactionBy` is an enum that defines how to map a transaction to a key.
/// It only supports mapping by function version ID.
#[derive(Debug, Default, Clone, Serialize, Deserialize, EnumString, Display)]
pub enum TransactionBy {
    #[default]
    #[strum(serialize = "F")]
    Function,
}

impl TransactionMapper for TransactionBy {
    fn key(&self, node: &FunctionNode) -> Result<TransactionKey, TdError> {
        match self {
            TransactionBy::Function => Ok(node.function_version_id.to_string().try_into()?),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction::TransactionBy;
    use ta_execution::transaction::TransactionMapperError;
    use td_objects::execution::graph::FunctionNode;
    use td_objects::types::basic::{
        CollectionId, CollectionName, FunctionName, FunctionVersionId, TransactionKey,
    };

    #[test]
    fn test_transaction_by_default() {
        assert!(matches!(TransactionBy::default(), TransactionBy::Function));
    }

    #[test]
    fn test_transaction_by_key() -> Result<(), TdError> {
        let mapper = TransactionBy::Function;

        let key = FunctionVersionId::default();
        let function = FunctionNode::builder()
            .collection_id(CollectionId::default())
            .collection(CollectionName::try_from("test")?)
            .function_version_id(key)
            .name(FunctionName::try_from("name")?)
            .build()?;

        assert_eq!(
            mapper.key(&function)?,
            TransactionKey::try_from(key.to_string())?
        );
        Ok(())
    }

    #[test]
    fn test_transaction_by_try_from() -> Result<(), TransactionMapperError> {
        assert!(matches!(
            TransactionBy::try_from("F")?,
            TransactionBy::Function
        ));
        Ok(())
    }
}
