//
// Copyright 2025 Tabs Data Inc.
//

use std::collections::HashMap;
use std::fmt::Display;
use std::str::FromStr;
use td_error::{td_error, TdError};
use td_objects::types::basic::{CollectionId, TransactionByStr, TransactionId, TransactionKey};
use td_objects::types::execution::FunctionVersionNode;

#[td_error]
pub enum TransactionMapperError {
    #[error("Missing transaction key: {0}")]
    MissingTransactionKey(TransactionKey) = 5000,
    #[error("Invalid transaction by: {0}")]
    InvalidTransactionBy(#[from] strum::ParseError) = 5001,
}

/// TransactionMapper trait is used to map a function version node to a transaction key.
/// Depending on the mapper, the key can have different forms. This will mark to which
/// transaction the function version node belongs.
pub trait TransactionMapper:
    Default + FromStr + for<'a> TryFrom<&'a str> + Display + Sized
{
    fn key(&self, node: &FunctionVersionNode) -> Result<TransactionKey, TdError>;

    fn transaction_by(&self) -> Result<TransactionByStr, TdError> {
        Ok(TransactionByStr::try_from(self.to_string())?)
    }
}

/// TransactionMap is a map that stores transaction keys and their corresponding transaction IDs.
/// It uses a generic TransactionMapper to determine the key for each function version node.
pub struct TransactionMap<T: TransactionMapper> {
    map: HashMap<TransactionKey, (TransactionId, CollectionId)>,
    mapper: T, // CARE: transaction_by enum can have more than one form
}

impl<T: TransactionMapper> TransactionMap<T> {
    pub fn new(mapper: T) -> Self {
        Self {
            map: HashMap::new(),
            mapper,
        }
    }

    pub fn mapper(&self) -> &T {
        &self.mapper
    }

    pub fn add(
        &mut self,
        v: &FunctionVersionNode,
    ) -> Result<&(TransactionId, CollectionId), TdError> {
        Ok(self
            .map
            .entry(self.mapper.key(v)?.clone())
            .or_insert_with(|| {
                let transaction_id = TransactionId::default();
                // This works because transactions last at most one collection.
                let collection_id = *v.collection_id();
                (transaction_id, collection_id)
            }))
    }

    pub fn iter(&self) -> impl Iterator<Item = &TransactionKey> {
        self.map.keys()
    }

    pub fn get(&self, key: &TransactionKey) -> Result<&(TransactionId, CollectionId), TdError> {
        self.map
            .get(key)
            .ok_or(TransactionMapperError::MissingTransactionKey(key.clone()).into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::transaction::TestTransactionBy;
    use td_objects::types::test_utils::execution::{function_node, FUNCTION_NAMES};

    #[test]
    fn test_add() -> Result<(), TdError> {
        let transaction_by = TestTransactionBy::default();
        let mut transaction_map = TransactionMap::new(transaction_by);
        let function = function_node(&FUNCTION_NAMES[0]);

        let id = *transaction_map.add(&function)?;
        let key = TransactionKey::try_from(FUNCTION_NAMES[0].to_string())?;
        assert_eq!(id, *transaction_map.get(&key)?);
        Ok(())
    }

    #[test]
    fn test_get_missing_key() -> Result<(), TdError> {
        let transaction_by = TestTransactionBy::default();
        let mut transaction_map = TransactionMap::new(transaction_by);
        let function = function_node(&FUNCTION_NAMES[0]);

        transaction_map.add(&function)?;
        let key = TransactionKey::try_from("error")?;
        assert!(transaction_map.get(&key).is_err());
        Ok(())
    }

    #[test]
    fn test_mapper() {
        let transaction_by = TestTransactionBy::default();
        let transaction_map = TransactionMap::new(transaction_by);

        let mapper = transaction_map.mapper();
        assert!(matches!(mapper, TestTransactionBy::Name));
    }

    #[test]
    fn test_iter() -> Result<(), TdError> {
        let transaction_by = TestTransactionBy::default();
        let mut transaction_map = TransactionMap::new(transaction_by);
        let function = function_node(&FUNCTION_NAMES[0]);

        transaction_map.add(&function)?;
        let keys: Vec<_> = transaction_map.iter().collect();
        assert_eq!(keys.len(), 1);
        assert_eq!(
            keys[0],
            &TransactionKey::try_from(FUNCTION_NAMES[0].to_string())?
        );
        Ok(())
    }
}
