//
// Copyright 2025 Tabs Data Inc.
//

use std::collections::HashMap;
use std::fmt::Display;
use std::str::FromStr;
use td_error::{TdError, td_error};
use td_objects::dxo::execution::ExecutionDB;
use td_objects::dxo::transaction::TransactionValue;
use td_objects::execution::graph::FunctionNode;
use td_objects::types::basic::{TransactionByStr, TransactionId, TransactionKey};

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
    fn key(&self, node: &FunctionNode) -> Result<TransactionKey, TdError>;

    fn transaction_by(&self) -> Result<TransactionByStr, TdError> {
        TransactionByStr::try_from(self.to_string())
    }
}

/// TransactionMap is a map that stores transaction keys and their corresponding transaction IDs.
/// It uses a generic TransactionMapper to determine the key for each function version node.
pub struct TransactionMap<T: TransactionMapper> {
    map: HashMap<TransactionKey, TransactionValue>,
    mapper: T, // CARE: transaction_by enum can have more than one form
}

impl<T: TransactionMapper> TransactionMap<T> {
    pub fn empty(mapper: T) -> Self {
        Self {
            map: HashMap::new(),
            mapper,
        }
    }

    pub fn from_map(map: HashMap<TransactionKey, TransactionValue>, mapper: T) -> Self {
        Self { map, mapper }
    }

    pub fn mapper(&self) -> &T {
        &self.mapper
    }

    pub fn add(&mut self, e: &ExecutionDB, v: &FunctionNode) -> Result<(), TdError> {
        let key = self.mapper.key(v)?;
        if !self.map.contains_key(&key) {
            let value = TransactionValue {
                id: TransactionId::default(),
                collection_id: v.collection_id,
                execution_id: e.id,
                transaction_by: self.mapper.transaction_by()?,
                transaction_key: key.clone(),
            };
            self.map.insert(key, value);
        }
        Ok(())
    }

    pub fn iter(&self) -> impl Iterator<Item = &TransactionKey> {
        self.map.keys()
    }

    pub fn get(&self, key: &TransactionKey) -> Result<&TransactionValue, TdError> {
        self.map
            .get(key)
            .ok_or(TransactionMapperError::MissingTransactionKey(key.clone()).into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::transaction::TestTransactionBy;
    use td_objects::dxo::execution::ExecutionDB;
    use td_objects::test_utils::graph::{FUNCTION_NAMES, function_node};

    fn dummy_execution() -> ExecutionDB {
        ExecutionDB {
            id: Default::default(),
            name: None,
            collection_id: Default::default(),
            function_version_id: Default::default(),
            triggered_on: Default::default(),
            triggered_by_id: Default::default(),
        }
    }

    #[test]
    fn test_add() -> Result<(), TdError> {
        let transaction_by = TestTransactionBy::default();
        let mut transaction_map = TransactionMap::empty(transaction_by);
        let function = function_node(&FUNCTION_NAMES[0]);
        let execution = dummy_execution();

        transaction_map.add(&execution, &function)?;
        let key = TransactionKey::try_from(FUNCTION_NAMES[0].to_string())?;
        let val = transaction_map.map.get(&key).unwrap();
        let val2 = transaction_map.get(&key)?;
        assert_eq!(val.id, val2.id);
        assert_eq!(val.transaction_key, val2.transaction_key);
        Ok(())
    }

    #[test]
    fn test_get_missing_key() -> Result<(), TdError> {
        let transaction_by = TestTransactionBy::default();
        let mut transaction_map = TransactionMap::empty(transaction_by);
        let function = function_node(&FUNCTION_NAMES[0]);
        let execution = dummy_execution();

        transaction_map.add(&execution, &function)?;
        let key = TransactionKey::try_from("error")?;
        assert!(transaction_map.get(&key).is_err());
        Ok(())
    }

    #[test]
    fn test_mapper() {
        let transaction_by = TestTransactionBy::default();
        let transaction_map = TransactionMap::empty(transaction_by);

        let mapper = transaction_map.mapper();
        assert!(matches!(mapper, TestTransactionBy::Name));
    }

    #[test]
    fn test_iter() -> Result<(), TdError> {
        let transaction_by = TestTransactionBy::default();
        let mut transaction_map = TransactionMap::empty(transaction_by);
        let function = function_node(&FUNCTION_NAMES[0]);
        let execution = dummy_execution();

        transaction_map.add(&execution, &function)?;
        let keys: Vec<_> = transaction_map.iter().collect();
        assert_eq!(keys.len(), 1);
        assert_eq!(
            keys[0],
            &TransactionKey::try_from(FUNCTION_NAMES[0].to_string())?
        );
        Ok(())
    }
}
