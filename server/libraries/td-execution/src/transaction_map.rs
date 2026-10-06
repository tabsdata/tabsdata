//
// Copyright 2025 Tabsdata Inc.
//

use std::collections::HashMap;
use ta_execution::{TransactionMapper, TransactionMapperError};
use td_error::TdError;
use td_objects::dxo::execution::ExecutionInsertDB;
use td_objects::dxo::graph::FunctionNode;
use td_objects::dxo::transaction::TransactionValue;
use td_objects::types::basic::{TransactionId, TransactionKey};

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

    pub fn add(&mut self, e: &ExecutionInsertDB, v: &FunctionNode) -> Result<(), TdError> {
        let key = self.mapper.key(v)?;
        if !self.map.contains_key(&key) {
            let value = TransactionValue {
                id: TransactionId::new(),
                collection_id: v.collection_id,
                execution_id: e.id.clone(),
                transaction_by: self.mapper.transaction_by()?,
                transaction_key: key.clone(),
                group_id: v.group_id,
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
    use crate::test_utils::TestTransactionBy;
    use td_common::datetime::Timestamp;
    use td_objects::test_utils::graph::{FUNCTION_NAMES, function_node};
    use td_objects::types::basic::TriggeredOn;
    use td_objects::types::basic::{ExecutionId, FunctionVersionId, TriggerType};

    fn dummy_execution() -> ExecutionInsertDB {
        ExecutionInsertDB {
            id: ExecutionId::new(),
            name: None,
            collection_id: Default::default(),
            function_id: Default::default(),
            function_version_id: FunctionVersionId::new(),
            triggered_on: TriggeredOn::asserted(Timestamp::default()),
            triggered_by_id: Default::default(),
            recursion_options: Default::default(),
            trigger_type: TriggerType::Manual,
            external_uid: None,
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
