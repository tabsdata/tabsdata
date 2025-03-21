//
// Copyright 2025 Tabs Data Inc.
//

pub use te_transaction::{TransactionBy, TransactionKey};

use std::collections::HashMap;
use td_common::dataset::DatasetRef;
use td_error::td_error;

pub struct TransactionMap<V> {
    map: HashMap<TransactionKey, V>,
    transaction_by: TransactionBy, // CARE: transaction_by can have more than one form
}

impl<V> TransactionMap<V>
where
    V: DatasetRef,
{
    pub fn add(&mut self, v: &V) -> &V {
        self.map
            .entry(self.transaction_by.key(v))
            .or_insert_with(|| v.clone())
    }
}

impl<V> TransactionMap<V> {
    pub fn new(transaction_by: &TransactionBy) -> Self {
        Self {
            map: HashMap::new(),
            transaction_by: transaction_by.clone(),
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (&TransactionKey, &V)> {
        self.map.iter()
    }

    pub fn insert_with(&mut self, key: TransactionKey, f: impl FnOnce() -> V) {
        self.map.entry(key).or_insert_with(f);
    }

    pub fn get(&self, key: &impl DatasetRef) -> Result<&V, TransactionMapError> {
        let key = self.transaction_by.key(key);
        self.map
            .get(&key)
            .ok_or(TransactionMapError::MissingTransactionKey(key))
    }

    pub fn map<F, VV>(&self, f: F) -> TransactionMap<VV>
    where
        F: Fn(&V) -> VV,
    {
        let map = self
            .map
            .iter()
            .map(|(key, value)| (key.clone(), f(value)))
            .collect();
        TransactionMap {
            map,
            transaction_by: self.transaction_by.clone(),
        }
    }
}

#[td_error]
pub enum TransactionMapError {
    #[error("Missing transaction key: {0}")]
    MissingTransactionKey(TransactionKey) = 5000,
}

#[cfg(test)]
mod tests {
    use super::*;
    use te_transaction::{TransactionBy, TransactionKey};

    #[test]
    fn test_add() {
        let transaction_by = TransactionBy::default();
        let mut transaction_map = TransactionMap::new(&transaction_by);
        let dataset = String::from("dataset1");

        transaction_map.add(&dataset);
        assert!(transaction_map.get(&dataset).is_ok());
    }

    #[test]
    fn test_insert_with() {
        let transaction_by = TransactionBy::default();
        let mut transaction_map = TransactionMap::new(&transaction_by);
        let key = TransactionKey::from("key1");

        transaction_map.insert_with(key.clone(), || String::from("dataset2"));
        assert!(transaction_map.get(&key).is_ok());
    }

    #[test]
    fn test_get_missing_key() {
        let transaction_by = TransactionBy::default();
        let transaction_map = TransactionMap::<String>::new(&transaction_by);
        let dataset = String::from("dataset3");

        assert!(transaction_map.get(&dataset).is_err());
    }

    #[test]
    fn test_map() {
        let transaction_by = TransactionBy::default();
        let mut transaction_map = TransactionMap::new(&transaction_by);
        let dataset = String::from("dataset4");

        transaction_map.add(&dataset);
        let new_map = transaction_map.map(|v| v.replace("dataset", "new_dataset"));
        assert!(new_map.get(&dataset).is_ok());
        assert_eq!(
            new_map.get(&dataset).unwrap(),
            &String::from("new_dataset4")
        );
    }
}
