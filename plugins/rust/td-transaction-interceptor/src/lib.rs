//
// Copyright 2025 Tabs Data Inc.
//

use serde::{Deserialize, Serialize};
use std::str::FromStr;
use strum::ParseError;
use strum_macros::{Display, EnumString};
use td_common::dataset::DatasetRef;

pub type TransactionKey = String;

#[derive(
    Debug, Clone, Eq, Hash, PartialEq, Default, Serialize, Deserialize, EnumString, Display,
)]
pub enum TransactionBy {
    #[default]
    #[strum(serialize = "F")]
    Function,
}

impl TryFrom<String> for TransactionBy {
    type Error = ParseError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        TransactionBy::from_str(s.as_str())
    }
}

impl TransactionBy {
    pub fn key(&self, key: &impl DatasetRef) -> TransactionKey {
        match self {
            TransactionBy::Function => key.dataset().to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::TransactionBy;
    use std::fmt::{Display, Formatter};
    use td_common::dataset::DatasetRef;

    #[derive(Eq, PartialEq, Hash, Clone, Debug)]
    struct TestDataset {
        collection: String,
        dataset: String,
    }

    impl Display for TestDataset {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}/{}", self.collection, self.dataset)
        }
    }

    impl DatasetRef for TestDataset {
        fn collection(&self) -> &str {
            &self.collection
        }

        fn dataset(&self) -> &str {
            &self.dataset
        }
    }

    #[test]
    fn test_transaction_by_default() {
        assert_eq!(TransactionBy::default(), TransactionBy::Function);
    }

    #[test]
    fn test_transaction_by_key() {
        let local = TransactionBy::Function;

        let dataset = TestDataset {
            collection: "ds0".to_string(),
            dataset: "d0".to_string(),
        };

        assert_eq!(local.key(&dataset), "d0");
    }

    #[test]
    fn test_transaction_by_try_from() {
        let local = "F".to_string();

        assert_eq!(
            TransactionBy::try_from(local).unwrap(),
            TransactionBy::Function
        );
    }
}
