//
// Copyright 2025 Tabs Data Inc.
//

use crate::{LiveEntry, QueryExecutor};
use ta_objects::Buildable;
use td_error::TdError;
use td_objects::dxo::execution::ExecutionDB;
use td_objects::dxo::transaction::TransactionDB;
use td_objects::types::basic::{TransactionId, TransactionKey};

pub(crate) async fn seed_transaction(
    db: impl QueryExecutor,
    execution: &ExecutionDB,
    transaction_key: &TransactionKey,
) -> Result<TransactionDB, TdError> {
    let transaction_db = TransactionDB::builder()
        .id(TransactionId::new())
        .collection_id(execution.collection_id)
        .execution_id(&execution.id)
        .try_transaction_by("ANY")?
        .transaction_key(transaction_key.clone())
        .triggered_on(execution.triggered_on)
        .triggered_by_id(execution.triggered_by_id)
        .build()?;

    db.live().insert(&transaction_db).await?;
    Ok(transaction_db)
}

#[cfg(test)]
mod tests {
    use crate::test_utils::SeedEntry;
    use ta_objects::Buildable;
    use td_database::pool::TdPool;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::types::basic::{BundleId, CollectionName, Decorator, TransactionKey, UserId};
    use td_security::ENCODED_ID_SYSTEM;

    #[td_test::test(db)]
    async fn test_seed_transaction(db: TdPool) {
        let collection = db
            .seed()
            .collection(
                &CollectionName::try_from("collection").unwrap(),
                &UserId::try_from(ENCODED_ID_SYSTEM).unwrap(),
            )
            .await
            .unwrap();

        let dependencies = None;
        let triggers = None;
        let tables = None;

        let create = FunctionRegister::builder()
            .try_name("joaquin")
            .unwrap()
            .try_description("function_foo description")
            .unwrap()
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")
            .unwrap()
            .decorator(Decorator::Publisher)
            .dependencies(dependencies)
            .triggers(triggers)
            .tables(tables)
            .try_runtime_values("mock runtime values")
            .unwrap()
            .reuse_frozen_tables(false)
            .build()
            .unwrap();

        let function_version = db.seed().function(&collection, &create).await.unwrap();

        let execution = db.seed().execution(&function_version).await.unwrap();

        let transaction_key = TransactionKey::try_from("ANY").unwrap();
        let transaction = db
            .seed()
            .transaction(&execution, &transaction_key)
            .await
            .unwrap();

        assert_eq!(transaction.collection_id, execution.collection_id);
        assert_eq!(transaction.execution_id, execution.id);
        assert_eq!(transaction.transaction_key, transaction_key);
        assert_eq!(transaction.triggered_on, transaction.triggered_on);
        assert_eq!(transaction.triggered_by_id, execution.triggered_by_id);
    }
}
