//
// Copyright 2025 Tabs Data Inc.
//

use crate::sql::{DaoQueries, Insert};
use crate::types::basic::{TransactionId, TransactionKey};
use crate::types::execution::{ExecutionDB, TransactionDB};
use td_database::sql::DbPool;

pub async fn seed_transaction(
    db: &DbPool,
    execution: &ExecutionDB,
    transaction_key: &TransactionKey,
) -> TransactionDB {
    let transaction_db = TransactionDB::builder()
        .id(TransactionId::default())
        .execution_id(execution.id())
        .try_transaction_by("ANY")
        .unwrap()
        .transaction_key(transaction_key)
        .triggered_on(execution.triggered_on())
        .triggered_by_id(execution.triggered_by_id())
        .build()
        .unwrap();

    let queries = DaoQueries::default();
    queries
        .insert::<TransactionDB>(&transaction_db)
        .unwrap()
        .build()
        .execute(db)
        .await
        .unwrap();

    transaction_db
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::seed_collection::seed_collection;
    use crate::test_utils::seed_execution::seed_execution;
    use crate::test_utils::seed_function::seed_function;
    use crate::types::basic::{BundleId, CollectionName, Decorator, UserId};
    use crate::types::function::FunctionRegister;
    use td_database::sql::DbPool;
    use td_security::ENCODED_ID_SYSTEM;

    #[td_test::test(sqlx)]
    async fn test_seed_transaction(db: DbPool) {
        let collection = seed_collection(
            &db,
            &CollectionName::try_from("collection").unwrap(),
            &UserId::try_from(ENCODED_ID_SYSTEM).unwrap(),
        )
        .await;

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

        let function_version = seed_function(&db, &collection, &create).await;

        let execution = seed_execution(&db, &function_version).await;

        let transaction_key = TransactionKey::try_from("ANY").unwrap();
        let transaction = seed_transaction(&db, &execution, &transaction_key).await;

        assert_eq!(transaction.execution_id(), execution.id());
        assert_eq!(*transaction.transaction_key(), transaction_key);
        assert_eq!(transaction.triggered_on(), transaction.triggered_on());
        assert_eq!(transaction.triggered_by_id(), execution.triggered_by_id());
    }
}
