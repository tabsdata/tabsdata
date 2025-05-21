//
// Copyright 2025 Tabs Data Inc.
//

use crate::sql::{DaoQueries, Insert};
use crate::types::basic::Trigger;
use crate::types::collection::CollectionDB;
use crate::types::execution::{ExecutionDB, FunctionRunDB, FunctionRunStatus, TransactionDB};
use crate::types::function::FunctionVersionDB;
use td_database::sql::DbPool;

pub async fn seed_function_run(
    db: &DbPool,
    collection: &CollectionDB,
    function_version: &FunctionVersionDB,
    execution: &ExecutionDB,
    transaction: &TransactionDB,
    status: &FunctionRunStatus,
) -> FunctionRunDB {
    let function_db = FunctionRunDB::builder()
        .collection_id(collection.id())
        .function_version_id(function_version.id())
        .execution_id(execution.id())
        .transaction_id(transaction.id())
        .triggered_on(transaction.triggered_on())
        .triggered_by_id(transaction.triggered_by_id())
        .trigger(Trigger::Manual)
        .status(status)
        .build()
        .unwrap();

    let queries = DaoQueries::default();
    queries
        .insert::<FunctionRunDB>(&function_db)
        .unwrap()
        .build()
        .execute(db)
        .await
        .unwrap();

    function_db
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::seed_collection::seed_collection;
    use crate::test_utils::seed_execution::seed_execution;
    use crate::test_utils::seed_function::seed_function;
    use crate::test_utils::seed_transaction::seed_transaction;
    use crate::types::basic::{BundleId, CollectionName, Decorator, TransactionKey, UserId};
    use crate::types::function::FunctionRegister;
    use td_database::sql::DbPool;
    use td_security::ENCODED_ID_SYSTEM;

    #[td_test::test(sqlx)]
    async fn test_seed_function_run(db: DbPool) {
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

        let (_, function_version) = seed_function(&db, &collection, &create).await;

        let execution = seed_execution(&db, &collection, &function_version).await;

        let transaction_key = TransactionKey::try_from("ANY").unwrap();
        let transaction = seed_transaction(&db, &execution, &transaction_key).await;

        let function_run = seed_function_run(
            &db,
            &collection,
            &function_version,
            &execution,
            &transaction,
            &FunctionRunStatus::Scheduled,
        )
        .await;

        assert_eq!(function_run.collection_id(), collection.id());
        assert_eq!(function_run.function_version_id(), function_version.id());
        assert_eq!(function_run.execution_id(), execution.id());
        assert_eq!(function_run.transaction_id(), transaction.id());
        assert_eq!(function_run.triggered_on(), transaction.triggered_on());
        assert_eq!(*function_run.trigger(), Trigger::Manual);
        assert_eq!(*function_run.status(), FunctionRunStatus::Scheduled);
    }
}
