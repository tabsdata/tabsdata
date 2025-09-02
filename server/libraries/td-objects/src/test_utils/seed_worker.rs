//
// Copyright 2025 Tabs Data Inc.
//

use crate::sql::{DaoQueries, Insert};
use crate::types::basic::WorkerStatus;
use crate::types::execution::{
    ExecutionDB, FunctionRunDB, TransactionDB, WorkerDB, WorkerMessageStatus,
};
use td_database::sql::DbPool;

pub async fn seed_worker(
    db: &DbPool,
    execution: &ExecutionDB,
    transaction: &TransactionDB,
    function_run: &FunctionRunDB,
    status: WorkerMessageStatus,
) -> WorkerDB {
    let worker_db = WorkerDB::builder()
        .collection_id(execution.collection_id())
        .execution_id(execution.id())
        .transaction_id(transaction.id())
        .function_run_id(function_run.id())
        .function_version_id(function_run.function_version_id())
        .message_status(status)
        .started_on(None)
        .ended_on(None)
        .status(WorkerStatus::RunRequested)
        .build()
        .unwrap();

    let queries = DaoQueries::default();
    queries
        .insert::<WorkerDB>(&worker_db)
        .unwrap()
        .build()
        .execute(db)
        .await
        .unwrap();

    worker_db
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::seed_collection::seed_collection;
    use crate::test_utils::seed_execution::seed_execution;
    use crate::test_utils::seed_function::seed_function;
    use crate::test_utils::seed_function_run::seed_function_run;
    use crate::test_utils::seed_transaction::seed_transaction;
    use crate::types::basic::TransactionKey;
    use crate::types::basic::{BundleId, CollectionName, Decorator, FunctionRunStatus, UserId};
    use crate::types::function::FunctionRegister;
    use td_database::sql::DbPool;

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_seed_worker(db: DbPool) {
        let collection = seed_collection(
            &db,
            &CollectionName::try_from("collection").unwrap(),
            &UserId::admin(),
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

        let function_run = seed_function_run(
            &db,
            &collection,
            &function_version,
            &execution,
            &transaction,
            &FunctionRunStatus::Scheduled,
        )
        .await;

        let worker = seed_worker(
            &db,
            &execution,
            &transaction,
            &function_run,
            WorkerMessageStatus::Locked,
        )
        .await;

        assert_eq!(worker.collection_id(), collection.id());
        assert_eq!(worker.execution_id(), execution.id());
        assert_eq!(worker.transaction_id(), transaction.id());
        assert_eq!(worker.function_run_id(), function_run.id());
        assert_eq!(
            worker.function_version_id(),
            function_run.function_version_id()
        );
        assert_eq!(*worker.message_status(), WorkerMessageStatus::Locked);
    }
}
