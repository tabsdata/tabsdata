//
// Copyright 2025 Tabs Data Inc.
//

use crate::sql::{DaoQueries, Insert};
use crate::types::execution::{
    ExecutionDB, FunctionRunDB, TransactionDB, WorkerMessageDB, WorkerMessageStatus,
};
use td_database::sql::DbPool;

pub async fn seed_worker_message(
    db: &DbPool,
    execution: &ExecutionDB,
    transaction: &TransactionDB,
    function_run: &FunctionRunDB,
    status: WorkerMessageStatus,
) -> WorkerMessageDB {
    let worker_message_db = WorkerMessageDB::builder()
        .collection_id(execution.collection_id())
        .execution_id(execution.id())
        .transaction_id(transaction.id())
        .function_run_id(function_run.id())
        .function_version_id(function_run.function_version_id())
        .message_status(status)
        .build()
        .unwrap();

    let queries = DaoQueries::default();
    queries
        .insert::<WorkerMessageDB>(&worker_message_db)
        .unwrap()
        .build()
        .execute(db)
        .await
        .unwrap();

    worker_message_db
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
    async fn test_seed_worker_message(db: DbPool) {
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

        let worker_message = seed_worker_message(
            &db,
            &execution,
            &transaction,
            &function_run,
            WorkerMessageStatus::Locked,
        )
        .await;

        assert_eq!(worker_message.collection_id(), collection.id());
        assert_eq!(worker_message.execution_id(), execution.id());
        assert_eq!(worker_message.transaction_id(), transaction.id());
        assert_eq!(worker_message.function_run_id(), function_run.id());
        assert_eq!(
            worker_message.function_version_id(),
            function_run.function_version_id()
        );
        assert_eq!(
            *worker_message.message_status(),
            WorkerMessageStatus::Locked
        );
    }
}
