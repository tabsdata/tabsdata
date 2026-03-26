//
// Copyright 2025 Tabs Data Inc.
//

use crate::BasicQueries;
use ta_objects::Buildable;
use td_database::sea_query::executor::SeaExecutor;
use td_error::TdError;
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::execution::ExecutionDB;
use td_objects::dxo::function::FunctionDB;
use td_objects::dxo::function_run::FunctionRunDB;
use td_objects::dxo::transaction::TransactionDB;
use td_objects::dxo::worker::WorkerDB;
use td_objects::types::basic::{WorkerMessageStatus, WorkerStatus, WorkerType};

pub(crate) async fn seed_worker(
    db: impl SeaExecutor + Send,
    collection: &CollectionDB,
    function: &FunctionDB,
    worker_type: &WorkerType,
    worker_status: &WorkerStatus,
    message_status: &WorkerMessageStatus,
) -> Result<WorkerDB, TdError> {
    let worker_db = WorkerDB::builder()
        .collection_id(collection.id)
        .execution_id(None)
        .transaction_id(None)
        .function_run_id(None)
        .function_version_id(&function.id)
        .message_status(message_status)
        .started_on(None)
        .ended_on(None)
        .status(worker_status)
        .worker_enabled(true)
        .worker_type(worker_type)
        .worker_config(None)
        .build()?;

    db.insert::<WorkerDB>(&worker_db).await?;
    Ok(worker_db)
}

pub(crate) async fn seed_function_runner_worker(
    db: impl SeaExecutor + Send,
    execution: &ExecutionDB,
    transaction: &TransactionDB,
    function_run: &FunctionRunDB,
    status: &WorkerMessageStatus,
) -> Result<WorkerDB, TdError> {
    let worker_db = WorkerDB::builder()
        .collection_id(execution.collection_id)
        .execution_id(Some(execution.id.clone()))
        .transaction_id(Some(transaction.id.clone()))
        .function_run_id(function_run.id)
        .function_version_id(&function_run.function_version_id)
        .message_status(status)
        .started_on(None)
        .ended_on(None)
        .status(WorkerStatus::RunRequested)
        .worker_enabled(true)
        .worker_type(WorkerType::FunctionRunner)
        .worker_config(None)
        .build()?;

    db.insert::<WorkerDB>(&worker_db).await?;
    Ok(worker_db)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::SeedQueries;
    use td_database::pool::TdPool;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::types::basic::{
        BundleId, CollectionName, Decorator, FunctionRunStatus, TransactionKey, UserId,
    };

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_seed_worker(db: TdPool) {
        let collection = db
            .seed_collection(
                &CollectionName::try_from("collection").unwrap(),
                &UserId::admin(),
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

        let function_version = db.seed_function(&collection, &create).await.unwrap();

        let execution = db.seed_execution(&function_version).await.unwrap();

        let transaction_key = TransactionKey::try_from("ANY").unwrap();
        let transaction = db
            .seed_transaction(&execution, &transaction_key)
            .await
            .unwrap();

        let function_run = db
            .seed_function_run(
                &collection,
                &function_version,
                &execution,
                &transaction,
                &FunctionRunStatus::Scheduled,
            )
            .await
            .unwrap();

        let worker = db
            .seed_function_runner_worker(
                &execution,
                &transaction,
                &function_run,
                &WorkerMessageStatus::Locked,
            )
            .await
            .unwrap();

        assert_eq!(worker.collection_id, collection.id);
        assert_eq!(worker.execution_id, Some(execution.id));
        assert_eq!(worker.transaction_id, Some(transaction.id));
        assert_eq!(worker.function_run_id, Some(function_run.id));
        assert_eq!(worker.function_version_id, function_run.function_version_id);
        assert_eq!(worker.message_status, WorkerMessageStatus::Locked);
    }
}
