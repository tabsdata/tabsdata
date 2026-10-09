//
// Copyright 2025 Tabsdata Inc.
//

use crate::Queries;
use crate::Writes;
use td_database::access::InProject;
use td_database::domain::Apiserver;
use td_error::TdError;
use td_objects::dxo::collection::ActiveCollectionDB;
use td_objects::dxo::execution::ExecutionDB;
use td_objects::dxo::function::FunctionDB;
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
use td_objects::dxo::transaction::TransactionDB;
use td_objects::dxo::worker::{CronTriggerConfig, WorkerDB, WorkerInsertDB};
use td_objects::types::basic::Enabled;
use td_objects::types::basic::{
    CronMask, WorkerConfig, WorkerId, WorkerMessageStatus, WorkerStatus, WorkerType,
};

pub(crate) async fn seed_worker(
    mut executor: impl Writes<Domain = Apiserver, Scope = InProject>,
    collection: &ActiveCollectionDB,
    function: &FunctionDB,
    worker_type: &WorkerType,
    worker_status: &WorkerStatus,
    message_status: &WorkerMessageStatus,
) -> Result<WorkerDB, TdError> {
    // Production cron-trigger workers always carry a serialized
    // `CronTriggerConfig` (the k8s start path reads the schedule mask
    // from it), so the seed mirrors that.
    let worker_config = match worker_type {
        WorkerType::CronTrigger => Some(WorkerConfig::serialize(CronTriggerConfig {
            start: None,
            end: None,
            mask: CronMask::try_from("* * * * *")?,
        })?),
        _ => None,
    };
    let worker_db = WorkerInsertDB {
        id: WorkerId::new(),
        collection_id: collection.id,
        execution_id: None,
        transaction_id: None,
        physical_function_run_id: None,
        function_version_id: function.base.id.clone(),
        message_status: message_status.clone(),
        started_on: None,
        ended_on: None,
        status: worker_status.clone(),
        worker_type: worker_type.clone(),
        worker_enabled: Enabled::TRUE,
        worker_config,
    };

    executor.reuse().insert(&worker_db).await?;
    Ok(executor.reuse().fetch_one(&worker_db.id).await?)
}

pub(crate) async fn seed_function_runner_worker(
    mut executor: impl Writes<Domain = Apiserver, Scope = InProject>,
    execution: &ExecutionDB,
    transaction: &TransactionDB,
    physical_function_run: &PhysicalFunctionRunDB,
    status: &WorkerMessageStatus,
) -> Result<WorkerDB, TdError> {
    let worker_db = WorkerInsertDB {
        id: WorkerId::new(),
        collection_id: execution.base.collection_id,
        execution_id: Some(execution.base.id.clone()),
        transaction_id: Some(transaction.base.id.clone()),
        physical_function_run_id: Some(physical_function_run.base.id.clone()),
        function_version_id: physical_function_run.base.function_version_id.clone(),
        message_status: status.clone(),
        started_on: None,
        ended_on: None,
        status: WorkerStatus::Requested,
        worker_type: WorkerType::FunctionRunner,
        worker_enabled: Enabled::TRUE,
        worker_config: None,
    };

    executor.reuse().insert(&worker_db).await?;
    Ok(executor.reuse().fetch_one(&worker_db.id).await?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::SeedEntry;
    use td_database::pool::TdPool;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::types::basic::ReuseFrozen;
    use td_objects::types::basic::{
        BundleId, CollectionName, Decorator, LogicalFunctionRunStatus, TransactionKey, UserId,
    };

    #[td_test::test(db)]
    async fn test_seed_worker(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(
                &CollectionName::try_from("collection").unwrap(),
                &UserId::admin(),
            )
            .await
            .unwrap();

        let dependencies = None;
        let triggers = None;
        let tables = None;

        let create = FunctionRegister {
            child_functions: None,
            name: "joaquin".try_into().unwrap(),
            description: "function_foo description".try_into().unwrap(),
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into().unwrap(),
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies,
            triggers,
            tables,
            table_types: None,
            parent_tables: None,
            runtime_values: "mock runtime values".try_into().unwrap(),
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let function_version = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await
            .unwrap();

        let execution = db
            .scoped(p.id)
            .seed()
            .execution(&function_version)
            .await
            .unwrap();

        let transaction_key = TransactionKey::try_from("ANY").unwrap();
        let transaction = db
            .scoped(p.id)
            .seed()
            .transaction(&execution, &transaction_key)
            .await
            .unwrap();

        let physical_function_run = db
            .scoped(p.id)
            .seed()
            .physical_function_run(
                &collection,
                &function_version,
                &execution,
                &transaction,
                &LogicalFunctionRunStatus::Scheduled,
            )
            .await
            .unwrap();

        let worker = db
            .scoped(p.id)
            .seed()
            .function_runner_worker(
                &execution,
                &transaction,
                &physical_function_run,
                &WorkerMessageStatus::Locked,
            )
            .await
            .unwrap();

        assert_eq!(worker.base.collection_id, collection.id);
        assert_eq!(worker.base.execution_id, Some(execution.base.id));
        assert_eq!(worker.base.transaction_id, Some(transaction.base.id));
        assert_eq!(
            worker.base.physical_function_run_id,
            Some(physical_function_run.base.id)
        );
        assert_eq!(
            worker.base.function_version_id,
            physical_function_run.base.function_version_id
        );
        assert_eq!(worker.base.message_status, WorkerMessageStatus::Locked);
    }
}
