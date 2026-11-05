//
// Copyright 2025 Tabsdata Inc.
//

use crate::Queries;
use crate::Writes;
use crate::test_utils::seed_logical_function_run::seed_logical_function_run;
use td_database::access::{Holds, InProject, LockInstant, ProjectLock};
use td_database::domain::Apiserver;
use td_error::TdError;
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::execution::ExecutionDB;
use td_objects::dxo::function::FunctionDB;
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
use td_objects::dxo::physical_function_run::PhysicalFunctionRunInsertDB;
use td_objects::dxo::transaction::TransactionDB;
use td_objects::types::basic::{
    LogicalFunctionRunStatus, PhysicalFunctionRunId, PhysicalFunctionRunStatus, Trigger,
};

pub(crate) async fn seed_physical_function_run(
    mut executor: impl Writes<Domain = Apiserver, Scope = InProject, Mode: Holds<ProjectLock>>,
    collection: &CollectionDB,
    function_version: &FunctionDB,
    execution: &ExecutionDB,
    transaction: &TransactionDB,
    status: &LogicalFunctionRunStatus,
    attempt_status: &PhysicalFunctionRunStatus,
) -> Result<PhysicalFunctionRunDB, TdError> {
    let logical = seed_logical_function_run(
        executor.reuse(),
        collection,
        function_version,
        execution,
        transaction,
        status,
    )
    .await?;

    let pfr_id = PhysicalFunctionRunId::new();
    let function_db = PhysicalFunctionRunInsertDB {
        id: pfr_id.clone(),
        collection_id: collection.collection_id,
        function_id: function_version.function_id,
        function_version_id: function_version.id.clone(),
        execution_id: execution.id.clone(),
        transaction_id: transaction.id.clone(),
        triggered_by_id: transaction.triggered_by_id,
        trigger: Trigger::Manual,
        base_path: function_version
            .base
            .base_path
            .clone()
            .or_else(|| collection.base_path.clone())
            .unwrap_or_default(),
        dispatched_on: None,
        started_on: None,
        ended_on: None,
        status: *attempt_status,
        function_version_patch_id: None,
        logical_function_run_id: logical.id.clone(),
    };

    executor
        .reuse()
        .scd2()
        .open(
            &function_db,
            LockInstant::asserted(*transaction.triggered_on),
        )
        .await?;

    Ok(executor
        .reuse()
        .timeline()
        .fetch_one(&function_db.id)
        .await?)
}

#[cfg(test)]
mod tests {
    use crate::test_utils::SeedEntry;
    use td_database::pool::TdPool;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::types::basic::ReuseFrozen;
    use td_objects::types::basic::{
        BundleId, CollectionName, Decorator, LogicalFunctionRunStatus, TransactionKey, Trigger,
        UserId,
    };
    use td_security::ENCODED_ID_SYSTEM;

    #[td_test::test(db)]
    async fn test_seed_function_run(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        let collection = db
            .scoped(p.project_id)
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
            .scoped(p.project_id)
            .seed()
            .function(&collection, &create)
            .await
            .unwrap();

        let execution = db
            .scoped(p.project_id)
            .seed()
            .execution(&function_version)
            .await
            .unwrap();

        let transaction_key = TransactionKey::try_from("ANY").unwrap();
        let transaction = db
            .scoped(p.project_id)
            .seed()
            .transaction(&execution, &transaction_key)
            .await
            .unwrap();

        let physical_function_run = db
            .scoped(p.project_id)
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

        assert_eq!(
            physical_function_run.collection_id,
            collection.collection_id
        );
        assert_eq!(
            physical_function_run.function_version_id,
            function_version.id
        );
        assert_eq!(physical_function_run.execution_id, execution.id);
        assert_eq!(physical_function_run.transaction_id, transaction.id);
        assert_eq!(
            *physical_function_run.interval.valid_from,
            *transaction.triggered_on
        );
        assert_eq!(physical_function_run.trigger, Trigger::Manual);
        // Status lives on the LFR; PhysicalFunctionRunDB no longer has it.
        let _ = LogicalFunctionRunStatus::Scheduled;
    }
}
