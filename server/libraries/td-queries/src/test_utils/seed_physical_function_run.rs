//
// Copyright 2025 Tabsdata Inc.
//

use crate::Queries;
use crate::Writes;
use crate::test_utils::seed_logical_function_run::seed_logical_function_run;
use td_database::access::{InProject, LockInstant};
use td_database::domain::Apiserver;
use td_error::TdError;
use td_objects::dxo::collection::ActiveCollectionDB;
use td_objects::dxo::execution::ExecutionDB;
use td_objects::dxo::function::FunctionDB;
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
use td_objects::dxo::physical_function_run::PhysicalFunctionRunInsertDB;
use td_objects::dxo::transaction::TransactionDB;
use td_objects::types::basic::{
    LogicalFunctionRunStatus, PhysicalFunctionRunId, PhysicalFunctionRunStatus, Trigger,
};

pub(crate) async fn seed_physical_function_run(
    mut executor: impl Writes<Domain = Apiserver, Scope = InProject>,
    collection: &ActiveCollectionDB,
    function_version: &FunctionDB,
    execution: &ExecutionDB,
    transaction: &TransactionDB,
    status: &LogicalFunctionRunStatus,
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
        collection_id: collection.id,
        function_id: function_version.base.function_id,
        function_version_id: function_version.base.id.clone(),
        execution_id: execution.base.id.clone(),
        transaction_id: transaction.base.id.clone(),
        triggered_by_id: transaction.base.triggered_by_id,
        trigger: Trigger::Manual,
        base_path: function_version
            .base
            .base_path
            .clone()
            .unwrap_or_else(|| collection.effective_base_path.clone()),
        dispatched_on: None,
        started_on: None,
        ended_on: None,
        status: PhysicalFunctionRunStatus::from(status),
        function_version_patch_id: None,
        logical_function_run_id: logical.base.id.clone(),
    };

    executor
        .reuse()
        .scd2()
        .open(
            &function_db,
            LockInstant::asserted(*transaction.base.triggered_on),
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
            .scoped(p.id)
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

        assert_eq!(physical_function_run.base.collection_id, collection.id);
        assert_eq!(
            physical_function_run.base.function_version_id,
            function_version.base.id
        );
        assert_eq!(physical_function_run.base.execution_id, execution.base.id);
        assert_eq!(
            physical_function_run.base.transaction_id,
            transaction.base.id
        );
        assert_eq!(
            *physical_function_run.interval.valid_from,
            *transaction.base.triggered_on
        );
        assert_eq!(physical_function_run.base.trigger, Trigger::Manual);
        // Status lives on the LFR; PhysicalFunctionRunDB no longer has it.
        let _ = LogicalFunctionRunStatus::Scheduled;
    }
}
