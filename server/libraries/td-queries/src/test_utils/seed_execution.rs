//
// Copyright 2025 Tabsdata Inc.
//

use crate::Queries;
use crate::Writes;
use td_common::datetime::Timestamp;
use td_database::access::InProject;
use td_database::domain::Apiserver;
use td_error::TdError;
use td_objects::dxo::execution::ExecutionDB;
use td_objects::dxo::execution::ExecutionInsertDB;
use td_objects::dxo::function::FunctionDB;
use td_objects::types::basic::{ExecutionId, ExecutionName, TriggerType, TriggeredOn, UserId};

pub(crate) async fn seed_execution(
    executor: impl Writes<Domain = Apiserver, Scope = InProject>,
    function_version: &FunctionDB,
) -> Result<ExecutionDB, TdError> {
    _seed_execution(executor, function_version, TriggerType::Manual).await
}

pub(crate) async fn seed_cron_execution(
    executor: impl Writes<Domain = Apiserver, Scope = InProject>,
    function_version: &FunctionDB,
) -> Result<ExecutionDB, TdError> {
    _seed_execution(executor, function_version, TriggerType::CronTrigger).await
}

async fn _seed_execution(
    mut executor: impl Writes<Domain = Apiserver, Scope = InProject>,
    function_version: &FunctionDB,
    trigger_type: TriggerType,
) -> Result<ExecutionDB, TdError> {
    let execution_db = ExecutionInsertDB {
        id: ExecutionId::new(),
        name: Some(ExecutionName::try_from("test_execution")?),
        collection_id: function_version.base.collection_id,
        function_id: function_version.base.function_id,
        function_version_id: function_version.base.id.clone(),
        triggered_on: TriggeredOn::asserted(Timestamp::default()),
        triggered_by_id: UserId::admin(),
        trigger_type,
        external_uid: None,
        recursion_options: None,
    };

    // Field-less row; runs on a project-scoped connection so `project_id`
    // defaults from the scope.
    executor.reuse().insert(&execution_db).await?;
    Ok(executor.reuse().fetch_one(&execution_db.id).await?)
}

#[cfg(test)]
mod tests {
    use crate::test_utils::SeedEntry;
    use td_common::datetime::Timestamp;
    use td_database::pool::TdPool;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::types::basic::ReuseFrozen;
    use td_objects::types::basic::{
        BundleId, CollectionName, Decorator, ExecutionName, TriggeredOn, UserId,
    };
    use td_security::ENCODED_ID_SYSTEM;

    #[td_test::test(db)]
    async fn test_seed_execution(db: TdPool) {
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
            parent_function: None,
            name: "joaquin".try_into().unwrap(),
            description: "function_foo description".try_into().unwrap(),
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into().unwrap(),
            decorator: Decorator::Publisher,
            function_type: None,
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
        assert_eq!(
            execution.base.name,
            Some(ExecutionName::try_from("test_execution").unwrap())
        );
        assert_eq!(execution.base.collection_id, collection.id);
        assert_eq!(execution.base.function_version_id, function_version.base.id);
        assert_eq!(
            **execution.base.triggered_by_id,
            **function_version.base.defined_by_id
        );
        assert!(execution.base.triggered_on < TriggeredOn::asserted(Timestamp::default()));
    }
}
