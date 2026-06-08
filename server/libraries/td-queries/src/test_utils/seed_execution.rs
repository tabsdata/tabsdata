//
// Copyright 2025 Tabs Data Inc.
//

use crate::LiveEntry;
use crate::QueryExecutor;
use ta_objects::Buildable;
use td_error::TdError;
use td_objects::dxo::execution::ExecutionDB;
use td_objects::dxo::function::FunctionDB;
use td_objects::types::basic::{ExecutionName, TriggerType, TriggeredOn, UserId};

pub(crate) async fn seed_execution(
    executor: impl QueryExecutor,
    function_version: &FunctionDB,
) -> Result<ExecutionDB, TdError> {
    _seed_execution(executor, function_version, TriggerType::Manual).await
}

pub(crate) async fn seed_cron_execution(
    executor: impl QueryExecutor,
    function_version: &FunctionDB,
) -> Result<ExecutionDB, TdError> {
    _seed_execution(executor, function_version, TriggerType::CronTrigger).await
}

async fn _seed_execution(
    executor: impl QueryExecutor,
    function_version: &FunctionDB,
    trigger_type: TriggerType,
) -> Result<ExecutionDB, TdError> {
    let execution_db = ExecutionDB::builder()
        .name(ExecutionName::try_from("test_execution")?)
        .collection_id(function_version.collection_id)
        .function_id(function_version.function_id)
        .function_version_id(&function_version.id)
        .triggered_on(TriggeredOn::now())
        .triggered_by_id(UserId::admin())
        .recursion_options(None)
        .trigger_type(trigger_type)
        .build()?;

    // Field-less row; runs on a project-scoped connection so `project_id`
    // defaults from the scope.
    executor.live().insert::<ExecutionDB>(&execution_db).await?;
    Ok(execution_db)
}

#[cfg(test)]
mod tests {
    use crate::test_utils::SeedEntry;
    use ta_objects::Buildable;
    use td_database::pool::TdPool;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::types::basic::{
        BundleId, CollectionName, Decorator, ExecutionName, ProjectId, TriggeredOn, UserId,
    };
    use td_security::ENCODED_ID_SYSTEM;

    #[td_test::test(db)]
    async fn test_seed_execution(db: TdPool) {
        let collection = db
            .scoped(ProjectId::sandbox())
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

        let function_version = db
            .scoped(ProjectId::sandbox())
            .seed()
            .function(&collection, &create)
            .await
            .unwrap();

        let execution = db
            .scoped(ProjectId::sandbox())
            .seed()
            .execution(&function_version)
            .await
            .unwrap();
        assert_eq!(
            execution.name,
            Some(ExecutionName::try_from("test_execution").unwrap())
        );
        assert_eq!(execution.collection_id, collection.id);
        assert_eq!(execution.function_version_id, function_version.id);
        assert_eq!(
            **execution.triggered_by_id,
            **function_version.defined_by_id
        );
        assert!(execution.triggered_on < TriggeredOn::now());
    }
}
