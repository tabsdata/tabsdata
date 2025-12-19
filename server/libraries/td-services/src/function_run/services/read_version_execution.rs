//
// Copyright 2025 Tabs Data Inc.
//

use ta_services::factory::service_factory;
use td_objects::dxo::crudl::ReadRequest;
use td_objects::dxo::execution::ExecutionDB;
use td_objects::dxo::function_run::{FunctionRun, FunctionRunBuilder, FunctionRunDBWithNames};
use td_objects::rest_urls::internal::FunctionVersionExecution;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::from::{
    BuildService, ExtractNameService, ExtractService, TryIntoService, With, combine,
};
use td_objects::tower_service::sql::{By, SqlSelectService};
use td_objects::types::basic::{ExecutionId, ExecutionIdName, FunctionVersionId};
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

// Similar to FunctionRunReadService, but with different parameters (function_version + execution)
#[service_factory(
    name = FunctionVersionExecutionReadService,
    request = ReadRequest<FunctionVersionExecution>,
    response = FunctionRun,
    connection = ConnectionProvider,
    context = DaoQueries,
)]
fn service() {
    layers!(
        from_fn(
            With::<ReadRequest<FunctionVersionExecution>>::extract_name::<FunctionVersionExecution>
        ),
        from_fn(With::<FunctionVersionExecution>::extract::<FunctionVersionId>),
        from_fn(With::<FunctionVersionExecution>::extract::<ExecutionIdName>),
        // find execution ID
        from_fn(By::<ExecutionIdName>::select::<ExecutionDB>),
        from_fn(With::<ExecutionDB>::extract::<ExecutionId>),
        // find function run
        from_fn(combine::<ExecutionId, FunctionVersionId>),
        from_fn(By::<(ExecutionId, FunctionVersionId)>::select::<FunctionRunDBWithNames>),
        // Build FunctionRun
        from_fn(With::<FunctionRunDBWithNames>::try_convert_to::<FunctionRunBuilder, _>),
        from_fn(With::<FunctionRunBuilder>::build::<FunctionRun, _>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use ta_services::service::TdService;
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_execution::seed_execution;
    use td_objects::test_utils::seed_function::seed_function;
    use td_objects::test_utils::seed_function_run::seed_function_run;
    use td_objects::test_utils::seed_transaction::seed_transaction;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, FunctionName, FunctionRunStatus,
        RoleId, TableNameDto, TransactionKey, UserId, UserName,
    };
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_read_version_execution(db: DbPool) {
        use td_tower::metadata::type_of_val;

        FunctionVersionExecutionReadService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<ReadRequest<FunctionVersionExecution>, FunctionRun>(&[
                type_of_val(
                    &With::<ReadRequest<FunctionVersionExecution>>::extract_name::<
                        FunctionVersionExecution,
                    >,
                ),
                type_of_val(&With::<FunctionVersionExecution>::extract::<FunctionVersionId>),
                type_of_val(&With::<FunctionVersionExecution>::extract::<ExecutionIdName>),
                // find execution ID
                type_of_val(&By::<ExecutionIdName>::select::<ExecutionDB>),
                type_of_val(&With::<ExecutionDB>::extract::<ExecutionId>),
                // find function run
                type_of_val(&combine::<ExecutionId, FunctionVersionId>),
                type_of_val(
                    &By::<(ExecutionId, FunctionVersionId)>::select::<FunctionRunDBWithNames>,
                ),
                // Build FunctionRun
                type_of_val(
                    &With::<FunctionRunDBWithNames>::try_convert_to::<FunctionRunBuilder, _>,
                ),
                type_of_val(&With::<FunctionRunBuilder>::build::<FunctionRun, _>),
            ]);
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_read_function_version_execution_run(db: DbPool) -> Result<(), TdError> {
        let collection = seed_collection(
            &db,
            &CollectionName::try_from("collection")?,
            &UserId::admin(),
        )
        .await;

        let dependencies = None;
        let triggers = None;
        let tables = vec![TableNameDto::try_from("table_version")?];
        let create = FunctionRegister::builder()
            .try_name("joaquin")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(dependencies)
            .triggers(triggers)
            .tables(tables.clone())
            .try_runtime_values("mock runtime values")?
            .reuse_frozen_tables(false)
            .build()?;
        let function_version = seed_function(&db, &collection, &create).await;
        let transaction_key = TransactionKey::try_from("ANY")?;

        let execution = seed_execution(&db, &function_version).await;
        let transaction = seed_transaction(&db, &execution, &transaction_key).await;
        let function_run = seed_function_run(
            &db,
            &collection,
            &function_version,
            &execution,
            &transaction,
            &FunctionRunStatus::Done,
        )
        .await;

        // Test
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).read(
                FunctionVersionExecution::builder()
                    .function_version_id(function_version.id)
                    .try_execution(format!("~{}", execution.id))?
                    .build()?,
            );

        let service = FunctionVersionExecutionReadService::with_defaults(db.clone())
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        let response = response?;
        assert_eq!(response.id, function_run.id);
        assert_eq!(response.collection_id, function_run.collection_id);
        assert_eq!(
            response.function_version_id,
            function_run.function_version_id
        );
        assert_eq!(response.execution_id, function_run.execution_id);
        assert_eq!(response.transaction_id, function_run.transaction_id);
        assert_eq!(response.triggered_on, function_run.triggered_on);
        assert_eq!(response.trigger, function_run.trigger);
        assert_eq!(response.started_on, function_run.started_on);
        assert_eq!(response.ended_on, function_run.ended_on);
        assert_eq!(response.status, function_run.status);
        assert_eq!(response.name, FunctionName::try_from("joaquin")?);
        assert_eq!(response.collection, collection.name);
        assert_eq!(response.execution, execution.name);
        assert_eq!(response.triggered_by, UserName::admin());
        Ok(())
    }
}
