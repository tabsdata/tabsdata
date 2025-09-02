//
// Copyright 2025 Tabs Data Inc.
//

use td_authz::{Authz, AuthzContext};
use td_objects::crudl::{ReadRequest, RequestContext};
use td_objects::rest_urls::FunctionRunParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{AuthzOn, CollAdmin, CollDev, CollExec, CollRead};
use td_objects::tower_service::from::{
    BuildService, ExtractNameService, ExtractService, TryIntoService, With, combine,
};
use td_objects::tower_service::sql::{By, SqlSelectService};
use td_objects::types::basic::{
    AtTime, CollectionId, CollectionIdName, ExecutionId, ExecutionIdName, FunctionIdName,
    FunctionStatus, FunctionVersionId,
};
use td_objects::types::collection::CollectionDB;
use td_objects::types::execution::{
    ExecutionDB, FunctionRun, FunctionRunBuilder, FunctionRunDBWithNames,
};
use td_objects::types::function::FunctionDBWithNames;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::{layers, service_factory};

#[service_factory(
    name = FunctionRunReadService,
    request = ReadRequest<FunctionRunParam>,
    response = FunctionRun,
    connection = ConnectionProvider,
    context = DaoQueries,
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<ReadRequest<FunctionRunParam>>::extract::<RequestContext>),
        from_fn(With::<ReadRequest<FunctionRunParam>>::extract_name::<FunctionRunParam>),
        from_fn(With::<FunctionRunParam>::extract::<CollectionIdName>),
        // find collection ID
        from_fn(By::<CollectionIdName>::select::<CollectionDB>),
        from_fn(With::<CollectionDB>::extract::<CollectionId>),
        // check requester has collection permissions
        from_fn(AuthzOn::<CollectionId>::set),
        from_fn(Authz::<CollAdmin, CollDev, CollExec, CollRead>::check),
        // find function version ID
        from_fn(With::<FunctionRunParam>::extract::<FunctionIdName>),
        from_fn(combine::<CollectionIdName, FunctionIdName>),
        from_fn(With::<RequestContext>::extract::<AtTime>),
        from_fn(FunctionStatus::active),
        from_fn(By::<(CollectionIdName, FunctionIdName)>::select_version::<FunctionDBWithNames>),
        from_fn(With::<FunctionDBWithNames>::extract::<FunctionVersionId>),
        // find execution ID
        from_fn(With::<FunctionRunParam>::extract::<ExecutionIdName>),
        from_fn(By::<ExecutionIdName>::select::<ExecutionDB>),
        from_fn(With::<ExecutionDB>::extract::<ExecutionId>),
        // find function run
        from_fn(combine::<ExecutionId, FunctionVersionId>),
        from_fn(By::<(ExecutionId, FunctionVersionId)>::select::<FunctionRunDBWithNames>),
        // Build FunctionRun
        from_fn(With::<FunctionRunDBWithNames>::convert_to::<FunctionRunBuilder, _>),
        from_fn(With::<FunctionRunBuilder>::build::<FunctionRun, _>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_objects::crudl::RequestContext;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_execution::seed_execution;
    use td_objects::test_utils::seed_function::seed_function;
    use td_objects::test_utils::seed_function_run::seed_function_run;
    use td_objects::test_utils::seed_transaction::seed_transaction;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, FunctionName, FunctionRunStatus,
        RoleId, TableNameDto, TransactionKey, UserId, UserName,
    };
    use td_objects::types::function::FunctionRegister;
    use td_tower::ctx_service::RawOneshot;
    use td_tower::td_service::TdService;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_read_function_run(db: DbPool) {
        use td_tower::metadata::type_of_val;

        FunctionRunReadService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<ReadRequest<FunctionRunParam>, FunctionRun>(&[
                type_of_val(&With::<ReadRequest<FunctionRunParam>>::extract::<RequestContext>),
                type_of_val(&With::<ReadRequest<FunctionRunParam>>::extract_name::<FunctionRunParam>),
                type_of_val(&With::<FunctionRunParam>::extract::<CollectionIdName>),
                // find collection ID
                type_of_val(&By::<CollectionIdName>::select::<CollectionDB>),
                type_of_val(&With::<CollectionDB>::extract::<CollectionId>),
                // check requester has collection permissions
                type_of_val(&AuthzOn::<CollectionId>::set),
                type_of_val(&Authz::<CollAdmin, CollDev, CollExec, CollRead>::check),
                // find function version ID
                type_of_val(&With::<FunctionRunParam>::extract::<FunctionIdName>),
                type_of_val(&combine::<CollectionIdName, FunctionIdName>),
                type_of_val(&With::<RequestContext>::extract::<AtTime>),
                type_of_val(&FunctionStatus::active),
                type_of_val(
                    &By::<(CollectionIdName, FunctionIdName)>::select_version::<FunctionDBWithNames>,
                ),
                type_of_val(&With::<FunctionDBWithNames>::extract::<FunctionVersionId>),
                // find execution ID
                type_of_val(&With::<FunctionRunParam>::extract::<ExecutionIdName>),
                type_of_val(&By::<ExecutionIdName>::select::<ExecutionDB>),
                type_of_val(&With::<ExecutionDB>::extract::<ExecutionId>),
                // find function run
                type_of_val(&combine::<ExecutionId, FunctionVersionId>),
                type_of_val(&By::<(ExecutionId, FunctionVersionId)>::select::<FunctionRunDBWithNames>),
                // Build FunctionRun
                type_of_val(&With::<FunctionRunDBWithNames>::convert_to::<FunctionRunBuilder, _>),
                type_of_val(&With::<FunctionRunBuilder>::build::<FunctionRun, _>),
            ]);
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_read_function_run(db: DbPool) -> Result<(), TdError> {
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
                FunctionRunParam::builder()
                    .try_collection(format!("{}", collection.name()))?
                    .try_function(format!("{}", function_version.name()))?
                    .try_execution(format!("~{}", execution.id()))?
                    .build()?,
            );

        let service = FunctionRunReadService::with_defaults(db.clone())
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        let response = response?;
        assert_eq!(response.id(), function_run.id());
        assert_eq!(response.collection_id(), function_run.collection_id());
        assert_eq!(
            response.function_version_id(),
            function_run.function_version_id()
        );
        assert_eq!(response.execution_id(), function_run.execution_id());
        assert_eq!(response.transaction_id(), function_run.transaction_id());
        assert_eq!(response.triggered_on(), function_run.triggered_on());
        assert_eq!(response.trigger(), function_run.trigger());
        assert_eq!(response.started_on(), function_run.started_on());
        assert_eq!(response.ended_on(), function_run.ended_on());
        assert_eq!(response.status(), function_run.status());
        assert_eq!(*response.name(), FunctionName::try_from("joaquin")?);
        assert_eq!(response.collection(), collection.name());
        assert_eq!(response.execution(), execution.name());
        assert_eq!(*response.triggered_by(), UserName::admin());
        Ok(())
    }
}
