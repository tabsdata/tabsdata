//
// Copyright 2025 Tabs Data Inc.
//

use crate::function_run::layers::resolve_bound_function_run;
use ta_services::factory::service_factory;
use td_authz::AuthzContext;
use td_authz::rbac::{Project, Rbac, Scope, ViewProject};
use td_objects::And;
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::crudl::{ReadRequest, RequestContext};
use td_objects::dxo::execution::ExecutionDB;
use td_objects::dxo::function::FunctionDBWithNames;
use td_objects::dxo::physical_function_run::{
    PhysicalFunctionRun, PhysicalFunctionRunBuilder, PhysicalFunctionRunDBWithNames,
};
use td_objects::rest_urls::PhysicalFunctionRunParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{
    CollectionIdName, ExecutionId, ExecutionIdName, FunctionIdName, FunctionVersionId, ProjectId,
};
use td_queries::By;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = FunctionRunReadService,
    request = ReadRequest<PhysicalFunctionRunParam>,
    response = PhysicalFunctionRun,
    connection = ConnectionProvider,
    scope = "Project",
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<ReadRequest<PhysicalFunctionRunParam>>::extract::<RequestContext>),
        from_fn(
            With::<ReadRequest<PhysicalFunctionRunParam>>::extract_name::<PhysicalFunctionRunParam>
        ),
        from_fn(With::<PhysicalFunctionRunParam>::extract::<CollectionIdName>),
        // find collection (resolves the owning project for the scope)
        from_fn(By::<CollectionIdName>::fetch_one::<CollectionDB>),
        from_fn(With::<CollectionDB>::extract::<ProjectId>),
        // check requester can view the project
        from_fn(Scope::<Project>::set),
        from_fn(Rbac::<Project, ViewProject>::check),
        // find function version ID
        from_fn(With::<PhysicalFunctionRunParam>::extract::<FunctionIdName>),
        from_fn(
            By::<And![CollectionIdName, FunctionIdName]>::fetch_one_current::<
                { FunctionDBWithNames::Executable },
                FunctionDBWithNames,
            >
        ),
        from_fn(With::<FunctionDBWithNames>::extract::<FunctionVersionId>),
        // find execution ID
        from_fn(With::<PhysicalFunctionRunParam>::extract::<ExecutionIdName>),
        from_fn(By::<ExecutionIdName>::fetch_one::<ExecutionDB>),
        from_fn(With::<ExecutionDB>::extract::<ExecutionId>),
        // find function run via LFR pointer: replays mint multiple FRs
        // under one LFR identity; the canonical one is whatever the
        // pointer currently binds to.
        from_fn(resolve_bound_function_run),
        // Build PhysicalFunctionRun
        from_fn(
            With::<PhysicalFunctionRunDBWithNames>::try_convert_to::<PhysicalFunctionRunBuilder, _>
        ),
        from_fn(With::<PhysicalFunctionRunBuilder>::build::<PhysicalFunctionRun>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_database::scoped::InProject;
    use td_error::TdError;
    use td_objects::Buildable;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, FunctionName, LogicalFunctionRunStatus,
        PhysicalFunctionRunStatus, ProjectId, RoleId, TableNameDto, TransactionKey, UserId,
        UserName,
    };
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_read_function_run(db: TdPool) {
        use td_database::scoped::Scoped;
        use td_objects::types::basic::ProjectId;
        use td_tower::metadata::type_of_val;

        FunctionRunReadService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<Scoped<ReadRequest<PhysicalFunctionRunParam>, ProjectId>, PhysicalFunctionRun>(&[
                type_of_val(
                    &With::<ReadRequest<PhysicalFunctionRunParam>>::extract::<RequestContext>,
                ),
                type_of_val(
                    &With::<ReadRequest<PhysicalFunctionRunParam>>::extract_name::<
                        PhysicalFunctionRunParam,
                    >,
                ),
                type_of_val(&With::<PhysicalFunctionRunParam>::extract::<CollectionIdName>),
                // find collection (resolves the owning project for the scope)
                type_of_val(&By::<CollectionIdName>::fetch_one::<CollectionDB>),
                type_of_val(&With::<CollectionDB>::extract::<ProjectId>),
                // check requester can view the project
                type_of_val(&Scope::<Project>::set),
                type_of_val(&Rbac::<Project, ViewProject>::check),
                // find function version ID
                type_of_val(&With::<PhysicalFunctionRunParam>::extract::<FunctionIdName>),
                type_of_val(
                    &By::<And![CollectionIdName, FunctionIdName]>::fetch_one_current::<
                        { FunctionDBWithNames::Executable },
                        FunctionDBWithNames,
                    >,
                ),
                type_of_val(&With::<FunctionDBWithNames>::extract::<FunctionVersionId>),
                // find execution ID
                type_of_val(&With::<PhysicalFunctionRunParam>::extract::<ExecutionIdName>),
                type_of_val(&By::<ExecutionIdName>::fetch_one::<ExecutionDB>),
                type_of_val(&With::<ExecutionDB>::extract::<ExecutionId>),
                // find function run (walks the LFR pointer log to the latest
                // bound PFR for (execution_id, function_version_id))
                type_of_val(&resolve_bound_function_run),
                // Build PhysicalFunctionRun
                type_of_val(
                    &With::<PhysicalFunctionRunDBWithNames>::try_convert_to::<
                        PhysicalFunctionRunBuilder,
                        _,
                    >,
                ),
                type_of_val(&With::<PhysicalFunctionRunBuilder>::build::<PhysicalFunctionRun>),
            ]);
    }

    #[td_test::test(db)]
    async fn test_read_function_run(db: TdPool) -> Result<(), TdError> {
        let collection = db
            .scoped(ProjectId::sandbox())
            .seed()
            .collection(&CollectionName::try_from("collection")?, &UserId::admin())
            .await?;

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
        let function_version = db
            .scoped(ProjectId::sandbox())
            .seed()
            .function(&collection, &create)
            .await?;
        let transaction_key = TransactionKey::try_from("ANY")?;

        let execution = db
            .scoped(ProjectId::sandbox())
            .seed()
            .execution(&function_version)
            .await?;
        let transaction = db
            .scoped(ProjectId::sandbox())
            .seed()
            .transaction(&execution, &transaction_key)
            .await?;
        let physical_function_run = db
            .scoped(ProjectId::sandbox())
            .seed()
            .physical_function_run(
                &collection,
                &function_version,
                &execution,
                &transaction,
                &LogicalFunctionRunStatus::Done,
            )
            .await?;

        // Test
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin())
                .read(
                    PhysicalFunctionRunParam::builder()
                        .collection(CollectionIdName::from_str(&format!("{}", collection.name))?)
                        .function(FunctionIdName::from_str(&format!(
                            "{}",
                            function_version.name
                        ))?)
                        .execution(ExecutionIdName::from_str(&format!("~{}", execution.id))?)
                        .build()?,
                )
                .in_project(ProjectId::sandbox());

        let service = FunctionRunReadService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;
        assert_eq!(response.id, physical_function_run.id);
        assert_eq!(response.collection_id, physical_function_run.collection_id);
        assert_eq!(
            response.function_version_id,
            physical_function_run.function_version_id
        );
        assert_eq!(response.execution_id, physical_function_run.execution_id);
        assert_eq!(
            response.transaction_id,
            physical_function_run.transaction_id
        );
        assert_eq!(response.triggered_on, physical_function_run.triggered_on);
        assert_eq!(response.trigger, physical_function_run.trigger);
        assert_eq!(response.started_on, physical_function_run.started_on);
        assert_eq!(response.ended_on, physical_function_run.ended_on);
        assert_eq!(response.status, PhysicalFunctionRunStatus::Done);
        assert_eq!(response.name, FunctionName::try_from("joaquin")?);
        assert_eq!(response.collection, collection.name);
        assert_eq!(response.execution, execution.name);
        assert_eq!(response.triggered_by, UserName::admin());
        Ok(())
    }
}
