//
// Copyright 2025 Tabs Data Inc.
//

use crate::execution::layers::execute;
use crate::execution::layers::execution_version::assert_function_version;
use ta_services::factory::service_factory;
use td_authz::AuthzContext;
use td_execution::TransactionBy;
use td_objects::dxo::crudl::{CreateRequest, RequestContext};
use td_objects::dxo::execution::{ExecutionRequest, ExecutionResponse};
use td_objects::dxo::function::FunctionDBWithNames;
use td_objects::rest_urls::internal::FunctionVersionIdParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::from::{
    ExtractDataService, ExtractNameService, ExtractService, With,
};
use td_objects::tower_service::sql::{By, SqlSelectService};
use td_objects::types::basic::{AtTime, FunctionId, FunctionVersionId};
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = ExecuteFunctionVersionService,
    request = CreateRequest<FunctionVersionIdParam, ExecutionRequest>,
    response = ExecutionResponse,
    connection = TransactionProvider,
    context = DaoQueries,
    context = AuthzContext,
    context = TransactionBy,
)]
fn service() {
    layers!(
        // Extract from request.
        from_fn(
            With::<CreateRequest<FunctionVersionIdParam, ExecutionRequest>>::extract::<
                RequestContext,
            >
        ),
        from_fn(
            With::<CreateRequest<FunctionVersionIdParam, ExecutionRequest>>::extract_name::<
                FunctionVersionIdParam,
            >
        ),
        from_fn(
            With::<CreateRequest<FunctionVersionIdParam, ExecutionRequest>>::extract_data::<
                ExecutionRequest,
            >
        ),
        from_fn(With::<RequestContext>::extract::<AtTime>),
        from_fn(With::<FunctionVersionIdParam>::extract::<FunctionVersionId>),
        // Select trigger function.
        from_fn(By::<FunctionVersionId>::select::<FunctionDBWithNames>),
        // Ensure the trigger function is the current active version (never allow older versions to execute)
        from_fn(With::<FunctionDBWithNames>::extract::<FunctionId>),
        from_fn(
            By::<FunctionId>::select_version::<{ FunctionDBWithNames::All }, FunctionDBWithNames>
        ),
        from_fn(assert_function_version),
        execute(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::function::services::update::UpdateFunctionService;
    use std::collections::HashSet;
    use ta_services::service::TdService;
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_objects::dxo::function::{FunctionRegister, FunctionUpdate};
    use td_objects::rest_urls::FunctionParam;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_function::seed_function;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, ExecutionName, FunctionName,
        FunctionRuntimeValues, RoleId, TableNameDto, UserId,
    };
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_execute_version(db: DbPool) {
        use crate::execution::layers::GraphOpts;
        use crate::execution::layers::execution::{
            assert_function_status, build_execution_plan, build_function_requirements,
            build_function_runs, build_response, build_table_data_versions, build_transaction_map,
            build_transactions, resolve_execution_plan_versions,
            update_initial_function_run_status, validate_execution_plan, with_recursion_options,
        };
        use crate::graph::layers::FunctionOrTable;
        use crate::graph::layers::build::GraphBuilder;
        use td_authz::Authz;
        use td_objects::dxo::crudl::RequestContext;
        use td_objects::dxo::dependency::DependencyDBWithNames;
        use td_objects::dxo::execution::{ExecutionDB, ExecutionDBBuilder, ExecutionRequest};
        use td_objects::dxo::function::FunctionDBWithNames;
        use td_objects::dxo::function_requirement::FunctionRequirementDB;
        use td_objects::dxo::function_run::{FunctionRunDB, FunctionRunDBBuilder};
        use td_objects::dxo::inter_collection_access::{
            InterCollectionAccess, InterCollectionAccessBuilder,
        };
        use td_objects::dxo::table_data_version::TableDataVersionDB;
        use td_objects::dxo::transaction::{TransactionDB, TransactionDBBuilder};
        use td_objects::dxo::trigger::TriggerDBWithNames;
        use td_objects::tower_service::authz::InterColl;
        use td_objects::tower_service::authz::{AuthzOn, CollAdmin, CollExec};
        use td_objects::tower_service::from::{
            BuildService, ExtractService, IntoService, SetService, TryIntoService, UpdateService,
            With,
        };
        use td_objects::tower_service::from::{ConvertIntoMapService, VecBuildService};
        use td_objects::tower_service::sql::{insert, insert_vec};
        use td_objects::types::basic::{CollectionId, FunctionId, FunctionOnly};
        use td_objects::types::recursion::RecursionOptions;
        use td_tower::metadata::type_of_val;

        ExecuteFunctionVersionService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<CreateRequest<FunctionVersionIdParam, ExecutionRequest>, ExecutionResponse>(&[
                // Extract from request.
                type_of_val(&
                    With::<CreateRequest<FunctionVersionIdParam, ExecutionRequest>>::extract::<
                        RequestContext,
                    >
                ),
                type_of_val(&
                    With::<CreateRequest<FunctionVersionIdParam, ExecutionRequest>>::extract_name::<
                        FunctionVersionIdParam,
                    >
                ),
                type_of_val(&
                    With::<CreateRequest<FunctionVersionIdParam, ExecutionRequest>>::extract_data::<
                        ExecutionRequest,
                    >
                ),
                type_of_val(&With::<RequestContext>::extract::<AtTime>),
                type_of_val(&With::<FunctionVersionIdParam>::extract::<FunctionVersionId>),
                // Select trigger function.
                type_of_val(&By::<FunctionVersionId>::select::<FunctionDBWithNames>),
                // Ensure the trigger function is the current active version (never allow older versions to execute)
                type_of_val(&With::<FunctionDBWithNames>::extract::<FunctionId>),
                type_of_val(&
                    By::<FunctionId>::select_version::<{ FunctionDBWithNames::All }, FunctionDBWithNames>
                ),
                type_of_val(&assert_function_version),
                type_of_val(&assert_function_status),
                // check requester is coll_admin or coll_exec for the function's collection
                type_of_val(&With::<FunctionDBWithNames>::extract::<CollectionId>),
                type_of_val(&AuthzOn::<CollectionId>::set),
                type_of_val(&Authz::<CollAdmin, CollExec>::check),
                // Get recursion options
                type_of_val(&With::<FunctionDBWithNames>::extract::<FunctionId>),
                type_of_val(&With::<FunctionId>::convert_to::<FunctionOrTable>),
                type_of_val(&With::<ExecutionRequest>::extract::<FunctionOnly>),
                type_of_val(&with_recursion_options),
                // Find td graph
                // Find triggers
                type_of_val(&GraphBuilder::<GraphOpts, _, _, _, _>::find_recursive_trigger_graph),
                // inter collection authz check
                type_of_val(
                    &With::<TriggerDBWithNames>::vec_convert_to::<InterCollectionAccessBuilder, _>,
                ),
                type_of_val(
                    &With::<InterCollectionAccessBuilder>::vec_build::<InterCollectionAccess, _>,
                ),
                type_of_val(&Authz::<InterColl>::check_inter_collection),
                // Find dependencies. For clarity, we always show the function, even if
                // generating a table centered graph, the same way we show the tables of
                // a function centered graph.
                type_of_val(&GraphBuilder::<GraphOpts, _, _, _, _>::main_function_id),
                type_of_val(&GraphBuilder::<GraphOpts, _, _, _, _>::unique_triggered_function_ids),
                type_of_val(
                    &GraphBuilder::<GraphOpts, _, _, _, _>::find_triggered_functions_dependency_graph,
                ),
                // inter collection authz check
                type_of_val(
                    &With::<DependencyDBWithNames>::vec_convert_to::<InterCollectionAccessBuilder, _>,
                ),
                type_of_val(
                    &With::<InterCollectionAccessBuilder>::vec_build::<InterCollectionAccess, _>,
                ),
                type_of_val(&Authz::<InterColl>::check_inter_collection),
                // Find output tables
                type_of_val(&GraphBuilder::<GraphOpts, _, _, _, _>::find_functions_output_tables),
                // Find all tables and functions
                type_of_val(&GraphBuilder::<GraphOpts, _, _, _, _>::find_all_tables_map),
                type_of_val(&GraphBuilder::<GraphOpts, _, _, _, _>::find_all_functions_map),
                // Create td graph and validate it
                type_of_val(&GraphBuilder::<GraphOpts, _, _, _, _>::build_td_graph),
                // Build execution plan
                type_of_val(&build_execution_plan),
                type_of_val(&validate_execution_plan),
                // Build execution
                type_of_val(&With::<FunctionDBWithNames>::try_convert_to::<ExecutionDBBuilder, _>),
                type_of_val(&With::<RequestContext>::update::<ExecutionDBBuilder, _>),
                type_of_val(&With::<ExecutionRequest>::update::<ExecutionDBBuilder, _>),
                type_of_val(&With::<RecursionOptions>::set::<ExecutionDBBuilder>),
                type_of_val(&With::<ExecutionDBBuilder>::build::<ExecutionDB, _>),
                type_of_val(&insert::<ExecutionDB>),
                // Build transactions
                type_of_val(&build_transaction_map),
                type_of_val(&With::<ExecutionDB>::try_convert_to::<TransactionDBBuilder, _>),
                type_of_val(&build_transactions),
                type_of_val(&insert_vec::<TransactionDB>),
                // Build new function runs
                type_of_val(&With::<ExecutionDB>::try_convert_to::<FunctionRunDBBuilder, _>),
                type_of_val(&build_function_runs),
                type_of_val(&insert_vec::<FunctionRunDB>),
                // Build new table data versions
                type_of_val(&build_table_data_versions),
                type_of_val(&insert_vec::<TableDataVersionDB>),
                // Create execution plan
                type_of_val(&resolve_execution_plan_versions),
                // Create steps
                type_of_val(&build_function_requirements),
                type_of_val(&insert_vec::<FunctionRequirementDB>),
                // Update function run initial status if needed
                type_of_val(&update_initial_function_run_status),
                // Execution plan response
                type_of_val(&build_response),
            ]);
    }

    // Similar to tests in execute, but just testing that the functions are found properly.
    // Tests in execute assert the correctness of the execution.
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_execute_version_ok(db: DbPool) -> Result<(), TdError> {
        let collection_0 = seed_collection(
            &db,
            &CollectionName::try_from("collection_0")?,
            &UserId::admin(),
        )
        .await;
        let function_0 = FunctionName::try_from("function_0")?;

        // Trigger function.
        let create = FunctionRegister::builder()
            .name(function_0.clone())
            .try_description("foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(vec![TableNameDto::try_from("table_0")?])
            .runtime_values(FunctionRuntimeValues::try_from("foo runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;
        let function = seed_function(&db, &collection_0, &create).await;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).create(
                FunctionVersionIdParam {
                    function_version_id: function.id,
                },
                ExecutionRequest::builder()
                    .name(Some(ExecutionName::try_from("test_execution")?))
                    .build()?,
            );

        let service = ExecuteFunctionVersionService::with_defaults(db.clone())
            .service()
            .await;
        let response = service.raw_oneshot(request).await?;
        assert_eq!(response.triggered_functions, HashSet::from([function.id]));
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_execute_version_previous_version(db: DbPool) -> Result<(), TdError> {
        let collection_0 = seed_collection(
            &db,
            &CollectionName::try_from("collection_0")?,
            &UserId::admin(),
        )
        .await;
        let function_0 = FunctionName::try_from("function_0")?;

        // Trigger function.
        let create = FunctionRegister::builder()
            .name(function_0.clone())
            .try_description("foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(vec![TableNameDto::try_from("table_0")?])
            .runtime_values(FunctionRuntimeValues::try_from("foo runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;
        let function = seed_function(&db, &collection_0, &create).await;

        // Dummy update to create new version
        let update = FunctionUpdate::builder().name(function_0).build()?;
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).update(
                FunctionParam::builder()
                    .try_collection(format!("~{}", collection_0.id))?
                    .try_function(format!("~{}", function.function_id))?
                    .build()?,
                update.clone(),
            );

        let service = UpdateFunctionService::with_defaults(db.clone())
            .service()
            .await;
        let updated = service.raw_oneshot(request).await?;

        // Now, using function.id will fail
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).create(
                FunctionVersionIdParam {
                    function_version_id: function.id,
                },
                ExecutionRequest::builder()
                    .name(Some(ExecutionName::try_from("test_execution")?))
                    .build()?,
            );

        let service = ExecuteFunctionVersionService::with_defaults(db.clone())
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        let err = response.unwrap_err();
        assert!(err.to_string().contains("Function version missmatch."));
        assert!(err.to_string().contains(format!(
            "Requested function version '{}' must be the current version of the function '{}'.",
            function.id, updated.id
        ).as_str()));
        Ok(())
    }
}
