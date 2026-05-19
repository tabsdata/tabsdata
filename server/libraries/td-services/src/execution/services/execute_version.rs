//
// Copyright 2025 Tabs Data Inc.
//

use crate::execution::layers::execute;
use crate::execution::layers::execution_version::{
    already_running_execute_internal_response, assert_function_version,
    executed_execute_internal_response, skip_if_running,
};
use ta_services::factory::service_factory;
use td_authz::AuthzContext;
use td_execution::TransactionBy;
use td_objects::And;
use td_objects::dxo::crudl::{CreateRequest, RequestContext};
use td_objects::dxo::execution::{
    ExecutionDB, ExecutionRequest, ExecutionRequestInternal, ExecutionResponseInternal,
};
use td_objects::dxo::function::FunctionDBWithNames;
use td_objects::rest_urls::FunctionVersionIdParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{AtTime, FunctionId, FunctionVersionId, SkipIfRunning, TriggerType};
use td_queries::By;
use td_tower::default_services::{Do, Else, If, TransactionProvider, conditional};
use td_tower::from_fn::from_fn;
use td_tower::{layers, service};

#[service_factory(
    name = ExecuteFunctionVersionService,
    request = CreateRequest<FunctionVersionIdParam, ExecutionRequestInternal>,
    response = ExecutionResponseInternal,
    connection = TransactionProvider,
    context = AuthzContext,
    context = TransactionBy,
)]
fn service() {
    layers!(
        // Extract from request.
        from_fn(
            With::<CreateRequest<FunctionVersionIdParam, ExecutionRequestInternal>>::extract::<
                RequestContext,
            >
        ),
        from_fn(
            With::<CreateRequest<FunctionVersionIdParam, ExecutionRequestInternal>>::extract_name::<
                FunctionVersionIdParam,
            >
        ),
        from_fn(
            With::<CreateRequest<FunctionVersionIdParam, ExecutionRequestInternal>>::extract_data::<
                ExecutionRequestInternal,
            >
        ),
        from_fn(With::<RequestContext>::extract::<AtTime>),
        from_fn(With::<FunctionVersionIdParam>::extract::<FunctionVersionId>),
        // Select trigger function.
        from_fn(By::<FunctionVersionId>::fetch_one::<FunctionDBWithNames>),
        // Ensure the trigger function is the current active version (never allow older versions to execute)
        from_fn(With::<FunctionDBWithNames>::extract::<FunctionId>),
        from_fn(
            By::<FunctionId>::historic_fetch_one::<{ FunctionDBWithNames::All }, FunctionDBWithNames>
        ),
        from_fn(assert_function_version),
        // Extract Trigger type
        from_fn(With::<ExecutionRequestInternal>::extract::<TriggerType>),
        conditional(
            If(service!(layers!(
                // Get latest function run at the time of executing if any
                // We only check for executions of the same trigger type
                from_fn(By::<And![FunctionVersionId, TriggerType]>::fetch_optional::<ExecutionDB>),
                from_fn(With::<ExecutionRequestInternal>::extract::<SkipIfRunning>),
                from_fn(skip_if_running),
            ))),
            Do(service!(layers!(
                // safe to unwrap, only skip if the execution is there
                from_fn(With::<Option<ExecutionDB>>::unwrap),
                from_fn(already_running_execute_internal_response)
            ))),
            Else(service!(layers!(
                // Extract trigger type and execute
                from_fn(With::<ExecutionRequestInternal>::convert_to::<ExecutionRequest>),
                execute(),
                from_fn(executed_execute_internal_response)
            )))
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::function::services::update::UpdateFunctionService;
    use std::collections::HashSet;
    use std::str::FromStr;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::Buildable;
    use td_objects::dxo::execution::ExecutionDB;
    use td_objects::dxo::function::{FunctionDB, FunctionRegister, FunctionUpdate};
    use td_objects::rest_urls::FunctionParam;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionIdName, CollectionName, Decorator, ExecutionName,
        FunctionIdName, FunctionName, FunctionRuntimeValues, LogicalFunctionRunStatus, RoleId,
        TableNameDto, TransactionKey, UserId,
    };
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_execute_version(db: TdPool) {
        use crate::execution::layers::GraphOpts;
        use crate::execution::layers::execution::{
            assert_function_status, build_execution_plan, build_initial_mint_lfr_pointers,
            build_initial_mint_ltdv_pointers, build_logical_function_run_inputs,
            build_logical_function_runs, build_logical_table_data_versions, build_response,
            build_transaction_map, build_transactions, resolve_execution_plan_versions,
            update_initial_function_run_status, validate_execution_plan, with_recursion_options,
        };
        use crate::graph::layers::FunctionOrTable;
        use crate::graph::layers::build::GraphBuilder;
        use td_authz::Authz;
        use td_authz::tower::InterColl;
        use td_authz::tower::{AuthzOn, CollAdmin, CollExec};
        use td_objects::dxo::crudl::RequestContext;
        use td_objects::dxo::dependency::DependencyDBWithNames;
        use td_objects::dxo::execution::{
            ExecutionDB, ExecutionDBBuilder, ExecutionRequestInternal,
        };
        use td_objects::dxo::function::FunctionDBWithNames;
        use td_objects::dxo::inter_collection_access::{
            InterCollectionAccess, InterCollectionAccessBuilder,
        };
        use td_objects::dxo::logical::{
            LogicalFunctionRunDB, LogicalFunctionRunInputDB, LogicalFunctionRunPointerDB,
            LogicalTableDataVersionDB, LogicalTableDataVersionPointerDB,
        };
        use td_objects::dxo::transaction::{TransactionDB, TransactionDBBuilder};
        use td_objects::dxo::trigger::TriggerDBWithNames;
        use td_objects::tower_service::with::With;
        use td_objects::types::basic::{
            CollectionId, FunctionId, FunctionOnly, OverrideDisabledTrigger, TriggerType,
        };
        use td_objects::types::recursion::RecursionOptions;
        use td_queries::{insert, insert_vec};
        use td_tower::metadata::type_of_val;

        ExecuteFunctionVersionService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<CreateRequest<FunctionVersionIdParam, ExecutionRequestInternal>, ExecutionResponseInternal>(&[
                // Extract from request.
                type_of_val(&
                    With::<CreateRequest<FunctionVersionIdParam, ExecutionRequestInternal>>::extract::<
                        RequestContext,
                    >
                ),
                type_of_val(&
                    With::<CreateRequest<FunctionVersionIdParam, ExecutionRequestInternal>>::extract_name::<
                        FunctionVersionIdParam,
                    >
                ),
                type_of_val(&
                    With::<CreateRequest<FunctionVersionIdParam, ExecutionRequestInternal>>::extract_data::<
                        ExecutionRequestInternal,
                    >
                ),
                type_of_val(&With::<RequestContext>::extract::<AtTime>),
                type_of_val(&With::<FunctionVersionIdParam>::extract::<FunctionVersionId>),
                // Select trigger function.
                type_of_val(&By::<FunctionVersionId>::fetch_one::<FunctionDBWithNames>),
                // Ensure the trigger function is the current active version (never allow older versions to execute)
                type_of_val(&With::<FunctionDBWithNames>::extract::<FunctionId>),
                type_of_val(&
                    By::<FunctionId>::historic_fetch_one::<{ FunctionDBWithNames::All }, FunctionDBWithNames>
                ),
                type_of_val(&assert_function_version),
                type_of_val(&With::<ExecutionRequestInternal>::extract::<TriggerType>),

                // if skip when running
                // Get latest function run at the time of executing if any
                // We only check for executions of the same trigger type
                type_of_val(&
                    By::<And![FunctionVersionId, TriggerType]>::fetch_optional::<ExecutionDB>
                ),
                type_of_val(&With::<ExecutionRequestInternal>::extract::<SkipIfRunning>),
                type_of_val(&skip_if_running),

                // do
                type_of_val(&With::<Option<ExecutionDB>>::unwrap),
                type_of_val(&already_running_execute_internal_response),

                // else
                type_of_val(&With::<ExecutionRequestInternal>::convert_to::<ExecutionRequest>),

                // execute()
                type_of_val(&With::<ExecutionRequest>::extract::<OverrideDisabledTrigger>),
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
                type_of_val(&GraphBuilder::<GraphOpts>::find_recursive_trigger_graph),
                // inter collection authz check
                type_of_val(
                    &With::<TriggerDBWithNames>::vec_convert_to::<InterCollectionAccessBuilder, _>,
                ),
                type_of_val(
                    &With::<Vec<InterCollectionAccessBuilder>>::build::<Vec<InterCollectionAccess>>,
                ),
                type_of_val(&Authz::<InterColl>::check_inter_collection),
                // Find dependencies. For clarity, we always show the function, even if
                // generating a table centered graph, the same way we show the tables of
                // a function centered graph.
                type_of_val(&GraphBuilder::<GraphOpts>::main_function_id),
                type_of_val(&GraphBuilder::<GraphOpts>::unique_triggered_function_ids),
                type_of_val(
                    &GraphBuilder::<GraphOpts>::find_triggered_functions_dependency_graph,
                ),
                // inter collection authz check
                type_of_val(
                    &With::<DependencyDBWithNames>::vec_convert_to::<InterCollectionAccessBuilder, _>,
                ),
                type_of_val(
                    &With::<Vec<InterCollectionAccessBuilder>>::build::<Vec<InterCollectionAccess>>,
                ),
                type_of_val(&Authz::<InterColl>::check_inter_collection),
                // Find output tables
                type_of_val(&GraphBuilder::<GraphOpts>::find_functions_output_tables),
                // Find all tables and functions
                type_of_val(&GraphBuilder::<GraphOpts>::find_all_tables_map),
                type_of_val(&GraphBuilder::<GraphOpts>::find_all_functions_map),
                // Create td graph and validate it
                type_of_val(&GraphBuilder::<GraphOpts>::build_td_graph),
                // Build execution plan
                type_of_val(&build_execution_plan),
                type_of_val(&validate_execution_plan),
                // Build execution
                type_of_val(&With::<FunctionDBWithNames>::try_convert_to::<ExecutionDBBuilder, _>),
                type_of_val(&With::<RequestContext>::update::<ExecutionDBBuilder, _>),
                type_of_val(&With::<ExecutionRequest>::update::<ExecutionDBBuilder, _>),
                type_of_val(&With::<TriggerType>::set::<ExecutionDBBuilder>),
                type_of_val(&With::<RecursionOptions>::set::<ExecutionDBBuilder>),
                type_of_val(&With::<ExecutionDBBuilder>::build::<ExecutionDB>),
                type_of_val(&insert::<ExecutionDB>),
                // Build transactions
                type_of_val(&build_transaction_map),
                type_of_val(&With::<ExecutionDB>::try_convert_to::<TransactionDBBuilder, _>),
                type_of_val(&build_transactions),
                type_of_val(&insert_vec::<TransactionDB>),
                // Logical LFRs — identity rows + InitialMint pointer events.
                type_of_val(&build_logical_function_runs),
                type_of_val(&insert_vec::<LogicalFunctionRunDB>),
                type_of_val(&build_initial_mint_lfr_pointers),
                type_of_val(&insert_vec::<LogicalFunctionRunPointerDB>),
                // Logical LTDVs — identity rows + InitialMint pointer events.
                type_of_val(&build_logical_table_data_versions),
                type_of_val(&insert_vec::<LogicalTableDataVersionDB>),
                type_of_val(&build_initial_mint_ltdv_pointers),
                type_of_val(&insert_vec::<LogicalTableDataVersionPointerDB>),
                // Resolve plan-build response.
                type_of_val(&resolve_execution_plan_versions),
                // Logical dependency-binding rows.
                type_of_val(&build_logical_function_run_inputs),
                type_of_val(&insert_vec::<LogicalFunctionRunInputDB>),
                // Initial-OnHold via logical chain.
                type_of_val(&update_initial_function_run_status),
                // Execution plan response
                type_of_val(&build_response),

                type_of_val(&executed_execute_internal_response),
            ]);
    }

    // Similar to tests in execute, but just testing that the functions are found properly.
    // Tests in execute assert the correctness of the execution.
    #[td_test::test(db)]
    async fn test_execute_version_no_previous_execution(db: TdPool) -> Result<(), TdError> {
        let (function, response) = test_execute_version(db, None, SkipIfRunning::FALSE).await?;
        let ExecutionResponseInternal::Executed(response) = response else {
            panic!("Expected executed response")
        };
        assert_eq!(response.triggered_functions, HashSet::from([function.id]));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_execute_version_no_previous_execution_skip_if_running(
        db: TdPool,
    ) -> Result<(), TdError> {
        let (function, response) = test_execute_version(db, None, SkipIfRunning::TRUE).await?;
        let ExecutionResponseInternal::Executed(response) = response else {
            panic!("Expected executed response")
        };
        assert_eq!(response.triggered_functions, HashSet::from([function.id]));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_execute_version_previous_execution_running(db: TdPool) -> Result<(), TdError> {
        let (function, response) = test_execute_version(
            db,
            Some(PreviousRun {
                status: LogicalFunctionRunStatus::Running,
                trigger_type: TriggerType::CronTrigger,
            }),
            SkipIfRunning::FALSE,
        )
        .await?;
        let ExecutionResponseInternal::Executed(response) = response else {
            panic!("Expected executed response")
        };
        assert_eq!(response.triggered_functions, HashSet::from([function.id]));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_execute_version_previous_execution_running_skip_if_running(
        db: TdPool,
    ) -> Result<(), TdError> {
        let (function, response) = test_execute_version(
            db.clone(),
            Some(PreviousRun {
                status: LogicalFunctionRunStatus::Running,
                trigger_type: TriggerType::CronTrigger,
            }),
            SkipIfRunning::TRUE,
        )
        .await?;
        let ExecutionResponseInternal::AlreadyRunning(response) = response else {
            panic!("Expected already running response")
        };
        // There should be only one, the previous one.
        let executions: Vec<ExecutionDB> = db
            .live()
            .fetch_all::<ExecutionDB>(&function.id)
            .await
            .unwrap();
        assert_eq!(executions.len(), 1);
        assert_eq!(executions[0].id, response);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_execute_version_previous_execution_committed(db: TdPool) -> Result<(), TdError> {
        let (function, response) = test_execute_version(
            db,
            Some(PreviousRun {
                status: LogicalFunctionRunStatus::Committed,
                trigger_type: TriggerType::CronTrigger,
            }),
            SkipIfRunning::FALSE,
        )
        .await?;
        let ExecutionResponseInternal::Executed(response) = response else {
            panic!("Expected executed response")
        };
        assert_eq!(response.triggered_functions, HashSet::from([function.id]));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_execute_version_previous_execution_commited_skip_if_running(
        db: TdPool,
    ) -> Result<(), TdError> {
        let (function, response) = test_execute_version(
            db,
            Some(PreviousRun {
                status: LogicalFunctionRunStatus::Committed,
                trigger_type: TriggerType::CronTrigger,
            }),
            SkipIfRunning::TRUE,
        )
        .await?;
        let ExecutionResponseInternal::Executed(response) = response else {
            panic!("Expected executed response")
        };
        assert_eq!(response.triggered_functions, HashSet::from([function.id]));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_execute_version_previous_version_different_type_running(
        db: TdPool,
    ) -> Result<(), TdError> {
        let (function, response) = test_execute_version(
            db,
            Some(PreviousRun {
                status: LogicalFunctionRunStatus::Running,
                trigger_type: TriggerType::Manual,
            }),
            SkipIfRunning::FALSE,
        )
        .await?;
        let ExecutionResponseInternal::Executed(response) = response else {
            panic!("Expected executed response")
        };
        assert_eq!(response.triggered_functions, HashSet::from([function.id]));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_execute_version_previous_version_different_type_running_skip_if_running(
        db: TdPool,
    ) -> Result<(), TdError> {
        let (function, response) = test_execute_version(
            db,
            Some(PreviousRun {
                status: LogicalFunctionRunStatus::Running,
                trigger_type: TriggerType::Manual,
            }),
            SkipIfRunning::TRUE,
        )
        .await?;
        let ExecutionResponseInternal::Executed(response) = response else {
            panic!("Expected executed response")
        };
        assert_eq!(response.triggered_functions, HashSet::from([function.id]));
        Ok(())
    }

    struct PreviousRun {
        status: LogicalFunctionRunStatus,
        trigger_type: TriggerType,
    }

    async fn test_execute_version(
        db: TdPool,
        previous_run: Option<PreviousRun>,
        skip_if_running: SkipIfRunning,
    ) -> Result<(FunctionDB, ExecutionResponseInternal), TdError> {
        let collection_0 = db
            .seed()
            .collection(&CollectionName::try_from("collection_0")?, &UserId::admin())
            .await?;
        let function_0 = FunctionName::try_from("function_0")?;

        // Create function.
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
        let function = db.seed().function(&collection_0, &create).await?;

        if let Some(previous_run) = previous_run {
            let execution = match previous_run.trigger_type {
                TriggerType::Manual => db.seed().execution(&function).await?,
                TriggerType::CronTrigger => db.seed().cron_execution(&function).await?,
                _ => panic!("Unexpected trigger type"),
            };
            let transaction_key = TransactionKey::try_from("ANY")?;
            let transaction = db.seed().transaction(&execution, &transaction_key).await?;

            let _function_run = db
                .seed()
                .physical_function_run(
                    &collection_0,
                    &function,
                    &execution,
                    &transaction,
                    &previous_run.status,
                )
                .await?;
        }

        // Trigger function
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).create(
                FunctionVersionIdParam {
                    function_version_id: function.id.clone(),
                },
                ExecutionRequestInternal::builder()
                    .name(Some(ExecutionName::try_from("test_execution")?))
                    .skip_if_running(skip_if_running)
                    .trigger_type(TriggerType::CronTrigger)
                    .build()?,
            );

        let service = ExecuteFunctionVersionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await?;
        Ok((function, response))
    }

    #[td_test::test(db)]
    async fn test_execute_version_previous_version_err(db: TdPool) -> Result<(), TdError> {
        let collection_0 = db
            .seed()
            .collection(&CollectionName::try_from("collection_0")?, &UserId::admin())
            .await?;
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
        let function = db.seed().function(&collection_0, &create).await?;

        // Dummy update to create new version
        let update = FunctionUpdate::builder().name(function_0).build()?;
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).update(
                FunctionParam::builder()
                    .collection(CollectionIdName::from_str(&format!(
                        "~{}",
                        collection_0.id
                    ))?)
                    .function(FunctionIdName::from_str(&format!(
                        "~{}",
                        function.function_id
                    ))?)
                    .build()?,
                update.clone(),
            );

        let service = UpdateFunctionService::with_defaults(db.clone()).service();
        let updated = service.raw_oneshot(request).await?;

        // Now, using function.id will fail
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).create(
                FunctionVersionIdParam {
                    function_version_id: function.id.clone(),
                },
                ExecutionRequestInternal::builder()
                    .name(Some(ExecutionName::try_from("test_execution")?))
                    .trigger_type(TriggerType::CronTrigger)
                    .build()?,
            );

        let service = ExecuteFunctionVersionService::with_defaults(db.clone()).service();
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
