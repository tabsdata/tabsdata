//
// Copyright 2025 Tabs Data Inc.
//

use crate::execution::layers::execute;
use crate::execution::layers::execution_version::assert_function_version;
use ta_services::factory::service_factory;
use td_authz::AuthzContext;
use td_execution::SchedulerNotify;
use td_execution::TransactionBy;
use td_objects::dxo::crudl::{CreateRequest, RequestContext};
use td_objects::dxo::execution::{ExecutionRequest, ExecutionRequestInternal, ExecutionResponse};
use td_objects::dxo::function::FunctionDBWithNames;
use td_objects::rest_urls::FunctionVersionIdParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{AtTime, FunctionId, FunctionVersionId, TriggerType};
use td_queries::By;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = ExecuteFunctionVersionService,
    request = CreateRequest<FunctionVersionIdParam, ExecutionRequestInternal>,
    response = ExecutionResponse,
    connection = TransactionProvider,
    scope = "Project",
    context = AuthzContext,
    context = TransactionBy,
    context = SchedulerNotify,
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
        // Convert to the public request shape so the shared `execute` layer can read it.
        from_fn(With::<ExecutionRequestInternal>::convert_to::<ExecutionRequest>),
        // Run the trigger gates (425 if running / 409 on external_uid conflict), build the
        // execution, notify the scheduler, and shape the response.
        execute(),
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
    use td_database::scoped::InProject;
    use td_error::{ApiError, TdError};
    use td_objects::Buildable;
    use td_objects::dxo::execution::ExecutionDB;
    use td_objects::dxo::function::{
        FunctionDB, FunctionDBWithNames, FunctionRegister, FunctionUpdate,
    };
    use td_objects::rest_urls::FunctionParam;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionIdName, CollectionName, Decorator, ExecutionName,
        ExternalUid, FunctionIdName, FunctionName, FunctionRuntimeValues, LogicalFunctionRunStatus,
        ProjectId, SkipIfRunning, TableNameDto, TransactionKey, UserId,
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
        use crate::execution::layers::execution_version::{
            dedup_lock, reject_if_external_uid_in_use, reject_if_running,
        };
        use crate::graph::layers::FunctionOrTable;
        use crate::graph::layers::build::GraphBuilder;
        use crate::scheduler::layers::request::notify_scheduler;
        use td_authz::rbac::{DevelopProject, ManageProject, Project, Rbac, Scope};
        use td_database::scoped::Scoped;
        use td_objects::And;
        use td_objects::dxo::crudl::RequestContext;
        use td_objects::dxo::execution::{
            ExecutionDB, ExecutionDBBuilder, ExecutionRequest, ExecutionRequestInternal,
        };
        use td_objects::dxo::function::FunctionDBWithNames;
        use td_objects::dxo::logical::{
            LogicalFunctionRunDB, LogicalFunctionRunInputDB, LogicalFunctionRunPointerDB,
            LogicalTableDataVersionDB, LogicalTableDataVersionPointerDB,
        };
        use td_objects::dxo::transaction::{TransactionDB, TransactionDBBuilder};
        use td_objects::tower_service::with::With;
        use td_objects::types::basic::ProjectId;
        use td_objects::types::basic::{
            FunctionId, FunctionOnly, OverrideDisabledTrigger, TriggerType,
        };
        use td_objects::types::recursion::RecursionOptions;
        use td_queries::{insert, insert_vec};
        use td_tower::metadata::type_of_val;

        ExecuteFunctionVersionService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<Scoped<CreateRequest<FunctionVersionIdParam, ExecutionRequestInternal>, ProjectId>, ExecutionResponse>(&[
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

                // Convert to the public request shape so the shared trigger gates can read it.
                type_of_val(&With::<ExecutionRequestInternal>::convert_to::<ExecutionRequest>),

                // trigger_guards(): 425 Too Early gate
                type_of_val(&
                    By::<And![FunctionVersionId, TriggerType]>::fetch_optional::<ExecutionDB>
                ),
                type_of_val(&With::<ExecutionRequest>::extract::<SkipIfRunning>),
                type_of_val(&reject_if_running),

                // trigger_guards(): 409 Conflict gate
                type_of_val(&With::<ExecutionRequest>::extract::<Option<ExternalUid>>),
                type_of_val(&dedup_lock),
                type_of_val(&reject_if_external_uid_in_use),

                // execute()
                type_of_val(&With::<ExecutionRequest>::extract::<OverrideDisabledTrigger>),
                type_of_val(&assert_function_status),
                // check requester can develop the current project (manage or develop)
                type_of_val(&Scope::<Project>::set),
                type_of_val(&Rbac::<Project, DevelopProject>::check),
                // Get recursion options
                type_of_val(&With::<FunctionDBWithNames>::extract::<FunctionId>),
                type_of_val(&With::<FunctionId>::convert_to::<FunctionOrTable>),
                type_of_val(&With::<ExecutionRequest>::extract::<FunctionOnly>),
                type_of_val(&with_recursion_options),
                // Find td graph
                // Find triggers
                type_of_val(&GraphBuilder::<GraphOpts>::find_recursive_trigger_graph),
                // Find dependencies. For clarity, we always show the function, even if
                // generating a table centered graph, the same way we show the tables of
                // a function centered graph.
                type_of_val(&GraphBuilder::<GraphOpts>::main_function_id),
                type_of_val(&GraphBuilder::<GraphOpts>::unique_triggered_function_ids),
                type_of_val(
                    &GraphBuilder::<GraphOpts>::find_triggered_functions_dependency_graph,
                ),
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

                // Notify scheduler of new function runs.
                type_of_val(&notify_scheduler),
            ]);
    }

    // Similar to tests in execute, but just testing that the functions are found properly.
    // Tests in execute assert the correctness of the execution.
    #[td_test::test(db)]
    async fn test_execute_version_no_previous_execution(db: TdPool) -> Result<(), TdError> {
        let (function, response) = test_execute_version(db, None, SkipIfRunning::FALSE).await?;
        assert_eq!(response.triggered_functions, HashSet::from([function.id]));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_execute_version_no_previous_execution_skip_if_running(
        db: TdPool,
    ) -> Result<(), TdError> {
        let (function, response) = test_execute_version(db, None, SkipIfRunning::TRUE).await?;
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
        assert_eq!(response.triggered_functions, HashSet::from([function.id]));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_execute_version_previous_execution_running_skip_if_running(
        db: TdPool,
    ) -> Result<(), TdError> {
        // skip_if_running + currently running -> 425 Too Early, no new execution.
        let err = test_execute_version(
            db.clone(),
            Some(PreviousRun {
                status: LogicalFunctionRunStatus::Running,
                trigger_type: TriggerType::CronTrigger,
            }),
            SkipIfRunning::TRUE,
        )
        .await
        .unwrap_err();
        assert_eq!(err.api_error(), ApiError::Any(425));

        // There should be only one, the previous one.
        let function: FunctionDBWithNames = db
            .live()
            .fetch_one::<FunctionDBWithNames>(&FunctionName::try_from("function_0")?)
            .await?;
        let executions: Vec<ExecutionDB> = db.live().fetch_all::<ExecutionDB>(&function.id).await?;
        assert_eq!(executions.len(), 1);
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
    ) -> Result<(FunctionDB, ExecutionResponse), TdError> {
        let collection_0 = db
            .scoped(ProjectId::sandbox())
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
        let function = db
            .scoped(ProjectId::sandbox())
            .seed()
            .function(&collection_0, &create)
            .await?;

        if let Some(previous_run) = previous_run {
            let execution = match previous_run.trigger_type {
                TriggerType::Manual => {
                    db.scoped(ProjectId::sandbox())
                        .seed()
                        .execution(&function)
                        .await?
                }
                TriggerType::CronTrigger => {
                    db.scoped(ProjectId::sandbox())
                        .seed()
                        .cron_execution(&function)
                        .await?
                }
                _ => panic!("Unexpected trigger type"),
            };
            let transaction_key = TransactionKey::try_from("ANY")?;
            let transaction = db
                .scoped(ProjectId::sandbox())
                .seed()
                .transaction(&execution, &transaction_key)
                .await?;

            let _function_run = db
                .scoped(ProjectId::sandbox())
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
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                FunctionVersionIdParam {
                    function_version_id: function.id.clone(),
                },
                ExecutionRequestInternal::builder()
                    .name(Some(ExecutionName::try_from("test_execution")?))
                    .skip_if_running(skip_if_running)
                    .trigger_type(TriggerType::CronTrigger)
                    .build()?,
            )
            .in_project(ProjectId::sandbox());

        let service = ExecuteFunctionVersionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await?;
        Ok((function, response))
    }

    #[td_test::test(db)]
    async fn test_execute_version_previous_version_err(db: TdPool) -> Result<(), TdError> {
        let collection_0 = db
            .scoped(ProjectId::sandbox())
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
        let function = db
            .scoped(ProjectId::sandbox())
            .seed()
            .function(&collection_0, &create)
            .await?;

        // Dummy update to create new version
        let update = FunctionUpdate::builder().name(function_0).build()?;
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
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
            )
            .in_project(ProjectId::sandbox());

        let service = UpdateFunctionService::with_defaults(db.clone()).service();
        let updated = service.raw_oneshot(request).await?;

        // Now, using function.id will fail
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                FunctionVersionIdParam {
                    function_version_id: function.id.clone(),
                },
                ExecutionRequestInternal::builder()
                    .name(Some(ExecutionName::try_from("test_execution")?))
                    .trigger_type(TriggerType::CronTrigger)
                    .build()?,
            )
            .in_project(ProjectId::sandbox());

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

    async fn seed_dedup_function(db: &TdPool) -> Result<FunctionDB, TdError> {
        let collection = db
            .scoped(ProjectId::sandbox())
            .seed()
            .collection(&CollectionName::try_from("dedup_coll")?, &UserId::admin())
            .await?;
        let create = FunctionRegister::builder()
            .name(FunctionName::try_from("dedup_fn")?)
            .try_description("d")?
            .bundle_id(BundleId::default())
            .try_snippet("s")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(vec![TableNameDto::try_from("t0")?])
            .runtime_values(FunctionRuntimeValues::try_from("rv")?)
            .reuse_frozen_tables(false)
            .build()?;
        db.scoped(ProjectId::sandbox())
            .seed()
            .function(&collection, &create)
            .await
    }

    async fn trigger_with_external_uid(
        db: &TdPool,
        function_version_id: &FunctionVersionId,
        external_uid: Option<ExternalUid>,
    ) -> Result<ExecutionResponse, TdError> {
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                FunctionVersionIdParam {
                    function_version_id: function_version_id.clone(),
                },
                ExecutionRequestInternal::builder()
                    .name(Some(ExecutionName::try_from("test_execution")?))
                    .external_uid(external_uid)
                    .trigger_type(TriggerType::CronTrigger)
                    .build()?,
            )
            .in_project(ProjectId::sandbox());
        ExecuteFunctionVersionService::with_defaults(db.clone())
            .service()
            .raw_oneshot(request)
            .await
    }

    #[td_test::test(db)]
    async fn test_execute_version_external_uid_conflicts(db: TdPool) -> Result<(), TdError> {
        let function = seed_dedup_function(&db).await?;
        let uid = ExternalUid::try_from("uid-123")?;

        trigger_with_external_uid(&db, &function.id, Some(uid.clone())).await?;

        // Same external_uid on a still-live execution -> 409 Conflict, no new row.
        let err = trigger_with_external_uid(&db, &function.id, Some(uid))
            .await
            .unwrap_err();
        assert_eq!(err.api_error(), ApiError::Any(409));

        let executions: Vec<ExecutionDB> = db
            .live()
            .fetch_all::<ExecutionDB>(&function.function_id)
            .await?;
        assert_eq!(executions.len(), 1);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_execute_version_null_external_uid_not_deduped(db: TdPool) -> Result<(), TdError> {
        let function = seed_dedup_function(&db).await?;

        trigger_with_external_uid(&db, &function.id, None).await?;
        trigger_with_external_uid(&db, &function.id, None).await?;

        let executions: Vec<ExecutionDB> = db
            .live()
            .fetch_all::<ExecutionDB>(&function.function_id)
            .await?;
        assert_eq!(executions.len(), 2);
        Ok(())
    }

    /// Concurrent identical-`external_uid` triggers must not race into a 500:
    /// `dedup_lock` serializes them so exactly one inserts (`Executed`) and the other is
    /// rejected with `409 Conflict`, leaving a single execution row. Before the lock, both
    /// could pass the conflict check and the INSERT loser would hit a constraint and surface
    /// a 500. Postgres-only — SQLite/MySQL serialize writers and the advisory lock is a no-op.
    #[td_test::test(db)]
    async fn test_execute_version_concurrent_external_uid_no_race(
        db: TdPool,
    ) -> Result<(), TdError> {
        use td_database::sea_query::kind::{DatabaseBackend, DatabaseKind};
        if !matches!(db.kind(), DatabaseBackend::Postgres) {
            return Ok(());
        }
        let function = seed_dedup_function(&db).await?;
        let uid = ExternalUid::try_from("uid-concurrent")?;

        // Fire two identical triggers concurrently on the shared pool.
        let (a, b) = tokio::join!(
            trigger_with_external_uid(&db, &function.id, Some(uid.clone())),
            trigger_with_external_uid(&db, &function.id, Some(uid.clone())),
        );

        let executed = [&a, &b].into_iter().filter(|r| r.is_ok()).count();
        let conflicts = [&a, &b]
            .into_iter()
            .filter(|r| matches!(r, Err(e) if e.api_error() == ApiError::Any(409)))
            .count();
        assert_eq!(executed, 1, "exactly one trigger executes: {a:?} / {b:?}");
        assert_eq!(
            conflicts, 1,
            "the racing trigger conflicts (409): {a:?} / {b:?}"
        );

        let executions: Vec<ExecutionDB> = db
            .live()
            .fetch_all::<ExecutionDB>(&function.function_id)
            .await?;
        assert_eq!(executions.len(), 1, "only one execution row must exist");
        Ok(())
    }
}
