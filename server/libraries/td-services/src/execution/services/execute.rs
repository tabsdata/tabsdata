//
// Copyright 2025 Tabsdata Inc.
//

use crate::execution::layers::execute;
use crate::execution::layers::execution::trigger_type_manual;
use ta_services::factory::service_factory;
use td_authz::AuthzContext;
use td_execution::SchedulerNotify;
use td_execution::TransactionBy;
use td_objects::And;
use td_objects::dxo::crudl::{CreateRequest, RequestContext};
use td_objects::dxo::execution::{ExecutionRequest, ExecutionResponse};
use td_objects::dxo::function::FunctionDBWithNames;
use td_objects::rest_urls::FunctionParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{
    AtTime, CollectionIdName, FunctionId, FunctionIdName, FunctionVersionId,
};
use td_queries::By;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = ExecuteFunctionService,
    request = CreateRequest<FunctionParam, ExecutionRequest>,
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
        from_fn(With::<CreateRequest<FunctionParam, ExecutionRequest>>::extract::<RequestContext>),
        from_fn(
            With::<CreateRequest<FunctionParam, ExecutionRequest>>::extract_name::<FunctionParam>
        ),
        from_fn(
            With::<CreateRequest<FunctionParam, ExecutionRequest>>::extract_data::<ExecutionRequest>
        ),
        from_fn(With::<RequestContext>::extract::<AtTime>),
        from_fn(With::<FunctionParam>::extract::<CollectionIdName>),
        from_fn(With::<FunctionParam>::extract::<FunctionIdName>),
        // Select trigger function.
        from_fn(
            By::<And![CollectionIdName, FunctionIdName]>::historic_fetch_one::<
                { FunctionDBWithNames::All },
                FunctionDBWithNames,
            >
        ),
        from_fn(With::<FunctionDBWithNames>::extract::<FunctionId>),
        from_fn(With::<FunctionDBWithNames>::extract::<FunctionVersionId>),
        from_fn(trigger_type_manual),
        // Run the trigger gates (425 if running / 409 on external_uid conflict), build the
        // execution, notify the scheduler, and shape the response.
        execute(),
    )
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::Context;
    use crate::collection::service::CollectionServices;
    use crate::execution::services::read::ExecutionReadService;
    use std::collections::HashSet;
    use std::error::Error;
    use std::str::FromStr;
    use std::sync::LazyLock;
    use ta_services::factory::ServiceFactory;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::InProject;
    use td_database::sea_query::kind::{DatabaseBackend, DatabaseKind};
    use td_error::{ApiError, TdError};
    use td_execution::EXECUTION_GRAPH_OPTIONS;
    use td_objects::dxo::collection::{ActiveCollectionDB, CollectionCreate};
    use td_objects::dxo::execution::{
        ExecutionDB, ExecutionDBWithStatus, ExecutionResponse, ResolvedVersionResponse,
    };
    use td_objects::dxo::function::{FunctionDB, FunctionDBWithNames, FunctionRegister};
    use td_objects::dxo::graph::GraphEdge;
    use td_objects::dxo::logical::{
        LogicalFunctionRunDB, LogicalFunctionRunInputDB, LogicalTableDataVersionDB,
        UpdateLogicalFunctionRunDB,
    };
    use td_objects::dxo::table::TableDB;
    use td_objects::dxo::transaction::TransactionDBWithStatus;
    use td_objects::rest_urls::ExecutionParam;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, Depth, Description, Enabled,
        ExecutionName, ExecutionStatus, FunctionName, FunctionRuntimeValues, GroupId, InputIdx,
        LogicalFunctionRunStatus, OverrideDisabledTrigger, ProjectId, ProjectName, TableName,
        TableNameDto, TransactionKey, TransactionStatus, TriggeredOn, UserId, VersionPos,
    };
    use td_objects::types::composed::{TableDependencyDto, TableTriggerDto};
    use td_objects::{Buildable, and};
    use td_queries::test_utils::SeedEntry;
    use td_queries::{CurrentEntry, LiveEntry};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_execute(db: TdPool) {
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
        use td_authz::rbac::{DevelopProject, Project, Rbac, Scope};
        use td_database::pool::scoped::Scoped;
        use td_objects::dxo::crudl::RequestContext;
        use td_objects::dxo::execution::{ExecutionDB, ExecutionDBBuilder, ExecutionRequest};
        use td_objects::dxo::function::FunctionDBWithNames;
        use td_objects::dxo::logical::{
            LogicalFunctionRunDB, LogicalFunctionRunInputDB, LogicalFunctionRunPointerDB,
            LogicalTableDataVersionDB, LogicalTableDataVersionPointerDB,
        };
        use td_objects::dxo::transaction::{TransactionDB, TransactionDBBuilder};
        use td_objects::tower_service::with::With;
        use td_objects::types::basic::ProjectId;
        use td_objects::types::basic::{
            ExternalUid, FunctionId, FunctionOnly, SkipIfRunning, TriggerType,
        };
        use td_objects::types::recursion::RecursionOptions;
        use td_queries::{insert, insert_vec};
        use td_tower::metadata::type_of_val;

        ExecuteFunctionService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<Scoped<CreateRequest<FunctionParam, ExecutionRequest>, ProjectId>, ExecutionResponse>(
                &[
                    // Extract from request.
                    type_of_val(
                        &With::<CreateRequest<FunctionParam, ExecutionRequest>>::extract::<
                            RequestContext,
                        >,
                    ),
                    type_of_val(
                        &With::<CreateRequest<FunctionParam, ExecutionRequest>>::extract_name::<
                            FunctionParam,
                        >,
                    ),
                    type_of_val(
                        &With::<CreateRequest<FunctionParam, ExecutionRequest>>::extract_data::<
                            ExecutionRequest,
                        >,
                    ),
                    type_of_val(&With::<RequestContext>::extract::<AtTime>),
                    type_of_val(&With::<FunctionParam>::extract::<CollectionIdName>),
                    type_of_val(&With::<FunctionParam>::extract::<FunctionIdName>),
                    // Select trigger function.
                    type_of_val(
                        &By::<And![CollectionIdName, FunctionIdName]>::historic_fetch_one::<
                            { FunctionDBWithNames::All },
                            FunctionDBWithNames,
                        >,
                    ),
                    type_of_val(&With::<FunctionDBWithNames>::extract::<FunctionId>),
                    type_of_val(&With::<FunctionDBWithNames>::extract::<FunctionVersionId>),
                    type_of_val(&trigger_type_manual),
                    // trigger_guards(): 425 Too Early gate
                    type_of_val(
                        &By::<And![FunctionVersionId, TriggerType]>::fetch_optional::<ExecutionDB>,
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
                    type_of_val(
                        &With::<FunctionDBWithNames>::try_convert_to::<ExecutionDBBuilder, _>,
                    ),
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
                ],
            );
    }

    // Projects are silos with no inter-collection checks: triggering across
    // collections within the same project is always allowed for a
    // develop_project holder.
    #[td_test::test(db)]
    async fn test_execute_dependencies_different_collections(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let _ = test_execute(db, p.id, true, false, false).await?;
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_execute_trigger_different_collections(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let _ = test_execute(db, p.id, false, true, false).await?;
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_execute_single_function(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let _ = test_execute(db, p.id, true, false, true).await?;
        Ok(())
    }

    /// Grouping is project-scoped end to end: a function mints its runs and
    /// transaction with its own project's default group — never another
    /// project's. Postgres-only: resolving the per-project `default` group
    /// relies on RLS to disambiguate it from every other project's
    /// identically-named group.
    #[td_test::test(db)]
    async fn test_execute_stamps_project_scoped_group(db: TdPool) -> Result<(), TdError> {
        if !matches!(db.kind(), DatabaseBackend::Postgres) {
            return Ok(());
        }

        // A fresh project, seeded with its own system groups.
        let project = db
            .seed()
            .project(ProjectName::try_from("grp_proj")?)
            .await?;

        // Create a collection in that project through the real service, which
        // resolves the `default` group within the project's scope.
        let collection_name = CollectionName::try_from("grp_coll")?;
        CollectionServices::build(&Context::with_defaults(db.clone()))
            .create
            .service()
            .raw_oneshot(
                RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .create(
                        (),
                        CollectionCreate::builder()
                            .name(&collection_name)
                            .description(&Description::try_from("c")?)
                            .build()?,
                    )
                    .in_project(project.id),
            )
            .await?;

        let collection: ActiveCollectionDB = db
            .scoped(project.id)
            .live()
            .fetch_one::<ActiveCollectionDB>(&collection_name)
            .await?;
        assert_eq!(collection.project_id, project.id);
        assert_eq!(
            collection.group_id,
            GroupId::try_from(td_security::ENCODED_ID_GROUP_DEFAULT).unwrap(),
            "system groups carry the same well-known id in every project; the \
             project's own row is what scopes it (asserted via project_id)",
        );

        // Register a publisher function in that collection and seed a run
        // chain on it — every run-level row must inherit the project's group,
        // never the global default.
        let create = FunctionRegister::builder()
            .name(FunctionName::try_from("grp_fn")?)
            .try_description("grp fn")?
            .bundle_id(BundleId::default())
            .try_snippet("grp snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(vec![TableNameDto::try_from("grp_table")?])
            .runtime_values(FunctionRuntimeValues::try_from("rt")?)
            .reuse_frozen_tables(false)
            .build()?;
        // Seed the run chain in the project the collection belongs to.
        let p = &project.id;
        let function = db.scoped(p).seed().function(&collection, &create).await?;
        let execution = db.scoped(p).seed().execution(&function).await?;
        let transaction = db
            .scoped(p)
            .seed()
            .transaction(&execution, &TransactionKey::try_from("ANY")?)
            .await?;
        db.scoped(p)
            .seed()
            .physical_function_run(
                &collection,
                &function,
                &execution,
                &transaction,
                &LogicalFunctionRunStatus::Scheduled,
            )
            .await?;

        // The transaction is stamped with the collection's project group at mint.
        assert_eq!(transaction.group_id, collection.group_id);

        // The minted LFR carries the project's group and project — not another's.
        let lfrs: Vec<LogicalFunctionRunDB> = db
            .scoped(p)
            .live()
            .fetch_all::<LogicalFunctionRunDB>(&function.function_id)
            .await?;
        assert_eq!(lfrs.len(), 1);
        assert_eq!(lfrs[0].project_id, project.id);
        assert_eq!(lfrs[0].group_id, collection.group_id);

        Ok(())
    }

    /// Seed a publisher with the given output tables and `versions`
    /// committed data versions of the first table. Returns the collection,
    /// the producer and its seeded executions (oldest first).
    async fn seed_producer_with_versions(
        db: &TdPool,
        p: ProjectId,
        tables: &[&str],
        versions: usize,
    ) -> Result<(ActiveCollectionDB, FunctionDB, Vec<ExecutionDB>), TdError> {
        let collection = db
            .scoped(p)
            .seed()
            .collection(&CollectionName::try_from("range_coll")?, &UserId::admin())
            .await?;

        let create = FunctionRegister::builder()
            .name(FunctionName::try_from("producer")?)
            .try_description("producer")?
            .bundle_id(BundleId::default())
            .try_snippet("producer snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(
                tables
                    .iter()
                    .map(|t| TableNameDto::try_from(*t))
                    .collect::<Result<Vec<_>, _>>()?,
            )
            .runtime_values(FunctionRuntimeValues::try_from("rt")?)
            .reuse_frozen_tables(false)
            .build()?;
        let producer = db.scoped(p).seed().function(&collection, &create).await?;
        let table: TableDB = db
            .current()
            .fetch_one(and!(&collection.id, TableName::try_from(tables[0])?))
            .await?;

        let mut executions = Vec::new();
        for _ in 0..versions {
            let execution = db.scoped(p).seed().execution(&producer).await?;
            let transaction = db
                .scoped(p)
                .seed()
                .transaction(&execution, &TransactionKey::try_from("ANY")?)
                .await?;
            let run = db
                .scoped(p)
                .seed()
                .physical_function_run(
                    &collection,
                    &producer,
                    &execution,
                    &transaction,
                    &LogicalFunctionRunStatus::Scheduled,
                )
                .await?;
            db.scoped(p)
                .seed()
                .physical_table_data_version_with_data(
                    &collection,
                    &execution,
                    &transaction,
                    &run,
                    &table,
                )
                .await?;
            executions.push(execution);
        }
        Ok((collection, producer, executions))
    }

    /// Register and execute a consumer with the given dependencies (and an
    /// optional trigger), returning the execution response.
    async fn execute_consumer(
        db: &TdPool,
        p: ProjectId,
        collection: &ActiveCollectionDB,
        dependencies: &[&str],
        trigger: Option<&str>,
    ) -> Result<ExecutionResponse, TdError> {
        let create = FunctionRegister::builder()
            .name(FunctionName::try_from("consumer")?)
            .try_description("consumer")?
            .bundle_id(BundleId::default())
            .try_snippet("consumer snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(
                dependencies
                    .iter()
                    .map(|d| TableDependencyDto::try_from(*d))
                    .collect::<Result<Vec<_>, _>>()?,
            )
            .triggers(
                trigger
                    .map(|t| Ok::<_, TdError>(vec![TableTriggerDto::try_from(t)?]))
                    .transpose()?,
            )
            .tables(vec![TableNameDto::try_from("table_c")?])
            .runtime_values(FunctionRuntimeValues::try_from("rt")?)
            .reuse_frozen_tables(false)
            .build()?;
        let _ = db.scoped(p).seed().function(collection, &create).await?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                FunctionParam::builder()
                    .collection(CollectionIdName::from_str(&collection.name.to_string())?)
                    .function(FunctionIdName::from_str("consumer")?)
                    .build()?,
                ExecutionRequest::builder()
                    .name(Some(ExecutionName::try_from("range_dep_execution")?))
                    .function_only(false)
                    .build()?,
            )
            .in_project(p);
        ExecuteFunctionService::with_defaults(db.clone())
            .service()
            .raw_oneshot(request)
            .await
    }

    /// LFRI row for a `(dep_pos, version_pos)` slot of the execution.
    fn lfri_slot(
        rows: &[LogicalFunctionRunInputDB],
        dep_pos: i32,
        version_pos: i32,
    ) -> &LogicalFunctionRunInputDB {
        rows.iter()
            .find(|r| {
                r.requirement_dependency_pos.map(|d| *d) == Some(dep_pos)
                    && *r.requirement_version_pos == version_pos
            })
            .unwrap_or_else(|| panic!("missing LFRI row dep {dep_pos} pos {version_pos}"))
    }

    /// The resolved versions of the response's dependency edge at `dep_pos`.
    fn dep_edge(response: &ExecutionResponse, dep_pos: i32) -> &ResolvedVersionResponse {
        response
            .relations
            .iter()
            .find_map(|(_, _, e)| match e {
                GraphEdge::Dependency {
                    versions,
                    dependency,
                } if *dependency.dep_pos == dep_pos => Some(versions),
                _ => None,
            })
            .unwrap_or_else(|| panic!("missing dependency edge at dep_pos {dep_pos}"))
    }

    fn idxs(versions: &ResolvedVersionResponse) -> Vec<i32> {
        versions.input_idxs.iter().map(|i| **i).collect()
    }

    /// Every version resolved by a range dependency carries its own
    /// `input_idx`, and the response renders the persisted idxs identically
    /// on the execute and read paths — even after upstream state changes
    /// would resolve differently.
    #[td_test::test(db)]
    async fn test_execute_range_dep_input_idx(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?.id;
        let (collection, producer, producer_executions) =
            seed_producer_with_versions(&db, p, &["table_p"], 3).await?;

        let response = execute_consumer(
            &db,
            p,
            &collection,
            &["table_p@HEAD~2..HEAD", "table_p"],
            Some("table_p"),
        )
        .await?;

        // Persisted LFRI rows: range slots get one idx per resolved version
        // (HEAD~2 → HEAD, oldest first), the single-version dep continues
        // the sequence, trigger rows carry no idx.
        let rows: Vec<LogicalFunctionRunInputDB> =
            db.scoped(p).live().fetch_all(&response.id).await?;

        let idx_of =
            |dep: i32, pos: i32| lfri_slot(&rows, dep, pos).requirement_input_idx.map(|i| *i);
        assert_eq!(idx_of(0, 0), Some(0));
        assert_eq!(idx_of(0, 1), Some(1));
        assert_eq!(idx_of(0, 2), Some(2));
        assert_eq!(idx_of(1, *VersionPos::SINGLE_VERSION), Some(3));

        let range_ltdvs: HashSet<_> = (0..3)
            .map(|pos| {
                lfri_slot(&rows, 0, pos)
                    .requirement_logical_table_data_version_id
                    .clone()
                    .expect("range slot must bind an LTDV")
            })
            .collect();
        assert_eq!(range_ltdvs.len(), 3, "each range slot binds its own LTDV");
        assert_eq!(
            lfri_slot(&rows, 1, *VersionPos::SINGLE_VERSION)
                .requirement_logical_table_data_version_id,
            lfri_slot(&rows, 0, 2).requirement_logical_table_data_version_id,
            "single-version dep binds the same LTDV as the range's HEAD slot",
        );
        for row in rows
            .iter()
            .filter(|r| r.requirement_dependency_pos.is_none())
        {
            assert_eq!(row.requirement_input_idx, None, "trigger rows carry no idx");
        }

        // Execute response renders the persisted idxs.
        assert_eq!(idxs(dep_edge(&response, 0)), vec![0, 1, 2]);
        assert!(dep_edge(&response, 0).inner.iter().all(|v| v.is_some()));
        assert_eq!(idxs(dep_edge(&response, 1)), vec![3]);
        for (_, _, e) in &response.relations {
            if let GraphEdge::Trigger { versions } = e {
                assert!(
                    versions.input_idxs.is_empty(),
                    "trigger edges carry no idxs"
                );
            }
        }

        // Read path reproduces them.
        let read_service =
            ExecutionReadService::<{ FunctionDB::Executable }>::with_defaults(db.clone()).service();
        let read_request = || {
            RequestContext::with(AccessTokenId::default(), UserId::admin()).read(
                ExecutionParam::builder()
                    .execution(response.id.clone())
                    .build()
                    .unwrap(),
            )
        };
        let read_response = read_service.clone().raw_oneshot(read_request()).await?;
        assert_eq!(idxs(dep_edge(&read_response, 0)), vec![0, 1, 2]);
        assert_eq!(idxs(dep_edge(&read_response, 1)), vec![3]);

        // Cancel the middle producer run: read-time re-resolution no longer
        // sees its version, but the idxs stay the persisted ones.
        let middle_lfr: LogicalFunctionRunDB = db
            .scoped(p)
            .live()
            .fetch_one(and!(&producer_executions[1].id, &producer.function_id))
            .await?;
        let update = UpdateLogicalFunctionRunDB::builder()
            .status(LogicalFunctionRunStatus::Canceled)
            .build()?;
        db.scoped(p)
            .live()
            .update::<_, LogicalFunctionRunDB>(&update, &middle_lfr.id)
            .await?;

        let drifted = read_service.raw_oneshot(read_request()).await?;
        assert_eq!(idxs(dep_edge(&drifted, 0)), vec![0, 1, 2]);
        assert_eq!(idxs(dep_edge(&drifted, 1)), vec![3]);

        Ok(())
    }

    /// Range slots that resolve to no data version carry `NO_DATA`, as does
    /// a single-version dep on a table with no data; bound slots keep their
    /// own sequential idxs.
    #[td_test::test(db)]
    async fn test_execute_range_dep_input_idx_partial(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?.id;
        let (collection, _, _) = seed_producer_with_versions(&db, p, &["table_p"], 1).await?;

        // `table_q` belongs to a publisher that never ran: no LTDV timeline.
        let create = FunctionRegister::builder()
            .name(FunctionName::try_from("producer_q")?)
            .try_description("producer_q")?
            .bundle_id(BundleId::default())
            .try_snippet("producer_q snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(vec![TableNameDto::try_from("table_q")?])
            .runtime_values(FunctionRuntimeValues::try_from("rt")?)
            .reuse_frozen_tables(false)
            .build()?;
        let _ = db.scoped(p).seed().function(&collection, &create).await?;

        // `table_p` has one version (range over three slots resolves two
        // empty), `table_q` has none.
        let response = execute_consumer(
            &db,
            p,
            &collection,
            &["table_p@HEAD~2..HEAD", "table_q"],
            None,
        )
        .await?;

        let rows: Vec<LogicalFunctionRunInputDB> =
            db.scoped(p).live().fetch_all(&response.id).await?;

        let idx_of =
            |dep: i32, pos: i32| lfri_slot(&rows, dep, pos).requirement_input_idx.map(|i| *i);
        assert_eq!(idx_of(0, 0), Some(*InputIdx::NO_DATA));
        assert_eq!(idx_of(0, 1), Some(*InputIdx::NO_DATA));
        assert_eq!(idx_of(0, 2), Some(0));
        assert_eq!(
            idx_of(1, *VersionPos::SINGLE_VERSION),
            Some(*InputIdx::NO_DATA)
        );
        assert!(
            lfri_slot(&rows, 0, 0)
                .requirement_logical_table_data_version_id
                .is_none()
        );
        assert!(
            lfri_slot(&rows, 0, 2)
                .requirement_logical_table_data_version_id
                .is_some()
        );

        assert_eq!(idxs(dep_edge(&response, 0)), vec![-1, -1, 0]);
        assert_eq!(idxs(dep_edge(&response, 1)), vec![-1]);

        Ok(())
    }

    static COLLECTION_NAMES: LazyLock<Vec<CollectionName>> = LazyLock::new(|| {
        vec![
            CollectionName::try_from("collection_0").unwrap(),
            CollectionName::try_from("collection_1").unwrap(),
            CollectionName::try_from("collection_2").unwrap(),
        ]
    });

    static FUNCTION_NAMES: LazyLock<Vec<FunctionName>> = LazyLock::new(|| {
        vec![
            FunctionName::try_from("function_0").unwrap(),
            FunctionName::try_from("function_1").unwrap(),
            FunctionName::try_from("function_2").unwrap(),
            FunctionName::try_from("function_3").unwrap(),
        ]
    });

    pub(crate) async fn test_execute(
        db: TdPool,
        project_id: ProjectId,
        deps_diff_collection: bool,
        triggers_diff_collection: bool,
        single_function: bool,
    ) -> Result<ExecutionResponse, TdError> {
        let collection_0 = db
            .scoped(project_id)
            .seed()
            .collection(&COLLECTION_NAMES[0], &UserId::admin())
            .await?;
        let collection_1 = db
            .scoped(project_id)
            .seed()
            .collection(&COLLECTION_NAMES[1], &UserId::admin())
            .await?;
        let collection_2 = db
            .scoped(project_id)
            .seed()
            .collection(&COLLECTION_NAMES[2], &UserId::admin())
            .await?;

        let graph_depth = if single_function {
            Some(Depth::ZERO)
        } else {
            EXECUTION_GRAPH_OPTIONS.max_downstream_depth()
        };

        // Trigger function without permission restrictions (collection_0), which triggers function_1
        // and function_2. These will trigger function_3, depending on the test.
        let create = FunctionRegister::builder()
            .name(FUNCTION_NAMES[0].clone())
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

        let _ = db
            .scoped(project_id)
            .seed()
            .function(&collection_0, &create)
            .await?;

        let create = FunctionRegister::builder()
            .name(FUNCTION_NAMES[1].clone())
            .try_description("foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(vec![TableTriggerDto::try_from(format!(
                "{}/table_0",
                COLLECTION_NAMES[0],
            ))?])
            .tables(vec![
                TableNameDto::try_from("table_1")?,
                TableNameDto::try_from("table_2")?,
            ])
            .runtime_values(FunctionRuntimeValues::try_from("foo runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let _ = db
            .scoped(project_id)
            .seed()
            .function(&collection_1, &create)
            .await?;

        let bundle_id = BundleId::default();
        let create = FunctionRegister::builder()
            .name(FUNCTION_NAMES[2].clone())
            .try_description("function_3 description")?
            .bundle_id(bundle_id)
            .try_snippet("function_3 snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(vec![TableTriggerDto::try_from(format!(
                "{}/table_0",
                COLLECTION_NAMES[0],
            ))?])
            .tables(Some(vec![
                TableNameDto::try_from("table_1")?,
                TableNameDto::try_from("table_2")?,
            ]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let _ = db
            .scoped(project_id)
            .seed()
            .function(&collection_2, &create)
            .await?;

        let deps_collection = if deps_diff_collection {
            &COLLECTION_NAMES[1]
        } else {
            &COLLECTION_NAMES[2]
        };
        let triggers_collection = if triggers_diff_collection {
            &COLLECTION_NAMES[1]
        } else {
            &COLLECTION_NAMES[2]
        };

        let create = FunctionRegister::builder()
            .name(FUNCTION_NAMES[3].clone())
            .try_description("foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(vec![
                TableDependencyDto::try_from(format!("{deps_collection}/table_1"))?,
                TableDependencyDto::try_from(format!("{deps_collection}/table_2"))?,
                TableDependencyDto::try_from("table_3")?,
            ])
            .triggers(vec![TableTriggerDto::try_from(format!(
                "{triggers_collection}/table_1"
            ))?])
            .tables(vec![
                TableNameDto::try_from("table_3")?,
                TableNameDto::try_from("table_4")?,
            ])
            .runtime_values(FunctionRuntimeValues::try_from("foo runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let _ = db
            .scoped(project_id)
            .seed()
            .function(&collection_2, &create)
            .await?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                FunctionParam::builder()
                    .collection(CollectionIdName::from_str(&collection_0.name.to_string())?)
                    .function(FunctionIdName::from_str(&FUNCTION_NAMES[0].to_string())?)
                    .build()?,
                ExecutionRequest::builder()
                    .name(Some(ExecutionName::try_from("test_execution")?))
                    .function_only(single_function)
                    .build()?,
            )
            .in_project(project_id);

        let service = ExecuteFunctionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;

        // Test assertions
        let expected_all_functions = match graph_depth {
            Some(depth) => FUNCTION_NAMES
                .iter()
                .take(*depth as usize + 1)
                .collect::<Vec<_>>(),
            None => FUNCTION_NAMES.iter().collect::<Vec<_>>(),
        };

        // Projects are silos: cross-collection triggering within the project
        // is always allowed, so every trigger succeeds.
        {
            let response = response?;

            // Check the response
            assert_eq!(
                response.name,
                Some(ExecutionName::try_from("test_execution")?)
            );
            assert!(response.triggered_on < TriggeredOn::now());

            let mut all_functions: Vec<_> =
                response.all_functions.values().map(|t| &t.name).collect();
            all_functions.sort();
            assert_eq!(expected_all_functions, all_functions);

            let expected_triggered_functions = match graph_depth {
                Some(depth) => FUNCTION_NAMES
                    .iter()
                    .take(*depth as usize + 1)
                    .collect::<Vec<_>>(),
                None => FUNCTION_NAMES.iter().collect::<Vec<_>>(),
            };
            let mut triggered_functions: Vec<_> = response
                .triggered_functions
                .iter()
                .map(|t| &response.all_functions[t].name)
                .collect();
            triggered_functions.sort();
            assert_eq!(expected_triggered_functions, triggered_functions);

            let manual_trigger = &response.all_functions[&response.manual_trigger];
            assert_eq!(manual_trigger.name, FUNCTION_NAMES[0]);

            let all_tables_with_collection = vec![
                TableName::try_from("collection_0/table_0")?,
                TableName::try_from("collection_1/table_1")?,
                TableName::try_from("collection_1/table_2")?,
                TableName::try_from("collection_2/table_1")?,
                TableName::try_from("collection_2/table_2")?,
                TableName::try_from("collection_2/table_3")?,
                TableName::try_from("collection_2/table_4")?,
            ];
            let expected_all_tables = match graph_depth {
                Some(depth) => all_tables_with_collection
                    .into_iter()
                    .take(*depth as usize + 1)
                    .collect::<Vec<_>>(),
                None => all_tables_with_collection.into_iter().collect::<Vec<_>>(),
            };

            let mut all_tables: Vec<_> = response
                .all_tables
                .values()
                .map(|t| TableName::try_from(format!("{}/{}", t.collection, t.name)).unwrap())
                .collect();
            all_tables.sort();
            assert_eq!(expected_all_tables, all_tables);

            // In this test, all tables are user tables.
            assert_eq!(response.user_tables.len(), all_tables.len());
            assert_eq!(response.system_tables.len(), 0);

            // In our test, created tables are all of them.
            let expected_created_tables = expected_all_tables;
            let mut created_tables: Vec<_> = response
                .created_tables
                .iter()
                .map(|t| {
                    TableName::try_from(format!(
                        "{}/{}",
                        &response.all_tables[t].collection, &response.all_tables[t].name
                    ))
                    .unwrap()
                })
                .collect();
            created_tables.sort();
            assert_eq!(expected_created_tables, created_tables);

            // Asser db

            // Execution
            let executions: Vec<ExecutionDBWithStatus> =
                db.live().fetch_all::<ExecutionDBWithStatus>(()).await?;
            assert_eq!(executions.len(), 1);
            assert_eq!(executions[0].id, response.id);
            assert_eq!(executions[0].name, response.name);
            assert_eq!(executions[0].collection_id, collection_0.id);
            assert_eq!(executions[0].status, ExecutionStatus::Scheduled);

            // Transaction
            let transactions: Vec<TransactionDBWithStatus> =
                db.live().fetch_all::<TransactionDBWithStatus>(()).await?;
            assert!(transactions.len() == 1 || transactions.len() == 3);
            for transaction in transactions {
                assert_eq!(&transaction.execution_id, &response.id);
                assert_eq!(transaction.status, TransactionStatus::Scheduled);
            }

            // LFRs (concrete FRs are minted at dispatch, not plan-build).
            let function: FunctionDBWithNames = db
                .live()
                .fetch_one::<FunctionDBWithNames>(&FUNCTION_NAMES[0])
                .await?;
            let lfrs: Vec<LogicalFunctionRunDB> = db
                .live()
                .fetch_all::<LogicalFunctionRunDB>(&function.function_id)
                .await?;
            assert_eq!(lfrs.len(), 1);
            assert_eq!(lfrs[0].collection_id, collection_0.id);
            assert_eq!(lfrs[0].execution_id, response.id);
            assert_eq!(lfrs[0].status, LogicalFunctionRunStatus::Scheduled);

            let function: FunctionDBWithNames = db
                .live()
                .fetch_one::<FunctionDBWithNames>(&FunctionName::try_from("function_2")?)
                .await?;
            let lfrs: Vec<LogicalFunctionRunDB> = db
                .live()
                .fetch_all::<LogicalFunctionRunDB>(&function.function_id)
                .await?;
            for lfr in lfrs {
                assert_eq!(&lfr.execution_id, &response.id);
                assert_eq!(lfr.status, LogicalFunctionRunStatus::Scheduled);
            }

            // LTDVs (concrete TDVs are minted at dispatch).
            let ltdvs: Vec<LogicalTableDataVersionDB> =
                db.live().fetch_all::<LogicalTableDataVersionDB>(()).await?;
            assert_eq!(ltdvs.len(), expected_created_tables.len());
            for ltdv in ltdvs {
                assert_eq!(&ltdv.execution_id, &response.id);
            }
            Ok(response)
        }
    }

    #[td_test::test(db)]
    async fn test_execute_initial_status_upstream_scheduled(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        test_execute_upstream_initial_status(
            db,
            p.id,
            LogicalFunctionRunStatus::Scheduled,
            LogicalFunctionRunStatus::Scheduled,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_execute_initial_status_upstream_on_hold(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        test_execute_upstream_initial_status(
            db,
            p.id,
            LogicalFunctionRunStatus::OnHold,
            LogicalFunctionRunStatus::OnHold,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_execute_initial_status_upstream_failed(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        test_execute_upstream_initial_status(
            db,
            p.id,
            LogicalFunctionRunStatus::Failed,
            LogicalFunctionRunStatus::OnHold,
        )
        .await
    }

    async fn test_execute_upstream_initial_status(
        db: TdPool,
        project_id: ProjectId,
        upstream_status: LogicalFunctionRunStatus,
        expected_initial_status: LogicalFunctionRunStatus,
    ) -> Result<(), TdError> {
        let collection = db
            .scoped(project_id)
            .seed()
            .collection(&COLLECTION_NAMES[0], &UserId::admin())
            .await?;

        let create = FunctionRegister::builder()
            .name(FUNCTION_NAMES[0].clone())
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

        let function_0 = db
            .scoped(project_id)
            .seed()
            .function(&collection, &create)
            .await?;

        let create = FunctionRegister::builder()
            .name(FUNCTION_NAMES[1].clone())
            .try_description("foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(vec![TableNameDto::try_from("table_1")?])
            .runtime_values(FunctionRuntimeValues::try_from("foo runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let function_1 = db
            .scoped(project_id)
            .seed()
            .function(&collection, &create)
            .await?;

        let create = FunctionRegister::builder()
            .name(FUNCTION_NAMES[2].clone())
            .try_description("foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(vec![
                TableDependencyDto::try_from("table_0")?,
                TableDependencyDto::try_from("table_1")?,
            ])
            .triggers(vec![
                TableTriggerDto::try_from("table_0")?,
                TableTriggerDto::try_from("table_1")?,
            ])
            .tables(vec![TableNameDto::try_from("table_2")?])
            .runtime_values(FunctionRuntimeValues::try_from("foo runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let _function_2 = db
            .scoped(project_id)
            .seed()
            .function(&collection, &create)
            .await?;

        // seed a physical_function_run for function_0, with OnHold status
        let execution = db.scoped(project_id).seed().execution(&function_0).await?;
        let transaction_key = TransactionKey::try_from("ANY")?;
        let transaction = db
            .scoped(project_id)
            .seed()
            .transaction(&execution, &transaction_key)
            .await?;
        let physical_function_run = db
            .scoped(project_id)
            .seed()
            .physical_function_run(
                &collection,
                &function_0,
                &execution,
                &transaction,
                &upstream_status,
            )
            .await?;
        let table_name = TableName::try_from("table_0")?;
        let table = db
            .live()
            .fetch_one::<TableDB>(and!(&collection.id, &table_name))
            .await?;
        let _table_data_version = db
            .scoped(project_id)
            .seed()
            .physical_table_data_version(
                &collection,
                &execution,
                &transaction,
                &physical_function_run,
                &table,
            )
            .await?;

        // seed a physical_function_run for function_1, with Done status
        let execution = db.scoped(project_id).seed().execution(&function_1).await?;
        let transaction_key = TransactionKey::try_from("ANY")?;
        let transaction = db
            .scoped(project_id)
            .seed()
            .transaction(&execution, &transaction_key)
            .await?;
        let physical_function_run = db
            .scoped(project_id)
            .seed()
            .physical_function_run(
                &collection,
                &function_1,
                &execution,
                &transaction,
                &LogicalFunctionRunStatus::Done,
            )
            .await?;
        let table_name = TableName::try_from("table_1")?;
        let table = db
            .live()
            .fetch_one::<TableDB>(and!(&collection.id, &table_name))
            .await?;
        let _table_data_version = db
            .scoped(project_id)
            .seed()
            .physical_table_data_version(
                &collection,
                &execution,
                &transaction,
                &physical_function_run,
                &table,
            )
            .await?;

        // And then run function_2
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                FunctionParam::builder()
                    .collection(CollectionIdName::from_str(&collection.name.to_string())?)
                    .function(FunctionIdName::from_str(&FUNCTION_NAMES[2].to_string())?)
                    .build()?,
                ExecutionRequest::builder()
                    .name(Some(ExecutionName::try_from("test_execution")?))
                    .build()?,
            )
            .in_project(project_id);

        let service = ExecuteFunctionService::with_defaults(db.clone()).service();
        let _response = service.raw_oneshot(request).await?;

        // there should only be one LFR for function_2 (concrete FRs are
        // minted at dispatch).
        let function_2: FunctionDBWithNames = db
            .live()
            .fetch_one::<FunctionDBWithNames>(&FUNCTION_NAMES[2])
            .await?;
        let lfrs: Vec<LogicalFunctionRunDB> = db
            .live()
            .fetch_all::<LogicalFunctionRunDB>(&function_2.function_id)
            .await?;
        assert_eq!(lfrs.len(), 1);

        // and in OnHold status, because even tho function_1 is Done, function_0 is not ready
        assert_eq!(lfrs[0].status, expected_initial_status);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_execute_disabled_trigger_err(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let res = test_execute_disabled_trigger(db, p.id, OverrideDisabledTrigger::FALSE).await?;
        let err = res.unwrap_err();
        assert_eq!(
            "Function 'function_0' must be enabled to be executed".to_string(),
            err.source().unwrap().to_string()
        );
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_execute_disabled_trigger_override(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let response =
            test_execute_disabled_trigger(db, p.id, OverrideDisabledTrigger::TRUE).await?;
        let response = response?;

        // Assert that the disabled function as executed, and downstream was OK as well.
        // Note that test_execute_disabled_trigger only created function 0 and 1.
        let graph_depth = EXECUTION_GRAPH_OPTIONS.max_downstream_depth();

        assert_eq!(
            response.name,
            Some(ExecutionName::try_from("test_execution")?)
        );
        assert!(response.triggered_on < TriggeredOn::now());

        let manual_trigger = &response.all_functions[&response.manual_trigger];
        assert_eq!(manual_trigger.name, FUNCTION_NAMES[0]);

        let expected_all_functions = match graph_depth {
            Some(depth) => FUNCTION_NAMES[0..2]
                .iter()
                .take(*depth as usize + 1)
                .collect::<Vec<_>>(),
            None => FUNCTION_NAMES[0..2].iter().collect::<Vec<_>>(),
        };
        let mut all_functions: Vec<_> = response.all_functions.values().map(|t| &t.name).collect();
        all_functions.sort();
        assert_eq!(expected_all_functions, all_functions);

        let all_tables_with_collection = vec![
            TableName::try_from("collection_0/table_0")?,
            TableName::try_from("collection_0/table_1")?,
        ];
        let expected_all_tables = match graph_depth {
            Some(depth) => all_tables_with_collection
                .into_iter()
                .take(*depth as usize + 1)
                .collect::<Vec<_>>(),
            None => all_tables_with_collection.into_iter().collect::<Vec<_>>(),
        };
        let mut created_tables: Vec<_> = response
            .created_tables
            .iter()
            .map(|t| {
                TableName::try_from(format!(
                    "{}/{}",
                    &response.all_tables[t].collection, &response.all_tables[t].name
                ))
                .unwrap()
            })
            .collect();
        created_tables.sort();
        assert_eq!(expected_all_tables, created_tables);
        Ok(())
    }

    async fn test_execute_disabled_trigger(
        db: TdPool,
        project_id: ProjectId,
        override_disabled_trigger: OverrideDisabledTrigger,
    ) -> Result<Result<ExecutionResponse, TdError>, TdError> {
        let collection = db
            .scoped(project_id)
            .seed()
            .collection(&COLLECTION_NAMES[0], &UserId::admin())
            .await?;

        let create = FunctionRegister::builder()
            .name(FUNCTION_NAMES[0].clone())
            .try_description("foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(vec![TableNameDto::try_from("table_0")?])
            .runtime_values(FunctionRuntimeValues::try_from("foo runtime values")?)
            .reuse_frozen_tables(false)
            .enabled(Enabled::FALSE)
            .build()?;
        let _function_0 = db
            .scoped(project_id)
            .seed()
            .function(&collection, &create)
            .await?;

        let create = FunctionRegister::builder()
            .name(FUNCTION_NAMES[1].clone())
            .try_description("foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(vec![TableDependencyDto::try_from("table_0")?])
            .triggers(vec![TableTriggerDto::try_from("table_0")?])
            .tables(vec![TableNameDto::try_from("table_1")?])
            .runtime_values(FunctionRuntimeValues::try_from("foo runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;
        let _function_1 = db
            .scoped(project_id)
            .seed()
            .function(&collection, &create)
            .await?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                FunctionParam::builder()
                    .collection(CollectionIdName::from_str(&collection.name.to_string())?)
                    .function(FunctionIdName::from_str(&FUNCTION_NAMES[0].to_string())?)
                    .build()?,
                ExecutionRequest::builder()
                    .name(Some(ExecutionName::try_from("test_execution")?))
                    .override_disabled_trigger(override_disabled_trigger)
                    .build()?,
            )
            .in_project(project_id);

        let service = ExecuteFunctionService::with_defaults(db.clone()).service();

        Ok(service.raw_oneshot(request).await)
    }

    #[td_test::test(db)]
    async fn test_execute_public_external_uid_conflicts(db: TdPool) -> Result<(), TdError> {
        use td_objects::types::basic::ExternalUid;

        let p = db.seed().test_project().await?.id;
        let collection = db
            .scoped(p)
            .seed()
            .collection(&COLLECTION_NAMES[0], &UserId::admin())
            .await?;
        let create = FunctionRegister::builder()
            .name(FUNCTION_NAMES[0].clone())
            .try_description("d")?
            .bundle_id(BundleId::default())
            .try_snippet("s")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(vec![TableNameDto::try_from("table_0")?])
            .runtime_values(FunctionRuntimeValues::try_from("rv")?)
            .reuse_frozen_tables(false)
            .build()?;
        let _function = db.scoped(p).seed().function(&collection, &create).await?;

        let trigger = |external_uid: Option<ExternalUid>| {
            RequestContext::with(AccessTokenId::default(), UserId::admin())
                .create(
                    FunctionParam::builder()
                        .collection(
                            CollectionIdName::from_str(&collection.name.to_string()).unwrap(),
                        )
                        .function(FunctionIdName::from_str(&FUNCTION_NAMES[0].to_string()).unwrap())
                        .build()
                        .unwrap(),
                    ExecutionRequest::builder()
                        .name(Some(ExecutionName::try_from("test_execution").unwrap()))
                        .external_uid(external_uid)
                        .build()
                        .unwrap(),
                )
                .in_project(p)
        };

        let uid = ExternalUid::try_from("uid-abc")?;
        let service = ExecuteFunctionService::with_defaults(db.clone()).service();
        service.raw_oneshot(trigger(Some(uid.clone()))).await?;

        // Same external_uid on a still-live execution -> 409 Conflict.
        let service = ExecuteFunctionService::with_defaults(db.clone()).service();
        let err = service.raw_oneshot(trigger(Some(uid))).await.unwrap_err();
        assert_eq!(err.api_error(), ApiError::Any(409));
        Ok(())
    }
}
