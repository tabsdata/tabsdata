//
// Copyright 2025 Tabsdata Inc.
//

use crate::execution::execute;
use ta_services::ctx_service::ReqCtx;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{DevelopProject, Project};
use td_database::access::{DbPool, InProject};
use td_error::TdError;
use td_execution::{SchedulerNotify, TransactionBy};
use td_objects::and;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::crudl::{CreateRequest, WithinProject};
use td_objects::dxo::execution::{ExecutionRequest, ExecutionResponse};
use td_objects::dxo::function::{FunctionDBWithNames, FunctionParam};
use td_objects::types::basic::TriggerType;
use td_queries::Queries;

#[service_factory]
async fn execute_function(
    authz: Ctx<AuthzContext>,
    transaction_by: Ctx<TransactionBy>,
    scheduler_notify: Ctx<SchedulerNotify>,
    msgs: &mut ReqCtx,
    db: DbPool<InProject>,
    request: WithinProject<CreateRequest<ExecutionRequest, (CollectionParam, FunctionParam)>>,
) -> Result<ExecutionResponse, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, (collection_param, function_param), execution_request) = request.into_parts();

    // Authz before any read (the 425/409 trigger-gate answers must not be
    // observable unauthorized) and before the exclusive lock (unauthorized
    // callers must not queue on it).
    let grant = {
        let mut conn = db.read().await?;
        authz
            .require::<Project, DevelopProject>(&mut conn, &ctx, &project)
            .await?
    };
    let mut trx = db.begin_exclusive().await?;
    let at = trx.locked_at();
    let conn = trx.get(grant);
    let function: FunctionDBWithNames = conn
        .scd2()
        .as_of(&ctx.time)
        .fetch_one(and!(&collection_param.collection, &function_param.function))
        .await?;
    let response = execute(
        conn,
        msgs,
        &transaction_by,
        ctx,
        at,
        execution_request,
        function,
        TriggerType::Manual,
    )
    .await?;
    trx.commit().await?;

    scheduler_notify.notify_scheduler();
    Ok(response)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::Services;
    use std::collections::HashSet;
    use std::error::Error;
    use std::str::FromStr;
    use std::sync::LazyLock;
    use td_database::access::LockInstant;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt;
    use td_error::{ApiError, TdError};
    use td_execution::EXECUTION_GRAPH_OPTIONS;
    use td_objects::dxo::collection::{ActiveCollectionDB, CollectionCreate};
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::execution::ExecutionParam;
    use td_objects::dxo::execution::{
        ExecutionDB, ExecutionDBWithNames, ExecutionResponse, ResolvedVersionResponse,
    };
    use td_objects::dxo::function::{FunctionDB, FunctionDBWithNames, FunctionRegister};
    use td_objects::dxo::graph::GraphEdge;
    use td_objects::dxo::group::GroupDB;
    use td_objects::dxo::logical::{
        LogicalFunctionRunDB, LogicalFunctionRunInputDB, LogicalFunctionRunStatusUpdateDB,
        LogicalTableDataVersionDB,
    };
    use td_objects::dxo::physical_function_run::{
        PhysicalFunctionRunDB, PhysicalFunctionRunInsertDB,
    };
    use td_objects::dxo::physical_table_data_version::{
        PhysicalTableDataVersionDB, PhysicalTableDataVersionInsertDB,
    };
    use td_objects::dxo::table::{TableDB, TableDBWithNames};
    use td_objects::dxo::transaction::TransactionDBWithNames;
    use td_objects::types::basic::AtTime;
    use td_objects::types::basic::ReuseFrozen;
    use td_objects::types::basic::{
        AccessTokenId, AllowDisabled, BundleId, CollectionName, Decorator, Depth, Description,
        Enabled, ExecutionName, ExecutionStatus, FunctionName, FunctionOnly, FunctionRuntimeValues,
        GroupName, InputIdx, LogicalFunctionRunStatus, LogicalTableDataVersionId,
        OverrideDisabledTrigger, PhysicalFunctionRunId, PhysicalTableDataVersionId, ProjectId,
        ProjectName, SkipIfRunning, TableName, TableNameDto, TransactionKey, TransactionStatus,
        TriggeredOn, UserId, VersionPos,
    };
    use td_objects::types::basic::{CollectionIdName, FunctionIdName, TableIdName};
    use td_objects::types::composed::{TableDependencyDto, TableTriggerDto};
    use td_queries::Queries;
    use td_queries::test_utils::SeedEntry;

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
        let project = db
            .seed()
            .project(ProjectName::try_from("grp_proj")?)
            .await?;

        // Through the real service so `default` resolves in the project's scope.
        let collection_name = CollectionName::try_from("grp_coll")?;
        Services::with_defaults(db.clone())
            .create_collection(
                RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .create(
                        (),
                        CollectionCreate {
                            name: collection_name.clone(),
                            description: Description::try_from("c")?,
                            group: GroupName::default_group(),
                            base_path: None,
                            connection: None,
                            secrets: None,
                        },
                    )
                    .in_project(project.id),
            )
            .await?;

        let collection: ActiveCollectionDB = db
            .scoped(project.id)
            .fetch_one::<ActiveCollectionDB>(&collection_name)
            .await?;
        assert_eq!(collection.project_id, project.id);
        let default_group: GroupDB = db
            .scoped(project.id)
            .fetch_one::<GroupDB>(&GroupName::default_group())
            .await?;
        assert_eq!(
            collection.group_id, default_group.base.id,
            "the collection lands in its own project's default group",
        );

        // Every run-level row must inherit the project's group.
        let create = FunctionRegister {
            name: FunctionName::try_from("grp_fn")?,
            description: "grp fn".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "grp snippet".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: vec![TableNameDto::try_from("grp_table")?].into(),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("rt")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
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

        assert_eq!(transaction.base.group_id, collection.group_id);

        let lfrs: Vec<LogicalFunctionRunDB> = db
            .scoped(p)
            .fetch_all::<LogicalFunctionRunDB>(&function.base.function_id)
            .await?;
        assert_eq!(lfrs.len(), 1);
        assert_eq!(lfrs[0].project_id, project.id);
        assert_eq!(lfrs[0].base.group_id, collection.group_id);

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

        let create = FunctionRegister {
            name: FunctionName::try_from("producer")?,
            description: "producer".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "producer snippet".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(
                tables
                    .iter()
                    .map(|t| TableNameDto::try_from(*t))
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("rt")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let producer = db.scoped(p).seed().function(&collection, &create).await?;
        let table: TableDB = db
            .scd2()
            .fetch_one::<TableDBWithNames>(and!(&collection.id, &TableIdName::from_str(tables[0])?))
            .await?
            .base;

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
        let create = FunctionRegister {
            name: FunctionName::try_from("consumer")?,
            description: "consumer".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "consumer snippet".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: Some(
                dependencies
                    .iter()
                    .map(|d| TableDependencyDto::try_from(*d))
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            triggers: trigger
                .map(|t| Ok::<_, TdError>(vec![TableTriggerDto::try_from(t)?]))
                .transpose()?,
            tables: vec![TableNameDto::try_from("table_c")?].into(),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("rt")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let _ = db.scoped(p).seed().function(collection, &create).await?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&collection.name.to_string())?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("consumer")?,
                    },
                ),
                ExecutionRequest {
                    name: Some(ExecutionName::try_from("range_dep_execution")?),
                    function_only: FunctionOnly::FALSE,
                    override_disabled_trigger: OverrideDisabledTrigger::default(),
                    skip_if_running: SkipIfRunning::default(),
                    external_uid: None,
                },
            )
            .in_project(p);
        Services::with_defaults(db.clone())
            .execute_function(request)
            .await
            .map(|response| response.data)
    }

    fn lfri_slot(
        rows: &[LogicalFunctionRunInputDB],
        dep_pos: i32,
        version_pos: i32,
    ) -> &LogicalFunctionRunInputDB {
        rows.iter()
            .find(|r| {
                r.base.requirement_dependency_pos.map(|d| *d) == Some(dep_pos)
                    && *r.base.requirement_version_pos == version_pos
            })
            .unwrap_or_else(|| panic!("missing LFRI row dep {dep_pos} pos {version_pos}"))
    }

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

        let rows: Vec<LogicalFunctionRunInputDB> = db.scoped(p).fetch_all(&response.id).await?;

        let idx_of = |dep: i32, pos: i32| {
            lfri_slot(&rows, dep, pos)
                .base
                .requirement_input_idx
                .map(|i| *i)
        };
        assert_eq!(idx_of(0, 0), Some(0));
        assert_eq!(idx_of(0, 1), Some(1));
        assert_eq!(idx_of(0, 2), Some(2));
        assert_eq!(idx_of(1, *VersionPos::SINGLE_VERSION), Some(3));

        let range_ltdvs: HashSet<_> = (0..3)
            .map(|pos| {
                lfri_slot(&rows, 0, pos)
                    .base
                    .requirement_logical_table_data_version_id
                    .clone()
                    .expect("range slot must bind an LTDV")
            })
            .collect();
        assert_eq!(range_ltdvs.len(), 3, "each range slot binds its own LTDV");
        assert_eq!(
            lfri_slot(&rows, 1, *VersionPos::SINGLE_VERSION)
                .base
                .requirement_logical_table_data_version_id,
            lfri_slot(&rows, 0, 2)
                .base
                .requirement_logical_table_data_version_id,
            "single-version dep binds the same LTDV as the range's HEAD slot",
        );
        for row in rows
            .iter()
            .filter(|r| r.base.requirement_dependency_pos.is_none())
        {
            assert_eq!(
                row.base.requirement_input_idx, None,
                "trigger rows carry no idx"
            );
        }

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

        let read_service = Services::with_defaults(db.clone());
        let read_request = || {
            RequestContext::with(AccessTokenId::default(), UserId::admin()).read((
                ExecutionParam {
                    execution: response.id.clone(),
                },
                AllowDisabled::FALSE,
            ))
        };
        let read_response = read_service
            .clone()
            .execution_read(read_request())
            .await?
            .data;
        assert_eq!(idxs(dep_edge(&read_response, 0)), vec![0, 1, 2]);
        assert_eq!(idxs(dep_edge(&read_response, 1)), vec![3]);

        // Cancel the middle producer run: read-time re-resolution no longer
        // sees its version, but the idxs stay the persisted ones.
        let middle_lfr: LogicalFunctionRunDB = db
            .scoped(p)
            .fetch_one(and!(
                &producer_executions[1].base.id,
                &producer.base.function_id
            ))
            .await?;
        let update = LogicalFunctionRunStatusUpdateDB {
            status: LogicalFunctionRunStatus::Canceled,
        };
        db.scoped(p).update(&update, &middle_lfr.base.id).await?;

        let drifted = read_service.execution_read(read_request()).await?.data;
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
        let create = FunctionRegister {
            name: FunctionName::try_from("producer_q")?,
            description: "producer_q".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "producer_q snippet".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: vec![TableNameDto::try_from("table_q")?].into(),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("rt")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let _ = db.scoped(p).seed().function(&collection, &create).await?;

        let response = execute_consumer(
            &db,
            p,
            &collection,
            &["table_p@HEAD~2..HEAD", "table_q"],
            None,
        )
        .await?;

        let rows: Vec<LogicalFunctionRunInputDB> = db.scoped(p).fetch_all(&response.id).await?;

        let idx_of = |dep: i32, pos: i32| {
            lfri_slot(&rows, dep, pos)
                .base
                .requirement_input_idx
                .map(|i| *i)
        };
        assert_eq!(idx_of(0, 0), Some(*InputIdx::NO_DATA));
        assert_eq!(idx_of(0, 1), Some(*InputIdx::NO_DATA));
        assert_eq!(idx_of(0, 2), Some(0));
        assert_eq!(
            idx_of(1, *VersionPos::SINGLE_VERSION),
            Some(*InputIdx::NO_DATA)
        );
        assert!(
            lfri_slot(&rows, 0, 0)
                .base
                .requirement_logical_table_data_version_id
                .is_none()
        );
        assert!(
            lfri_slot(&rows, 0, 2)
                .base
                .requirement_logical_table_data_version_id
                .is_some()
        );

        assert_eq!(idxs(dep_edge(&response, 0)), vec![-1, -1, 0]);
        assert_eq!(idxs(dep_edge(&response, 1)), vec![-1]);

        Ok(())
    }

    fn referenced_ltdvs(response: &ExecutionResponse) -> HashSet<LogicalTableDataVersionId> {
        response
            .relations
            .iter()
            .flat_map(|(_, _, e)| e.versions().inner.iter().flatten().cloned())
            .collect()
    }

    fn output_edge<'a>(
        response: &'a ExecutionResponse,
        table: &str,
    ) -> &'a ResolvedVersionResponse {
        response
            .relations
            .iter()
            .find_map(|(_, t, e)| match e {
                GraphEdge::Output { versions, .. }
                    if response.all_tables[t].name.to_string() == table =>
                {
                    Some(versions)
                }
                _ => None,
            })
            .unwrap_or_else(|| panic!("missing output edge to {table}"))
    }

    /// Dependency slots are the logical versions the plan persisted as the
    /// consumer's inputs, and `relations_info` holds exactly the versions
    /// the edges reference, keyed by logical id.
    #[td_test::test(db)]
    async fn test_execute_relations_are_logical(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?.id;
        let (collection, _, _) = seed_producer_with_versions(&db, p, &["table_p"], 2).await?;

        let response =
            execute_consumer(&db, p, &collection, &["table_p@HEAD~1..HEAD"], None).await?;

        let rows: Vec<LogicalFunctionRunInputDB> = db.scoped(p).fetch_all(&response.id).await?;
        let bound: Vec<_> = (0..2)
            .map(|pos| {
                lfri_slot(&rows, 0, pos)
                    .base
                    .requirement_logical_table_data_version_id
                    .clone()
            })
            .collect();
        assert!(bound.iter().all(Option::is_some));
        assert_eq!(dep_edge(&response, 0).inner, bound);

        let keys: HashSet<_> = response.relations_info.keys().cloned().collect();
        assert_eq!(keys, referenced_ltdvs(&response));
        for (id, info) in &response.relations_info {
            assert_eq!(&info.id, id);
        }
        for id in bound.iter().flatten() {
            assert!(
                response.relations_info[id]
                    .physical_table_data_version_id
                    .is_some(),
                "seeded inputs are materialized"
            );
        }

        Ok(())
    }

    /// An output slot is the logical version this execution minted, present
    /// before anything materializes it; its current-materialization fields
    /// stay empty until a physical version exists.
    #[td_test::test(db)]
    async fn test_execute_output_relation_before_materialization(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?.id;
        let (collection, _, _) = seed_producer_with_versions(&db, p, &["table_p"], 1).await?;

        let response = execute_consumer(&db, p, &collection, &["table_p"], None).await?;

        let [Some(ltdv)] = output_edge(&response, "table_c").inner.as_slice() else {
            panic!("output edge must resolve to one logical version");
        };
        let minted: LogicalTableDataVersionDB = db.scoped(p).timeline().fetch_one(ltdv).await?;
        assert_eq!(minted.base.execution_id, response.id);

        let info = &response.relations_info[ltdv];
        assert_eq!(info.execution_id, response.id);
        assert!(info.physical_table_data_version_id.is_none());
        assert!(info.physical_status.is_none());

        Ok(())
    }

    /// Re-materializing an input (as a replay does) leaves the plan alone:
    /// the read keeps the same logical slots and attaches the newly current
    /// physical version.
    #[td_test::test(db)]
    async fn test_read_relations_follow_rematerialization(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?.id;
        let (collection, _, _) = seed_producer_with_versions(&db, p, &["table_p"], 1).await?;

        let response = execute_consumer(&db, p, &collection, &["table_p"], None).await?;
        let ltdv = dep_edge(&response, 0).inner[0]
            .clone()
            .expect("dependency resolves to the seeded version");
        let first_ptdv_id = response.relations_info[&ltdv]
            .physical_table_data_version_id
            .clone()
            .expect("seeded version is materialized");

        let first: PhysicalTableDataVersionDB =
            db.scoped(p).timeline().fetch_one(&first_ptdv_id).await?;
        let first_run: PhysicalFunctionRunDB = db
            .scoped(p)
            .timeline()
            .fetch_one(&first.base.physical_function_run_id)
            .await?;
        let at = LockInstant::asserted(*AtTime::now());
        let replayed_run = PhysicalFunctionRunInsertDB {
            id: PhysicalFunctionRunId::new(),
            ..first_run.base.clone()
        };
        db.scoped(p).scd2().open(&replayed_run, at).await?;
        let replayed = PhysicalTableDataVersionInsertDB {
            id: PhysicalTableDataVersionId::new(),
            physical_function_run_id: replayed_run.id.clone(),
            ..first.base.clone()
        };
        db.scoped(p).scd2().open(&replayed, at).await?;

        let read = Services::with_defaults(db.clone())
            .execution_read(
                RequestContext::with(AccessTokenId::default(), UserId::admin()).read((
                    ExecutionParam {
                        execution: response.id.clone(),
                    },
                    AllowDisabled::FALSE,
                )),
            )
            .await?
            .data;

        assert_eq!(read.relations, response.relations);
        assert_eq!(
            read.relations_info[&ltdv].physical_table_data_version_id,
            Some(replayed.id),
        );

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

        // collection_0/function_0 triggers function_1 and function_2, which
        // may trigger function_3 depending on the test.
        let create = FunctionRegister {
            name: FUNCTION_NAMES[0].clone(),
            description: "foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "foo snippet".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: vec![TableNameDto::try_from("table_0")?].into(),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("foo runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let _ = db
            .scoped(project_id)
            .seed()
            .function(&collection_0, &create)
            .await?;

        let create = FunctionRegister {
            name: FUNCTION_NAMES[1].clone(),
            description: "foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "foo snippet".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: vec![TableTriggerDto::try_from(format!(
                "{}/table_0",
                COLLECTION_NAMES[0],
            ))?]
            .into(),
            tables: vec![
                TableNameDto::try_from("table_1")?,
                TableNameDto::try_from("table_2")?,
            ]
            .into(),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("foo runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let _ = db
            .scoped(project_id)
            .seed()
            .function(&collection_1, &create)
            .await?;

        let bundle_id = BundleId::default();
        let create = FunctionRegister {
            name: FUNCTION_NAMES[2].clone(),
            description: "function_3 description".try_into()?,
            bundle_id,
            snippet: "function_3 snippet".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: vec![TableTriggerDto::try_from(format!(
                "{}/table_0",
                COLLECTION_NAMES[0],
            ))?]
            .into(),
            tables: Some(vec![
                TableNameDto::try_from("table_1")?,
                TableNameDto::try_from("table_2")?,
            ]),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

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

        let create = FunctionRegister {
            name: FUNCTION_NAMES[3].clone(),
            description: "foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "foo snippet".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: vec![
                TableDependencyDto::try_from(format!("{deps_collection}/table_1"))?,
                TableDependencyDto::try_from(format!("{deps_collection}/table_2"))?,
                TableDependencyDto::try_from("table_3")?,
            ]
            .into(),
            triggers: vec![TableTriggerDto::try_from(format!(
                "{triggers_collection}/table_1"
            ))?]
            .into(),
            tables: vec![
                TableNameDto::try_from("table_3")?,
                TableNameDto::try_from("table_4")?,
            ]
            .into(),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("foo runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let _ = db
            .scoped(project_id)
            .seed()
            .function(&collection_2, &create)
            .await?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&collection_0.name.to_string())?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str(&FUNCTION_NAMES[0].to_string())?,
                    },
                ),
                ExecutionRequest {
                    name: Some(ExecutionName::try_from("test_execution")?),
                    function_only: single_function.into(),
                    override_disabled_trigger: OverrideDisabledTrigger::default(),
                    skip_if_running: SkipIfRunning::default(),
                    external_uid: None,
                },
            )
            .in_project(project_id);

        let service = Services::with_defaults(db.clone());
        let response = service.execute_function(request).await;

        let expected_all_functions = match graph_depth {
            Some(depth) => FUNCTION_NAMES
                .iter()
                .take(*depth as usize + 1)
                .collect::<Vec<_>>(),
            None => FUNCTION_NAMES.iter().collect::<Vec<_>>(),
        };

        {
            let response = response?.data;

            assert_eq!(
                response.name,
                Some(ExecutionName::try_from("test_execution")?)
            );
            assert!(response.triggered_on < TriggeredOn::asserted(*AtTime::now()));

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

            assert_eq!(response.user_tables.len(), all_tables.len());
            assert_eq!(response.system_tables.len(), 0);

            let expected_created_tables = expected_all_tables;
            let mut created_tables: Vec<_> = response
                .created_tables
                .iter()
                .map(|t| {
                    TableName::try_from(format!(
                        "{}/{}",
                        response.all_tables[t].collection, response.all_tables[t].name
                    ))
                    .unwrap()
                })
                .collect();
            created_tables.sort();
            assert_eq!(expected_created_tables, created_tables);

            let executions: Vec<ExecutionDBWithNames> =
                db.fetch_all::<ExecutionDBWithNames>(()).await?;
            assert_eq!(executions.len(), 1);
            assert_eq!(executions[0].base.base.id, response.id);
            assert_eq!(executions[0].base.base.name, response.name);
            assert_eq!(executions[0].base.base.collection_id, collection_0.id);
            // The status views aggregate the plan's runs; the plan's shape is
            // the edition's business and asserted in its te-execution tests.
            let runs: Vec<LogicalFunctionRunDB> = db.fetch_all::<LogicalFunctionRunDB>(()).await?;
            let all_scheduled = runs
                .iter()
                .all(|run| run.base.status == LogicalFunctionRunStatus::Scheduled);
            let expected = if all_scheduled {
                ExecutionStatus::Scheduled
            } else {
                ExecutionStatus::Running
            };
            assert_eq!(executions[0].base.status, expected);

            let transactions: Vec<TransactionDBWithNames> =
                db.fetch_all::<TransactionDBWithNames>(()).await?;
            let transaction_ids: HashSet<_> =
                runs.iter().map(|run| &run.base.transaction_id).collect();
            assert_eq!(transactions.len(), transaction_ids.len());
            for transaction in transactions {
                assert_eq!(&transaction.base.base.execution_id, &response.id);
                let scheduled = runs
                    .iter()
                    .filter(|run| run.base.transaction_id == transaction.base.base.id)
                    .all(|run| run.base.status == LogicalFunctionRunStatus::Scheduled);
                let expected = if scheduled {
                    TransactionStatus::Scheduled
                } else {
                    TransactionStatus::Stalled
                };
                assert_eq!(transaction.base.status, expected);
            }

            // Concrete FRs/TDVs are minted at dispatch, not plan-build.
            let function: FunctionDBWithNames = db
                .scd2()
                .fetch_one::<FunctionDBWithNames>(and!(
                    &collection_0.id,
                    &FunctionIdName::Name(FUNCTION_NAMES[0].clone())
                ))
                .await?;
            let lfrs: Vec<LogicalFunctionRunDB> = db
                .fetch_all::<LogicalFunctionRunDB>(&function.base.base.function_id)
                .await?;
            assert_eq!(lfrs.len(), 1);
            assert_eq!(lfrs[0].base.collection_id, collection_0.id);
            assert_eq!(lfrs[0].base.execution_id, response.id);
            assert_eq!(lfrs[0].base.status, LogicalFunctionRunStatus::Scheduled);

            let function: FunctionDBWithNames = db
                .scd2()
                .fetch_one::<FunctionDBWithNames>(and!(
                    &collection_2.id,
                    &FunctionIdName::Name(FUNCTION_NAMES[2].clone())
                ))
                .await?;
            let lfrs: Vec<LogicalFunctionRunDB> = db
                .fetch_all::<LogicalFunctionRunDB>(&function.base.base.function_id)
                .await?;
            for lfr in lfrs {
                assert_eq!(&lfr.base.execution_id, &response.id);
            }

            let ltdvs: Vec<LogicalTableDataVersionDB> = db
                .timeline()
                .fetch_all::<LogicalTableDataVersionDB>(())
                .await?;
            assert_eq!(ltdvs.len(), expected_created_tables.len());
            for ltdv in ltdvs {
                assert_eq!(&ltdv.base.execution_id, &response.id);
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

        let create = FunctionRegister {
            name: FUNCTION_NAMES[0].clone(),
            description: "foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "foo snippet".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: vec![TableNameDto::try_from("table_0")?].into(),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("foo runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let function_0 = db
            .scoped(project_id)
            .seed()
            .function(&collection, &create)
            .await?;

        let create = FunctionRegister {
            name: FUNCTION_NAMES[1].clone(),
            description: "foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "foo snippet".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: vec![TableNameDto::try_from("table_1")?].into(),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("foo runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let function_1 = db
            .scoped(project_id)
            .seed()
            .function(&collection, &create)
            .await?;

        let create = FunctionRegister {
            name: FUNCTION_NAMES[2].clone(),
            description: "foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "foo snippet".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: vec![
                TableDependencyDto::try_from("table_0")?,
                TableDependencyDto::try_from("table_1")?,
            ]
            .into(),
            triggers: vec![
                TableTriggerDto::try_from("table_0")?,
                TableTriggerDto::try_from("table_1")?,
            ]
            .into(),
            tables: vec![TableNameDto::try_from("table_2")?].into(),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("foo runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let _function_2 = db
            .scoped(project_id)
            .seed()
            .function(&collection, &create)
            .await?;

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
            .scd2()
            .fetch_one::<TableDBWithNames>(and!(
                &collection.id,
                &TableIdName::Name(table_name.clone())
            ))
            .await?
            .base;
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
            .scd2()
            .fetch_one::<TableDBWithNames>(and!(
                &collection.id,
                &TableIdName::Name(table_name.clone())
            ))
            .await?
            .base;
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

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&collection.name.to_string())?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str(&FUNCTION_NAMES[2].to_string())?,
                    },
                ),
                ExecutionRequest {
                    name: Some(ExecutionName::try_from("test_execution")?),
                    function_only: FunctionOnly::default(),
                    override_disabled_trigger: OverrideDisabledTrigger::default(),
                    skip_if_running: SkipIfRunning::default(),
                    external_uid: None,
                },
            )
            .in_project(project_id);

        let service = Services::with_defaults(db.clone());
        let _response = service.execute_function(request).await?.data;

        let function_2: FunctionDBWithNames = db
            .scd2()
            .fetch_one::<FunctionDBWithNames>(and!(
                &collection.id,
                &FunctionIdName::Name(FUNCTION_NAMES[2].clone())
            ))
            .await?;
        let lfrs: Vec<LogicalFunctionRunDB> = db
            .fetch_all::<LogicalFunctionRunDB>(&function_2.base.base.function_id)
            .await?;
        assert_eq!(lfrs.len(), 1);
        assert_eq!(lfrs[0].base.status, expected_initial_status);
        let execution: ExecutionDB = db
            .scoped(project_id)
            .fetch_one(&lfrs[0].base.execution_id)
            .await?;
        let expected_execution_status = match expected_initial_status {
            LogicalFunctionRunStatus::OnHold => ExecutionStatus::Stalled,
            _ => ExecutionStatus::Scheduled,
        };
        assert_eq!(execution.status, expected_execution_status);
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

        // test_execute_disabled_trigger only creates functions 0 and 1.
        let graph_depth = EXECUTION_GRAPH_OPTIONS.max_downstream_depth();

        assert_eq!(
            response.name,
            Some(ExecutionName::try_from("test_execution")?)
        );
        assert!(response.triggered_on < TriggeredOn::asserted(*AtTime::now()));

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
                    response.all_tables[t].collection, response.all_tables[t].name
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

        let create = FunctionRegister {
            name: FUNCTION_NAMES[0].clone(),
            description: "foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "foo snippet".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: vec![TableNameDto::try_from("table_0")?].into(),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("foo runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: Some(Enabled::FALSE),
            workers: None,
        };
        let _function_0 = db
            .scoped(project_id)
            .seed()
            .function(&collection, &create)
            .await?;

        let create = FunctionRegister {
            name: FUNCTION_NAMES[1].clone(),
            description: "foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "foo snippet".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: vec![TableDependencyDto::try_from("table_0")?].into(),
            triggers: vec![TableTriggerDto::try_from("table_0")?].into(),
            tables: vec![TableNameDto::try_from("table_1")?].into(),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("foo runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let _function_1 = db
            .scoped(project_id)
            .seed()
            .function(&collection, &create)
            .await?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&collection.name.to_string())?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str(&FUNCTION_NAMES[0].to_string())?,
                    },
                ),
                ExecutionRequest {
                    name: Some(ExecutionName::try_from("test_execution")?),
                    function_only: FunctionOnly::default(),
                    override_disabled_trigger,
                    skip_if_running: SkipIfRunning::default(),
                    external_uid: None,
                },
            )
            .in_project(project_id);

        let service = Services::with_defaults(db.clone());

        Ok(service
            .execute_function(request)
            .await
            .map(|response| response.data))
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
        let create = FunctionRegister {
            name: FUNCTION_NAMES[0].clone(),
            description: "d".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "s".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: vec![TableNameDto::try_from("table_0")?].into(),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("rv")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let _function = db.scoped(p).seed().function(&collection, &create).await?;

        let trigger = |external_uid: Option<ExternalUid>| {
            RequestContext::with(AccessTokenId::default(), UserId::admin())
                .create(
                    (
                        CollectionParam {
                            collection: CollectionIdName::from_str(&collection.name.to_string())
                                .unwrap(),
                        },
                        FunctionParam {
                            function: FunctionIdName::from_str(&FUNCTION_NAMES[0].to_string())
                                .unwrap(),
                        },
                    ),
                    ExecutionRequest {
                        name: Some(ExecutionName::try_from("test_execution").unwrap()),
                        function_only: FunctionOnly::default(),
                        override_disabled_trigger: OverrideDisabledTrigger::default(),
                        skip_if_running: SkipIfRunning::default(),
                        external_uid,
                    },
                )
                .in_project(p)
        };

        let uid = ExternalUid::try_from("uid-abc")?;
        let service = Services::with_defaults(db.clone());
        service.execute_function(trigger(Some(uid.clone()))).await?;

        let service = Services::with_defaults(db.clone());
        let err = service
            .execute_function(trigger(Some(uid)))
            .await
            .unwrap_err();
        assert_eq!(err.api_error(), ApiError::Any(409));
        Ok(())
    }
}
