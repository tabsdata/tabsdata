//
// Copyright 2025 Tabsdata Inc.
//

use crate::collection::resolve_collection_id;
use crate::execution::execution_version::refusal;
use crate::execution::{execute, trigger_gate};
use ta_services::ctx_service::ReqCtx;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{DevelopProject, Project};
use td_database::access::{DbPool, InProject, retrying};
use td_error::TdError;
use td_execution::{Concerns, ProjectWakes, TransactionBy};
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
    wakes: Ctx<ProjectWakes>,
    msgs: &mut ReqCtx,
    db: DbPool<InProject>,
    request: WithinProject<CreateRequest<ExecutionRequest, (CollectionParam, FunctionParam)>>,
) -> Result<ExecutionResponse, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, (collection_param, function_param), execution_request) = request.into_parts();

    // Authz before any read (the 425/409 trigger-gate answers must not be
    // observable unauthorized) and before the project's lock (unauthorized
    // callers must not queue on it).
    let grant = {
        let mut conn = db.read().await?;
        authz
            .require::<Project, DevelopProject>(&mut conn, &ctx, &project)
            .await?
    };
    let (response, recorded) = retrying(|attempt| async {
        let mut recorded = ReqCtx::default();
        let mut trx = db.lock_project().begin(attempt).await?;
        let at = trx.locked_at();
        let conn = trx.get(grant);
        let collection_id = resolve_collection_id(conn, &collection_param.collection).await?;
        let function: FunctionDBWithNames = conn
            .scd2()
            .fetch_one(and!(&collection_id, &function_param.function))
            .await?;

        if let Some(refused) =
            trigger_gate(conn, &execution_request, &function, &TriggerType::Manual).await?
        {
            return Err(refusal(&refused));
        }
        let response = execute(
            conn,
            &mut recorded,
            &transaction_by,
            ctx,
            at,
            execution_request.clone(),
            function,
            TriggerType::Manual,
        )
        .await?;
        trx.commit().await?;
        Ok((response, recorded))
    })
    .await?;
    msgs.merge(recorded);

    wakes.wake(&project, Concerns::RUNS);
    Ok(response)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::Services;
    use crate::collection::services::connection::test_support::create_collection;
    use crate::execution::TriggerError;
    use crate::function::children::ChildFunctionError;
    use std::collections::{HashMap, HashSet};
    use std::error::Error;
    use std::str::FromStr;
    use std::sync::LazyLock;
    use td_database::access::LockInstant;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt;
    use td_error::{ApiError, TdError};
    use td_objects::dxo::collection::{CollectionCreate, CollectionDB};
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::execution::ExecutionParam;
    use td_objects::dxo::execution::{
        ExecutionDB, ExecutionDBWithNames, ExecutionResponse, ResolvedVersionResponse,
    };
    use td_objects::dxo::function::{
        ChildFunction, FunctionDB, FunctionDBWithNames, FunctionRegister, FunctionUpdate,
    };
    use td_objects::dxo::graph::{GraphEdge, GraphRelation};
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
    use td_objects::types::basic::CollectionLabel;
    use td_objects::types::basic::FunctionType;
    use td_objects::types::basic::ReuseFrozen;
    use td_objects::types::basic::TableType;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, ConnectionKind, Decorator, Description, Enabled,
        ExecutionName, ExecutionStatus, FunctionName, FunctionOnly, FunctionRuntimeValues,
        GroupName, InputIdx, LogicalFunctionRunStatus, LogicalTableDataVersionId,
        OverrideDisabledTrigger, PhysicalFunctionRunId, PhysicalTableDataVersionId, ProjectId,
        ProjectName, SkipIfRunning, TableName, TableNameDto, TransactionKey, TransactionStatus,
        TriggeredOn, UserId, VersionPos, WorkerConfig, WorkerTypeDto,
    };
    use td_objects::types::basic::{CollectionIdName, ExternalUid, FunctionIdName, TableIdName};
    use td_objects::types::composed::{TableDependencyDto, TableTriggerDto};
    use td_queries::Queries;
    use td_queries::test_utils::SeedEntry;

    // Projects are silos with no inter-collection checks: triggering across
    // collections within the same project is always allowed for a
    // develop_project holder.
    #[td_test::test(db)]
    async fn test_execute_dependencies_different_collections(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let _ = Box::pin(test_execute(db, p.project_id, true, false, false)).await?;
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_execute_trigger_different_collections(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let _ = Box::pin(test_execute(db, p.project_id, false, true, false)).await?;
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_execute_single_function(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let _ = Box::pin(test_execute(db, p.project_id, true, false, true)).await?;
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
                    .in_project(project.project_id),
            )
            .await?;

        let collection: CollectionDB = db
            .scoped(project.project_id)
            .scd2()
            .fetch_one::<CollectionDB>(&collection_name)
            .await?;
        assert_eq!(collection.project_id, project.project_id);
        let default_group: GroupDB = db
            .scoped(project.project_id)
            .scd2()
            .fetch_one::<GroupDB>(&GroupName::default_group())
            .await?;
        assert_eq!(
            collection.group_id, default_group.group_id,
            "the collection lands in its own project's default group",
        );

        // Every run-level row must inherit the project's group.
        let create = FunctionRegister {
            child_functions: None,
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
            parent_tables: None,
            runtime_values: FunctionRuntimeValues::try_from("rt")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let p = &project.project_id;
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

        assert_eq!(transaction.group_id, collection.group_id);

        let lfrs: Vec<LogicalFunctionRunDB> = db
            .scoped(p)
            .fetch_all::<LogicalFunctionRunDB>(&function.function_id)
            .await?;
        assert_eq!(lfrs.len(), 1);
        assert_eq!(lfrs[0].project_id, project.project_id);
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
    ) -> Result<(CollectionDB, FunctionDB, Vec<ExecutionDB>), TdError> {
        let collection = db
            .scoped(p)
            .seed()
            .collection(&CollectionName::try_from("range_coll")?, &UserId::admin())
            .await?;

        let create = FunctionRegister {
            child_functions: None,
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
            parent_tables: None,
            runtime_values: FunctionRuntimeValues::try_from("rt")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let producer = db.scoped(p).seed().function(&collection, &create).await?;
        let table: TableDB = db
            .scd2()
            .fetch_one::<TableDBWithNames>(and!(
                &collection.collection_id,
                &TableIdName::from_str(tables[0])?
            ))
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
        collection: &CollectionDB,
        dependencies: &[&str],
        trigger: Option<&str>,
    ) -> Result<ExecutionResponse, TdError> {
        let create = FunctionRegister {
            child_functions: None,
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
            parent_tables: None,
            runtime_values: FunctionRuntimeValues::try_from("rt")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let _ = db.scoped(p).seed().function(collection, &create).await?;
        execute_named(db, p, collection, "consumer").await
    }

    async fn execute_named(
        db: &TdPool,
        p: ProjectId,
        collection: &CollectionDB,
        function: &str,
    ) -> Result<ExecutionResponse, TdError> {
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&collection.name.to_string())?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str(function)?,
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

    /// `consumer` reads table_w, which `writer` writes once `consumer`'s
    /// table_c triggers it: a plan running both writes table_w after its reader
    /// runs, so the reader gets what was committed before, not a version it
    /// would wait on forever.
    #[td_test::test(db)]
    async fn test_a_table_written_later_in_the_plan_reads_its_committed_version(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?.project_id;
        let (collection, _, _) = seed_producer_with_versions(&db, p, &["table_p"], 1).await?;
        let writer = FunctionRegister {
            name: FunctionName::try_from("writer")?,
            tables: Some(vec![TableNameDto::try_from("table_w")?]),
            triggers: Some(vec![TableTriggerDto::try_from("table_p")?]),
            ..register_named("writer")?
        };
        db.scoped(p).seed().function(&collection, &writer).await?;
        let consumer = FunctionRegister {
            dependencies: Some(vec![
                TableDependencyDto::try_from("table_p")?,
                TableDependencyDto::try_from("table_w")?,
            ]),
            triggers: Some(vec![TableTriggerDto::try_from("table_p")?]),
            tables: Some(vec![TableNameDto::try_from("table_c")?]),
            ..register_named("consumer")?
        };
        db.scoped(p).seed().function(&collection, &consumer).await?;
        update_named(
            &db,
            p,
            &collection,
            "writer",
            FunctionUpdate {
                tables: Some(vec![TableNameDto::try_from("table_w")?]),
                triggers: Some(vec![TableTriggerDto::try_from("table_c")?]),
                ..Default::default()
            },
        )
        .await?;

        let response = execute_named(&db, p, &collection, "consumer").await?;
        assert_eq!(dep_edge(&response, 1).inner, vec![None]);
        Ok(())
    }

    /// Reading an execution returns the plan it ran, whatever its project
    /// became afterwards: version selectors, triggers, enablement and new
    /// functions that would plan it differently today leave it unchanged.
    #[td_test::test(db)]
    async fn test_reading_an_execution_ignores_later_topology(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?.project_id;
        let (collection, _, _) = seed_producer_with_versions(&db, p, &["table_p"], 2).await?;
        let executed = execute_consumer(
            &db,
            p,
            &collection,
            &["table_p@HEAD^", "table_p"],
            Some("table_p"),
        )
        .await?;
        let read = || async {
            let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).read(
                ExecutionParam {
                    execution: executed.id.clone(),
                },
            );
            Services::with_defaults(db.clone())
                .execution_read(request)
                .await
                .map(|response| response.data)
        };
        let before = read().await?;
        assert!(before == executed, "the read shows the plan as executed");

        update_named(
            &db,
            p,
            &collection,
            "consumer",
            FunctionUpdate {
                dependencies: Some(vec![TableDependencyDto::try_from("table_p@HEAD~1..HEAD")?]),
                triggers: Some(vec![]),
                tables: Some(vec![TableNameDto::try_from("table_c")?]),
                ..Default::default()
            },
        )
        .await?;
        update_named(
            &db,
            p,
            &collection,
            "producer",
            FunctionUpdate {
                enabled: Some(Enabled::FALSE),
                ..Default::default()
            },
        )
        .await?;
        let downstream = FunctionRegister {
            dependencies: Some(vec![TableDependencyDto::try_from("table_c")?]),
            tables: Some(vec![TableNameDto::try_from("table_d")?]),
            ..register_named("downstream")?
        };
        db.scoped(p)
            .seed()
            .function(&collection, &downstream)
            .await?;

        assert!(read().await? == before, "the read is unchanged");
        let replanned = execute_named(&db, p, &collection, "consumer").await?;
        assert_ne!(
            dep_edge(&replanned, 0).original,
            dep_edge(&before, 0).original,
            "planning now would differ"
        );
        Ok(())
    }

    async fn update_named(
        db: &TdPool,
        p: ProjectId,
        collection: &CollectionDB,
        function: &str,
        update: FunctionUpdate,
    ) -> Result<(), TdError> {
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&collection.name.to_string())?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str(function)?,
                    },
                ),
                update,
            )
            .in_project(p);
        Services::with_defaults(db.clone())
            .update_function(request)
            .await?;
        Ok(())
    }

    fn register_named(name: &str) -> Result<FunctionRegister, TdError> {
        Ok(FunctionRegister {
            child_functions: None,
            name: FunctionName::try_from(name)?,
            description: name.try_into()?,
            bundle_id: BundleId::default(),
            snippet: "snippet".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: None,
            table_types: None,
            parent_tables: None,
            runtime_values: FunctionRuntimeValues::try_from("rt")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        })
    }

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

    fn dep_edge(response: &ExecutionResponse, dep_pos: i32) -> &ResolvedVersionResponse {
        response
            .relations
            .iter()
            .find_map(|r| match &r.edge {
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
        let p = db.seed().test_project().await?.project_id;
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
            .filter(|r| r.requirement_dependency_pos.is_none())
        {
            assert_eq!(row.requirement_input_idx, None, "trigger rows carry no idx");
        }

        assert_eq!(idxs(dep_edge(&response, 0)), vec![0, 1, 2]);
        assert!(dep_edge(&response, 0).inner.iter().all(|v| v.is_some()));
        assert_eq!(idxs(dep_edge(&response, 1)), vec![3]);
        for e in response.relations.iter().map(|r| &r.edge) {
            if let GraphEdge::Trigger { versions } = e {
                assert!(
                    versions.input_idxs.is_empty(),
                    "trigger edges carry no idxs"
                );
            }
        }

        let read_service = Services::with_defaults(db.clone());
        let read_request = || {
            RequestContext::with(AccessTokenId::default(), UserId::admin()).read(ExecutionParam {
                execution: response.id.clone(),
            })
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
            .fetch_one(and!(&producer_executions[1].id, &producer.function_id))
            .await?;
        let update = LogicalFunctionRunStatusUpdateDB {
            status: LogicalFunctionRunStatus::Canceled,
        };
        db.scoped(p).update(&update, &middle_lfr.id).await?;

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
        let p = db.seed().test_project().await?.project_id;
        let (collection, _, _) = seed_producer_with_versions(&db, p, &["table_p"], 1).await?;

        // `table_q` belongs to a publisher that never ran: no LTDV timeline.
        let create = FunctionRegister {
            child_functions: None,
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
            parent_tables: None,
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
            .flat_map(|r| r.edge.versions().inner.iter().flatten().cloned())
            .collect()
    }

    fn output_edge<'a>(
        response: &'a ExecutionResponse,
        table: &str,
    ) -> &'a ResolvedVersionResponse {
        response
            .relations
            .iter()
            .find_map(|r| match &r.edge {
                GraphEdge::Output { versions, .. }
                    if response.all_tables[&r.table_version_id].name.to_string() == table =>
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
        let p = db.seed().test_project().await?.project_id;
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
        let p = db.seed().test_project().await?.project_id;
        let (collection, _, _) = seed_producer_with_versions(&db, p, &["table_p"], 1).await?;

        let response = execute_consumer(&db, p, &collection, &["table_p"], None).await?;

        let [Some(ltdv)] = output_edge(&response, "table_c").inner.as_slice() else {
            panic!("output edge must resolve to one logical version");
        };
        let minted: LogicalTableDataVersionDB = db.scoped(p).timeline().fetch_one(ltdv).await?;
        assert_eq!(minted.execution_id, response.id);

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
        let p = db.seed().test_project().await?.project_id;
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
            .fetch_one(&first.physical_function_run_id)
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
                RequestContext::with(AccessTokenId::default(), UserId::admin()).read(
                    ExecutionParam {
                        execution: response.id.clone(),
                    },
                ),
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

        // collection_0/function_0 triggers function_1 and function_2, which
        // may trigger function_3 depending on the test.
        let create = FunctionRegister {
            child_functions: None,
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
            parent_tables: None,
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
            child_functions: None,
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
            parent_tables: None,
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
            child_functions: None,
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
            parent_tables: None,
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
            child_functions: None,
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
            parent_tables: None,
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

        {
            let response = response?.data;

            assert_eq!(
                response.name,
                Some(ExecutionName::try_from("test_execution")?)
            );
            assert!(response.triggered_on < TriggeredOn::asserted(*AtTime::now()));

            // The plan's shape is the edition's business, asserted in its
            // te-execution tests; here, what every edition guarantees.
            let manual_trigger = &response.all_functions[&response.manual_trigger];
            assert_eq!(manual_trigger.name, FUNCTION_NAMES[0]);
            assert!(
                response
                    .triggered_functions
                    .contains(&response.manual_trigger)
            );
            let triggered: HashSet<_> = response.triggered_functions.iter().collect();
            let all: HashSet<_> = response.all_functions.keys().collect();
            assert!(triggered.is_subset(&all));

            let created: HashSet<_> = response.created_tables.iter().collect();
            let tables: HashSet<_> = response.all_tables.keys().collect();
            assert!(created.is_subset(&tables));
            assert!(
                response
                    .all_tables
                    .values()
                    .filter(|t| t.collection == CollectionLabel::new(COLLECTION_NAMES[0].clone()))
                    .any(|t| t.name.to_string() == "table_0"),
                "the trigger's own table is in the plan"
            );
            assert_eq!(response.user_tables.len(), tables.len());
            assert_eq!(response.system_tables.len(), 0);
            for GraphRelation {
                function_version_id: function,
                table_version_id: table,
                ..
            } in &response.relations
            {
                assert!(response.all_functions.contains_key(function));
                assert!(response.all_tables.contains_key(table));
            }

            let executions: Vec<ExecutionDBWithNames> =
                db.fetch_all::<ExecutionDBWithNames>(()).await?;
            assert_eq!(executions.len(), 1);
            assert_eq!(executions[0].id, response.id);
            assert_eq!(executions[0].name, response.name);
            assert_eq!(executions[0].collection_id, collection_0.collection_id);
            // The status views aggregate the plan's runs; the plan's shape is
            // the edition's business and asserted in its te-execution tests.
            let runs: Vec<LogicalFunctionRunDB> = db.fetch_all::<LogicalFunctionRunDB>(()).await?;
            let all_scheduled = runs
                .iter()
                .all(|run| run.status == LogicalFunctionRunStatus::Scheduled);
            let expected = if all_scheduled {
                ExecutionStatus::Scheduled
            } else {
                ExecutionStatus::Running
            };
            assert_eq!(executions[0].status, expected);

            let transactions: Vec<TransactionDBWithNames> =
                db.fetch_all::<TransactionDBWithNames>(()).await?;
            let transaction_ids: HashSet<_> = runs.iter().map(|run| &run.transaction_id).collect();
            assert_eq!(transactions.len(), transaction_ids.len());
            for transaction in transactions {
                assert_eq!(&transaction.execution_id, &response.id);
                let scheduled = runs
                    .iter()
                    .filter(|run| run.transaction_id == transaction.id)
                    .all(|run| run.status == LogicalFunctionRunStatus::Scheduled);
                let expected = if scheduled {
                    TransactionStatus::Scheduled
                } else {
                    TransactionStatus::Stalled
                };
                assert_eq!(transaction.status, expected);
            }

            // Concrete FRs/TDVs are minted at dispatch, not plan-build.
            let function: FunctionDBWithNames = db
                .scd2()
                .fetch_one::<FunctionDBWithNames>(and!(
                    &collection_0.collection_id,
                    &FunctionIdName::Name(FUNCTION_NAMES[0].clone())
                ))
                .await?;
            let lfrs: Vec<LogicalFunctionRunDB> = db
                .fetch_all::<LogicalFunctionRunDB>(&function.function_id)
                .await?;
            assert_eq!(lfrs.len(), 1);
            assert_eq!(lfrs[0].collection_id, collection_0.collection_id);
            assert_eq!(lfrs[0].execution_id, response.id);
            assert_eq!(lfrs[0].status, LogicalFunctionRunStatus::Scheduled);

            let function: FunctionDBWithNames = db
                .scd2()
                .fetch_one::<FunctionDBWithNames>(and!(
                    &collection_2.collection_id,
                    &FunctionIdName::Name(FUNCTION_NAMES[2].clone())
                ))
                .await?;
            let lfrs: Vec<LogicalFunctionRunDB> = db
                .fetch_all::<LogicalFunctionRunDB>(&function.function_id)
                .await?;
            for lfr in lfrs {
                assert_eq!(&lfr.execution_id, &response.id);
            }

            let ltdvs: Vec<LogicalTableDataVersionDB> = db
                .timeline()
                .fetch_all::<LogicalTableDataVersionDB>(())
                .await?;
            assert_eq!(ltdvs.len(), response.created_tables.len());
            for ltdv in ltdvs {
                assert_eq!(&ltdv.execution_id, &response.id);
            }
            Ok(response)
        }
    }

    #[td_test::test(db)]
    async fn test_execute_initial_status_upstream_scheduled(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        Box::pin(test_execute_upstream_initial_status(
            db,
            p.project_id,
            LogicalFunctionRunStatus::Scheduled,
            LogicalFunctionRunStatus::Scheduled,
        ))
        .await
    }

    #[td_test::test(db)]
    async fn test_execute_initial_status_upstream_on_hold(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        Box::pin(test_execute_upstream_initial_status(
            db,
            p.project_id,
            LogicalFunctionRunStatus::OnHold,
            LogicalFunctionRunStatus::OnHold,
        ))
        .await
    }

    #[td_test::test(db)]
    async fn test_execute_initial_status_upstream_failed(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        Box::pin(test_execute_upstream_initial_status(
            db,
            p.project_id,
            LogicalFunctionRunStatus::Failed,
            LogicalFunctionRunStatus::OnHold,
        ))
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
            child_functions: None,
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
            parent_tables: None,
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
            child_functions: None,
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
            parent_tables: None,
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
            child_functions: None,
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
            parent_tables: None,
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
                &collection.collection_id,
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
                &collection.collection_id,
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
                &collection.collection_id,
                &FunctionIdName::Name(FUNCTION_NAMES[2].clone())
            ))
            .await?;
        let lfrs: Vec<LogicalFunctionRunDB> = db
            .fetch_all::<LogicalFunctionRunDB>(&function_2.function_id)
            .await?;
        assert_eq!(lfrs.len(), 1);
        assert_eq!(lfrs[0].status, expected_initial_status);
        let execution: ExecutionDB = db
            .scoped(project_id)
            .fetch_one(&lfrs[0].execution_id)
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
        let res =
            test_execute_disabled_trigger(db, p.project_id, OverrideDisabledTrigger::FALSE).await?;
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
            test_execute_disabled_trigger(db, p.project_id, OverrideDisabledTrigger::TRUE).await?;
        let response = response?;

        assert_eq!(
            response.name,
            Some(ExecutionName::try_from("test_execution")?)
        );
        assert!(response.triggered_on < TriggeredOn::asserted(*AtTime::now()));

        let manual_trigger = &response.all_functions[&response.manual_trigger];
        assert_eq!(manual_trigger.name, FUNCTION_NAMES[0]);
        assert!(
            response
                .triggered_functions
                .contains(&response.manual_trigger),
            "the overridden disabled trigger runs"
        );
        assert!(
            response
                .created_tables
                .iter()
                .any(|t| response.all_tables[t].name.to_string() == "table_0"),
            "and writes its table"
        );
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
            child_functions: None,
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
            parent_tables: None,
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
            child_functions: None,
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
            parent_tables: None,
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
        let p = db.seed().test_project().await?.project_id;
        let collection = db
            .scoped(p)
            .seed()
            .collection(&COLLECTION_NAMES[0], &UserId::admin())
            .await?;
        let create = FunctionRegister {
            child_functions: None,
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
            parent_tables: None,
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

    fn parent_with_child() -> Result<FunctionRegister, TdError> {
        Ok(FunctionRegister {
            child_functions: Some(vec![ChildFunction {
                name: FunctionName::try_from("child_fn")?,
                function_type: FunctionType::default(),
                bundle_id: BundleId::default(),
                snippet: "child snippet".try_into()?,
                dependencies: Some(vec![TableDependencyDto::try_from("parent_table")?]),
                tables: Some(vec![TableNameDto::try_from("child_table")?]),
                table_types: Some(HashMap::from([(
                    TableNameDto::try_from("child_table")?,
                    TableType::DQSummaryTrends,
                )])),
            }]),
            name: FunctionName::try_from("parent_fn")?,
            description: "parent".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "parent snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: vec![TableNameDto::try_from("parent_table")?].into(),
            table_types: None,
            parent_tables: Some(HashMap::from([(
                TableNameDto::try_from("child_table")?,
                TableNameDto::try_from("parent_table")?,
            )])),
            runtime_values: FunctionRuntimeValues::try_from("rt")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        })
    }

    async fn execute_by_name(
        db: &TdPool,
        project: ProjectId,
        collection: &str,
        function: &str,
        function_only: FunctionOnly,
        override_disabled_trigger: OverrideDisabledTrigger,
    ) -> Result<ExecutionResponse, TdError> {
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(collection)?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str(function)?,
                    },
                ),
                ExecutionRequest {
                    name: None,
                    function_only,
                    override_disabled_trigger,
                    skip_if_running: SkipIfRunning::default(),
                    external_uid: None,
                },
            )
            .in_project(project);
        Ok(Services::with_defaults(db.clone())
            .execute_function(request)
            .await?
            .data)
    }

    async fn register_parent_with_child(
        db: &TdPool,
        project: ProjectId,
        collection: &str,
    ) -> Result<(), TdError> {
        create_collection(db, project, collection, None).await;
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam {
                    collection: CollectionIdName::from_str(collection)?,
                },
                parent_with_child()?,
            )
            .in_project(project);
        Services::with_defaults(db.clone())
            .register_function(request)
            .await?;
        Ok(())
    }

    fn planned_names(response: &ExecutionResponse) -> HashSet<String> {
        response
            .triggered_functions
            .iter()
            .map(|f| response.all_functions[f].name.to_string())
            .collect()
    }

    fn shares_one_transaction(response: &ExecutionResponse) -> bool {
        response.transactions.len() == 1
            && response
                .transactions
                .values()
                .all(|functions| functions.len() == 2)
    }

    #[td_test::test(db)]
    async fn test_executing_the_parent_plans_its_children(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        register_parent_with_child(&db, p.project_id, "plan_children").await?;

        let response = execute_by_name(
            &db,
            p.project_id,
            "plan_children",
            "parent_fn",
            FunctionOnly::FALSE,
            OverrideDisabledTrigger::FALSE,
        )
        .await?;

        assert_eq!(
            planned_names(&response),
            HashSet::from(["parent_fn".to_string(), "child_fn".to_string()])
        );
        assert!(shares_one_transaction(&response));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_reading_the_execution_shows_its_children(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        register_parent_with_child(&db, p.project_id, "read_children").await?;
        let executed = execute_by_name(
            &db,
            p.project_id,
            "read_children",
            "parent_fn",
            FunctionOnly::FALSE,
            OverrideDisabledTrigger::FALSE,
        )
        .await?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin()).read(ExecutionParam {
                execution: executed.id.clone(),
            });
        let read = Services::with_defaults(db.clone())
            .execution_read(request)
            .await?
            .data;

        assert_eq!(planned_names(&read), planned_names(&executed));
        assert!(shares_one_transaction(&read));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_function_only_still_plans_the_children(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        register_parent_with_child(&db, p.project_id, "plan_only").await?;

        let response = execute_by_name(
            &db,
            p.project_id,
            "plan_only",
            "parent_fn",
            FunctionOnly::TRUE,
            OverrideDisabledTrigger::FALSE,
        )
        .await?;

        assert_eq!(
            planned_names(&response),
            HashSet::from(["parent_fn".to_string(), "child_fn".to_string()])
        );
        assert!(shares_one_transaction(&response));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_overriding_a_disabled_parent_runs_its_children(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        create_collection(&db, p.project_id, "plan_override", None).await;
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam {
                    collection: CollectionIdName::from_str("plan_override")?,
                },
                FunctionRegister {
                    enabled: Some(Enabled::FALSE),
                    ..parent_with_child()?
                },
            )
            .in_project(p.project_id);
        Services::with_defaults(db.clone())
            .register_function(request)
            .await?;

        let response = execute_by_name(
            &db,
            p.project_id,
            "plan_override",
            "parent_fn",
            FunctionOnly::FALSE,
            OverrideDisabledTrigger::TRUE,
        )
        .await?;

        assert_eq!(
            planned_names(&response),
            HashSet::from(["parent_fn".to_string(), "child_fn".to_string()])
        );
        assert!(shares_one_transaction(&response));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_a_disabled_child_is_not_planned(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection = db
            .scoped(p.project_id)
            .seed()
            .collection(
                &CollectionName::try_from("plan_disabled")?,
                &UserId::admin(),
            )
            .await?;
        let mut parent = parent_with_child()?;
        let declared = parent.child_functions.take().expect("child").remove(0);
        let parent = db
            .scoped(p.project_id)
            .seed()
            .function(&collection, &parent)
            .await?;
        let child = FunctionRegister {
            name: declared.name,
            tables: declared.tables,
            dependencies: declared.dependencies,
            triggers: Some(vec![TableTriggerDto::try_from("parent_table")?]),
            enabled: Some(Enabled::FALSE),
            ..parent_with_child()?
        };
        db.scoped(p.project_id)
            .seed()
            .child_function(
                &collection,
                &FunctionRegister {
                    child_functions: None,
                    ..child
                },
                &parent.function_id,
            )
            .await?;

        let response = execute_by_name(
            &db,
            p.project_id,
            "plan_disabled",
            "parent_fn",
            FunctionOnly::FALSE,
            OverrideDisabledTrigger::FALSE,
        )
        .await?;

        assert_eq!(
            planned_names(&response),
            HashSet::from(["parent_fn".to_string()])
        );
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_triggering_a_child_function_is_refused(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        register_parent_with_child(&db, p.project_id, "trigger_guard").await?;

        let err = execute_by_name(
            &db,
            p.project_id,
            "trigger_guard",
            "child_fn",
            FunctionOnly::FALSE,
            OverrideDisabledTrigger::FALSE,
        )
        .await
        .expect_err("a child function must not be triggerable");

        assert!(matches!(
            err.domain_err::<ChildFunctionError>(),
            ChildFunctionError::NotUserManaged(_)
        ));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_manually_triggering_a_stage_publisher_is_refused(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection = db
            .scoped(p.project_id)
            .seed()
            .collection(&CollectionName::try_from("stage_guard")?, &UserId::admin())
            .await?;
        let create = FunctionRegister {
            workers: Some(HashMap::from([(
                WorkerTypeDto::StageTrigger,
                WorkerConfig::default(),
            )])),
            child_functions: None,
            name: FunctionName::try_from("stream_pub")?,
            description: "d".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "s".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: Some(ConnectionKind::try_from("mysqlSrcConn")?),
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: vec![TableNameDto::try_from("table_0")?].into(),
            table_types: None,
            parent_tables: None,
            runtime_values: FunctionRuntimeValues::try_from("rv")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
        };
        db.scoped(p.project_id)
            .seed()
            .function(&collection, &create)
            .await?;

        let err = execute_by_name(
            &db,
            p.project_id,
            "stage_guard",
            "stream_pub",
            FunctionOnly::FALSE,
            OverrideDisabledTrigger::FALSE,
        )
        .await
        .expect_err("a stage publisher must not be triggered manually");

        assert!(matches!(
            err.domain_err::<TriggerError>(),
            TriggerError::StageTriggered(_)
        ));
        Ok(())
    }
}
