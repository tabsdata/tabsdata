//
// Copyright 2025 Tabsdata Inc.
//

use itertools::Itertools;
use std::collections::HashSet;
use ta_services::ctx_service::{Message, ReqCtx};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::{TdError, td_error};
use td_objects::and;
use td_objects::dxo::logical::{
    LogicalFunctionRunCommitDB, LogicalFunctionRunDB, LogicalFunctionRunUpdateDB,
};
use td_objects::dxo::physical_function_run::{PhysicalFunctionRunDB, UpdatePhysicalFunctionRunDB};
use td_objects::dxo::physical_table_data_version::{
    PhysicalTableDataVersionDB, PhysicalTableDataVersionDBWithNames, UpdateTableDataVersionDB,
};
use td_objects::dxo::worker::{CallbackRequest, WorkerDB, WorkerUpdateDB};
use td_objects::dxo::yaml::WorkerOutput;
use td_objects::dxo::yaml::function::WrittenTable;
use td_objects::types::basic::{
    AtTime, ExecutionId, FunctionId, HasData, LogicalFunctionRunId, LogicalFunctionRunStatus,
    PhysicalFunctionRunId, PhysicalFunctionRunStatus, SchemaChanged, WorkerId,
};
use td_queries::{ChunkedQueries, LfrEntry, LiveEntry, LtdvEntry};
use tracing::{debug, instrument};

#[td_error]
enum UpdateStatusRunError {
    #[error("Cannot change final '{1:?}' status for LFR [{0}]")]
    StickyFinalStatus(LogicalFunctionRunId, LogicalFunctionRunStatus) = 0,
    #[error("Unexpected LFR status transition for LFR [{0}]: {1:?} -> {2:?}")]
    UnexpectedFunctionRunStatusTransition(
        LogicalFunctionRunId,
        LogicalFunctionRunStatus,
        LogicalFunctionRunStatus,
    ) = 3,
    #[error("No LFR status update was performed")]
    NoOpStatusUpdate = 4,
    #[error("Absorbed replay-attempt callback for terminal LFR [{0}] (status {1:?})")]
    ReplayCallbackAbsorbed(LogicalFunctionRunId, LogicalFunctionRunStatus) = 5,
}

#[instrument(
    name = "worker.callback",
    skip_all,
    fields(worker_id = %*worker_id),
    level = "debug",
    err(Debug)
)]
pub async fn update_worker_status(
    conn: &mut DomainConnection<Apiserver>,
    update: &WorkerUpdateDB,
    worker_id: &WorkerId,
) -> Result<(), TdError> {
    // Worker status update is never conditional, we want to know exactly what got reported,
    // as it doesn't affect execution flow, it's just informational.
    let _ = conn
        .live()
        .update::<_, WorkerDB>(update, &*worker_id)
        .await?;

    Ok(())
}

/// Recover of an LFR stuck in `Requested` re-dispatches it, but its
/// original attempt may still materialize later (a parked queue message
/// finally spawning a pod). Terminalizing the attempt's in-flight PFR as
/// `Failed` here makes any late callback from it drop instead of racing the new
/// attempt.
#[instrument(
    name = "execution.recover_abandon",
    skip_all,
    level = "debug",
    err(Debug)
)]
pub async fn abandon_requested_physical_runs(
    conn: &mut DomainConnection<Apiserver>,
    at: &AtTime,
    logical_function_runs: &[LogicalFunctionRunDB],
) -> Result<(), TdError> {
    let requested: HashSet<(&ExecutionId, &FunctionId)> = logical_function_runs
        .iter()
        .filter(|lfr| lfr.status == LogicalFunctionRunStatus::Requested)
        .map(|lfr| (&lfr.execution_id, &lfr.function_id))
        .collect();
    if requested.is_empty() {
        return Ok(());
    }

    let execution_ids: Vec<ExecutionId> = requested
        .iter()
        .map(|(execution_id, _)| (*execution_id).clone())
        .unique()
        .collect();
    let physical_function_runs: Vec<PhysicalFunctionRunDB> = conn
        .fetch_all_chunked::<PhysicalFunctionRunDB, ExecutionId>(&execution_ids)
        .await?;
    let abandoned: Vec<PhysicalFunctionRunId> = physical_function_runs
        .iter()
        .filter(|pfr| {
            PhysicalFunctionRunStatus::IN_FLIGHT.contains(&pfr.status)
                && requested.contains(&(&pfr.execution_id, &pfr.function_id))
        })
        .map(|pfr| pfr.id.clone())
        .collect();
    if abandoned.is_empty() {
        return Ok(());
    }

    let update = UpdatePhysicalFunctionRunDB::failed_at(at)?;
    conn.update_chunked::<_, PhysicalFunctionRunDB, _>(&update, &abandoned)
        .await?;
    debug!(
        abandoned_count = abandoned.len(),
        "execution.recover_abandon: terminalized in-flight PFR(s) of Requested LFR(s)"
    );
    Ok(())
}

/// `Absorb` is a legal no-op, `Skip` a silent one. `Refuse` fails the request
/// for a caller driving the transition; the cascade downgrades it to a warning.
enum Transition {
    Apply,
    Skip,
    Absorb(UpdateStatusRunError),
    Refuse(UpdateStatusRunError),
}

/// Arm order is load-bearing: the sticky-final arms come first, so a Committed
/// LFR is never demoted by a later arm.
fn transition(current: &LogicalFunctionRunDB, target: &LogicalFunctionRunStatus) -> Transition {
    match (&current.status, target) {
        // Sticky-final terminals (Committed, Canceled). Worker callbacks
        // for an in-flight replay attempt (Running, Done, Error, Failed)
        // absorb — the trailing `redirect_pointers_at_finish` is the step
        // that actually captures the replay outcome at the pointer.
        // Same-state transitions also absorb (idempotent). Any other
        // target is an operator action and gets refused.
        (
            s @ (LogicalFunctionRunStatus::Committed | LogicalFunctionRunStatus::Canceled),
            LogicalFunctionRunStatus::Running
            | LogicalFunctionRunStatus::Done
            | LogicalFunctionRunStatus::Error
            | LogicalFunctionRunStatus::Failed,
        ) => Transition::Absorb(UpdateStatusRunError::StickyFinalStatus(
            current.id.clone(),
            *s,
        )),
        (s @ LogicalFunctionRunStatus::Yanked, LogicalFunctionRunStatus::Yanked)
        | (
            s @ (LogicalFunctionRunStatus::Committed | LogicalFunctionRunStatus::Canceled),
            LogicalFunctionRunStatus::Committed | LogicalFunctionRunStatus::Canceled,
        ) if s == target => Transition::Absorb(UpdateStatusRunError::StickyFinalStatus(
            current.id.clone(),
            *s,
        )),
        (
            s @ (LogicalFunctionRunStatus::Committed
            | LogicalFunctionRunStatus::Canceled
            | LogicalFunctionRunStatus::Yanked),
            _,
        ) => Transition::Refuse(UpdateStatusRunError::StickyFinalStatus(
            current.id.clone(),
            *s,
        )),

        // Non-sticky terminals (Failed, Done) under an in-flight replay
        // attempt. Running / Error / Failed callbacks absorb (no
        // downgrade). `Failed -> Done` is the user-visible "replay healed
        // a failed run" promotion; `Done -> Done` is idempotent.
        (
            LogicalFunctionRunStatus::Failed | LogicalFunctionRunStatus::Done,
            LogicalFunctionRunStatus::Running
            | LogicalFunctionRunStatus::Error
            | LogicalFunctionRunStatus::Failed,
        )
        | (LogicalFunctionRunStatus::Done, LogicalFunctionRunStatus::Done) => Transition::Absorb(
            UpdateStatusRunError::ReplayCallbackAbsorbed(current.id.clone(), current.status),
        ),
        (LogicalFunctionRunStatus::Failed, LogicalFunctionRunStatus::Done) => Transition::Apply,

        (LogicalFunctionRunStatus::Scheduled, LogicalFunctionRunStatus::Requested) => {
            Transition::Apply
        }
        (LogicalFunctionRunStatus::ReScheduled, LogicalFunctionRunStatus::Requested) => {
            Transition::Apply
        }
        (LogicalFunctionRunStatus::Requested, LogicalFunctionRunStatus::Running) => {
            Transition::Apply
        }
        (LogicalFunctionRunStatus::Running, LogicalFunctionRunStatus::Done) => Transition::Apply,
        (
            LogicalFunctionRunStatus::Requested | LogicalFunctionRunStatus::Running,
            LogicalFunctionRunStatus::Error,
        ) => Transition::Apply,
        (
            LogicalFunctionRunStatus::Requested | LogicalFunctionRunStatus::Running,
            LogicalFunctionRunStatus::Failed,
        ) => Transition::Apply,

        // Recover status: failed / on-hold runs, plus runs stuck in
        // Requested (dispatched but the worker never spawned, so no
        // callback will ever arrive — `abandon_requested_physical_runs`
        // terminalizes the dead attempt). Otherwise just no-op.
        (
            LogicalFunctionRunStatus::Failed
            | LogicalFunctionRunStatus::OnHold
            | LogicalFunctionRunStatus::Requested,
            LogicalFunctionRunStatus::ReScheduled,
        ) => Transition::Apply,
        (_, LogicalFunctionRunStatus::ReScheduled) => Transition::Skip,

        (_, LogicalFunctionRunStatus::Canceled) => Transition::Apply,

        (current, new) if current == new => Transition::Skip,

        _ => Transition::Refuse(UpdateStatusRunError::UnexpectedFunctionRunStatusTransition(
            current.id.clone(),
            current.status,
            *target,
        )),
    }
}

/// [`transition`] has no `OnHold` or `Yanked` arms — nothing *requests* those,
/// they only arrive by propagation. A cascade respects stickiness instead.
fn cascade_transition(
    current: &LogicalFunctionRunDB,
    target: &LogicalFunctionRunStatus,
) -> Transition {
    let status = current.status;
    if status == *target {
        return Transition::Skip;
    }
    if matches!(
        status,
        LogicalFunctionRunStatus::Committed
            | LogicalFunctionRunStatus::Canceled
            | LogicalFunctionRunStatus::Yanked
    ) {
        return Transition::Absorb(UpdateStatusRunError::StickyFinalStatus(
            current.id.clone(),
            status,
        ));
    }
    Transition::Apply
}

#[instrument(
    name = "execution.update_status",
    skip_all,
    fields(target_status = %update.status, input_count = logical_function_runs.len()),
    level = "debug",
    err(Debug)
)]
pub async fn update_function_run_status(
    msgs: &mut ReqCtx,
    conn: &mut DomainConnection<Apiserver>,
    logical_function_runs: &Vec<LogicalFunctionRunDB>,
    update: &LogicalFunctionRunUpdateDB,
) -> Result<(), TdError> {
    let input_count = logical_function_runs.len();
    let mut lfr_ids = Vec::new();
    for current in logical_function_runs {
        match transition(current, &update.status) {
            Transition::Apply => lfr_ids.push(current.id.clone()),
            Transition::Skip => {}
            Transition::Absorb(reported) => msgs.warning(Message::of(reported)),
            Transition::Refuse(refused) => return Err(refused.into()),
        }
    }

    debug!(
        input_count = input_count,
        actionable_count = lfr_ids.len(),
        "execution.update_status: filtered transitions"
    );

    if lfr_ids.is_empty() {
        debug!("execution.update_status: no-op (all filtered)");
        msgs.warning(Message::of(UpdateStatusRunError::NoOpStatusUpdate));
        return Ok(());
    }

    let _ = conn
        .update_chunked::<_, LogicalFunctionRunDB, _>(update, &lfr_ids)
        .await?;
    debug!(
        updated_count = lfr_ids.len(),
        "execution.update_status: updated LFR(s)"
    );

    // Publish LFRs if needed, including downstream publishing.
    if update.status == LogicalFunctionRunStatus::Done {
        let updated_set: HashSet<&LogicalFunctionRunId> = lfr_ids.iter().collect();
        let execution_ids: Vec<ExecutionId> = logical_function_runs
            .iter()
            .filter(|f| updated_set.contains(&f.id))
            .map(|f| f.execution_id.clone())
            .unique()
            .collect();
        let to_commit_ffrs: Vec<LogicalFunctionRunId> =
            conn.lfr().to_commit(&execution_ids).await?;
        debug!(
            commit_count = to_commit_ffrs.len(),
            "execution.commit: LFR(s) eligible to commit"
        );

        if !to_commit_ffrs.is_empty() {
            let commit_update = LogicalFunctionRunCommitDB::default();
            let _ = conn
                .update_chunked::<_, LogicalFunctionRunDB, _>(&commit_update, &to_commit_ffrs)
                .await?;
            debug!(
                committed_count = to_commit_ffrs.len(),
                "execution.commit: committed LFR(s)"
            );
        }
    }

    let downstream_status = match update.status {
        LogicalFunctionRunStatus::Canceled => Some(LogicalFunctionRunStatus::Canceled),
        LogicalFunctionRunStatus::Yanked => Some(LogicalFunctionRunStatus::Yanked),
        LogicalFunctionRunStatus::ReScheduled => Some(LogicalFunctionRunStatus::ReScheduled),
        LogicalFunctionRunStatus::Failed => Some(LogicalFunctionRunStatus::OnHold),
        _ => None,
    };

    if let Some(downstream_status) = downstream_status
        && !lfr_ids.is_empty()
    {
        cascade_downstream_status(msgs, conn, &lfr_ids, &downstream_status).await?;
    }

    Ok(())
}

/// The cone crosses executions (`downstream_one_hop` has no `execution_id`
/// bound), so it reaches rows the caller never named. A declined row warns
/// rather than failing the request: the operator named the ancestor.
#[instrument(
    name = "execution.cascade_status",
    skip_all,
    fields(cascade_status = %status, from_count = starting.len()),
    level = "debug",
    err(Debug)
)]
async fn cascade_downstream_status(
    msgs: &mut ReqCtx,
    conn: &mut DomainConnection<Apiserver>,
    starting: &[LogicalFunctionRunId],
    status: &LogicalFunctionRunStatus,
) -> Result<(), TdError> {
    let downstream_ids = bfs_downstream_lfrs(&mut *conn, starting).await?;
    if downstream_ids.is_empty() {
        return Ok(());
    }
    let downstream: Vec<LogicalFunctionRunDB> = conn
        .fetch_all_chunked::<LogicalFunctionRunDB, LogicalFunctionRunId>(&downstream_ids)
        .await?;

    let mut to_update = Vec::new();
    for current in &downstream {
        match cascade_transition(current, status) {
            Transition::Apply => to_update.push(current.id.clone()),
            Transition::Skip => {}
            Transition::Absorb(reported) | Transition::Refuse(reported) => {
                msgs.warning(Message::of(reported))
            }
        }
    }
    if to_update.is_empty() {
        return Ok(());
    }

    let cascade_update = LogicalFunctionRunUpdateDB { status: *status };
    let _ = conn
        .update_chunked::<_, LogicalFunctionRunDB, _>(&cascade_update, &to_update)
        .await?;
    debug!(
        cascade_status = %status,
        cascade_count = to_update.len(),
        skipped_count = downstream.len() - to_update.len(),
        "execution.cascade_status: propagated to downstream LFR(s)"
    );
    Ok(())
}

/// BFS over the logic dependency graph from `starting`. Each iteration
/// runs a one-hop downstream query against the current frontier, threading
/// the visited set in as a SQL `NOT IN` exclusion so already-walked LFRs
/// aren't re-emitted. Loop ends when no new LFR ids surface. Returns the
/// union of all reachable downstream LFRs, excluding the starting set
/// itself.
async fn bfs_downstream_lfrs(
    conn: &mut DomainConnection<Apiserver>,
    starting: &[LogicalFunctionRunId],
) -> Result<Vec<LogicalFunctionRunId>, TdError> {
    let starting_set: HashSet<LogicalFunctionRunId> = starting.iter().cloned().collect();
    let mut visited: HashSet<LogicalFunctionRunId> = starting_set.clone();
    let mut frontier: Vec<LogicalFunctionRunId> = starting.to_vec();

    while !frontier.is_empty() {
        let exclude: Vec<LogicalFunctionRunId> = visited.iter().cloned().collect();
        let next: Vec<LogicalFunctionRunId> =
            conn.lfr().downstream_one_hop(&frontier, &exclude).await?;
        let mut new_frontier = Vec::new();
        for id in next {
            if visited.insert(id.clone()) {
                new_frontier.push(id);
            }
        }
        frontier = new_frontier;
    }

    Ok(visited
        .into_iter()
        .filter(|id| !starting_set.contains(id))
        .collect())
}

pub async fn update_table_data_version_status(
    conn: &mut DomainConnection<Apiserver>,
    physical_function_run_id: &PhysicalFunctionRunId,
    callback: &CallbackRequest,
) -> Result<(), TdError> {
    if let Some(WorkerOutput::Function(output)) = &callback.context {
        for written in &output.output {
            let (table_name, table_info) = match written {
                WrittenTable::NoData { table } => (table, None),
                WrittenTable::Data { table, info } => (table, Some(info)),
                // TODO partitions should be handled differently, creating partitions and setting
                // the table to has_data = true and partition = true.
                WrittenTable::Partitions { table, info, .. } => (table, Some(info)),
            };

            // one because single table data version per execution
            let data_version = conn
                .live()
                .fetch_one::<PhysicalTableDataVersionDBWithNames>(and!(
                    physical_function_run_id.clone(),
                    table_name,
                ))
                .await?;

            // Walk LTDV pointer history for this table at or before this
            // TDV's triggered_on to find the previous canonically-bound
            // with-data TDV. Pointer-walk rather than a raw physical scan
            // so in-flight replays don't poison the schema-change check.
            let latest_version_with_data = conn
                .ltdv()
                .latest_bound_tdv_with_data_for_table(
                    &data_version.base.base.table_id,
                    &data_version.base.base.triggered_on,
                    false,
                )
                .await?;

            let update = if let Some(table_info) = table_info {
                let schema_changed = match &latest_version_with_data {
                    Some(prev) => SchemaChanged::from(
                        prev.base.base.schema_hash.as_ref() != Some(&table_info.schema_hash),
                    ),
                    None => SchemaChanged::FALSE,
                };

                UpdateTableDataVersionDB {
                    has_data: Some(HasData::TRUE),
                    schema_changed: Some(schema_changed),
                    column_count: Some(table_info.column_count),
                    row_count: Some(table_info.row_count),
                    data_bytes: table_info.data_bytes,
                    schema_hash: Some(table_info.schema_hash.clone()),
                    with_data_table_data_version_id: Some(data_version.base.base.id.clone()),
                }
            } else if let Some(latest_version_with_data) = latest_version_with_data {
                UpdateTableDataVersionDB {
                    has_data: Some(HasData::FALSE),
                    schema_changed: Some(SchemaChanged::FALSE),
                    column_count: latest_version_with_data.base.base.column_count,
                    row_count: latest_version_with_data.base.base.row_count,
                    data_bytes: latest_version_with_data.base.base.data_bytes,
                    schema_hash: latest_version_with_data.base.base.schema_hash.clone(),
                    with_data_table_data_version_id: Some(latest_version_with_data.base.base.id),
                }
            } else {
                UpdateTableDataVersionDB {
                    has_data: Some(HasData::FALSE),
                    schema_changed: Some(SchemaChanged::FALSE),
                    column_count: None,
                    row_count: None,
                    data_bytes: None,
                    schema_hash: None,
                    with_data_table_data_version_id: None,
                }
            };

            conn.live()
                .update::<_, PhysicalTableDataVersionDB>(&update, &data_version.base.base.id)
                .await?;
        }
    }

    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use itertools::Itertools;
    use std::collections::{HashMap, HashSet};
    use std::future::Future;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::and;
    use td_objects::dxo::collection::ActiveCollectionDB;
    use td_objects::dxo::execution::{ExecutionDB, ExecutionDBWithStatus};
    use td_objects::dxo::function::{FunctionDB, FunctionRegister};
    use td_objects::dxo::logical::LogicalFunctionRunDB;
    use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
    use td_objects::dxo::physical_table_data_version::{
        PhysicalTableDataVersionDB, PhysicalTableDataVersionDBWithFunction,
    };
    use td_objects::dxo::table::TableDB;
    use td_objects::dxo::transaction::{TransactionDB, TransactionDBWithStatus};
    use td_objects::dxo::worker::WorkerDB;
    use td_objects::types::basic::ReuseFrozen;
    use td_objects::types::basic::{
        BundleId, CollectionName, Decorator, DependencyPos, ExecutionStatus, FunctionName,
        FunctionRuntimeValues, LogicalFunctionRunStatus, ProjectId, TableName, TableNameDto,
        TransactionStatus, UserId, VersionPos, WorkerMessageStatus,
    };
    use td_objects::types::composed::TableDependencyDto;
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;

    #[derive(Debug, Clone, Eq, PartialEq, Hash)]
    pub(crate) struct TestExecution {
        pub(crate) expected_status: ExecutionStatus,
        pub(crate) transactions: Vec<TestTransaction>,
    }

    #[derive(Debug, Clone, Eq, PartialEq, Hash)]
    pub(crate) struct TestTransaction {
        pub(crate) expected_status: TransactionStatus,
        pub(crate) functions: Vec<TestFunction>,
    }

    #[derive(Debug, Clone, Eq, PartialEq, Hash)]
    pub(crate) struct TestFunction {
        pub(crate) collection: CollectionName,
        pub(crate) name: FunctionName,
        pub(crate) dependencies: Vec<TableDependencyDto>,
        pub(crate) tables: Vec<TableNameDto>,
        pub(crate) initial_status: LogicalFunctionRunStatus,
        pub(crate) expected_status: LogicalFunctionRunStatus,
    }

    // Creates proper setup for status update tests. It creates entities in test_executions. If names
    // are the same, they are not recreated, but reused.
    pub(crate) async fn test_status_update<F, Fut>(
        db: TdPool,
        project_id: ProjectId,
        test_executions: &Vec<TestExecution>,
        test_run: F,
    ) -> Result<(), TdError>
    where
        F: Fn(
            &HashMap<&CollectionName, ActiveCollectionDB>,
            &HashMap<&TestExecution, ExecutionDB>,
            &HashMap<&TestTransaction, TransactionDB>,
            &HashMap<&TestFunction, PhysicalFunctionRunDB>,
            &HashMap<&TestFunction, WorkerDB>,
        ) -> Fut,
        Fut: Future<Output = Result<(), TdError>>,
    {
        let collections: HashSet<_> = test_executions
            .iter()
            .flat_map(|e| &e.transactions)
            .flat_map(|t| &t.functions)
            .map(|f| &f.collection)
            .collect();
        let collections_futures = collections
            .into_iter()
            .map(|c| {
                let db = db.clone();
                async move {
                    let existing = db
                        .live()
                        .fetch_optional::<ActiveCollectionDB>(c)
                        .await
                        .unwrap();

                    if let Some(existing) = existing {
                        (c, existing)
                    } else {
                        (
                            c,
                            db.scoped(project_id)
                                .seed()
                                .collection(c, &UserId::admin())
                                .await
                                .unwrap(),
                        )
                    }
                }
            })
            .collect::<Vec<_>>();
        let collections = futures::future::join_all(collections_futures)
            .await
            .into_iter()
            .collect::<HashMap<_, _>>();

        // We use a default table to seed functions, so we can use it in the implicit
        // function requirements, similar to fn_state.
        let default_table = TableNameDto::try_from("default")?;
        let mut function_versions = HashMap::new();
        for test_function in test_executions
            .iter()
            .flat_map(|e| &e.transactions)
            .flat_map(|t| &t.functions)
        {
            let collection = collections.get(&test_function.collection).unwrap();
            let existing = db
                .live()
                .fetch_optional::<FunctionDB>(and!(&collection.id, &test_function.name))
                .await?;

            if let Some(existing) = existing {
                function_versions.insert(test_function, existing);
            } else {
                let tables = test_function
                    .tables
                    .iter()
                    .cloned()
                    .chain(std::iter::once(default_table.clone()))
                    .collect::<Vec<_>>();

                let create = FunctionRegister {
                    name: test_function.name.clone(),
                    description: "foo description".try_into()?,
                    bundle_id: BundleId::default(),
                    snippet: "foo snippet".try_into()?,
                    decorator: Decorator::Publisher,
                    connector: None,
                    connection: None,
                    base_path: None,
                    dependencies: Some(test_function.dependencies.clone()),
                    triggers: None,
                    tables: Some(tables),
                    table_types: None,
                    runtime_values: FunctionRuntimeValues::default(),
                    reuse_frozen_tables: ReuseFrozen::FALSE,
                    enabled: None,
                    workers: None,
                };

                let function_version = db
                    .scoped(project_id)
                    .seed()
                    .function(collection, &create)
                    .await?;
                function_versions.insert(test_function, function_version);
            }
        }

        let executions_futures = test_executions
            .iter()
            .map(|e| {
                let function_versions = function_versions.clone();
                {
                    let value = db.clone();
                    async move {
                        // use first as the function version for the execution is informational
                        let (_, function) =
                            function_versions.iter().find_or_first(|_| false).unwrap();
                        (
                            e,
                            value
                                .scoped(project_id)
                                .seed()
                                .execution(function)
                                .await
                                .unwrap(),
                        )
                    }
                }
            })
            .collect::<Vec<_>>();
        let executions = futures::future::join_all(executions_futures)
            .await
            .into_iter()
            .collect::<HashMap<_, _>>();

        let transactions: HashSet<_> = test_executions
            .iter()
            .flat_map(|e| e.transactions.iter().map(move |t| (e, t)))
            .collect();
        let transactions_futures = transactions
            .iter()
            .enumerate()
            .map(|(i, (e, t))| {
                let db = db.clone();
                {
                    let executions = executions.clone();
                    async move {
                        (
                            *t,
                            db.scoped(project_id)
                                .seed()
                                .transaction(
                                    executions.get(e).unwrap(),
                                    &i.to_string().try_into().unwrap(),
                                )
                                .await
                                .unwrap(),
                        )
                    }
                }
            })
            .collect::<Vec<_>>();
        let transactions = futures::future::join_all(transactions_futures)
            .await
            .into_iter()
            .collect::<HashMap<_, _>>();

        let physical_function_runs: HashSet<_> = test_executions
            .iter()
            .flat_map(|e| e.transactions.iter().map(move |t| (e, t, &t.functions)))
            .flat_map(|(e, t, f)| f.iter().map(move |f| (e, t, f)))
            .collect();
        let function_runs_futures = physical_function_runs
            .iter()
            .map(|(e, t, f)| {
                let db = db.clone();
                let default_table = default_table.clone();
                let function_versions = function_versions.clone();
                let collections = collections.clone();
                let executions = executions.clone();
                let transactions = transactions.clone();
                async move {
                    let physical_function_run = db
                        .scoped(project_id)
                        .seed()
                        .physical_function_run(
                            collections.get(&f.collection).unwrap(),
                            function_versions.get(f).unwrap(),
                            executions.get(e).unwrap(),
                            transactions.get(t).unwrap(),
                            &f.initial_status,
                        )
                        .await
                        .unwrap();

                    let tables = db
                        .live()
                        .fetch_all::<TableDB>(&function_versions.get(f).unwrap().id)
                        .await
                        .unwrap();

                    for table in &tables {
                        let _ = db
                            .scoped(project_id)
                            .seed()
                            .physical_table_data_version(
                                collections.get(&f.collection).unwrap(),
                                executions.get(e).unwrap(),
                                transactions.get(t).unwrap(),
                                &physical_function_run,
                                table,
                            )
                            .await
                            .unwrap();
                    }

                    let _ = db
                        .scoped(project_id)
                        .seed()
                        .logical_input(
                            collections.get(&f.collection).unwrap(),
                            executions.get(e).unwrap(),
                            transactions.get(t).unwrap(),
                            &physical_function_run,
                            tables
                                .iter()
                                .find(|f| f.name == TableName::try_from(&default_table).unwrap())
                                .unwrap(),
                            None,
                            None,
                            None,
                            &VersionPos::try_from(0).unwrap(),
                        )
                        .await
                        .unwrap();

                    (*f, physical_function_run)
                }
            })
            .collect::<Vec<_>>();
        let physical_function_runs = futures::future::join_all(function_runs_futures)
            .await
            .into_iter()
            .collect::<HashMap<_, _>>();

        let function_reqs: HashSet<_> = test_executions
            .iter()
            .flat_map(|e| e.transactions.iter().map(move |t| (e, t, &t.functions)))
            .flat_map(|(e, t, f)| f.iter().map(move |f| (e, t, f)))
            .flat_map(|(e, t, f)| f.dependencies.iter().map(move |d| (e, t, f, d)))
            .collect();
        let function_reqs_futures = function_reqs
            .iter()
            .map(|(e, t, f, d)| {
                let db = db.clone();
                let function_versions = function_versions.clone();
                let physical_function_runs = physical_function_runs.clone();
                let collections = collections.clone();
                let executions = executions.clone();
                let transactions = transactions.clone();
                async move {
                    let dependency_f = test_executions
                        .iter()
                        .flat_map(|e| e.transactions.iter().map(move |t| &t.functions))
                        .flatten()
                        .find(|&f| {
                            let collection = d.collection.as_ref().unwrap_or(&f.collection);
                            f.collection == *collection
                                && f.tables.iter().any(|tbl| *tbl == d.table)
                        })
                        .unwrap();

                    let tables = db
                        .live()
                        .fetch_all::<TableDB>(&function_versions.get(dependency_f).unwrap().id)
                        .await
                        .unwrap();
                    let tables = tables
                        .iter()
                        .map(|tdv| (&tdv.name, tdv))
                        .collect::<HashMap<_, _>>();

                    let physical_table_data_versions = db
                        .live()
                        .fetch_all::<PhysicalTableDataVersionDB>(
                            &function_versions.get(dependency_f).unwrap().id,
                        )
                        .await
                        .unwrap();
                    let physical_table_data_versions = physical_table_data_versions
                        .iter()
                        .map(|tdv| (&tdv.name, tdv))
                        .collect::<HashMap<_, _>>();

                    let req = db
                        .scoped(project_id)
                        .seed()
                        .logical_input(
                            collections.get(&f.collection).unwrap(),
                            executions.get(e).unwrap(),
                            transactions.get(t).unwrap(),
                            physical_function_runs.get(f).unwrap(),
                            tables.get(&TableName::try_from(&d.table).unwrap()).unwrap(),
                            Some(physical_function_runs.get(dependency_f).unwrap()),
                            physical_table_data_versions
                                .get(&TableName::try_from(&d.table).unwrap())
                                .cloned(),
                            Some(&DependencyPos::try_from(0).unwrap()),
                            &VersionPos::try_from(0).unwrap(),
                        )
                        .await
                        .unwrap();

                    (*f, req)
                }
            })
            .collect::<Vec<_>>();
        let _function_reqs = futures::future::join_all(function_reqs_futures)
            .await
            .into_iter()
            .collect::<HashMap<_, _>>();

        let workers: HashSet<_> = test_executions
            .iter()
            .flat_map(|e| e.transactions.iter().map(move |t| (e, t, &t.functions)))
            .flat_map(|(e, t, f)| f.iter().map(move |f| (e, t, f)))
            .collect();
        let workers_futures = workers
            .iter()
            .map(|(e, t, f)| {
                let db = db.clone();
                let executions = executions.clone();
                let transactions = transactions.clone();
                let physical_function_runs = physical_function_runs.clone();
                async move {
                    let req = db
                        .scoped(project_id)
                        .seed()
                        .function_runner_worker(
                            executions.get(e).unwrap(),
                            transactions.get(t).unwrap(),
                            physical_function_runs.get(f).unwrap(),
                            &WorkerMessageStatus::Unlocked,
                        )
                        .await
                        .unwrap();

                    (*f, req)
                }
            })
            .collect::<Vec<_>>();
        let workers = futures::future::join_all(workers_futures)
            .await
            .into_iter()
            .collect::<HashMap<_, _>>();

        test_run(
            &collections,
            &executions,
            &transactions,
            &physical_function_runs,
            &workers,
        )
        .await?;

        for test_execution in test_executions {
            let executions = db
                .live()
                .fetch_all::<ExecutionDBWithStatus>(&executions.get(test_execution).unwrap().id)
                .await?;
            assert_eq!(executions.len(), 1);
            let execution = &executions[0];
            assert_eq!(execution.status, test_execution.expected_status);
        }

        for test_transaction in test_executions.iter().flat_map(|e| &e.transactions) {
            let transactions = db
                .live()
                .fetch_all::<TransactionDBWithStatus>(
                    &transactions.get(test_transaction).unwrap().id,
                )
                .await?;
            assert_eq!(transactions.len(), 1);
            let transaction = &transactions[0];
            assert_eq!(transaction.status, test_transaction.expected_status);
        }

        for test_function in test_executions
            .iter()
            .flat_map(|e| &e.transactions)
            .flat_map(|t| &t.functions)
        {
            let physical_function_run = physical_function_runs.get(test_function).unwrap();

            // Assert all physical_function_runs are in expected state. The LFR
            // carries the canonical flow-control status; derive it via
            // (execution_id, function_id).
            let physical_function_runs = db
                .live()
                .fetch_all::<PhysicalFunctionRunDB>(&physical_function_run.id)
                .await?;
            assert_eq!(physical_function_runs.len(), 1);
            let physical_function_run = &physical_function_runs[0];
            let lfr: LogicalFunctionRunDB = db
                .live()
                .fetch_one::<LogicalFunctionRunDB>(and!(
                    &physical_function_run.execution_id,
                    &physical_function_run.function_id
                ))
                .await?;
            assert_eq!(lfr.status, test_function.expected_status);

            // Per-attempt status on physical_table_data_versions__with_function
            // mirrors FR.status (the producer attempt's lifecycle).
            // Canonical (sticky-Committed) status lives on the producer LFR.
            let physical_table_data_versions = db
                .live()
                .fetch_all::<PhysicalTableDataVersionDBWithFunction>(&physical_function_run.id)
                .await?;
            for physical_table_data_version in &physical_table_data_versions {
                assert_eq!(
                    physical_table_data_version.status,
                    physical_function_run.status
                );
            }
        }

        Ok(())
    }
}
