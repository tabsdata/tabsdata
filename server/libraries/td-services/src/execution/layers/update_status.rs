//
// Copyright 2025 Tabs Data Inc.
//

use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use td_database::dynamic::DynamicConnection;
use td_error::{TdError, td_error};
use td_objects::dxo::function_requirement::FunctionRequirementDBWithNames;
use td_objects::dxo::function_run::{CommitFunctionRunDB, FunctionRunDB, UpdateFunctionRunDB};
use td_objects::dxo::table_data_version::{
    TableDataVersionDB, TableDataVersionDBWithNames, UpdateTableDataVersionDB,
};
use td_objects::dxo::worker::{CallbackRequest, UpdateWorkerDB, WorkerDB};
use td_objects::dxo::yaml::WorkerOutput;
use td_objects::dxo::yaml::function_runner::v2::WrittenTable;
use td_objects::types::basic::{
    Depth, ExecutionId, FunctionRunId, FunctionRunStatus, HasData, SchemaChanged, WorkerId,
};
use td_objects::types::recursion::RecursionOptions;
use td_objects::{Buildable, and, lte};
use td_queries::{BasicQueries, FunctionRunQueries, OptionsBuilder, RecursiveQueries};
use td_tower::ctx_service::ReqCtx;
use td_tower::extractors::{MutRef, Ref};

#[td_error]
enum UpdateStatusRunError {
    #[error("Cannot change final 'Committed' status for function run [{0}]")]
    AlreadyCommitted(FunctionRunId) = 0,
    #[error("Cannot change final 'Canceled' status for function run [{0}]")]
    AlreadyCanceled(FunctionRunId) = 1,
    #[error("Cannot change final 'Yanked' status for function run [{0}]")]
    AlreadyYanked(FunctionRunId) = 2,
    #[error("Unexpected function run status transition for function run [{0}]: {1:?} -> {2:?}")]
    UnexpectedFunctionRunStatusTransition(FunctionRunId, FunctionRunStatus, FunctionRunStatus) = 3,
    #[error("No function run status update where performed")]
    NoOpStatusUpdate = 4,
}

pub async fn update_worker_status(
    mut conn: MutRef<DynamicConnection<'static>>,
    update: Ref<UpdateWorkerDB>,
    worker_id: Ref<WorkerId>,
) -> Result<(), TdError> {
    // Worker status update is never conditional, we want to know exactly what got reported,
    // as it doesn't affect execution flow, it's just informational.
    let _ = conn
        .update::<_, WorkerDB>(update.deref(), &*worker_id)
        .await?;

    Ok(())
}

pub async fn update_function_run_status(
    mut ctx: MutRef<ReqCtx>,
    mut conn: MutRef<DynamicConnection<'static>>,
    function_runs: Ref<Vec<FunctionRunDB>>,
    update: Ref<UpdateFunctionRunDB>,
) -> Result<(), TdError> {
    // Validate status transitions and filter out no-op transitions.
    let function_run_ids = function_runs
        .iter()
        .filter_map(|current| {
            match (&current.status, &update.status) {
                // Final status.
                (FunctionRunStatus::Committed, _) => {
                    Some(Err(UpdateStatusRunError::AlreadyCommitted(current.id)))
                }

                (FunctionRunStatus::Yanked, FunctionRunStatus::Yanked) => {
                    ctx.warning(UpdateStatusRunError::AlreadyYanked(current.id));
                    None
                }
                (FunctionRunStatus::Yanked, _) => {
                    Some(Err(UpdateStatusRunError::AlreadyYanked(current.id)))
                }

                (
                    FunctionRunStatus::Canceled,
                    // Callback status transition, never returning error to avoid race conditions.
                    FunctionRunStatus::Running
                    | FunctionRunStatus::Done
                    | FunctionRunStatus::Error
                    | FunctionRunStatus::Failed
                    // No-op already canceled status transition.
                    | FunctionRunStatus::Canceled,
                ) => {
                    ctx.warning(UpdateStatusRunError::AlreadyCanceled(current.id));
                    None
                }
                (FunctionRunStatus::Canceled, _) => {
                    Some(Err(UpdateStatusRunError::AlreadyCanceled(current.id)))
                }

                // Mutable status
                (FunctionRunStatus::Scheduled, FunctionRunStatus::RunRequested) => {
                    Some(Ok(current.id))
                }
                (FunctionRunStatus::ReScheduled, FunctionRunStatus::RunRequested) => {
                    Some(Ok(current.id))
                }
                (FunctionRunStatus::RunRequested, FunctionRunStatus::Running) => {
                    Some(Ok(current.id))
                }
                (FunctionRunStatus::Running, FunctionRunStatus::Done) => Some(Ok(current.id)),
                (
                    FunctionRunStatus::RunRequested | FunctionRunStatus::Running,
                    FunctionRunStatus::Error,
                ) => Some(Ok(current.id)),
                (
                    FunctionRunStatus::RunRequested | FunctionRunStatus::Running,
                    FunctionRunStatus::Failed,
                ) => Some(Ok(current.id)),

                // Recover status, only for failed function runs, otherwise just no-op.
                (
                    FunctionRunStatus::Failed | FunctionRunStatus::OnHold,
                    FunctionRunStatus::ReScheduled,
                ) => Some(Ok(current.id)),
                (_, FunctionRunStatus::ReScheduled) => None,

                // Cancel status.
                (_, FunctionRunStatus::Canceled) => Some(Ok(current.id)),

                // No-op for transitions between the same states
                (current, new) if current == new => None,

                // Error in transition, not safe to proceed.
                _ => Some(Err(
                    UpdateStatusRunError::UnexpectedFunctionRunStatusTransition(
                        current.id,
                        current.status.clone(),
                        update.status.clone(),
                    ),
                )),
            }
        })
        .collect::<Result<Vec<_>, _>>()?;

    // Early return if no function runs are to be updated.
    // This is a no-op update, so we just return.
    if function_run_ids.is_empty() {
        ctx.warning(UpdateStatusRunError::NoOpStatusUpdate);
        return Ok(());
    }

    // TODO this is not getting chunked
    let _ = conn
        .update::<_, FunctionRunDB>(update.deref(), &function_run_ids)
        .await?;

    // Publish function runs if needed, including downstream publishing.
    if update.status == FunctionRunStatus::Done {
        // Collect the unique execution_ids of the function runs that just became Done.
        // This scopes the commit query to all transactions in these executions, ensuring
        // downstream transactions whose requirements are now satisfied are also committed.
        let execution_ids: Vec<ExecutionId> = function_runs
            .iter()
            .filter(|fr| function_run_ids.contains(&fr.id))
            .map(|fr| fr.execution_id.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let to_commit = conn.fetch_to_commit(&execution_ids).await?;
        let to_commit: Vec<_> = to_commit.iter().map(|f| f.id).collect();

        let update = CommitFunctionRunDB::default();
        // TODO this is not getting chunked
        let _ = conn.update::<_, FunctionRunDB>(&update, &to_commit).await?;
    }

    // Downstream updates.
    let mut downstream_function_run_updates = HashMap::new();
    for current in function_run_ids.iter() {
        let downstream_status = match update.status {
            FunctionRunStatus::Canceled => Some(FunctionRunStatus::Canceled),
            FunctionRunStatus::Yanked => Some(FunctionRunStatus::Yanked),
            FunctionRunStatus::ReScheduled => Some(FunctionRunStatus::ReScheduled),
            FunctionRunStatus::Failed => Some(FunctionRunStatus::OnHold),
            _ => None,
        };

        if let Some(downstream_status) = downstream_status {
            let function_runs = conn
                .fetch_all_recursive::<FunctionRequirementDBWithNames, FunctionRunDB>(
                    OptionsBuilder::default()
                        .recursion(RecursionOptions::new(Some(Depth::ZERO), None))
                        .direct_reference(current)
                        .build()?,
                )
                .await?;
            let function_run_ids: Vec<_> = function_runs
                .iter()
                .map(|f| f.function_run_id)
                .filter(|id| id != current) // current is already in the required state
                .collect();

            downstream_function_run_updates
                .entry(downstream_status)
                .or_insert_with(HashSet::new)
                .extend(function_run_ids);
        }
    }

    for (status, function_run_ids) in downstream_function_run_updates {
        let update = UpdateFunctionRunDB::builder().status(status).build()?;
        let function_run_ids: Vec<_> = function_run_ids.iter().collect();
        // TODO this is not getting chunked
        let _ = conn
            .update::<_, FunctionRunDB>(&update, &function_run_ids)
            .await?;
    }

    Ok(())
}

pub async fn update_table_data_version_status(
    mut conn: MutRef<DynamicConnection<'static>>,
    function_run_id: Ref<FunctionRunId>,
    callback: Ref<CallbackRequest>,
) -> Result<(), TdError> {
    if let Some(context) = &callback.context
        && let WorkerOutput::V2(output) | WorkerOutput::FunctionV2(output) = context
    {
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
                .fetch_one::<TableDataVersionDBWithNames>(and!(*function_run_id, table_name,))
                .await?;

            let update = if let Some(table_info) = table_info {
                let latest_version_with_data = conn
                    .fetch_latest_before::<{ TableDataVersionDBWithNames::WithData }, TableDataVersionDBWithNames>(
                        and!(&data_version.table_id, lte!(&data_version.triggered_on)),
                        0,
                    )
                    .await?;

                let schema_changed = match &latest_version_with_data {
                    Some(prev) => SchemaChanged::from(
                        prev.schema_hash.as_ref() != Some(&table_info.schema_hash),
                    ),
                    None => SchemaChanged::TRUE,
                };

                UpdateTableDataVersionDB {
                    has_data: Some(HasData::TRUE),
                    schema_changed: Some(schema_changed),
                    column_count: Some(table_info.column_count),
                    row_count: Some(table_info.row_count),
                    schema_hash: Some(table_info.schema_hash.clone()),
                    with_data_table_data_version_id: Some(data_version.id.clone()),
                }
            } else {
                let latest_version_with_data = conn
                        .fetch_latest_before::<{ TableDataVersionDBWithNames::WithData }, TableDataVersionDBWithNames>(
                            and!(&data_version.table_id, lte!(&data_version.triggered_on)),
                            0,
                        )
                        .await?;

                if let Some(latest_version_with_data) = latest_version_with_data {
                    UpdateTableDataVersionDB {
                        has_data: Some(HasData::FALSE),
                        schema_changed: Some(SchemaChanged::FALSE),
                        column_count: latest_version_with_data.column_count,
                        row_count: latest_version_with_data.row_count,
                        schema_hash: latest_version_with_data.schema_hash.clone(),
                        with_data_table_data_version_id: Some(latest_version_with_data.id),
                    }
                } else {
                    UpdateTableDataVersionDB {
                        has_data: Some(HasData::FALSE),
                        schema_changed: Some(SchemaChanged::FALSE),
                        column_count: None,
                        row_count: None,
                        schema_hash: None,
                        with_data_table_data_version_id: None,
                    }
                }
            };

            conn.update::<_, TableDataVersionDB>(&update, &data_version.id)
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
    use td_objects::Buildable;
    use td_objects::and;
    use td_objects::dxo::collection::CollectionDB;
    use td_objects::dxo::execution::{ExecutionDB, ExecutionDBWithStatus};
    use td_objects::dxo::function::{FunctionDB, FunctionRegister};
    use td_objects::dxo::function_run::FunctionRunDB;
    use td_objects::dxo::table::TableDB;
    use td_objects::dxo::table_data_version::{TableDataVersionDB, TableDataVersionDBWithFunction};
    use td_objects::dxo::transaction::{TransactionDB, TransactionDBWithStatus};
    use td_objects::dxo::worker::WorkerDB;
    use td_objects::types::basic::{
        BundleId, CollectionName, Decorator, DependencyPos, ExecutionStatus, FunctionName,
        FunctionRunStatus, FunctionRuntimeValues, TableName, TableNameDto, TransactionStatus,
        UserId, VersionPos, WorkerMessageStatus,
    };
    use td_objects::types::composed::TableDependencyDto;
    use td_queries::BasicQueries;
    use td_queries::test_utils::SeedQueries;

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
        pub(crate) initial_status: FunctionRunStatus,
        pub(crate) expected_status: FunctionRunStatus,
    }

    // Creates proper setup for status update tests. It creates entities in test_executions. If names
    // are the same, they are not recreated, but reused.
    pub(crate) async fn test_status_update<F, Fut>(
        db: TdPool,
        test_executions: &Vec<TestExecution>,
        test_run: F,
    ) -> Result<(), TdError>
    where
        F: Fn(
            &HashMap<&CollectionName, CollectionDB>,
            &HashMap<&TestExecution, ExecutionDB>,
            &HashMap<&TestTransaction, TransactionDB>,
            &HashMap<&TestFunction, FunctionRunDB>,
            &HashMap<&TestFunction, WorkerDB>,
        ) -> Fut,
        Fut: Future<Output = Result<(), TdError>>,
    {
        // Create collections
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
                    let existing = db.fetch_optional::<CollectionDB>(c).await.unwrap();

                    if let Some(existing) = existing {
                        (c, existing)
                    } else {
                        (c, db.seed_collection(c, &UserId::admin()).await.unwrap())
                    }
                }
            })
            .collect::<Vec<_>>();
        let collections = futures::future::join_all(collections_futures)
            .await
            .into_iter()
            .collect::<HashMap<_, _>>();

        // Create functions
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

                let create = FunctionRegister::builder()
                    .name(&test_function.name)
                    .try_description("foo description")?
                    .bundle_id(BundleId::default())
                    .try_snippet("foo snippet")?
                    .decorator(Decorator::Publisher)
                    .dependencies(test_function.dependencies.clone())
                    .triggers(None)
                    .tables(tables)
                    .runtime_values(FunctionRuntimeValues::default())
                    .reuse_frozen_tables(false)
                    .build()?;

                let function_version = db.seed_function(collection, &create).await?;
                function_versions.insert(test_function, function_version);
            }
        }

        // Create executions
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
                        (e, value.seed_execution(function).await.unwrap())
                    }
                }
            })
            .collect::<Vec<_>>();
        let executions = futures::future::join_all(executions_futures)
            .await
            .into_iter()
            .collect::<HashMap<_, _>>();

        // Create transactions
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
                            db.seed_transaction(
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

        // Create function runs and table data versions
        let function_runs: HashSet<_> = test_executions
            .iter()
            .flat_map(|e| e.transactions.iter().map(move |t| (e, t, &t.functions)))
            .flat_map(|(e, t, f)| f.iter().map(move |f| (e, t, f)))
            .collect();
        let function_runs_futures = function_runs
            .iter()
            .map(|(e, t, f)| {
                let db = db.clone();
                let default_table = default_table.clone();
                let function_versions = function_versions.clone();
                let collections = collections.clone();
                let executions = executions.clone();
                let transactions = transactions.clone();
                async move {
                    let function_run = db
                        .seed_function_run(
                            collections.get(&f.collection).unwrap(),
                            function_versions.get(f).unwrap(),
                            executions.get(e).unwrap(),
                            transactions.get(t).unwrap(),
                            &f.initial_status,
                        )
                        .await
                        .unwrap();

                    let tables = db
                        .fetch_all::<TableDB>(&function_versions.get(f).unwrap().id)
                        .await
                        .unwrap();

                    for table in &tables {
                        let _ = db
                            .seed_table_data_version(
                                collections.get(&f.collection).unwrap(),
                                executions.get(e).unwrap(),
                                transactions.get(t).unwrap(),
                                &function_run,
                                table,
                            )
                            .await
                            .unwrap();
                    }

                    let _ = db
                        .seed_function_requirement(
                            collections.get(&f.collection).unwrap(),
                            executions.get(e).unwrap(),
                            transactions.get(t).unwrap(),
                            &function_run,
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

                    (*f, function_run)
                }
            })
            .collect::<Vec<_>>();
        let function_runs = futures::future::join_all(function_runs_futures)
            .await
            .into_iter()
            .collect::<HashMap<_, _>>();

        // Create function requirements (one per dependency)
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
                let function_runs = function_runs.clone();
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
                        .fetch_all::<TableDB>(&function_versions.get(dependency_f).unwrap().id)
                        .await
                        .unwrap();
                    let tables = tables
                        .iter()
                        .map(|tdv| (&tdv.name, tdv))
                        .collect::<HashMap<_, _>>();

                    let table_data_versions = db
                        .fetch_all::<TableDataVersionDB>(
                            &function_versions.get(dependency_f).unwrap().id,
                        )
                        .await
                        .unwrap();
                    let table_data_versions = table_data_versions
                        .iter()
                        .map(|tdv| (&tdv.name, tdv))
                        .collect::<HashMap<_, _>>();

                    let req = db
                        .seed_function_requirement(
                            collections.get(&f.collection).unwrap(),
                            executions.get(e).unwrap(),
                            transactions.get(t).unwrap(),
                            function_runs.get(f).unwrap(),
                            tables.get(&TableName::try_from(&d.table).unwrap()).unwrap(),
                            Some(function_runs.get(dependency_f).unwrap()),
                            table_data_versions
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

        // Create workers (1 per function run)
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
                let function_runs = function_runs.clone();
                async move {
                    let req = db
                        .seed_function_runner_worker(
                            executions.get(e).unwrap(),
                            transactions.get(t).unwrap(),
                            function_runs.get(f).unwrap(),
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

        // Execute test
        test_run(
            &collections,
            &executions,
            &transactions,
            &function_runs,
            &workers,
        )
        .await?;

        // Assert expected state
        // Assert execution
        for test_execution in test_executions {
            let executions = db
                .fetch_all::<ExecutionDBWithStatus>(&executions.get(test_execution).unwrap().id)
                .await?;
            assert_eq!(executions.len(), 1);
            let execution = &executions[0];
            assert_eq!(execution.status, test_execution.expected_status);
        }

        // Assert transactions
        for test_transaction in test_executions.iter().flat_map(|e| &e.transactions) {
            let transactions = db
                .fetch_all::<TransactionDBWithStatus>(
                    &transactions.get(test_transaction).unwrap().id,
                )
                .await?;
            assert_eq!(transactions.len(), 1);
            let transaction = &transactions[0];
            assert_eq!(transaction.status, test_transaction.expected_status);
        }

        // Assert function runs and table data versions
        for test_function in test_executions
            .iter()
            .flat_map(|e| &e.transactions)
            .flat_map(|t| &t.functions)
        {
            let function_run = function_runs.get(test_function).unwrap();

            // Assert all function_runs are in expected state
            let function_runs = db.fetch_all::<FunctionRunDB>(&function_run.id).await?;
            assert_eq!(function_runs.len(), 1);
            let function_run = &function_runs[0];
            assert_eq!(function_run.status, test_function.expected_status);

            // Assert all table_data_versions are in expected state
            let table_data_versions = db
                .fetch_all::<TableDataVersionDBWithFunction>(&function_run.id)
                .await?;
            for table_data_version in &table_data_versions {
                assert_eq!(table_data_version.status, test_function.expected_status);
            }
        }

        Ok(())
    }
}
