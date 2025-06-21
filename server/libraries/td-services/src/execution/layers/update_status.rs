//
// Copyright 2025 Tabs Data Inc.
//

use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use td_error::td_error;
use td_error::TdError;
use td_objects::crudl::{assert_one, handle_sql_err};
use td_objects::sql::recursive::RecursiveQueries;
use td_objects::sql::{DerefQueries, SelectBy, UpdateBy};
use td_objects::types::basic::{FunctionRunId, FunctionRunStatus, WorkerId};
use td_objects::types::execution::{
    CallbackRequest, CommitFunctionRunDB, FunctionRequirementDBWithNames, FunctionRunDB,
    TableDataVersionDB, UpdateFunctionRunDB, UpdateTableDataVersionDB, WorkerDB,
};
use td_objects::types::execution::{FunctionRunToCommitDB, UpdateWorkerDB};
use td_objects::types::worker::v2::WrittenTableV2;
use td_objects::types::worker::FunctionOutput;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection, ReqCtx, SrvCtx};

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

pub async fn update_worker_status<Q: DerefQueries>(
    SrvCtx(queries): SrvCtx<Q>,
    Connection(connection): Connection,
    Input(update): Input<UpdateWorkerDB>,
    Input(callback): Input<CallbackRequest>,
) -> Result<(), TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    // Worker status update is never conditional, we want to know exactly what got reported,
    // as it doesn't affect execution flow, it's just informational.
    let worker_id = WorkerId::try_from(callback.id())?;
    let _ = queries
        .update_by::<_, WorkerDB>(update.deref(), &(&worker_id))?
        .build()
        .execute(&mut *conn)
        .await
        .map_err(handle_sql_err)?;

    Ok(())
}

pub async fn update_function_run_status<Q: DerefQueries>(
    ReqCtx(ctx): ReqCtx,
    SrvCtx(queries): SrvCtx<Q>,
    Connection(connection): Connection,
    Input(function_runs): Input<Vec<FunctionRunDB>>,
    Input(update): Input<UpdateFunctionRunDB>,
) -> Result<(), TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    // Validate status transitions and filter out no-op transitions.
    let futures: Vec<_> = function_runs
        .iter()
        .map(|current| {
            let ctx = ctx.clone();
            let update = update.clone();
            async move {
                match (current.status(), update.status()) {
                    // Final status.
                    (FunctionRunStatus::Committed, _) => {
                        Some(Err(UpdateStatusRunError::AlreadyCommitted(*current.id())))
                    }

                    (FunctionRunStatus::Yanked, FunctionRunStatus::Yanked) => {
                        ctx.warning(UpdateStatusRunError::AlreadyYanked(*current.id()))
                            .await;
                        None
                    }
                    (FunctionRunStatus::Yanked, _) => {
                        Some(Err(UpdateStatusRunError::AlreadyYanked(*current.id())))
                    }

                    (
                        FunctionRunStatus::Canceled,
                        FunctionRunStatus::Running
                        | FunctionRunStatus::Done
                        | FunctionRunStatus::Error
                        | FunctionRunStatus::Failed,
                    ) => {
                        // Callback status transition, never returning error to avoid race conditions.
                        ctx.warning(UpdateStatusRunError::AlreadyCanceled(*current.id()))
                            .await;
                        None
                    }
                    (FunctionRunStatus::Canceled, _) => {
                        Some(Err(UpdateStatusRunError::AlreadyCanceled(*current.id())))
                    }

                    // Mutable status
                    (FunctionRunStatus::Scheduled, FunctionRunStatus::RunRequested) => {
                        Some(Ok(current.id()))
                    }
                    (FunctionRunStatus::ReScheduled, FunctionRunStatus::RunRequested) => {
                        Some(Ok(current.id()))
                    }
                    (FunctionRunStatus::RunRequested, FunctionRunStatus::Running) => {
                        Some(Ok(current.id()))
                    }
                    (FunctionRunStatus::Running, FunctionRunStatus::Done) => Some(Ok(current.id())),
                    (
                        FunctionRunStatus::RunRequested | FunctionRunStatus::Running,
                        FunctionRunStatus::Error,
                    ) => Some(Ok(current.id())),
                    (
                        FunctionRunStatus::RunRequested | FunctionRunStatus::Running,
                        FunctionRunStatus::Failed,
                    ) => Some(Ok(current.id())),

                    // Recover status, only for failed function runs, otherwise just no-op.
                    (
                        FunctionRunStatus::Failed | FunctionRunStatus::OnHold,
                        FunctionRunStatus::ReScheduled,
                    ) => Some(Ok(current.id())),
                    (_, FunctionRunStatus::ReScheduled) => None,

                    // Cancel status.
                    (_, FunctionRunStatus::Canceled) => Some(Ok(current.id())),

                    // No-op for transitions between the same states
                    (current, new) if current == new => None,

                    // Error in transition, not safe to proceed.
                    _ => Some(Err(
                        UpdateStatusRunError::UnexpectedFunctionRunStatusTransition(
                            *current.id(),
                            current.status().clone(),
                            update.status().clone(),
                        ),
                    )),
                }
            }
        })
        .collect();

    // Update function runs with new statuses.
    let function_run_ids = futures::future::join_all(futures)
        .await
        .into_iter()
        .flatten()
        .collect::<Result<Vec<_>, _>>()?;

    // Early return if no function runs are to be updated.
    // This is a no-op update, so we just return.
    if function_run_ids.is_empty() {
        ctx.warning(UpdateStatusRunError::NoOpStatusUpdate).await;
        return Ok(());
    }

    // TODO this is not getting chunked
    let _ = queries
        .update_all_by::<_, FunctionRunDB>(update.deref(), &(function_run_ids))?
        .build()
        .execute(&mut *conn)
        .await
        .map_err(handle_sql_err)?;

    // Publish function runs if needed, including downstream publishing.
    if *update.status() == FunctionRunStatus::Done {
        let to_commit: Vec<FunctionRunToCommitDB> = queries
            .select_by::<FunctionRunToCommitDB>(&())?
            .build_query_as()
            .fetch_all(&mut *conn)
            .await
            .map_err(handle_sql_err)?;
        let to_commit: Vec<_> = to_commit.iter().map(|f| f.id()).collect();

        let update = CommitFunctionRunDB::default();
        // TODO this is not getting chunked
        let _ = queries
            .update_all_by::<_, FunctionRunDB>(&update, &(to_commit))?
            .build()
            .execute(&mut *conn)
            .await
            .map_err(handle_sql_err)?;
    }

    // Downstream updates.
    let mut downstream_function_run_updates = HashMap::new();
    for current in function_run_ids.iter() {
        let downstream_status = match update.status() {
            FunctionRunStatus::Canceled => Some(FunctionRunStatus::Canceled),
            FunctionRunStatus::Yanked => Some(FunctionRunStatus::Yanked),
            FunctionRunStatus::ReScheduled => Some(FunctionRunStatus::ReScheduled),
            FunctionRunStatus::Failed => Some(FunctionRunStatus::OnHold),
            _ => None,
        };

        if let Some(downstream_status) = downstream_status {
            let function_runs: Vec<FunctionRequirementDBWithNames> = queries
                .select_recursive_versions_at::<FunctionRequirementDBWithNames, FunctionRunDB, _>(
                    None, None, None, None, *current,
                )?
                .build_query_as()
                .fetch_all(&mut *conn)
                .await
                .map_err(handle_sql_err)?;
            let function_run_ids: Vec<_> = function_runs
                .iter()
                .map(|f| *f.function_run_id())
                .filter(|id| id != *current) // current is already in the required state
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
        let _ = queries
            .update_all_by::<_, FunctionRunDB>(&update, &(function_run_ids))?
            .build()
            .execute(&mut *conn)
            .await
            .map_err(handle_sql_err)?;
    }

    Ok(())
}

pub async fn update_table_data_version_status<Q: DerefQueries>(
    SrvCtx(queries): SrvCtx<Q>,
    Connection(connection): Connection,
    Input(function_run_id): Input<FunctionRunId>,
    Input(callback): Input<CallbackRequest>,
) -> Result<(), TdError> {
    if let Some(context) = callback.context() {
        let futures: Vec<_> = match context {
            FunctionOutput::V1(_) => unreachable!(),
            FunctionOutput::V2(output) => {
                output
                    .output()
                    .iter()
                    .map(|written| {
                        let queries = queries.clone();
                        let connection = connection.clone();
                        let function_run_id = function_run_id.clone();
                        async move {
                            let (table_name, has_data) = match written {
                                WrittenTableV2::NoData { table } => (table, false),
                                WrittenTableV2::Data { table } => (table, true),
                                // TODO partitions should be handled differently, creating partitions and setting
                                // the table to has_data = true and partition = true.
                                WrittenTableV2::Partitions { table, .. } => (table, true),
                            };

                            let update = UpdateTableDataVersionDB::builder()
                                .has_data(Some(has_data.into()))
                                .build()?;

                            let mut conn = connection.lock().await;
                            let conn = conn.get_mut_connection()?;

                            let res = queries
                                .update_by::<_, TableDataVersionDB>(
                                    &update,
                                    &(&*function_run_id, table_name),
                                )?
                                .build()
                                .execute(&mut *conn)
                                .await
                                .map_err(handle_sql_err)?;
                            assert_one(res)?;
                            Ok::<_, TdError>(())
                        }
                    })
                    .collect()
            }
        };

        let _ = futures::future::try_join_all(futures).await?;
    }

    Ok(())
}

#[cfg(test)]
pub(crate) mod test {
    use itertools::Itertools;
    use std::collections::{HashMap, HashSet};
    use std::future::Future;
    use std::sync::Arc;
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_objects::crudl::handle_sql_err;
    use td_objects::sql::{DaoQueries, SelectBy};
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_execution::seed_execution;
    use td_objects::test_utils::seed_function::seed_function;
    use td_objects::test_utils::seed_function_requirement::seed_function_requirement;
    use td_objects::test_utils::seed_function_run::seed_function_run;
    use td_objects::test_utils::seed_table_data_version::seed_table_data_version;
    use td_objects::test_utils::seed_transaction::seed_transaction;
    use td_objects::types::basic::{
        BundleId, CollectionName, Decorator, DependencyPos, ExecutionStatus, FunctionName,
        FunctionRunStatus, FunctionRuntimeValues, TableDependencyDto, TableName, TableNameDto,
        TransactionStatus, UserId, VersionPos,
    };
    use td_objects::types::collection::CollectionDB;
    use td_objects::types::execution::{
        ExecutionDB, FunctionRunDB, TableDataVersionDB, TableDataVersionDBWithFunction,
        TransactionDB,
    };
    use td_objects::types::execution::{ExecutionDBWithStatus, TransactionDBWithStatus};
    use td_objects::types::function::FunctionRegister;
    use td_objects::types::table::TableDB;

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

    pub(crate) async fn test_status_update<F, Fut>(
        db: DbPool,
        test_executions: &Vec<TestExecution>,
        test_run: F,
    ) -> Result<(), TdError>
    where
        F: Fn(
            &HashMap<&CollectionName, CollectionDB>,
            &HashMap<&TestExecution, ExecutionDB>,
            &HashMap<&TestTransaction, TransactionDB>,
            &HashMap<&TestFunction, FunctionRunDB>,
        ) -> Fut,
        Fut: Future<Output = Result<(), TdError>>,
    {
        let queries = Arc::new(DaoQueries::default());

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
                let queries = queries.clone();
                async move {
                    let existing: Option<CollectionDB> = queries
                        .select_by::<CollectionDB>(&c)
                        .unwrap()
                        .build_query_as()
                        .fetch_optional(&db)
                        .await
                        .unwrap();

                    if let Some(existing) = existing {
                        (c, existing)
                    } else {
                        (c, seed_collection(&db, c, &UserId::admin()).await)
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

            let collection = collections.get(&test_function.collection).unwrap();
            let function_version = seed_function(&db, collection, &create).await;
            function_versions.insert(test_function, function_version);
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
                        (e, seed_execution(&value, function).await)
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
                            seed_transaction(
                                &db,
                                executions.get(e).unwrap(),
                                &i.to_string().try_into().unwrap(),
                            )
                            .await,
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
                let queries = queries.clone();
                let default_table = default_table.clone();
                let function_versions = function_versions.clone();
                let collections = collections.clone();
                let executions = executions.clone();
                let transactions = transactions.clone();
                async move {
                    let function_run = seed_function_run(
                        &db,
                        collections.get(&f.collection).unwrap(),
                        function_versions.get(f).unwrap(),
                        executions.get(e).unwrap(),
                        transactions.get(t).unwrap(),
                        &f.initial_status,
                    )
                    .await;

                    let tables: Vec<TableDB> = queries
                        .select_by::<TableDB>(&(function_versions.get(f).unwrap().id()))
                        .unwrap()
                        .build_query_as()
                        .fetch_all(&db)
                        .await
                        .unwrap();

                    for table in &tables {
                        let _ = seed_table_data_version(
                            &db,
                            collections.get(&f.collection).unwrap(),
                            executions.get(e).unwrap(),
                            transactions.get(t).unwrap(),
                            &function_run,
                            table,
                        )
                        .await;
                    }

                    let _ = seed_function_requirement(
                        &db,
                        collections.get(&f.collection).unwrap(),
                        executions.get(e).unwrap(),
                        transactions.get(t).unwrap(),
                        &function_run,
                        tables
                            .iter()
                            .find(|f| f.name() == &TableName::try_from(&default_table).unwrap())
                            .unwrap(),
                        None,
                        None,
                        None,
                        &VersionPos::try_from(0).unwrap(),
                    )
                    .await;

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
                let queries = queries.clone();
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
                            let collection = d.collection().as_ref().unwrap_or(&f.collection);
                            f.collection == *collection
                                && f.tables.iter().any(|tbl| tbl == d.table())
                        })
                        .unwrap();

                    let tables: Vec<TableDB> = queries
                        .select_by::<TableDB>(&(function_versions.get(dependency_f).unwrap().id()))
                        .unwrap()
                        .build_query_as()
                        .fetch_all(&db)
                        .await
                        .unwrap();
                    let tables = tables
                        .iter()
                        .map(|tdv| (tdv.name(), tdv))
                        .collect::<HashMap<_, _>>();

                    let table_data_versions: Vec<TableDataVersionDB> = queries
                        .select_by::<TableDataVersionDBWithFunction>(
                            &(function_versions.get(dependency_f).unwrap().id()),
                        )
                        .unwrap()
                        .build_query_as()
                        .fetch_all(&db)
                        .await
                        .unwrap();
                    let table_data_versions = table_data_versions
                        .iter()
                        .map(|tdv| (tdv.name(), tdv))
                        .collect::<HashMap<_, _>>();

                    let req = seed_function_requirement(
                        &db,
                        collections.get(&f.collection).unwrap(),
                        executions.get(e).unwrap(),
                        transactions.get(t).unwrap(),
                        function_runs.get(f).unwrap(),
                        tables
                            .get(&TableName::try_from(d.table()).unwrap())
                            .unwrap(),
                        Some(function_runs.get(dependency_f).unwrap()),
                        table_data_versions
                            .get(&TableName::try_from(d.table()).unwrap())
                            .cloned(),
                        Some(&DependencyPos::try_from(0).unwrap()),
                        &VersionPos::try_from(0).unwrap(),
                    )
                    .await;

                    (*f, req)
                }
            })
            .collect::<Vec<_>>();
        let _function_reqs = futures::future::join_all(function_reqs_futures)
            .await
            .into_iter()
            .collect::<HashMap<_, _>>();

        // Execute test
        test_run(&collections, &executions, &transactions, &function_runs).await?;

        // Assert expected state
        // Assert execution
        for test_execution in test_executions {
            let executions: Vec<ExecutionDBWithStatus> = queries
                .select_by::<ExecutionDBWithStatus>(
                    &(executions.get(test_execution).unwrap().id()),
                )?
                .build_query_as()
                .fetch_all(&db)
                .await
                .map_err(handle_sql_err)?;
            assert_eq!(executions.len(), 1);
            let execution = &executions[0];
            assert_eq!(*execution.status(), test_execution.expected_status);
        }

        // Assert transactions
        for test_transaction in test_executions.iter().flat_map(|e| &e.transactions) {
            let transactions: Vec<TransactionDBWithStatus> = queries
                .select_by::<TransactionDBWithStatus>(
                    &transactions.get(test_transaction).unwrap().id(),
                )?
                .build_query_as()
                .fetch_all(&db)
                .await
                .map_err(handle_sql_err)?;
            assert_eq!(transactions.len(), 1);
            let transaction = &transactions[0];
            assert_eq!(*transaction.status(), test_transaction.expected_status);
        }

        // Assert function runs and table data versions
        for test_function in test_executions
            .iter()
            .flat_map(|e| &e.transactions)
            .flat_map(|t| &t.functions)
        {
            let function_version = function_versions.get(test_function).unwrap();

            // Assert all function_runs are in expected state
            let function_runs: Vec<FunctionRunDB> = queries
                .select_by::<FunctionRunDB>(&(function_runs.get(test_function).unwrap().id()))?
                .build_query_as()
                .fetch_all(&db)
                .await
                .map_err(handle_sql_err)?;
            assert_eq!(function_runs.len(), 1);
            let function_run = &function_runs[0];
            assert_eq!(*function_run.status(), test_function.expected_status);

            // Assert all table_data_versions are in expected state
            let table_data_versions: Vec<TableDataVersionDBWithFunction> = queries
                .select_by::<TableDataVersionDBWithFunction>(&(function_version.id()))?
                .build_query_as()
                .fetch_all(&db)
                .await
                .map_err(handle_sql_err)?;
            for table_data_version in &table_data_versions {
                assert_eq!(*table_data_version.status(), test_function.expected_status);
            }
        }

        Ok(())
    }
}
