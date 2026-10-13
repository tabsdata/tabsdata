//
// Copyright 2026 Tabsdata Inc.
//

//! A transaction publishes all its tables or none: Done runs beside a
//! Canceled one are discarded, and an all-Done transaction commits, whichever
//! path left it so. The scheduler tick settles what no event did.

use crate::Services;
use crate::execution::update_status::tests::{
    TestExecution, TestFunction, TestTransaction, run_status_scenario,
};
use crate::scheduler::test_wiring::dispatch_services;
use crate::tests::late_callbacks::post_callback;
use td_common::execution_status::WorkerCallbackStatus;
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::execution::ExecutionParam;
use td_objects::dxo::logical::LogicalFunctionRunDB;
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
use td_objects::dxo::transaction::TransactionDB;
use td_objects::dxo::worker::WorkerDB;
use td_objects::types::basic::{
    AccessTokenId, CollectionName, ExecutionId, ExecutionStatus, FunctionName,
    LogicalFunctionRunStatus, PhysicalFunctionRunStatus, TableNameDto, TransactionId,
    TransactionStatus, UserId, WorkerStatus,
};
use td_objects::types::composed::TableDependencyDto;
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;

#[derive(Clone, Copy)]
enum Order {
    DoneThenCancel,
    CancelThenDone,
}

/// ep-0 runs `upstream`; ep-1 holds one transaction with `reader` (reads
/// `upstream`'s t_up) and `sibling` (reads nothing), the shape ee's
/// per-collection transactions give.
fn executions() -> Result<Vec<TestExecution>, TdError> {
    let function = |name: &str, dependencies: Vec<TableDependencyDto>, table: &str, initial| {
        Ok::<_, TdError>(TestFunction {
            collection: CollectionName::try_from("c_0")?,
            name: FunctionName::try_from(name)?,
            dependencies,
            tables: vec![TableNameDto::try_from(table)?],
            initial_status: initial,
            expected_status: initial,
        })
    };
    Ok(vec![
        TestExecution {
            expected_status: ExecutionStatus::Running,
            transactions: vec![TestTransaction {
                expected_status: TransactionStatus::Running,
                functions: vec![function(
                    "upstream",
                    vec![],
                    "t_up",
                    LogicalFunctionRunStatus::Running,
                )?],
            }],
        },
        TestExecution {
            expected_status: ExecutionStatus::Running,
            transactions: vec![TestTransaction {
                expected_status: TransactionStatus::Running,
                functions: vec![
                    function(
                        "reader",
                        vec![TableDependencyDto::try_from("t_up")?],
                        "t_reader",
                        LogicalFunctionRunStatus::Scheduled,
                    )?,
                    function(
                        "sibling",
                        vec![],
                        "t_sibling",
                        LogicalFunctionRunStatus::Running,
                    )?,
                ],
            }],
        },
    ])
}

async fn cancel_execution(db: &TdPool, execution: ExecutionId) -> Result<(), TdError> {
    let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
        .update(ExecutionParam { execution }, ());
    Services::with_defaults(db.clone())
        .execution_cancel(request)
        .await
        .map(|response| response.data)
}

async fn assert_discarded(db: &TdPool, transaction: &TransactionId) -> Result<(), TdError> {
    let rolled_up: TransactionDB = db.fetch_one(transaction).await?;
    let runs: Vec<LogicalFunctionRunDB> = db.fetch_all(transaction).await?;
    let statuses: Vec<_> = runs.iter().map(|r| r.base.status).collect();
    assert!(
        statuses
            .iter()
            .all(|s| *s == LogicalFunctionRunStatus::Canceled),
        "the transaction is discarded whole: {statuses:?}"
    );
    assert_eq!(rolled_up.status, TransactionStatus::Canceled);
    Ok(())
}

async fn split_transaction_settles(db: TdPool, order: Order) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let test_executions = executions()?;
    run_status_scenario(db.clone(), p.id, &test_executions, |_, e, t, f, w| {
        let db = db.clone();
        let upstream_execution = e[&test_executions[0]].base.id.clone();
        let split = &test_executions[1].transactions[0];
        let transaction = t[split].base.id.clone();
        let sibling = &split.functions[1];
        let sibling_run = f[sibling].base.id.clone();
        let sibling_worker = w[sibling].base.id.clone();
        async move {
            match order {
                Order::DoneThenCancel => {
                    post_callback(
                        &db,
                        &sibling_run,
                        &sibling_worker,
                        WorkerCallbackStatus::Done,
                    )
                    .await?;
                    cancel_execution(&db, upstream_execution).await?;
                }
                Order::CancelThenDone => {
                    cancel_execution(&db, upstream_execution).await?;
                    post_callback(
                        &db,
                        &sibling_run,
                        &sibling_worker,
                        WorkerCallbackStatus::Done,
                    )
                    .await?;
                }
            }
            dispatch_services(&db).request(()).await?;
            assert_discarded(&db, &transaction).await?;
            if matches!(order, Order::CancelThenDone) {
                let attempt: PhysicalFunctionRunDB = db.timeline().fetch_one(&sibling_run).await?;
                assert_eq!(attempt.base.status, PhysicalFunctionRunStatus::Failed);
                let worker: WorkerDB = db.fetch_one(&sibling_worker).await?;
                assert_eq!(worker.base.status, WorkerStatus::Failed);
            }
            Ok(())
        }
    })
    .await
    .map(|_| ())
}

/// `sibling` finishes Done, then canceling ep-0 reaches `reader`, so
/// the transaction is discarded: `sibling`'s output is never published.
#[td_test::test(db)]
async fn test_a_done_run_beside_a_cascaded_cancel_settles(db: TdPool) -> Result<(), TdError> {
    split_transaction_settles(db, Order::DoneThenCancel).await
}

/// The cancel reaches `reader` first and ends `sibling`'s attempt, so
/// `sibling`'s late Done is stale and the transaction stays discarded.
#[td_test::test(db)]
async fn test_a_done_after_a_cascaded_cancel_settles(db: TdPool) -> Result<(), TdError> {
    split_transaction_settles(db, Order::CancelThenDone).await
}

async fn seed_transaction(
    db: &TdPool,
    statuses: &[LogicalFunctionRunStatus],
) -> Result<TransactionId, TdError> {
    let p = db.seed().test_project().await?;
    let functions = statuses
        .iter()
        .enumerate()
        .map(|(i, status)| {
            Ok::<_, TdError>(TestFunction {
                collection: CollectionName::try_from("c_0")?,
                name: FunctionName::try_from(format!("f_{i}"))?,
                dependencies: vec![],
                tables: vec![TableNameDto::try_from(format!("t_{i}"))?],
                initial_status: *status,
                expected_status: *status,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let test_executions = vec![TestExecution {
        expected_status: ExecutionStatus::Running,
        transactions: vec![TestTransaction {
            expected_status: TransactionStatus::Running,
            functions,
        }],
    }];
    let (_, transactions, _) =
        run_status_scenario(db.clone(), p.id, &test_executions, |_, _, _, _, _| async {
            Ok(())
        })
        .await?;
    Ok(transactions[&test_executions[0].transactions[0]]
        .base
        .id
        .clone())
}

/// A transaction left holding Done beside Canceled by any path (here, seeded
/// so) is discarded by the next tick.
#[td_test::test(db)]
async fn test_the_tick_discards_done_runs_beside_a_canceled_one(db: TdPool) -> Result<(), TdError> {
    let transaction = seed_transaction(
        &db,
        &[
            LogicalFunctionRunStatus::Done,
            LogicalFunctionRunStatus::Canceled,
        ],
    )
    .await?;
    dispatch_services(&db).request(()).await?;
    assert_discarded(&db, &transaction).await
}

/// An all-Done transaction no Done callback committed is committed by the
/// next tick.
#[td_test::test(db)]
async fn test_the_tick_commits_an_all_done_transaction(db: TdPool) -> Result<(), TdError> {
    let transaction = seed_transaction(
        &db,
        &[
            LogicalFunctionRunStatus::Done,
            LogicalFunctionRunStatus::Done,
        ],
    )
    .await?;
    dispatch_services(&db).request(()).await?;
    let runs: Vec<LogicalFunctionRunDB> = db.fetch_all(&transaction).await?;
    assert!(
        runs.iter()
            .all(|r| r.base.status == LogicalFunctionRunStatus::Committed),
        "the transaction is committed whole"
    );
    let rolled_up: TransactionDB = db.fetch_one(&transaction).await?;
    assert_eq!(rolled_up.status, TransactionStatus::Committed);
    Ok(())
}

/// ep-0's transaction holds `upstream` and `other`; ep-1's holds `reader`,
/// which reads `upstream`'s t_up.
fn chained() -> Result<Vec<TestExecution>, TdError> {
    let function = |name: &str, dependencies: Vec<TableDependencyDto>, table: &str| {
        Ok::<_, TdError>(TestFunction {
            collection: CollectionName::try_from("c_0")?,
            name: FunctionName::try_from(name)?,
            dependencies,
            tables: vec![TableNameDto::try_from(table)?],
            initial_status: LogicalFunctionRunStatus::Running,
            expected_status: LogicalFunctionRunStatus::Running,
        })
    };
    Ok(vec![
        TestExecution {
            expected_status: ExecutionStatus::Running,
            transactions: vec![TestTransaction {
                expected_status: TransactionStatus::Running,
                functions: vec![
                    function("upstream", vec![], "t_up")?,
                    function("other", vec![], "t_other")?,
                ],
            }],
        },
        TestExecution {
            expected_status: ExecutionStatus::Running,
            transactions: vec![TestTransaction {
                expected_status: TransactionStatus::Running,
                functions: vec![function(
                    "reader",
                    vec![TableDependencyDto::try_from("t_up")?],
                    "t_reader",
                )?],
            }],
        },
    ])
}

async fn run_status(
    db: &TdPool,
    run: &PhysicalFunctionRunDB,
) -> Result<LogicalFunctionRunStatus, TdError> {
    let lfr: LogicalFunctionRunDB = db.fetch_one(&run.base.logical_function_run_id).await?;
    Ok(lfr.base.status)
}

/// A transaction commits only once the transactions it read from have: a
/// committed run never has an uncommitted ancestor. So canceling the upstream
/// transaction still reaches `reader`, which is Done but not committed.
#[td_test::test(db)]
async fn test_a_transaction_does_not_commit_before_the_ones_it_read(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let test_executions = chained()?;
    run_status_scenario(db.clone(), p.id, &test_executions, |_, e, _, f, w| {
        let db = db.clone();
        let upstream_execution = e[&test_executions[0]].base.id.clone();
        let upstream = &test_executions[0].transactions[0].functions[0];
        let reader = &test_executions[1].transactions[0].functions[0];
        let (upstream_run, upstream_worker) = (f[upstream].clone(), w[upstream].base.id.clone());
        let (reader_run, reader_worker) = (f[reader].clone(), w[reader].base.id.clone());
        async move {
            post_callback(
                &db,
                &upstream_run.base.id,
                &upstream_worker,
                WorkerCallbackStatus::Done,
            )
            .await?;
            post_callback(
                &db,
                &reader_run.base.id,
                &reader_worker,
                WorkerCallbackStatus::Done,
            )
            .await?;
            dispatch_services(&db).request(()).await?;
            assert_eq!(
                run_status(&db, &reader_run).await?,
                LogicalFunctionRunStatus::Done,
                "reader read upstream, whose transaction has not committed"
            );

            cancel_execution(&db, upstream_execution).await?;
            assert_eq!(
                run_status(&db, &reader_run).await?,
                LogicalFunctionRunStatus::Canceled,
                "the cancel discards what read the canceled data"
            );
            Ok(())
        }
    })
    .await
    .map(|_| ())
}

/// Once the upstream transaction commits, the next tick commits the one that
/// read it.
#[td_test::test(db)]
async fn test_a_transaction_commits_after_the_ones_it_read(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let test_executions = chained()?;
    run_status_scenario(db.clone(), p.id, &test_executions, |_, _, _, f, w| {
        let db = db.clone();
        let runs: Vec<_> = [
            &test_executions[1].transactions[0].functions[0],
            &test_executions[0].transactions[0].functions[0],
            &test_executions[0].transactions[0].functions[1],
        ]
        .into_iter()
        .map(|function| (f[function].clone(), w[function].base.id.clone()))
        .collect();
        async move {
            for (run, worker) in &runs {
                post_callback(&db, &run.base.id, worker, WorkerCallbackStatus::Done).await?;
            }
            dispatch_services(&db).request(()).await?;
            for (run, _) in &runs {
                assert_eq!(
                    run_status(&db, run).await?,
                    LogicalFunctionRunStatus::Committed
                );
            }
            Ok(())
        }
    })
    .await
    .map(|_| ())
}
