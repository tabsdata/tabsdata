//
// Copyright 2026 Tabsdata Inc.
//

//! A subscriber writes outside Tabsdata, where no failure takes the write
//! back, so it runs only on data that is certain to commit: after the
//! transactions it reads from have committed.

use crate::execution::update_status::tests::{
    TestExecution, TestFunction, TestTransaction, run_status_scenario_with,
};
use crate::scheduler::test_wiring::dispatch_services;
use crate::tests::late_callbacks::post_callback;
use std::slice::from_ref;
use td_common::execution_status::WorkerCallbackStatus;
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::execution::ExecutionParam;
use td_objects::dxo::logical::LogicalFunctionRunDB;
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
use td_objects::types::basic::{
    AccessTokenId, AtTime, CollectionName, ExecutionStatus, FunctionName, LogicalFunctionRunStatus,
    TableNameDto, TransactionStatus, UserId,
};
use td_objects::types::composed::TableDependencyDto;
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;

const SUBSCRIBERS: &[&str] = &["subscriber", "later"];

fn function(
    collection: &str,
    name: &str,
    reads: &[&str],
    table: &str,
    initial_status: LogicalFunctionRunStatus,
) -> Result<TestFunction, TdError> {
    Ok(TestFunction {
        collection: CollectionName::try_from(collection)?,
        name: FunctionName::try_from(name)?,
        dependencies: reads
            .iter()
            .map(|table| TableDependencyDto::try_from(*table))
            .collect::<Result<_, _>>()?,
        tables: vec![TableNameDto::try_from(table)?],
        initial_status,
        expected_status: initial_status,
    })
}

fn transaction(functions: Vec<TestFunction>) -> TestTransaction {
    TestTransaction {
        expected_status: TransactionStatus::Running,
        functions,
    }
}

/// One plan: c_0 holds `checked` and `beside`, both running; c_1 holds
/// `subscriber` and c_2 `eager`, a function that is not a subscriber, both
/// reading `beside`'s table alone.
fn delivery(subscriber: LogicalFunctionRunStatus) -> Result<Vec<TestExecution>, TdError> {
    use LogicalFunctionRunStatus::{Running, Scheduled};
    Ok(vec![TestExecution {
        expected_status: ExecutionStatus::Running,
        transactions: vec![
            transaction(vec![
                function("c_0", "checked", &[], "t_checked", Running)?,
                function("c_0", "beside", &[], "t_beside", Running)?,
            ]),
            transaction(vec![function(
                "c_1",
                "subscriber",
                &["c_0/t_beside"],
                "t_out",
                subscriber,
            )?]),
            transaction(vec![function(
                "c_2",
                "eager",
                &["c_0/t_beside"],
                "t_eager",
                Scheduled,
            )?]),
        ],
    }])
}

/// A later plan: c_3 holds `later`, a subscriber reading the table `beside`
/// wrote in the first plan.
fn later_plan() -> Result<TestExecution, TdError> {
    Ok(TestExecution {
        expected_status: ExecutionStatus::Running,
        transactions: vec![transaction(vec![function(
            "c_3",
            "later",
            &["c_0/t_beside"],
            "t_later",
            LogicalFunctionRunStatus::Scheduled,
        )?])],
    })
}

async fn status(
    db: &TdPool,
    run: &PhysicalFunctionRunDB,
) -> Result<LogicalFunctionRunStatus, TdError> {
    let lfr: LogicalFunctionRunDB = db.fetch_one(&run.logical_function_run_id).await?;
    Ok(lfr.status)
}

async fn ready(db: &TdPool, run: &PhysicalFunctionRunDB) -> Result<bool, TdError> {
    let ready = db.logical_runs().to_execute(&AtTime::now(), 50).await?;
    Ok(ready
        .iter()
        .any(|lfr| lfr.id == run.logical_function_run_id))
}

/// `beside` is Done and its transaction is not committed: `eager` may start
/// on it, `subscriber` may not. Once `checked` fails, the transaction cannot
/// commit and `subscriber` goes on hold, though it reads nothing of
/// `checked`'s. Recovering the plan releases it, to wait for the commit.
#[td_test::test(db)]
async fn test_a_subscriber_does_not_read_a_transaction_that_has_not_committed(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let test_executions = delivery(LogicalFunctionRunStatus::Scheduled)?;
    run_status_scenario_with(
        db.clone(),
        p.project_id,
        &test_executions,
        SUBSCRIBERS,
        |_, e, _, f, w| {
            let db = db.clone();
            let execution = e[&test_executions[0]].id.clone();
            let [checked, beside] = &test_executions[0].transactions[0].functions[..] else {
                unreachable!()
            };
            let subscriber = &test_executions[0].transactions[1].functions[0];
            let eager = &test_executions[0].transactions[2].functions[0];
            let (checked, checked_worker) = (f[checked].clone(), w[checked].id.clone());
            let (beside, beside_worker) = (f[beside].clone(), w[beside].id.clone());
            let (subscriber, eager) = (f[subscriber].clone(), f[eager].clone());
            async move {
                post_callback(&db, &beside.id, &beside_worker, WorkerCallbackStatus::Done).await?;
                assert!(ready(&db, &eager).await?, "eager starts on a Done run");
                assert!(
                    !ready(&db, &subscriber).await?,
                    "subscriber started on a transaction that has not committed"
                );

                post_callback(
                    &db,
                    &checked.id,
                    &checked_worker,
                    WorkerCallbackStatus::Failed,
                )
                .await?;
                assert_eq!(
                    status(&db, &subscriber).await?,
                    LogicalFunctionRunStatus::OnHold,
                    "the transaction subscriber reads cannot commit"
                );
                assert_eq!(
                    status(&db, &eager).await?,
                    LogicalFunctionRunStatus::Scheduled
                );
                let blockers = db
                    .logical_runs()
                    .blockers(from_ref(&subscriber.logical_function_run_id))
                    .await?;
                assert_eq!(
                    blockers,
                    vec![(
                        subscriber.logical_function_run_id.clone(),
                        checked.logical_function_run_id.clone()
                    )],
                    "what holds subscriber is the failed run beside the one it reads"
                );

                let recover = RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .update(ExecutionParam { execution }, ());
                dispatch_services(&db).execution_recover(recover).await?;
                assert_eq!(
                    status(&db, &subscriber).await?,
                    LogicalFunctionRunStatus::ReScheduled
                );
                assert!(!ready(&db, &subscriber).await?);
                Ok(())
            }
        },
    )
    .await
    .map(|_| ())
}

/// The transaction `subscriber` reads commits once both its runs are Done;
/// only then may `subscriber` start.
#[td_test::test(db)]
async fn test_a_subscriber_starts_once_the_transaction_it_reads_commits(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let test_executions = delivery(LogicalFunctionRunStatus::Scheduled)?;
    run_status_scenario_with(
        db.clone(),
        p.project_id,
        &test_executions,
        SUBSCRIBERS,
        |_, _, _, f, w| {
            let db = db.clone();
            let [checked, beside] = &test_executions[0].transactions[0].functions[..] else {
                unreachable!()
            };
            let subscriber = &test_executions[0].transactions[1].functions[0];
            let (checked, checked_worker) = (f[checked].clone(), w[checked].id.clone());
            let (beside, beside_worker) = (f[beside].clone(), w[beside].id.clone());
            let subscriber = f[subscriber].clone();
            async move {
                post_callback(&db, &beside.id, &beside_worker, WorkerCallbackStatus::Done).await?;
                assert!(!ready(&db, &subscriber).await?);

                post_callback(
                    &db,
                    &checked.id,
                    &checked_worker,
                    WorkerCallbackStatus::Done,
                )
                .await?;
                assert_eq!(
                    status(&db, &beside).await?,
                    LogicalFunctionRunStatus::Committed
                );
                assert!(
                    ready(&db, &subscriber).await?,
                    "subscriber waits on a committed transaction"
                );
                Ok(())
            }
        },
    )
    .await
    .map(|_| ())
}

/// A subscriber that started before its transaction's commit was required has
/// already written. A failure beside the run it read does not put it on hold,
/// and recovering the plan does not run it again.
#[td_test::test(db)]
async fn test_a_subscriber_that_already_wrote_is_not_run_again(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let test_executions = delivery(LogicalFunctionRunStatus::Done)?;
    run_status_scenario_with(
        db.clone(),
        p.project_id,
        &test_executions,
        SUBSCRIBERS,
        |_, e, _, f, w| {
            let db = db.clone();
            let execution = e[&test_executions[0]].id.clone();
            let [checked, beside] = &test_executions[0].transactions[0].functions[..] else {
                unreachable!()
            };
            let subscriber = &test_executions[0].transactions[1].functions[0];
            let (checked, checked_worker) = (f[checked].clone(), w[checked].id.clone());
            let (beside, beside_worker) = (f[beside].clone(), w[beside].id.clone());
            let subscriber = f[subscriber].clone();
            async move {
                post_callback(&db, &beside.id, &beside_worker, WorkerCallbackStatus::Done).await?;
                post_callback(
                    &db,
                    &checked.id,
                    &checked_worker,
                    WorkerCallbackStatus::Failed,
                )
                .await?;
                assert_eq!(
                    status(&db, &subscriber).await?,
                    LogicalFunctionRunStatus::Done,
                    "a subscriber that wrote went on hold"
                );

                let recover = RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .update(ExecutionParam { execution }, ());
                dispatch_services(&db).execution_recover(recover).await?;
                assert_eq!(
                    status(&db, &subscriber).await?,
                    LogicalFunctionRunStatus::Done,
                    "a subscriber that wrote was scheduled again"
                );
                Ok(())
            }
        },
    )
    .await
    .map(|_| ())
}

/// A plan made after the failure reads the table `beside` wrote in a
/// transaction that cannot commit: its subscriber starts on hold.
#[td_test::test(db)]
async fn test_a_later_plan_reading_a_stalled_transaction_starts_on_hold(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let mut test_executions = delivery(LogicalFunctionRunStatus::Scheduled)?;
    test_executions.push(later_plan()?);
    run_status_scenario_with(
        db.clone(),
        p.project_id,
        &test_executions,
        SUBSCRIBERS,
        |_, e, _, f, w| {
            let db = db.clone();
            let later_execution = e[&test_executions[1]].id.clone();
            let [checked, beside] = &test_executions[0].transactions[0].functions[..] else {
                unreachable!()
            };
            let later = &test_executions[1].transactions[0].functions[0];
            let (checked, checked_worker) = (f[checked].clone(), w[checked].id.clone());
            let (beside, beside_worker) = (f[beside].clone(), w[beside].id.clone());
            let later = f[later].clone();
            async move {
                post_callback(&db, &beside.id, &beside_worker, WorkerCallbackStatus::Done).await?;
                assert!(
                    db.logical_runs()
                        .blocked_ids(&later_execution)
                        .await?
                        .is_empty(),
                    "a transaction that can still commit holds nothing"
                );

                post_callback(
                    &db,
                    &checked.id,
                    &checked_worker,
                    WorkerCallbackStatus::Failed,
                )
                .await?;
                assert_eq!(
                    db.logical_runs().blocked_ids(&later_execution).await?,
                    vec![later.logical_function_run_id.clone()]
                );
                Ok(())
            }
        },
    )
    .await
    .map(|_| ())
}
