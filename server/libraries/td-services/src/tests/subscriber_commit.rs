//
// Copyright 2026 Tabsdata Inc.
//

//! A subscriber writes outside Tabsdata, where no failure takes the write
//! back, so it runs only on data that is certain to commit: after the
//! transactions it reads from have committed.

use crate::Services;
use crate::execution::holds::HoldEvaluator;
use crate::execution::holds::tests::{STATEMENTS, Statements};
use crate::execution::plan::rows::update_initial_function_run_status;
use crate::execution::release::release_unblocked;
use crate::execution::update_status::tests::{
    TestExecution, TestFunction, TestTransaction, run_status_scenario_with,
};
use crate::scheduler::test_wiring::dispatch_services;
use crate::tests::catalog_races::function_service_gate;
use crate::tests::late_callbacks::post_callback;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::iter::once;
use std::slice::from_ref;
use td_common::execution_status::WorkerCallbackStatus;
use td_database::access::{AcrossProjects, DbPool, Grant};
use td_database::domain::DomainPools;
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::crudl::{RequestContext, UpdateRequest};
use td_objects::dxo::execution::{ExecutionDB, ExecutionParam};
use td_objects::dxo::function::{FunctionDB, FunctionRegister};
use td_objects::dxo::logical::{LogicalFunctionRunDB, LogicalFunctionRunParam};
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionDB;
use td_objects::dxo::table::TableDB;
use td_objects::dxo::transaction::TransactionDB;
use td_objects::dxo::worker::WorkerDB;
use td_objects::types::basic::{
    AccessTokenId, AtTime, BundleId, CollectionName, Decorator, ExecutionId, ExecutionStatus,
    FunctionName, FunctionRuntimeValues, LogicalFunctionRunId, LogicalFunctionRunStatus, ProjectId,
    ProjectName, ReuseFrozen, TableName, TableNameDto, TransactionKey, TransactionStatus, UserId,
    VersionPos, WorkerId,
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

/// One plan: c_0 holds `checked` and `beside`, both running. A chain of
/// `relays` running transformers follows, each alone in the next collection
/// and reading the table of the one before it, the first `beside`'s; the
/// collection after the last holds `subscriber`, reading the last one's table.
fn relayed_delivery(
    relays: usize,
    subscriber: LogicalFunctionRunStatus,
) -> Result<Vec<TestExecution>, TdError> {
    use LogicalFunctionRunStatus::Running;
    let mut transactions = vec![transaction(vec![
        function("c_0", "checked", &[], "t_checked", Running)?,
        function("c_0", "beside", &[], "t_beside", Running)?,
    ])];
    let mut read = String::from("c_0/t_beside");
    for i in 1..=relays {
        let (collection, table) = (format!("c_{i}"), format!("t_relay_{i}"));
        transactions.push(transaction(vec![function(
            &collection,
            &format!("relay_{i}"),
            &[read.as_str()],
            &table,
            Running,
        )?]));
        read = format!("{collection}/{table}");
    }
    transactions.push(transaction(vec![function(
        &format!("c_{}", relays + 1),
        "subscriber",
        &[read.as_str()],
        "t_out",
        subscriber,
    )?]));
    Ok(vec![TestExecution {
        expected_status: ExecutionStatus::Running,
        transactions,
    }])
}

#[derive(Debug)]
struct AfterFailure {
    subscriber: LogicalFunctionRunStatus,
    relays: Vec<LogicalFunctionRunStatus>,
    execution: ExecutionStatus,
    blockers: Vec<(LogicalFunctionRunId, LogicalFunctionRunId)>,
    subscriber_id: LogicalFunctionRunId,
    checked_id: LogicalFunctionRunId,
}

async fn fail_behind_relays(db: &TdPool, relays: usize) -> Result<AfterFailure, TdError> {
    let p = db.seed().test_project().await?;
    let test_executions = relayed_delivery(relays, LogicalFunctionRunStatus::Scheduled)?;
    let transactions = &test_executions[0].transactions;
    let [checked, beside] = &transactions[0].functions[..] else {
        unreachable!()
    };
    let subscriber = &transactions[transactions.len() - 1].functions[0];
    let relayed: Vec<&TestFunction> = transactions[1..transactions.len() - 1]
        .iter()
        .map(|t| &t.functions[0])
        .collect();
    let (e, _, f) = run_status_scenario_with(
        db.clone(),
        p.project_id,
        &test_executions,
        SUBSCRIBERS,
        |_, _, _, f, w| {
            let db = db.clone();
            let (checked, checked_worker) = (f[checked].clone(), w[checked].id.clone());
            let (beside, beside_worker) = (f[beside].clone(), w[beside].id.clone());
            let relays: Vec<_> = relayed
                .iter()
                .map(|relay| (f[*relay].clone(), w[*relay].id.clone()))
                .collect();
            let subscriber = f[subscriber].clone();
            async move {
                post_callback(&db, &beside.id, &beside_worker, WorkerCallbackStatus::Done).await?;
                for (relay, worker) in &relays {
                    post_callback(&db, &relay.id, worker, WorkerCallbackStatus::Done).await?;
                }
                for run in once(&beside).chain(relays.iter().map(|(relay, _)| relay)) {
                    assert_eq!(status(&db, run).await?, LogicalFunctionRunStatus::Done);
                }
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
                    status(&db, &checked).await?,
                    LogicalFunctionRunStatus::Failed
                );
                Ok(())
            }
        },
    )
    .await?;

    let subscriber = &f[subscriber];
    let mut relays = Vec::new();
    for relay in &relayed {
        relays.push(status(db, &f[*relay]).await?);
    }
    let execution: ExecutionDB = db.fetch_one(&e[&test_executions[0]].id).await?;
    Ok(AfterFailure {
        subscriber: status(db, subscriber).await?,
        relays,
        execution: execution.status,
        blockers: blockers_of(db, p.project_id, &subscriber.logical_function_run_id).await?,
        subscriber_id: subscriber.logical_function_run_id.clone(),
        checked_id: f[checked].logical_function_run_id.clone(),
    })
}

fn grant() -> Grant {
    Grant::asserted("subscriber commit test")
}

fn pools(db: &TdPool) -> DbPool<AcrossProjects> {
    DbPool::<AcrossProjects>::new(DomainPools::of_apiserver(db.clone()), AcrossProjects)
}

async fn blockers_of(
    db: &TdPool,
    project: ProjectId,
    run: &LogicalFunctionRunId,
) -> Result<Vec<(LogicalFunctionRunId, LogicalFunctionRunId)>, TdError> {
    let mut conn = DbPool::in_project(&pools(db), &project).read().await?;
    let held = HoldEvaluator::default()
        .held(conn.get(grant()), from_ref(run), &HashSet::new())
        .await?;
    Ok(held
        .get(run)
        .into_iter()
        .flatten()
        .map(|blocker| (run.clone(), blocker.clone()))
        .collect())
}

async fn blocked_in(
    db: &TdPool,
    project: ProjectId,
    execution: &ExecutionId,
) -> Result<Vec<LogicalFunctionRunId>, TdError> {
    let runs: Vec<_> = db
        .fetch_all::<LogicalFunctionRunDB>(execution)
        .await?
        .into_iter()
        .map(|lfr| lfr.base.id)
        .collect();
    let mut conn = DbPool::in_project(&pools(db), &project).read().await?;
    let held = HoldEvaluator::default()
        .held(conn.get(grant()), &runs, &HashSet::new())
        .await?;
    Ok(runs
        .into_iter()
        .filter(|run| held.contains_key(run))
        .collect())
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
            let project = p.project_id;
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
                let blockers =
                    blockers_of(&db, project, &subscriber.logical_function_run_id).await?;
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
            let project = p.project_id;
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
                    blocked_in(&db, project, &later_execution).await?.is_empty(),
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
                    blocked_in(&db, project, &later_execution).await?,
                    vec![later.logical_function_run_id.clone()]
                );
                Ok(())
            }
        },
    )
    .await
    .map(|_| ())
}

/// `relay_1` reads `beside`, Done in a transaction that has not committed,
/// and runs to Done; `subscriber` reads `relay_1` and waits for its
/// transaction to commit. Once `checked` fails, neither transaction can
/// commit: `subscriber` goes on hold behind `checked`, though it reads nothing
/// of its transaction, and `relay_1`, which already wrote, stays Done.
#[td_test::test(db)]
async fn test_a_subscriber_behind_a_transaction_that_cannot_commit_goes_on_hold(
    db: TdPool,
) -> Result<(), TdError> {
    let after = fail_behind_relays(&db, 1).await?;
    assert_eq!(
        after.subscriber,
        LogicalFunctionRunStatus::OnHold,
        "subscriber waits for a transaction that cannot commit: {after:?}"
    );
    assert_eq!(after.relays, [LogicalFunctionRunStatus::Done], "{after:?}");
    assert_eq!(after.execution, ExecutionStatus::Running, "{after:?}");
    assert!(
        after
            .blockers
            .contains(&(after.subscriber_id.clone(), after.checked_id.clone())),
        "what holds subscriber is the failure its relay read beside: {after:?}"
    );
    Ok(())
}

/// The same with two relays: `relay_2` reads `relay_1` and `subscriber`
/// reads `relay_2`.
#[td_test::test(db)]
async fn test_a_subscriber_two_transactions_behind_one_that_cannot_commit_goes_on_hold(
    db: TdPool,
) -> Result<(), TdError> {
    let after = fail_behind_relays(&db, 2).await?;
    assert_eq!(
        after.subscriber,
        LogicalFunctionRunStatus::OnHold,
        "subscriber waits for a transaction that cannot commit: {after:?}"
    );
    assert_eq!(
        after.relays,
        [
            LogicalFunctionRunStatus::Done,
            LogicalFunctionRunStatus::Done
        ],
        "{after:?}"
    );
    Ok(())
}

#[derive(Clone)]
struct Run {
    run: PhysicalFunctionRunDB,
    worker: WorkerId,
}

impl Run {
    fn of(
        function: &TestFunction,
        f: &HashMap<&TestFunction, PhysicalFunctionRunDB>,
        w: &HashMap<&TestFunction, WorkerDB>,
    ) -> Self {
        Run {
            run: f[function].clone(),
            worker: w[function].id.clone(),
        }
    }

    fn id(&self) -> &LogicalFunctionRunId {
        &self.run.logical_function_run_id
    }

    async fn report(&self, db: &TdPool, status: WorkerCallbackStatus) -> Result<(), TdError> {
        post_callback(db, &self.run.id, &self.worker, status).await
    }

    async fn status(&self, db: &TdPool) -> Result<LogicalFunctionRunStatus, TdError> {
        status(db, &self.run).await
    }

    async fn attempt(&self, db: &TdPool) -> Result<PhysicalFunctionRunDB, TdError> {
        Ok(db.scd2().fetch_one(self.id()).await?)
    }
}

#[derive(Clone)]
struct Relayed {
    checked: Run,
    beside: Run,
    relays: Vec<Run>,
    subscriber: Run,
}

impl Relayed {
    fn of(
        plan: &TestExecution,
        f: &HashMap<&TestFunction, PhysicalFunctionRunDB>,
        w: &HashMap<&TestFunction, WorkerDB>,
    ) -> Self {
        let transactions = &plan.transactions;
        let [checked, beside] = &transactions[0].functions[..] else {
            unreachable!()
        };
        Relayed {
            checked: Run::of(checked, f, w),
            beside: Run::of(beside, f, w),
            relays: transactions[1..transactions.len() - 1]
                .iter()
                .map(|t| Run::of(&t.functions[0], f, w))
                .collect(),
            subscriber: Run::of(&transactions[transactions.len() - 1].functions[0], f, w),
        }
    }

    /// `beside` and every relay report Done, then `checked` Failed.
    async fn fail_behind(&self, db: &TdPool) -> Result<(), TdError> {
        self.beside.report(db, WorkerCallbackStatus::Done).await?;
        for relay in &self.relays {
            relay.report(db, WorkerCallbackStatus::Done).await?;
        }
        self.checked.report(db, WorkerCallbackStatus::Failed).await
    }
}

fn execution_request(execution: &ExecutionId) -> UpdateRequest<(), ExecutionParam> {
    RequestContext::with(AccessTokenId::default(), UserId::admin()).update(
        ExecutionParam {
            execution: execution.clone(),
        },
        (),
    )
}

/// `checked` fails while `relay_1` is still running: `subscriber` goes on
/// hold at once, since `relay_1` reads `checked`'s transaction from the plan
/// on, and `relay_1` reporting Done afterwards does not release it.
#[td_test::test(db)]
async fn test_a_subscriber_behind_a_relay_still_running_goes_on_hold(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let test_executions = relayed_delivery(1, LogicalFunctionRunStatus::Scheduled)?;
    run_status_scenario_with(
        db.clone(),
        p.project_id,
        &test_executions,
        SUBSCRIBERS,
        |_, _, _, f, w| {
            let db = db.clone();
            let project = p.project_id;
            let runs = Relayed::of(&test_executions[0], f, w);
            async move {
                let relay = &runs.relays[0];
                runs.beside.report(&db, WorkerCallbackStatus::Done).await?;
                runs.checked
                    .report(&db, WorkerCallbackStatus::Failed)
                    .await?;
                assert_eq!(relay.status(&db).await?, LogicalFunctionRunStatus::Running);
                assert_eq!(
                    runs.subscriber.status(&db).await?,
                    LogicalFunctionRunStatus::OnHold
                );

                relay.report(&db, WorkerCallbackStatus::Done).await?;
                assert_eq!(relay.status(&db).await?, LogicalFunctionRunStatus::Done);
                assert_eq!(
                    runs.subscriber.status(&db).await?,
                    LogicalFunctionRunStatus::OnHold
                );
                assert!(
                    blockers_of(&db, project, runs.subscriber.id())
                        .await?
                        .contains(&(runs.subscriber.id().clone(), runs.checked.id().clone()))
                );
                Ok(())
            }
        },
    )
    .await
    .map(|_| ())
}

/// The project's release pass leaves `subscriber` on hold while `checked`
/// is Failed. Recovering the plan releases it, to wait for both transactions
/// to commit; once `checked`'s new attempt is Done, `beside`'s transaction
/// commits and then `relay_1`'s, and only then does `subscriber` start.
/// `relay_1` keeps its one attempt throughout.
#[td_test::test(db)]
async fn test_a_subscriber_behind_a_transaction_that_cannot_commit_starts_once_both_commit(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let test_executions = relayed_delivery(1, LogicalFunctionRunStatus::Scheduled)?;
    run_status_scenario_with(
        db.clone(),
        p.project_id,
        &test_executions,
        SUBSCRIBERS,
        |_, e, _, f, w| {
            let db = db.clone();
            let project = p.project_id;
            let execution = e[&test_executions[0]].id.clone();
            let runs = Relayed::of(&test_executions[0], f, w);
            async move {
                let relay = &runs.relays[0];
                runs.fail_behind(&db).await?;
                let relay_attempt = relay.attempt(&db).await?;

                let (mut gate, _) = function_service_gate(&db, project).await?;
                release_unblocked(gate.get(grant())).await?;
                gate.commit().await?;
                assert_eq!(
                    runs.subscriber.status(&db).await?,
                    LogicalFunctionRunStatus::OnHold,
                    "the release pass freed a subscriber behind a failed run"
                );

                dispatch_services(&db)
                    .execution_recover(execution_request(&execution))
                    .await?;
                assert_eq!(
                    runs.checked.status(&db).await?,
                    LogicalFunctionRunStatus::ReScheduled
                );
                assert_eq!(
                    runs.subscriber.status(&db).await?,
                    LogicalFunctionRunStatus::ReScheduled
                );
                assert_eq!(relay.status(&db).await?, LogicalFunctionRunStatus::Done);
                assert!(!ready(&db, &runs.subscriber.run).await?);

                dispatch_services(&db).tick().await?;
                let retry = runs.checked.attempt(&db).await?;
                assert_ne!(retry.id, runs.checked.run.id);
                assert!(!ready(&db, &runs.subscriber.run).await?);
                let workers: Vec<WorkerDB> = db.fetch_all(&retry.id).await?;
                let retry = Run {
                    run: retry,
                    worker: workers[0].id.clone(),
                };
                retry.report(&db, WorkerCallbackStatus::Running).await?;
                retry.report(&db, WorkerCallbackStatus::Done).await?;

                for run in [&runs.checked, &runs.beside, relay] {
                    assert_eq!(run.status(&db).await?, LogicalFunctionRunStatus::Committed);
                }
                assert!(
                    ready(&db, &runs.subscriber.run).await?,
                    "subscriber waits on committed transactions"
                );
                assert_eq!(
                    relay.attempt(&db).await?.id,
                    relay_attempt.id,
                    "the relay ran again"
                );
                Ok(())
            }
        },
    )
    .await
    .map(|_| ())
}

/// A replay heals `checked`: both transactions commit at once, and
/// `subscriber` is released and may start, with no release pass.
#[td_test::test(db)]
async fn test_a_replay_that_heals_the_failure_releases_the_subscriber_behind_it(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let test_executions = relayed_delivery(1, LogicalFunctionRunStatus::Scheduled)?;
    run_status_scenario_with(
        db.clone(),
        p.project_id,
        &test_executions,
        SUBSCRIBERS,
        |_, _, _, f, w| {
            let db = db.clone();
            let runs = Relayed::of(&test_executions[0], f, w);
            async move {
                runs.fail_behind(&db).await?;
                let replay = RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .create(
                        LogicalFunctionRunParam {
                            logical_function_run: runs.checked.id().clone(),
                        },
                        (),
                    );
                let replayed = dispatch_services(&db)
                    .replay_logical_function_run(replay)
                    .await?
                    .data;
                dispatch_services(&db).tick().await?;
                let workers: Vec<WorkerDB> = db.fetch_all(&replayed.new_function_run_id).await?;
                for status in [WorkerCallbackStatus::Running, WorkerCallbackStatus::Done] {
                    post_callback(&db, &replayed.new_function_run_id, &workers[0].id, status)
                        .await?;
                }

                assert_eq!(
                    runs.relays[0].status(&db).await?,
                    LogicalFunctionRunStatus::Committed
                );
                assert_eq!(
                    runs.subscriber.status(&db).await?,
                    LogicalFunctionRunStatus::ReScheduled,
                    "the healed failure left subscriber on hold"
                );
                assert!(ready(&db, &runs.subscriber.run).await?);
                Ok(())
            }
        },
    )
    .await
    .map(|_| ())
}

/// `relay_1` and `subscriber` are a later plan, reading `beside` of a plan
/// whose `checked` fails. Recovering the later plan cannot help: `subscriber`
/// stays on hold behind the failure it does not own.
#[td_test::test(db)]
async fn test_recovering_its_own_plan_leaves_a_subscriber_on_hold_behind_another(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let mut test_executions = relayed_delivery(1, LogicalFunctionRunStatus::Scheduled)?;
    let later = test_executions[0].transactions.split_off(1);
    test_executions.push(TestExecution {
        expected_status: ExecutionStatus::Running,
        transactions: later,
    });
    run_status_scenario_with(
        db.clone(),
        p.project_id,
        &test_executions,
        SUBSCRIBERS,
        |_, e, _, f, w| {
            let db = db.clone();
            let project = p.project_id;
            let later_execution = e[&test_executions[1]].id.clone();
            let [checked, beside] = &test_executions[0].transactions[0].functions[..] else {
                unreachable!()
            };
            let (checked, beside) = (Run::of(checked, f, w), Run::of(beside, f, w));
            let relay = Run::of(&test_executions[1].transactions[0].functions[0], f, w);
            let subscriber = Run::of(&test_executions[1].transactions[1].functions[0], f, w);
            async move {
                beside.report(&db, WorkerCallbackStatus::Done).await?;
                relay.report(&db, WorkerCallbackStatus::Done).await?;
                checked.report(&db, WorkerCallbackStatus::Failed).await?;
                assert_eq!(
                    subscriber.status(&db).await?,
                    LogicalFunctionRunStatus::OnHold
                );

                dispatch_services(&db)
                    .execution_recover(execution_request(&later_execution))
                    .await?;
                assert_eq!(
                    subscriber.status(&db).await?,
                    LogicalFunctionRunStatus::OnHold,
                    "recovering its own plan released a subscriber behind another"
                );
                assert_eq!(relay.status(&db).await?, LogicalFunctionRunStatus::Done);
                assert!(
                    blockers_of(&db, project, subscriber.id())
                        .await?
                        .contains(&(subscriber.id().clone(), checked.id().clone()))
                );
                let rolled_up: ExecutionDB = db.fetch_one(&later_execution).await?;
                assert_eq!(rolled_up.status, ExecutionStatus::Running);
                Ok(())
            }
        },
    )
    .await
    .map(|_| ())
}

/// A plan made after the failure has a subscriber reading `relay_1`'s
/// table, in a transaction that can only commit after `checked`'s: it starts
/// on hold.
#[td_test::test(db)]
async fn test_a_later_plan_reading_behind_a_stalled_transaction_starts_on_hold(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let mut test_executions = relayed_delivery(1, LogicalFunctionRunStatus::Scheduled)?;
    test_executions.push(TestExecution {
        expected_status: ExecutionStatus::Running,
        transactions: vec![transaction(vec![function(
            "c_3",
            "later",
            &["c_1/t_relay_1"],
            "t_later",
            LogicalFunctionRunStatus::Scheduled,
        )?])],
    });
    run_status_scenario_with(
        db.clone(),
        p.project_id,
        &test_executions,
        SUBSCRIBERS,
        |_, e, _, f, w| {
            let db = db.clone();
            let project = p.project_id;
            let later_execution = e[&test_executions[1]].id.clone();
            let runs = Relayed::of(&test_executions[0], f, w);
            let later = Run::of(&test_executions[1].transactions[0].functions[0], f, w);
            async move {
                runs.beside.report(&db, WorkerCallbackStatus::Done).await?;
                runs.relays[0]
                    .report(&db, WorkerCallbackStatus::Done)
                    .await?;
                assert!(
                    blocked_in(&db, project, &later_execution).await?.is_empty(),
                    "transactions that can still commit hold nothing"
                );

                runs.checked
                    .report(&db, WorkerCallbackStatus::Failed)
                    .await?;
                assert_eq!(
                    blocked_in(&db, project, &later_execution).await?,
                    vec![later.id().clone()]
                );
                Ok(())
            }
        },
    )
    .await
    .map(|_| ())
}

/// Canceling the plan whose `checked` failed cancels every run of it,
/// `subscriber` on hold behind the failure included.
#[td_test::test(db)]
async fn test_canceling_the_failed_plan_cancels_the_subscriber_held_behind_it(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let test_executions = relayed_delivery(1, LogicalFunctionRunStatus::Scheduled)?;
    run_status_scenario_with(
        db.clone(),
        p.project_id,
        &test_executions,
        SUBSCRIBERS,
        |_, e, _, f, w| {
            let db = db.clone();
            let execution = e[&test_executions[0]].id.clone();
            let runs = Relayed::of(&test_executions[0], f, w);
            async move {
                runs.fail_behind(&db).await?;
                assert_eq!(
                    runs.subscriber.status(&db).await?,
                    LogicalFunctionRunStatus::OnHold
                );

                Services::with_defaults(db.clone())
                    .execution_cancel(execution_request(&execution))
                    .await?;
                let statuses: Vec<LogicalFunctionRunStatus> = db
                    .fetch_all::<LogicalFunctionRunDB>(&execution)
                    .await?
                    .into_iter()
                    .map(|lfr| lfr.status)
                    .collect();
                assert_eq!(
                    statuses,
                    vec![LogicalFunctionRunStatus::Canceled; statuses.len()],
                    "{statuses:?}"
                );
                let rolled_up: ExecutionDB = db.fetch_one(&execution).await?;
                assert_eq!(rolled_up.status, ExecutionStatus::Finished);
                Ok(())
            }
        },
    )
    .await
    .map(|_| ())
}

/// A subscriber already running behind `relay_1` has written: the failure
/// behind its relay does not hold it, and a recover does not run it again.
#[td_test::test(db)]
async fn test_a_started_subscriber_behind_a_transaction_that_cannot_commit_is_untouched(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let test_executions = relayed_delivery(1, LogicalFunctionRunStatus::Running)?;
    run_status_scenario_with(
        db.clone(),
        p.project_id,
        &test_executions,
        SUBSCRIBERS,
        |_, e, _, f, w| {
            let db = db.clone();
            let execution = e[&test_executions[0]].id.clone();
            let runs = Relayed::of(&test_executions[0], f, w);
            async move {
                runs.fail_behind(&db).await?;
                assert_eq!(
                    runs.subscriber.status(&db).await?,
                    LogicalFunctionRunStatus::Running
                );
                dispatch_services(&db)
                    .execution_recover(execution_request(&execution))
                    .await?;
                assert_eq!(
                    runs.subscriber.status(&db).await?,
                    LogicalFunctionRunStatus::Running
                );
                assert_eq!(
                    runs.subscriber.attempt(&db).await?.id,
                    runs.subscriber.run.id
                );
                Ok(())
            }
        },
    )
    .await
    .map(|_| ())
}

/// A plan whose `tfr` reads its own state from an earlier run on hold, and
/// whose `subscriber`, in the same transaction, reads `tfr`'s table: `tfr`
/// starts on hold through its state, and `subscriber` through `tfr`, which
/// was not on hold before the plan was built.
#[td_test::test(db)]
async fn test_a_plan_holds_the_readers_of_a_run_it_holds(db: TdPool) -> Result<(), TdError> {
    use LogicalFunctionRunStatus::{OnHold, Scheduled};
    let p = db.seed().test_project().await?;
    let test_executions = vec![
        TestExecution {
            expected_status: ExecutionStatus::Stalled,
            transactions: vec![transaction(vec![function(
                "c_0",
                "tfr",
                &[],
                "t_state",
                OnHold,
            )?])],
        },
        TestExecution {
            expected_status: ExecutionStatus::Running,
            transactions: vec![transaction(vec![
                function("c_0", "tfr", &[], "t_state", Scheduled)?,
                function("c_0", "subscriber", &[], "t_out", Scheduled)?,
            ])],
        },
    ];
    run_status_scenario_with(
        db.clone(),
        p.project_id,
        &test_executions,
        SUBSCRIBERS,
        |c, e, t, f, _| {
            let db = db.clone();
            let project = p.project_id;
            let collection = c[&CollectionName::try_from("c_0").unwrap()].clone();
            let plan = &test_executions[1];
            let execution = e[plan].clone();
            let transaction = t[&plan.transactions[0]].clone();
            let earlier = f[&test_executions[0].transactions[0].functions[0]].clone();
            let tfr = f[&plan.transactions[0].functions[0]].clone();
            let subscriber = f[&plan.transactions[0].functions[1]].clone();
            async move {
                let state = TableName::try_from("t_state")?;
                let versions: Vec<PhysicalTableDataVersionDB> =
                    db.timeline().fetch_all(&tfr.function_version_id).await?;
                let written_by = |run: &PhysicalFunctionRunDB| {
                    versions
                        .iter()
                        .find(|v| v.physical_function_run_id == run.id && v.name == state)
                        .cloned()
                        .unwrap()
                };
                for (reader, read) in [
                    (&tfr, written_by(&earlier)),
                    (&subscriber, written_by(&tfr)),
                ] {
                    db.scoped(project)
                        .seed()
                        .logical_input(
                            &collection,
                            &execution,
                            &transaction,
                            reader,
                            Some(&read),
                            None,
                            &VersionPos::try_from(0)?,
                        )
                        .await?;
                }

                let (mut gate, _) = function_service_gate(&db, project).await?;
                let runs = db
                    .fetch_all::<LogicalFunctionRunDB>(&execution.id)
                    .await?
                    .into_iter()
                    .map(|lfr| lfr.base.id)
                    .collect();
                update_initial_function_run_status(gate.get(grant()), &execution.base, runs)
                    .await?;
                gate.commit().await?;
                assert_eq!(status(&db, &tfr).await?, OnHold);
                assert_eq!(
                    status(&db, &subscriber).await?,
                    OnHold,
                    "subscriber reads a run its plan holds"
                );
                Ok(())
            }
        },
    )
    .await
    .map(|_| ())
}

/// A plan whose `tfr` reads its own state from an earlier run on hold, beside
/// `other` in the same transaction, and whose `subscriber`, in another
/// transaction, reads only `other`: `tfr` starts on hold, and its transaction
/// can no longer commit, so `subscriber` starts on hold too.
#[td_test::test(db)]
async fn test_a_plan_holds_a_subscriber_waiting_on_a_transaction_it_stalls(
    db: TdPool,
) -> Result<(), TdError> {
    use LogicalFunctionRunStatus::{OnHold, Scheduled};
    let p = db.seed().test_project().await?;
    let test_executions = vec![
        TestExecution {
            expected_status: ExecutionStatus::Stalled,
            transactions: vec![transaction(vec![function(
                "c_0",
                "tfr",
                &[],
                "t_state",
                OnHold,
            )?])],
        },
        TestExecution {
            expected_status: ExecutionStatus::Running,
            transactions: vec![
                transaction(vec![
                    function("c_0", "tfr", &[], "t_state", Scheduled)?,
                    function("c_0", "other", &[], "t_other", Scheduled)?,
                ]),
                transaction(vec![function(
                    "c_1",
                    "subscriber",
                    &[],
                    "t_out",
                    Scheduled,
                )?]),
            ],
        },
    ];
    run_status_scenario_with(
        db.clone(),
        p.project_id,
        &test_executions,
        SUBSCRIBERS,
        |c, e, t, f, _| {
            let db = db.clone();
            let project = p.project_id;
            let plan = &test_executions[1];
            let execution = e[plan].clone();
            let earlier = f[&test_executions[0].transactions[0].functions[0]].clone();
            let tfr = f[&plan.transactions[0].functions[0]].clone();
            let other = f[&plan.transactions[0].functions[1]].clone();
            let subscriber = f[&plan.transactions[1].functions[0]].clone();
            let reads = [
                (
                    c[&CollectionName::try_from("c_0").unwrap()].clone(),
                    t[&plan.transactions[0]].clone(),
                    tfr.clone(),
                    earlier.clone(),
                    "t_state",
                ),
                (
                    c[&CollectionName::try_from("c_1").unwrap()].clone(),
                    t[&plan.transactions[1]].clone(),
                    subscriber.clone(),
                    other.clone(),
                    "t_other",
                ),
            ];
            async move {
                for (collection, transaction, reader, read, table) in &reads {
                    let table = TableName::try_from(*table)?;
                    let versions: Vec<PhysicalTableDataVersionDB> =
                        db.timeline().fetch_all(&read.function_version_id).await?;
                    let written = versions
                        .into_iter()
                        .find(|v| v.physical_function_run_id == read.id && v.name == table)
                        .unwrap();
                    db.scoped(project)
                        .seed()
                        .logical_input(
                            collection,
                            &execution,
                            transaction,
                            reader,
                            Some(&written),
                            None,
                            &VersionPos::try_from(0)?,
                        )
                        .await?;
                }

                let (mut gate, _) = function_service_gate(&db, project).await?;
                let runs = db
                    .fetch_all::<LogicalFunctionRunDB>(&execution.id)
                    .await?
                    .into_iter()
                    .map(|lfr| lfr.base.id)
                    .collect();
                update_initial_function_run_status(gate.get(grant()), &execution.base, runs)
                    .await?;
                gate.commit().await?;
                assert_eq!(status(&db, &tfr).await?, OnHold);
                assert_eq!(status(&db, &other).await?, Scheduled);
                assert_eq!(
                    status(&db, &subscriber).await?,
                    OnHold,
                    "subscriber waits on a transaction its plan stalls"
                );
                Ok(())
            }
        },
    )
    .await
    .map(|_| ())
}

/// `relay_1` and `subscriber` are a later plan reading `beside`. Canceling the
/// plan whose `checked` failed cancels them with it.
#[td_test::test(db)]
async fn test_canceling_the_failed_plan_cancels_its_readers_in_a_later_plan(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let mut test_executions = relayed_delivery(1, LogicalFunctionRunStatus::Scheduled)?;
    let later = test_executions[0].transactions.split_off(1);
    test_executions.push(TestExecution {
        expected_status: ExecutionStatus::Running,
        transactions: later,
    });
    run_status_scenario_with(
        db.clone(),
        p.project_id,
        &test_executions,
        SUBSCRIBERS,
        |_, e, _, f, w| {
            let db = db.clone();
            let failed_execution = e[&test_executions[0]].id.clone();
            let [checked, beside] = &test_executions[0].transactions[0].functions[..] else {
                unreachable!()
            };
            let (checked, beside) = (Run::of(checked, f, w), Run::of(beside, f, w));
            let relay = Run::of(&test_executions[1].transactions[0].functions[0], f, w);
            let subscriber = Run::of(&test_executions[1].transactions[1].functions[0], f, w);
            async move {
                beside.report(&db, WorkerCallbackStatus::Done).await?;
                relay.report(&db, WorkerCallbackStatus::Done).await?;
                checked.report(&db, WorkerCallbackStatus::Failed).await?;
                assert_eq!(
                    subscriber.status(&db).await?,
                    LogicalFunctionRunStatus::OnHold
                );

                Services::with_defaults(db.clone())
                    .execution_cancel(execution_request(&failed_execution))
                    .await?;
                assert_eq!(relay.status(&db).await?, LogicalFunctionRunStatus::Canceled);
                assert_eq!(
                    subscriber.status(&db).await?,
                    LogicalFunctionRunStatus::Canceled
                );
                Ok(())
            }
        },
    )
    .await
    .map(|_| ())
}

/// `relay_1` and `subscriber` are a later plan reading `beside`. Recovering
/// the plan whose `checked` failed releases `subscriber`, to wait for both
/// transactions to commit.
#[td_test::test(db)]
async fn test_recovering_the_failed_plan_releases_a_subscriber_in_a_later_plan(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let mut test_executions = relayed_delivery(1, LogicalFunctionRunStatus::Scheduled)?;
    let later = test_executions[0].transactions.split_off(1);
    test_executions.push(TestExecution {
        expected_status: ExecutionStatus::Running,
        transactions: later,
    });
    run_status_scenario_with(
        db.clone(),
        p.project_id,
        &test_executions,
        SUBSCRIBERS,
        |_, e, _, f, w| {
            let db = db.clone();
            let failed_execution = e[&test_executions[0]].id.clone();
            let [checked, beside] = &test_executions[0].transactions[0].functions[..] else {
                unreachable!()
            };
            let (checked, beside) = (Run::of(checked, f, w), Run::of(beside, f, w));
            let relay = Run::of(&test_executions[1].transactions[0].functions[0], f, w);
            let subscriber = Run::of(&test_executions[1].transactions[1].functions[0], f, w);
            async move {
                beside.report(&db, WorkerCallbackStatus::Done).await?;
                relay.report(&db, WorkerCallbackStatus::Done).await?;
                checked.report(&db, WorkerCallbackStatus::Failed).await?;
                assert_eq!(
                    subscriber.status(&db).await?,
                    LogicalFunctionRunStatus::OnHold
                );

                dispatch_services(&db)
                    .execution_recover(execution_request(&failed_execution))
                    .await?;
                assert_eq!(
                    checked.status(&db).await?,
                    LogicalFunctionRunStatus::ReScheduled
                );
                assert_eq!(
                    subscriber.status(&db).await?,
                    LogicalFunctionRunStatus::ReScheduled
                );
                assert_eq!(relay.status(&db).await?, LogicalFunctionRunStatus::Done);
                assert!(!ready(&db, &subscriber.run).await?);
                Ok(())
            }
        },
    )
    .await
    .map(|_| ())
}

/// One function of [`stacked_behind_a_failure`], and the table it writes.
struct Stacked {
    collection: CollectionDB,
    function: FunctionDB,
    table: TableDB,
}

impl Stacked {
    async fn register(
        db: &TdPool,
        collection: &CollectionDB,
        name: &str,
        decorator: Decorator,
    ) -> Result<Self, TdError> {
        let table = TableName::try_from(format!("t_{name}"))?;
        let create = FunctionRegister {
            child_functions: None,
            name: FunctionName::try_from(name)?,
            description: "d".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "s".try_into()?,
            decorator,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(vec![TableNameDto::try_from(table.as_str())?]),
            table_types: None,
            parent_tables: None,
            runtime_values: FunctionRuntimeValues::try_from("rv")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let function = db
            .scoped(collection.project_id)
            .seed()
            .function(collection, &create)
            .await?;
        let tables: Vec<TableDB> = db.timeline().fetch_all(&function.id).await?;
        let table = tables.into_iter().find(|t| t.name == table).unwrap();
        Ok(Stacked {
            collection: collection.clone(),
            function,
            table,
        })
    }

    /// A run in `transaction` writing its table and reading `reads`.
    async fn run(
        &self,
        db: &TdPool,
        execution: &ExecutionDB,
        transaction: &TransactionDB,
        status: LogicalFunctionRunStatus,
        reads: &[&PhysicalTableDataVersionDB],
    ) -> Result<(PhysicalFunctionRunDB, PhysicalTableDataVersionDB), TdError> {
        let project = self.collection.project_id;
        let run = db
            .scoped(project)
            .seed()
            .physical_function_run(
                &self.collection,
                &self.function,
                execution,
                transaction,
                &status,
            )
            .await?;
        let written = db
            .scoped(project)
            .seed()
            .physical_table_data_version(
                &self.collection,
                execution,
                transaction,
                &run,
                &self.table,
            )
            .await?;
        for read in reads {
            db.scoped(project)
                .seed()
                .logical_input(
                    &self.collection,
                    execution,
                    transaction,
                    &run,
                    Some(read),
                    None,
                    &VersionPos::try_from(0)?,
                )
                .await?;
        }
        Ok((run, written))
    }
}

async fn plan(
    db: &TdPool,
    project: ProjectId,
    function: &FunctionDB,
    transactions: usize,
) -> Result<(ExecutionDB, Vec<TransactionDB>), TdError> {
    let execution = db.scoped(project).seed().execution(function).await?;
    let mut opened = Vec::new();
    for key in 0..transactions {
        opened.push(
            db.scoped(project)
                .seed()
                .transaction(&execution, &TransactionKey::try_from(key.to_string())?)
                .await?,
        );
    }
    Ok((execution, opened))
}

#[derive(Debug)]
struct Measured {
    pass: Statements,
    plan_build: Statements,
    release: Statements,
}

/// `plans` plans behind one failure: in the first, `checked` failed beside
/// `beside`; in each, `relay` reads what the one before wrote (`beside` in
/// the first) and is Done, and `subscriber` reads `relay` and its own previous
/// output and is on hold. Measures the project's release pass, the build of
/// one more plan, and the release once `checked` is rescheduled.
async fn stacked_behind_a_failure(db: &TdPool, plans: usize) -> Result<Measured, TdError> {
    use LogicalFunctionRunStatus::{Done, Failed, OnHold, ReScheduled, Scheduled};
    let project = db
        .seed()
        .project(ProjectName::try_from(format!("stack_{plans}"))?)
        .await?
        .project_id;
    let mut collections = Vec::new();
    for name in ["c_0", "c_1", "c_2"] {
        collections.push(
            db.scoped(project)
                .seed()
                .collection(&CollectionName::try_from(name)?, &UserId::admin())
                .await?,
        );
    }
    let checked = Stacked::register(db, &collections[0], "checked", Decorator::Transformer).await?;
    let beside = Stacked::register(db, &collections[0], "beside", Decorator::Transformer).await?;
    let relay = Stacked::register(db, &collections[1], "relay", Decorator::Transformer).await?;
    let subscriber =
        Stacked::register(db, &collections[2], "subscriber", Decorator::Subscriber).await?;

    let (execution, transactions) = plan(db, project, &checked.function, 3).await?;
    let (failed, _) = checked
        .run(db, &execution, &transactions[0], Failed, &[])
        .await?;
    let (_, mut relayed) = beside
        .run(db, &execution, &transactions[0], Done, &[])
        .await?;
    let mut subscribers = Vec::new();
    let mut out = None;
    for _ in 0..plans {
        let (execution, transactions) = if subscribers.is_empty() {
            (execution.clone(), transactions[1..].to_vec())
        } else {
            plan(db, project, &relay.function, 2).await?
        };
        relayed = relay
            .run(db, &execution, &transactions[0], Done, &[&relayed])
            .await?
            .1;
        let reads: Vec<&PhysicalTableDataVersionDB> = once(&relayed).chain(out.as_ref()).collect();
        let (run, written) = subscriber
            .run(db, &execution, &transactions[1], OnHold, &reads)
            .await?;
        subscribers.push(run);
        out = Some(written);
    }

    let (mut gate, _) = function_service_gate(db, project).await?;
    let pass = STATEMENTS
        .scope(RefCell::default(), async {
            release_unblocked(gate.get(grant())).await?;
            Ok::<_, TdError>(STATEMENTS.with(|statements| *statements.borrow()))
        })
        .await?;
    gate.commit().await?;
    for run in &subscribers {
        assert_eq!(status(db, run).await?, OnHold);
    }

    let (execution, transactions) = plan(db, project, &relay.function, 2).await?;
    let (next_relay, next_relayed) = relay
        .run(db, &execution, &transactions[0], Scheduled, &[&relayed])
        .await?;
    let reads: Vec<&PhysicalTableDataVersionDB> = once(&next_relayed).chain(out.as_ref()).collect();
    let (next_subscriber, _) = subscriber
        .run(db, &execution, &transactions[1], Scheduled, &reads)
        .await?;
    let runs = db
        .fetch_all::<LogicalFunctionRunDB>(&execution.id)
        .await?
        .into_iter()
        .map(|lfr| lfr.base.id)
        .collect();
    let (mut gate, _) = function_service_gate(db, project).await?;
    let plan_build = STATEMENTS
        .scope(RefCell::default(), async {
            update_initial_function_run_status(gate.get(grant()), &execution.base, runs).await?;
            Ok::<_, TdError>(STATEMENTS.with(|statements| *statements.borrow()))
        })
        .await?;
    gate.commit().await?;
    assert_eq!(status(db, &next_relay).await?, Scheduled);
    assert_eq!(status(db, &next_subscriber).await?, OnHold);
    subscribers.push(next_subscriber);

    let (mut gate, _) = function_service_gate(db, project).await?;
    gate.get(grant())
        .logical_runs()
        .set_status(from_ref(&failed.logical_function_run_id), ReScheduled)
        .await?;
    let release = STATEMENTS
        .scope(RefCell::default(), async {
            release_unblocked(gate.get(grant())).await?;
            Ok::<_, TdError>(STATEMENTS.with(|statements| *statements.borrow()))
        })
        .await?;
    gate.commit().await?;
    for run in &subscribers {
        assert_eq!(status(db, run).await?, ReScheduled);
    }

    Ok(Measured {
        pass,
        plan_build,
        release,
    })
}

/// The cost added per plan from 10 to 50 plans is at most twice the cost
/// added per plan from 1 to 10: a quadratic cost adds about six times as much.
fn assert_linear(what: &str, [one, ten, fifty]: [usize; 3]) {
    let early = ten.saturating_sub(one) as f64 / 9.0;
    let late = fifty.saturating_sub(ten) as f64 / 40.0;
    assert!(
        late <= 2.0 * early + 1.0,
        "{what} grows faster than the plans: {one}, {ten}, {fifty}"
    );
}

/// What holds a run is walked upstream only as far as needed, and what was
/// fetched is remembered: what the project's release pass, a plan build and
/// the release after the failure is rescheduled read grows at most linearly
/// with the plans stacked behind one failure, and a plan build walks no
/// lineage.
#[td_test::test(db)]
async fn test_plans_stacked_behind_a_failure_cost_linearly(db: TdPool) -> Result<(), TdError> {
    let mut measured = Vec::new();
    for plans in [1, 10, 50] {
        measured.push(stacked_behind_a_failure(&db, plans).await?);
    }
    for m in &measured {
        assert_eq!(m.plan_build.reads, 0, "{m:?}");
    }
    let of = |pick: fn(&Measured) -> usize| -> [usize; 3] {
        [pick(&measured[0]), pick(&measured[1]), pick(&measured[2])]
    };
    assert_eq!(
        measured[2].plan_build.total(),
        measured[0].plan_build.total(),
        "{measured:?}"
    );
    assert_eq!(
        measured[2].pass.total(),
        measured[0].pass.total(),
        "{measured:?}"
    );
    assert_linear("pass rows", of(|m| m.pass.rows));
    assert_linear("plan build rows", of(|m| m.plan_build.rows));
    assert_linear("release statements", of(|m| m.release.total()));
    assert_linear("release rows", of(|m| m.release.rows));
    Ok(())
}
