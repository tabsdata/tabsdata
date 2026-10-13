//
// Copyright 2026 Tabsdata Inc.
//

//! A logical run has at most one attempt in flight, and what an attempt
//! settles reaches the runs waiting on it.

use crate::execution::update_status::tests::{
    TestExecution, TestFunction, TestTransaction, run_status_scenario, test_status_update,
};
use crate::scheduler::test_wiring::dispatch_services;
use crate::tests::fixtures::register;
use crate::tests::late_callbacks::post_callback;
use td_common::execution_status::WorkerCallbackStatus;
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::execution::ExecutionParam;
use td_objects::dxo::logical::{LogicalFunctionRunDB, LogicalFunctionRunParam};
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
use td_objects::dxo::worker::WorkerDB;
use td_objects::types::basic::{
    AccessTokenId, CollectionName, ExecutionStatus, FunctionName, LogicalFunctionRunStatus,
    PhysicalFunctionRunStatus, TableNameDto, TransactionKey, TransactionStatus, UserId,
};
use td_objects::types::composed::TableDependencyDto;
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;

/// An operator replays a Failed run, and before the replay finishes,
/// also recovers the execution. The run must end up with one attempt in
/// flight, not the replay plus a fresh attempt.
#[td_test::test(db)]
async fn test_recover_during_replay_keeps_one_attempt_in_flight(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let collection = db
        .scoped(p.id)
        .seed()
        .collection(&CollectionName::try_from("c_twice")?, &UserId::admin())
        .await?;
    let function = db
        .scoped(p.id)
        .seed()
        .function(&collection, &register("f_twice", None))
        .await?;
    let execution = db.scoped(p.id).seed().execution(&function).await?;
    let transaction = db
        .scoped(p.id)
        .seed()
        .transaction(&execution, &TransactionKey::try_from("ANY")?)
        .await?;
    let failed = db
        .scoped(p.id)
        .seed()
        .physical_function_run(
            &collection,
            &function,
            &execution,
            &transaction,
            &LogicalFunctionRunStatus::Failed,
        )
        .await?;
    let run_id = failed.base.logical_function_run_id.clone();

    let replay = RequestContext::with(AccessTokenId::default(), UserId::admin()).create(
        LogicalFunctionRunParam {
            logical_function_run: run_id.clone(),
        },
        (),
    );
    dispatch_services(&db)
        .replay_logical_function_run(replay)
        .await?;
    dispatch_services(&db).request(()).await?;

    let recover = RequestContext::with(AccessTokenId::default(), UserId::admin()).update(
        ExecutionParam {
            execution: execution.base.id.clone(),
        },
        (),
    );
    dispatch_services(&db).execution_recover(recover).await?;
    dispatch_services(&db).request(()).await?;

    let run: LogicalFunctionRunDB = db.fetch_one(&run_id).await?;
    let attempts: Vec<PhysicalFunctionRunDB> = db.timeline().fetch_all(&run_id).await?;
    let in_flight: Vec<_> = attempts
        .iter()
        .filter(|a| PhysicalFunctionRunStatus::IN_FLIGHT.contains(&a.base.status))
        .map(|a| format!("{} {:?}", a.base.id, a.base.status))
        .collect();
    assert!(
        in_flight.len() <= 1,
        "run {:?} has {} attempts in flight: {in_flight:?}",
        run.base.status,
        in_flight.len()
    );
    Ok(())
}

/// f_0 failed, so f_1 (which reads t_0) went On Hold. Replaying f_0
/// heals it; f_1 must be released, not left On Hold until someone recovers.
#[td_test::test(db)]
async fn test_replay_that_heals_a_producer_releases_its_on_hold_consumers(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let test_executions = vec![TestExecution {
        expected_status: ExecutionStatus::Running,
        transactions: vec![TestTransaction {
            expected_status: TransactionStatus::Running,
            functions: vec![
                TestFunction {
                    collection: CollectionName::try_from("c_0")?,
                    name: FunctionName::try_from("f_0")?,
                    dependencies: vec![],
                    tables: vec![TableNameDto::try_from("t_0")?],
                    initial_status: LogicalFunctionRunStatus::Failed,
                    expected_status: LogicalFunctionRunStatus::Done,
                },
                TestFunction {
                    collection: CollectionName::try_from("c_0")?,
                    name: FunctionName::try_from("f_1")?,
                    dependencies: vec![TableDependencyDto::try_from("t_0")?],
                    tables: vec![TableNameDto::try_from("t_1")?],
                    initial_status: LogicalFunctionRunStatus::OnHold,
                    expected_status: LogicalFunctionRunStatus::ReScheduled,
                },
            ],
        }],
    }];
    test_status_update(db.clone(), p.id, &test_executions, |_, _, _, f, _| {
        let db = db.clone();
        let producer = f[&test_executions[0].transactions[0].functions[0]].clone();
        let consumer = f[&test_executions[0].transactions[0].functions[1]].clone();
        async move {
            let replay = RequestContext::with(AccessTokenId::default(), UserId::admin()).create(
                LogicalFunctionRunParam {
                    logical_function_run: producer.base.logical_function_run_id.clone(),
                },
                (),
            );
            let replayed = dispatch_services(&db)
                .replay_logical_function_run(replay)
                .await?
                .data;
            dispatch_services(&db).request(()).await?;
            let workers: Vec<WorkerDB> = db.fetch_all(&replayed.new_function_run_id).await?;
            assert_eq!(workers.len(), 1);

            for status in [WorkerCallbackStatus::Running, WorkerCallbackStatus::Done] {
                post_callback(
                    &db,
                    &replayed.new_function_run_id,
                    &workers[0].base.id,
                    status,
                )
                .await?;
            }

            let consumer: LogicalFunctionRunDB =
                db.fetch_one(&consumer.base.logical_function_run_id).await?;
            assert_ne!(
                consumer.base.status,
                LogicalFunctionRunStatus::OnHold,
                "the healed producer's consumer must be released"
            );
            Ok(())
        }
    })
    .await
}

/// The backstop: f_1 is On Hold although its producer is Done (a path
/// that healed f_0 without releasing it). The next tick releases f_1.
#[td_test::test(db)]
async fn test_the_tick_releases_a_run_nothing_holds(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let test_executions = vec![TestExecution {
        expected_status: ExecutionStatus::Running,
        transactions: vec![TestTransaction {
            expected_status: TransactionStatus::Running,
            functions: vec![
                TestFunction {
                    collection: CollectionName::try_from("c_0")?,
                    name: FunctionName::try_from("f_0")?,
                    dependencies: vec![],
                    tables: vec![TableNameDto::try_from("t_0")?],
                    initial_status: LogicalFunctionRunStatus::Done,
                    expected_status: LogicalFunctionRunStatus::Done,
                },
                TestFunction {
                    collection: CollectionName::try_from("c_0")?,
                    name: FunctionName::try_from("f_1")?,
                    dependencies: vec![TableDependencyDto::try_from("t_0")?],
                    tables: vec![TableNameDto::try_from("t_1")?],
                    initial_status: LogicalFunctionRunStatus::OnHold,
                    expected_status: LogicalFunctionRunStatus::OnHold,
                },
            ],
        }],
    }];
    run_status_scenario(db.clone(), p.id, &test_executions, |_, _, _, f, _| {
        let db = db.clone();
        let consumer = f[&test_executions[0].transactions[0].functions[1]].clone();
        async move {
            dispatch_services(&db).request(()).await?;
            let consumer: LogicalFunctionRunDB =
                db.fetch_one(&consumer.base.logical_function_run_id).await?;
            assert_ne!(
                consumer.base.status,
                LogicalFunctionRunStatus::OnHold,
                "nothing holds f_1, so the tick releases it"
            );
            Ok(())
        }
    })
    .await
    .map(|_| ())
}
