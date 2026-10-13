//
// Copyright 2026 Tabsdata Inc.
//

//! Units write a run's worker, then its attempt, then the run itself, so two
//! units on one run never wait on each other's rows in a cycle. Each test
//! holds the row a unit writes first and checks, once the unit waits on it,
//! that it has not locked a row it should write later.

use crate::Services;
use crate::execution::update_status::tests::{
    TestExecution, TestFunction, TestTransaction, run_status_scenario,
};
use crate::tests::late_callbacks::post_callback;
use std::time::Duration;
use td_common::execution_status::WorkerCallbackStatus;
use td_database::access::Attempt;
use td_database::access::{AcrossProjects, DbPool, GatedConnection, Grant, RW};
use td_database::connection::PgConnectionExt;
use td_database::domain::DomainPools;
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::execution::ExecutionParam;
use td_objects::dxo::logical::LogicalFunctionRunDB;
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
use td_objects::types::basic::{
    AccessTokenId, CollectionName, ExecutionStatus, FunctionName, LogicalFunctionRunStatus,
    PhysicalFunctionRunStatus, TableNameDto, TransactionStatus, UserId,
};
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;
use td_queries::test_utils::probes::lock_row_nowait;

const BLOCKED_WITHIN: Duration = Duration::from_secs(10);

async fn gate(db: &TdPool) -> Result<GatedConnection<RW, AcrossProjects>, TdError> {
    DbPool::<AcrossProjects>::new(DomainPools::of_apiserver(db.clone()), AcrossProjects)
        .begin(Attempt::unretried())
        .await
}

fn running() -> Result<Vec<TestExecution>, TdError> {
    Ok(vec![TestExecution {
        expected_status: ExecutionStatus::Running,
        transactions: vec![TestTransaction {
            expected_status: TransactionStatus::Running,
            functions: vec![TestFunction {
                collection: CollectionName::try_from("c_0")?,
                name: FunctionName::try_from("f_0")?,
                dependencies: vec![],
                tables: vec![TableNameDto::try_from("t_0")?],
                initial_status: LogicalFunctionRunStatus::Running,
                expected_status: LogicalFunctionRunStatus::Running,
            }],
        }],
    }])
}

#[td_test::test(db)]
async fn test_a_cancel_writes_the_worker_before_the_attempt(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let test_executions = running()?;
    run_status_scenario(db.clone(), p.id, &test_executions, |_, e, _, f, w| {
        let db = db.clone();
        let function = &test_executions[0].transactions[0].functions[0];
        let execution = e[&test_executions[0]].base.id.clone();
        let attempt = f[function].base.id.clone();
        let worker = w[function].base.id.clone();
        async move {
            let mut gate = gate(&db).await?;
            lock_row_nowait(gate.get(Grant::asserted("worker held")), "workers", &worker)
                .await
                .expect("lock worker");

            let services = Services::with_defaults(db.clone());
            let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
                .update(ExecutionParam { execution }, ());
            let cancelling =
                tokio::spawn(async move { services.execution_cancel(request).await.map(|_| ()) });
            assert!(
                gate.get(Grant::asserted("blocking probe"))
                    .wait_until_blocking(BLOCKED_WITHIN)
                    .await
                    .expect("blocking probe"),
                "the cancel never waited on the worker"
            );
            let probe = lock_row_nowait(
                gate.get(Grant::asserted("attempt probe")),
                "physical_function_runs",
                &attempt,
            )
            .await;
            drop(gate);
            cancelling.await.expect("cancel task")?;

            assert!(
                probe.is_ok(),
                "the cancel locked the attempt before the worker: {probe:?}"
            );
            let ended: PhysicalFunctionRunDB = db.timeline().fetch_one(&attempt).await?;
            assert_eq!(ended.base.status, PhysicalFunctionRunStatus::Failed);
            Ok(())
        }
    })
    .await
    .map(|_| ())
}

#[td_test::test(db)]
async fn test_a_callback_writes_the_attempt_before_the_run(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let test_executions = running()?;
    run_status_scenario(db.clone(), p.id, &test_executions, |_, _, _, f, w| {
        let db = db.clone();
        let function = &test_executions[0].transactions[0].functions[0];
        let attempt = f[function].base.id.clone();
        let run = f[function].base.logical_function_run_id.clone();
        let worker = w[function].base.id.clone();
        async move {
            let mut gate = gate(&db).await?;
            lock_row_nowait(
                gate.get(Grant::asserted("attempt held")),
                "physical_function_runs",
                &attempt,
            )
            .await
            .expect("lock attempt");

            let reporting = tokio::spawn({
                let db = db.clone();
                let attempt = attempt.clone();
                async move {
                    post_callback(&db, &attempt, &worker, WorkerCallbackStatus::Done).await
                }
            });
            assert!(
                gate.get(Grant::asserted("blocking probe"))
                    .wait_until_blocking(BLOCKED_WITHIN)
                    .await
                    .expect("blocking probe"),
                "the callback never waited on the attempt"
            );
            let probe = lock_row_nowait(
                gate.get(Grant::asserted("run probe")),
                "logical_function_runs",
                &run,
            )
            .await;
            drop(gate);
            reporting.await.expect("callback task")?;

            assert!(
                probe.is_ok(),
                "the callback locked the run before the attempt: {probe:?}"
            );
            let reported: LogicalFunctionRunDB = db.fetch_one(&run).await?;
            assert_eq!(reported.base.status, LogicalFunctionRunStatus::Committed);
            Ok(())
        }
    })
    .await
    .map(|_| ())
}
