//
// Copyright 2026 Tabsdata Inc.
//

//! A tick's unit that a concurrent write aborts runs again within the tick:
//! once the other write commits, the unit's rerun lands its work, instead of
//! leaving it to the next tick.

use crate::scheduler::test_wiring::{dispatch_services, sending_to};
use crate::tests::fixtures::{dispatched_publisher, register};
use std::str::FromStr;
use std::time::Duration;
use ta_services::service::Ctx;
use td_bridge::Orchestrator;
use td_bridge::test::{StaticWorkloadObserver, TestBridgeProvider};
use td_database::access::{AcrossProjects, Attempt, DbPool, GatedConnection, Grant, InProject, RW};
use td_database::connection::PgConnectionExt;
use td_database::domain::DomainPools;
use td_database::pool::TdPool;
use td_database::pool::scoped::ScopedExt as _;
use td_error::TdError;
use td_execution::{DispatchGrace, RecoveryConfig, StuckRequestedTimeout};
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::execution::ExecutionRequest;
use td_objects::dxo::function::FunctionParam;
use td_objects::dxo::logical::LogicalFunctionRunDB;
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
use td_objects::dxo::worker::WorkerDB;
use td_objects::types::basic::{
    AccessTokenId, CollectionIdName, CollectionName, FunctionIdName, FunctionOnly,
    LogicalFunctionRunStatus, OverrideDisabledTrigger, PhysicalFunctionRunStatus, ProjectId,
    SkipIfRunning, UserId,
};
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;
use tokio::task::JoinHandle;

const BLOCKED_WITHIN: Duration = Duration::from_secs(10);

fn pools(db: &TdPool) -> DbPool<AcrossProjects> {
    DbPool::<AcrossProjects>::new(DomainPools::of_apiserver(db.clone()), AcrossProjects)
}

/// Another unit's uncommitted write of `row` of `table`.
async fn hold_a_write(
    db: &TdPool,
    project: &ProjectId,
    table: &str,
    row: i64,
) -> Result<GatedConnection<RW, InProject>, TdError> {
    let mut gate = DbPool::in_project(&pools(db), project)
        .begin(Attempt::unretried())
        .await?;
    gate.get(Grant::asserted("concurrent write"))
        .execute_batch(&[format!("UPDATE {table} SET id = id WHERE id = {row}")])
        .await
        .expect("concurrent write");
    Ok(gate)
}

/// Commits the held write once the tick waits on it, then lets the tick end.
async fn release_once_waited_on(
    mut gate: GatedConnection<RW, InProject>,
    ticking: JoinHandle<Result<(), TdError>>,
) -> Result<(), TdError> {
    assert!(
        gate.get(Grant::asserted("blocking probe"))
            .wait_until_blocking(BLOCKED_WITHIN)
            .await
            .expect("blocking probe"),
        "the tick never waited on the concurrent write"
    );
    gate.commit().await?;
    ticking.await.expect("tick task")
}

#[td_test::test(db)]
async fn test_a_dispatch_aborted_by_a_concurrent_write_lands_in_the_same_tick(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let collection = db
        .scoped(p.id)
        .seed()
        .collection(&CollectionName::try_from("c_retry")?, &UserId::admin())
        .await?;
    db.scoped(p.id)
        .seed()
        .function(&collection, &register("f_retry", None))
        .await?;
    let execute = RequestContext::with(AccessTokenId::default(), UserId::admin())
        .create(
            (
                CollectionParam {
                    collection: CollectionIdName::from_str("c_retry")?,
                },
                FunctionParam {
                    function: FunctionIdName::from_str("f_retry")?,
                },
            ),
            ExecutionRequest {
                name: None,
                function_only: FunctionOnly::default(),
                override_disabled_trigger: OverrideDisabledTrigger::default(),
                skip_if_running: SkipIfRunning::default(),
                external_uid: None,
            },
        )
        .in_project(p.id);
    let execution = dispatch_services(&db).execute_function(execute).await?.data;
    let runs: Vec<LogicalFunctionRunDB> = db.fetch_all(&execution.id).await?;
    let [run] = <[_; 1]>::try_from(runs).expect("one run");

    let gate = hold_a_write(&db, &p.id, "transactions", run.base.transaction_id.as_i64()).await?;
    let tick = dispatch_services(&db);
    let ticking = tokio::spawn(async move { tick.request(()).await.map(|_| ()) });
    release_once_waited_on(gate, ticking).await?;

    let after: LogicalFunctionRunDB = db.fetch_one(&run.base.id).await?;
    assert_eq!(after.base.status, LogicalFunctionRunStatus::Requested);
    Ok(())
}

#[td_test::test(db)]
async fn test_a_recovery_verdict_aborted_by_a_concurrent_write_lands_in_the_same_tick(
    db: TdPool,
) -> Result<(), TdError> {
    let (project, attempt) = dispatched_publisher(&db, "c_retry", "f_retry").await?;
    let workers: Vec<WorkerDB> = db.fetch_all(&attempt.base.id).await?;
    let [worker] = <[_; 1]>::try_from(workers).expect("one worker");
    let mut recovery = dispatch_services(&db);
    recovery.recovery_config = Ctx::new(RecoveryConfig {
        dispatch_grace: DispatchGrace(Duration::ZERO),
        ..RecoveryConfig::default()
    });
    let (recovery, _) = sending_to(
        recovery,
        TestBridgeProvider::default()
            .with_orchestrator(Orchestrator::Kubernetes)
            .with_workload_observer(StaticWorkloadObserver::default()),
    );

    let gate = hold_a_write(&db, &project, "workers", worker.base.id.as_i64()).await?;
    let ticking = tokio::spawn(async move { recovery.recovery(()).await.map(|_| ()) });
    release_once_waited_on(gate, ticking).await?;

    let after: PhysicalFunctionRunDB = db.timeline().fetch_one(&attempt.base.id).await?;
    assert_eq!(after.base.status, PhysicalFunctionRunStatus::Failed);
    Ok(())
}

#[td_test::test(db)]
async fn test_a_sweep_aborted_by_a_concurrent_write_lands_in_the_same_tick(
    db: TdPool,
) -> Result<(), TdError> {
    let (project, attempt) = dispatched_publisher(&db, "c_retry", "f_retry").await?;
    let workers: Vec<WorkerDB> = db.fetch_all(&attempt.base.id).await?;
    let [worker] = <[_; 1]>::try_from(workers).expect("one worker");
    let mut sweep = dispatch_services(&db);
    sweep.recovery_config = Ctx::new(RecoveryConfig {
        stuck_requested_timeout: StuckRequestedTimeout(Duration::ZERO),
        ..RecoveryConfig::default()
    });
    let (sweep, _) = sending_to(
        sweep,
        TestBridgeProvider::default().with_orchestrator(Orchestrator::Kubernetes),
    );

    let gate = hold_a_write(&db, &project, "workers", worker.base.id.as_i64()).await?;
    let ticking = tokio::spawn(async move { sweep.recovery(()).await.map(|_| ()) });
    release_once_waited_on(gate, ticking).await?;

    let after: PhysicalFunctionRunDB = db.timeline().fetch_one(&attempt.base.id).await?;
    assert_eq!(after.base.status, PhysicalFunctionRunStatus::Failed);
    Ok(())
}
