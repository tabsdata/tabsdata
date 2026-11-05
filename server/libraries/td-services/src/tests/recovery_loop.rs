//
// Copyright 2026 Tabsdata Inc.
//

//! The Kubernetes recovery tick against a fixed cluster observation: what it
//! does with attempts whose workloads ran long, vanished or went quiet.

use crate::Services;
use crate::scheduler::test_wiring::{dispatch_services, sending_to};
use crate::tests::fixtures::register;
use chrono::Utc;
use std::collections::HashSet;
use std::str::FromStr;
use std::time::Duration;
use ta_services::service::Ctx;
use td_bridge::Orchestrator;
use td_bridge::messaging::memory::InMemoryOrchestratorMessageQueue;
use td_bridge::messaging::{
    CronRequest, FunctionRequest, OrchestratorMessagePayload, StageTriggerRequest,
};
use td_bridge::observer::{ObservedWorkload, WorkloadKind, WorkloadObservation, WorkloadPhase};
use td_bridge::test::{StaticWorkloadObserver, TestBridgeProvider};
use td_common::execution_status::WorkerCallbackStatus;
use td_common::status::ExitStatus;
use td_common::worker::Decorator as WorkloadDecorator;
use td_database::access::Attempt as DbAttempt;
use td_database::access::{AcrossProjects, DbPool, Grant};
use td_database::domain::DomainPools;
use td_database::pool::TdPool;
use td_database::pool::scoped::ScopedExt;
use td_error::TdError;
use td_execution::{DispatchGrace, MaxRunningTime, RecoveryConfig};
use td_objects::and;
use td_objects::dxo::callback::CallbackRequest;
use td_objects::dxo::callback::ExecutionCallbackRequest;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::execution::{ExecutionParam, ExecutionRequest, ExecutionResponse};
use td_objects::dxo::function::{FunctionDB, FunctionParam, FunctionRegister};
use td_objects::dxo::logical::{LogicalFunctionRunDB, LogicalFunctionRunStatusUpdateDB};
use td_objects::dxo::physical_function_run::{
    FunctionRunParam, PhysicalFunctionRunDB, PhysicalFunctionRunUpdateDB,
};
use td_objects::dxo::worker::{WorkerDB, WorkerParam, WorkerUpdateDB};
use td_objects::types::basic::{
    AccessTokenId, AtTime, CollectionIdName, CollectionName, Decorator, FunctionIdName,
    FunctionName, LogicalFunctionRunStatus, PhysicalFunctionRunStatus, ProjectId, UserId,
    WorkerStatus, WorkerType,
};
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;
use td_queries::test_utils::probes::wait_until_queued_on_lock;

const COLLECTION: &str = "c_recovery";
const FUNCTION: &str = "f_recovery";

/// One dispatched attempt and the worker its pod runs as.
struct Attempt {
    execution: ExecutionResponse,
    pfr: PhysicalFunctionRunDB,
    worker: WorkerDB,
}

fn ago(duration: chrono::Duration) -> AtTime {
    AtTime::from(Utc::now() - duration)
}

fn observing(
    db: &TdPool,
    config: RecoveryConfig,
    workloads: Vec<ObservedWorkload>,
) -> (Services, InMemoryOrchestratorMessageQueue) {
    let mut services = dispatch_services(db);
    services.recovery_config = Ctx::new(config);
    sending_to(
        services,
        TestBridgeProvider::default()
            .with_orchestrator(Orchestrator::Kubernetes)
            .with_workload_observer(StaticWorkloadObserver(WorkloadObservation {
                workloads,
                queued: HashSet::new(),
            })),
    )
}

/// Registers the function, executes it and runs the request tick, which
/// dispatches its only attempt.
async fn dispatch(db: &TdPool, decorator: Decorator) -> Result<(ProjectId, Attempt), TdError> {
    let p = db.seed().test_project().await?;
    let collection = db
        .scoped(p.project_id)
        .seed()
        .collection(&CollectionName::try_from(COLLECTION)?, &UserId::admin())
        .await?;
    db.scoped(p.project_id)
        .seed()
        .function(
            &collection,
            &FunctionRegister {
                decorator,
                ..register(FUNCTION, None)
            },
        )
        .await?;
    let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
        .create(
            (
                CollectionParam {
                    collection: CollectionIdName::from_str(COLLECTION)?,
                },
                FunctionParam {
                    function: FunctionIdName::from_str(FUNCTION)?,
                },
            ),
            ExecutionRequest {
                name: None,
                function_only: Default::default(),
                override_disabled_trigger: Default::default(),
                skip_if_running: Default::default(),
                external_uid: None,
            },
        )
        .in_project(p.project_id);
    let execution = dispatch_services(db).execute_function(request).await?.data;
    dispatch_services(db).tick().await?;
    let attempt = current_attempt(db, &execution).await?;
    Ok((p.project_id, attempt))
}

async fn attempts(
    db: &TdPool,
    execution: &ExecutionResponse,
) -> Result<Vec<PhysicalFunctionRunDB>, TdError> {
    Ok(db
        .timeline()
        .fetch_all::<PhysicalFunctionRunDB>(&(&execution.manual_trigger))
        .await?)
}

async fn current_attempt(db: &TdPool, execution: &ExecutionResponse) -> Result<Attempt, TdError> {
    let mut pfrs = attempts(db, execution).await?;
    pfrs.sort_by_key(|pfr| pfr.interval.valid_from);
    let pfr = pfrs.pop().expect("an attempt");
    let workers: Vec<WorkerDB> = db.fetch_all::<WorkerDB>(()).await?;
    let worker = workers
        .into_iter()
        .find(|w| {
            w.worker_type == WorkerType::FunctionRunner
                && w.physical_function_run_id.as_ref() == Some(&pfr.id)
        })
        .expect("the attempt's worker");
    Ok(Attempt {
        execution: execution.clone(),
        pfr,
        worker,
    })
}

async fn lfr(db: &TdPool, pfr: &PhysicalFunctionRunDB) -> Result<LogicalFunctionRunDB, TdError> {
    Ok(db
        .fetch_one::<LogicalFunctionRunDB>(and!(&pfr.execution_id, &pfr.function_id))
        .await?)
}

/// The pod's begin-execution report, dated: the attempt and its run go
/// Running.
async fn started(
    db: &TdPool,
    pfr: &PhysicalFunctionRunDB,
    dispatched_on: AtTime,
    started_on: AtTime,
) -> Result<(), TdError> {
    let update = PhysicalFunctionRunUpdateDB {
        dispatched_on: Some(dispatched_on.into()),
        started_on: Some(started_on.into()),
        ended_on: None,
        status: PhysicalFunctionRunStatus::Running,
    };
    db.scd2().update(&update, &pfr.id).await?;
    let run = lfr(db, pfr).await?;
    db.update(
        &LogicalFunctionRunStatusUpdateDB {
            status: LogicalFunctionRunStatus::Running,
        },
        &run.id,
    )
    .await?;
    Ok(())
}

fn workload(
    attempt: &Attempt,
    decorator: WorkloadDecorator,
    phase: WorkloadPhase,
    since: AtTime,
) -> ObservedWorkload {
    ObservedWorkload {
        worker_id: Some(attempt.worker.id.clone()),
        function_id: Some(attempt.pfr.function_id),
        function: FunctionName::try_from(FUNCTION).ok(),
        kind: WorkloadKind::Function,
        decorator: Some(decorator),
        name: format!("fn-{}", attempt.worker.id),
        phase,
        phase_since: Some(since),
        finished_at: None,
        message: None,
        deleting: false,
    }
}

/// A subscriber's pod does its work, the destination write included,
/// in init; a pod observed Running past the completion allowance is
/// reporting completion, so its write has landed. Any attempt recovery
/// dispatches for it belongs to the same transaction, the key a destination
/// dedupes on.
#[td_test::test(db)]
async fn test_a_redispatched_subscriber_keeps_its_transaction(db: TdPool) -> Result<(), TdError> {
    let (_, attempt) = dispatch(&db, Decorator::Subscriber).await?;
    let hour = chrono::Duration::hours(1);
    started(&db, &attempt.pfr, ago(hour), ago(hour)).await?;
    let finishing = workload(
        &attempt,
        WorkloadDecorator::Subscriber,
        WorkloadPhase::Running,
        ago(hour),
    );

    observing(&db, RecoveryConfig::default(), vec![finishing])
        .0
        .recovery(())
        .await?;
    dispatch_services(&db).tick().await?;

    for pfr in attempts(&db, &attempt.execution).await? {
        assert_eq!(pfr.transaction_id, attempt.pfr.transaction_id);
    }
    Ok(())
}

/// An enabled trigger of its own function, in its own collection, with its
/// worker at `status`.
async fn trigger_worker(
    db: &TdPool,
    project: ProjectId,
    name: &str,
    worker_type: WorkerType,
    status: WorkerStatus,
) -> Result<(FunctionDB, WorkerDB), TdError> {
    let collection = db
        .scoped(project)
        .seed()
        .collection(&CollectionName::try_from(name)?, &UserId::admin())
        .await?;
    let function = db
        .scoped(project)
        .seed()
        .function(&collection, &register(name, None))
        .await?;
    let worker = db
        .scoped(project)
        .seed()
        .worker(&collection, &function, &worker_type, &status)
        .await?;
    Ok((function, worker))
}

fn queued(
    sent: &InMemoryOrchestratorMessageQueue,
) -> Vec<OrchestratorMessagePayload<serde_yaml::Value>> {
    sent.messages()
        .into_iter()
        .map(|message| message.payload)
        .collect()
}

/// The stage sidecar's listing throws once (cloud throttling, a
/// credential refresh) and posts Failed to the stage worker of an enabled
/// function; the orphan pass has already stopped its streamer. Recovery must
/// eventually start a streamer for that trigger again, without a user
/// re-enabling or updating the function. Backoff is allowed: the failure is
/// an hour old here.
#[td_test::test(db)]
async fn test_a_transiently_failed_streamer_is_started_again(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let (_, stage) = trigger_worker(
        &db,
        p.project_id,
        "c_stage",
        WorkerType::StageTrigger,
        WorkerStatus::Running,
    )
    .await?;
    let hour = chrono::Duration::hours(1);
    let failed: CallbackRequest = CallbackRequest {
        start: Some(ago(hour * 2)),
        end: Some(ago(hour)),
        status: WorkerCallbackStatus::Failed,
        error_message: Some("listing the stage failed: throttled".to_string()),
        exit_status: ExitStatus::GeneralError.code(),
        output: None,
    };
    let callback = RequestContext::with(AccessTokenId::default(), UserId::admin()).update(
        WorkerParam {
            worker: stage.id.clone(),
        },
        failed,
    );
    dispatch_services(&db).worker_callback(callback).await?;

    let (services, sent) = observing(&db, RecoveryConfig::default(), vec![]);
    for _ in 0..3 {
        services.clone().recovery(()).await?;
        services.clone().tick().await?;
    }

    let started = queued(&sent).into_iter().any(|payload| {
        matches!(
            payload,
            OrchestratorMessagePayload::OrchestratorStageTriggerRequestMessagePayload(
                StageTriggerRequest::Start(_)
            )
        )
    });
    assert!(
        started,
        "the stage trigger stays stopped after a transient error"
    );
    Ok(())
}

/// A streamer's pod exits without reporting (an OOM kill, a crash
/// before its responder ran). Recovery fails the stage worker and spaces its
/// restart by the same backoff as a reported failure, instead of restarting it
/// at once on every tick.
#[td_test::test(db)]
async fn test_a_streamer_that_exits_unreported_restarts_with_backoff(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let (_, stage) = trigger_worker(
        &db,
        p.project_id,
        "c_stage_exit",
        WorkerType::StageTrigger,
        WorkerStatus::Running,
    )
    .await?;
    let exited = ObservedWorkload {
        worker_id: Some(stage.id.clone()),
        function_id: None,
        function: None,
        kind: WorkloadKind::StageTrigger,
        decorator: None,
        name: format!("stm-{}", stage.id),
        phase: WorkloadPhase::Failed,
        phase_since: Some(ago(chrono::Duration::minutes(5))),
        finished_at: None,
        message: None,
        deleting: false,
    };

    let (services, sent) = observing(&db, RecoveryConfig::default(), vec![exited]);
    services.clone().recovery(()).await?;

    let after: WorkerDB = db.fetch_one(&stage.id).await?;
    assert_eq!(after.status, WorkerStatus::Failed);
    assert_eq!(*after.failures, 1);
    assert!(
        after.retry_at.is_some_and(|at| *at > AtTime::now()),
        "the restart is spaced by the backoff: {:?}",
        after.retry_at
    );
    assert!(
        queued(&sent).is_empty(),
        "nothing restarts before the retry is due"
    );
    Ok(())
}

fn stop_sent(sent: &InMemoryOrchestratorMessageQueue, worker: &WorkerDB) -> bool {
    sent.messages().into_iter().any(|message| {
        message.id == worker.id.to_string()
            && matches!(
                message.payload,
                OrchestratorMessagePayload::OrchestratorFunctionRequestMessagePayload(
                    FunctionRequest::Stop(_)
                )
            )
    })
}

async fn assert_recovery_waits_on(db: &TdPool, project: &ProjectId) {
    assert!(
        wait_until_queued_on_lock(db, project, Duration::from_secs(10)).await,
        "the recovery tick never waited on the gate's write"
    );
}

/// Recovery decides to stop a run that exceeded its running time
/// and not to re-run it, but the tick ends (cancelled by its timeout) while
/// waiting on a row it must write to record that. On the next tick the run
/// must still not be re-run: either no stop was sent for the unrecorded
/// verdict, so the workload is still there, or the verdict survived.
#[td_test::test(db)]
async fn test_a_no_recover_verdict_survives_an_aborted_tick(db: TdPool) -> Result<(), TdError> {
    let (p, attempt) = dispatch(&db, Decorator::Publisher).await?;
    let hour = chrono::Duration::hours(1);
    started(&db, &attempt.pfr, ago(hour), ago(hour)).await?;
    let working = workload(
        &attempt,
        WorkloadDecorator::Publisher,
        WorkloadPhase::Pending,
        ago(hour),
    );
    let config = RecoveryConfig {
        max_running_time: MaxRunningTime(Some(Duration::from_secs(60))),
        ..RecoveryConfig::default()
    };

    let pools =
        DbPool::<AcrossProjects>::new(DomainPools::of_apiserver(db.clone()), AcrossProjects);
    let mut gate = DbPool::in_project(&pools, &p)
        .lock_project()
        .begin(DbAttempt::unretried())
        .await?;
    let _: WorkerDB = gate
        .get(Grant::asserted("the run's worker held open"))
        .for_update()
        .fetch_one(&attempt.worker.id)
        .await?;
    let (first, first_sent) = observing(&db, config, vec![working.clone()]);
    let ticking = first.clone();
    let tick = tokio::spawn(async move { ticking.recovery(()).await });
    assert_recovery_waits_on(&db, &p).await;
    tick.abort();
    let _ = tick.await;
    gate.commit().await?;

    let still_there = if stop_sent(&first_sent, &attempt.worker) {
        vec![]
    } else {
        vec![working]
    };
    observing(&db, config, still_there).0.recovery(()).await?;

    let run = lfr(&db, &attempt.pfr).await?;
    assert!(
        !matches!(
            run.status,
            LogicalFunctionRunStatus::ReScheduled | LogicalFunctionRunStatus::Requested
        ),
        "a run recovery decided not to re-run was re-run: {:?}",
        run.status
    );
    Ok(())
}

/// An attempt whose workload keeps disappearing (an eviction loop,
/// a node that never schedules it) is judged missing and re-dispatched on
/// every recovery tick. Re-dispatch must be bounded: within `CYCLES`
/// recover-and-dispatch rounds the run stops getting new attempts and ends.
/// `CYCLES` is well above the bound.
#[td_test::test(db)]
async fn test_a_run_whose_workload_keeps_vanishing_is_not_redispatched_forever(
    db: TdPool,
) -> Result<(), TdError> {
    const CYCLES: usize = 20;
    let (_, attempt) = dispatch(&db, Decorator::Publisher).await?;
    let (services, _) = observing(
        &db,
        RecoveryConfig {
            dispatch_grace: DispatchGrace(Duration::ZERO),
            ..RecoveryConfig::default()
        },
        vec![],
    );
    for _ in 0..CYCLES {
        services.clone().recovery(()).await?;
        services.clone().tick().await?;
    }

    let dispatched = attempts(&db, &attempt.execution).await?.len();
    let run = lfr(&db, &attempt.pfr).await?;
    assert!(
        dispatched < CYCLES
            && !matches!(
                run.status,
                LogicalFunctionRunStatus::Requested | LogicalFunctionRunStatus::ReScheduled
            ),
        "{dispatched} attempts after {CYCLES} rounds, run {:?}",
        run.status
    );
    Ok(())
}

/// Once recovery gives up on a run, a user recover gives it a fresh
/// budget of automatic re-dispatches.
#[td_test::test(db)]
async fn test_a_user_recover_starts_the_recovery_count_over(db: TdPool) -> Result<(), TdError> {
    let (_, attempt) = dispatch(&db, Decorator::Publisher).await?;
    let (services, _) = observing(
        &db,
        RecoveryConfig {
            dispatch_grace: DispatchGrace(Duration::ZERO),
            ..RecoveryConfig::default()
        },
        vec![],
    );
    for _ in 0..5 {
        services.clone().recovery(()).await?;
        services.clone().tick().await?;
    }
    let gave_up = lfr(&db, &attempt.pfr).await?;
    assert_eq!(gave_up.status, LogicalFunctionRunStatus::Failed);
    assert_eq!(*gave_up.recoveries, 3);

    let recover = RequestContext::with(AccessTokenId::default(), UserId::admin()).update(
        ExecutionParam {
            execution: attempt.execution.id.clone(),
        },
        (),
    );
    services.clone().execution_recover(recover).await?;

    let recovered = lfr(&db, &attempt.pfr).await?;
    assert_eq!(recovered.status, LogicalFunctionRunStatus::ReScheduled);
    assert_eq!(*recovered.recoveries, 0);
    Ok(())
}

/// A CronJob is created suspended and released once its companions
/// are attached; a crash in between leaves it suspended, so the enabled
/// trigger never fires. Recovery must repair it (stop it so it is
/// recreated, or recreate it) once it has stayed suspended past the grace.
#[td_test::test(db)]
async fn test_a_cron_job_left_suspended_is_repaired(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let (function, cron) = trigger_worker(
        &db,
        p.project_id,
        "c_cron",
        WorkerType::CronTrigger,
        WorkerStatus::Running,
    )
    .await?;
    let suspended = ObservedWorkload {
        worker_id: Some(cron.id.clone()),
        function_id: Some(function.function_id),
        function: Some(function.name.clone()),
        kind: WorkloadKind::CronTrigger,
        decorator: None,
        name: format!("crn-{}", cron.id),
        phase: WorkloadPhase::Suspended,
        phase_since: Some(ago(chrono::Duration::hours(1))),
        finished_at: None,
        message: None,
        deleting: false,
    };

    let (services, sent) = observing(&db, RecoveryConfig::default(), vec![suspended]);
    services.clone().recovery(()).await?;

    let repaired = sent
        .messages()
        .iter()
        .any(|message| message.id == cron.id.to_string());
    assert!(repaired, "a suspended CronJob was left suspended");
    Ok(())
}

/// Recovery reads a cron trigger's worker as in flight with no
/// CronJob and decides to recreate it; before it puts that start, a
/// rotation (function update or disable) stops the worker and commits.
/// Recovery must not then start the stopped worker, whose start would
/// displace the newer version's in the reconciler.
#[td_test::test(db)]
async fn test_recovery_does_not_start_a_trigger_stopped_since_its_snapshot(
    db: TdPool,
) -> Result<(), TdError> {
    let (p, _) = dispatch(&db, Decorator::Publisher).await?;
    let (_, cron) = trigger_worker(
        &db,
        p,
        "c_cron",
        WorkerType::CronTrigger,
        WorkerStatus::Requested,
    )
    .await?;
    let (services, sent) = observing(
        &db,
        RecoveryConfig {
            dispatch_grace: DispatchGrace(Duration::ZERO),
            ..RecoveryConfig::default()
        },
        vec![],
    );

    let pools =
        DbPool::<AcrossProjects>::new(DomainPools::of_apiserver(db.clone()), AcrossProjects);
    let mut gate = DbPool::in_project(&pools, &p)
        .lock_project()
        .begin(DbAttempt::unretried())
        .await?;
    let stopped = WorkerUpdateDB::stop_scheduled();
    gate.get(Grant::asserted("rotation held open"))
        .update(&stopped, &cron.id)
        .await?;
    let ticking = services.clone();
    let tick = tokio::spawn(async move { ticking.recovery(()).await });
    assert_recovery_waits_on(&db, &p).await;
    gate.commit().await?;
    tick.await.expect("recovery task")?;

    let restarted = sent.messages().into_iter().any(|message| {
        message.id == cron.id.to_string()
            && matches!(
                message.payload,
                OrchestratorMessagePayload::OrchestratorCronRequestMessagePayload(
                    CronRequest::Start(_)
                )
            )
    });
    assert!(
        !restarted,
        "recovery started a trigger stopped after its snapshot"
    );
    Ok(())
}

async fn report_running(db: &TdPool, attempt: &Attempt) -> Result<(), TdError> {
    let running: CallbackRequest = CallbackRequest {
        start: Some(ago(chrono::Duration::minutes(10))),
        end: None,
        status: WorkerCallbackStatus::Running,
        error_message: None,
        exit_status: ExitStatus::Success.code(),
        output: None,
    };
    call_back(db, attempt, ExecutionCallbackRequest::report(running)).await
}

async fn call_back(
    db: &TdPool,
    attempt: &Attempt,
    callback: ExecutionCallbackRequest,
) -> Result<(), TdError> {
    let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).update(
        (
            FunctionRunParam {
                function_run: attempt.pfr.id.clone(),
            },
            WorkerParam {
                worker: attempt.worker.id.clone(),
            },
        ),
        callback,
    );
    dispatch_services(db).execution_callback(request).await?;
    Ok(())
}

async fn lose_workload(db: &TdPool) -> Result<(), TdError> {
    observing(
        db,
        RecoveryConfig {
            dispatch_grace: DispatchGrace(Duration::ZERO),
            ..RecoveryConfig::default()
        },
        vec![],
    )
    .0
    .recovery(())
    .await?;
    Ok(())
}

/// A subscriber reported that it was about to write to a destination
/// a re-run would write to again, and then its pod was lost. Recovery leaves
/// it to the user instead of re-running it.
#[td_test::test(db)]
async fn test_a_run_lost_after_it_began_its_destination_write_is_not_re_run(
    db: TdPool,
) -> Result<(), TdError> {
    let (_, attempt) = dispatch(&db, Decorator::Subscriber).await?;
    report_running(&db, &attempt).await?;
    call_back(&db, &attempt, ExecutionCallbackRequest::destination_write()).await?;

    lose_workload(&db).await?;

    let run = lfr(&db, &attempt.pfr).await?;
    assert_eq!(run.status, LogicalFunctionRunStatus::Failed);
    Ok(())
}

/// A subscriber lost before it began its write is re-run as before.
#[td_test::test(db)]
async fn test_a_run_lost_before_its_write_is_re_run(db: TdPool) -> Result<(), TdError> {
    let (_, attempt) = dispatch(&db, Decorator::Subscriber).await?;
    report_running(&db, &attempt).await?;

    lose_workload(&db).await?;

    let run = lfr(&db, &attempt.pfr).await?;
    assert_eq!(run.status, LogicalFunctionRunStatus::ReScheduled);
    Ok(())
}
