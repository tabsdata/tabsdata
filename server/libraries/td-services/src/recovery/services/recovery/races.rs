//
// Copyright 2026 Tabsdata Inc.
//

//! Recovery judges an attempt on a snapshot, then writes its verdict in a
//! later unit: what happened in between must win over the verdict.

use super::{apply_project, take_recovery_snapshot};
use crate::recovery::{RecoveryActions, classify_divergence};
use crate::tests::fixtures::dispatched_publisher;
use std::collections::HashSet;
use std::time::Duration;
use ta_services::ctx_service::ReqCtx;
use td_bridge::observer::WorkloadObservation;
use td_database::access::{AcrossProjects, DbPool, Grant};
use td_database::domain::DomainPools;
use td_database::pool::TdPool;
use td_error::TdError;
use td_execution::{DispatchGrace, RecoveryConfig};
use td_objects::dxo::physical_function_run::{PhysicalFunctionRunDB, PhysicalFunctionRunUpdateDB};
use td_objects::dxo::worker::WorkerDB;
use td_objects::types::basic::{AtTime, PhysicalFunctionRunStatus, ProjectId, WorkerStatus};
use td_queries::Queries;

const COLLECTION: &str = "c_race";
const FUNCTION: &str = "f_race";

fn grant() -> Grant {
    Grant::asserted("recovery race test")
}

fn pools(db: &TdPool) -> DbPool<AcrossProjects> {
    DbPool::<AcrossProjects>::new(DomainPools::of_apiserver(db.clone()), AcrossProjects)
}

fn no_grace() -> RecoveryConfig {
    RecoveryConfig {
        dispatch_grace: DispatchGrace(Duration::ZERO),
        ..RecoveryConfig::default()
    }
}

/// The pod's start report.
async fn started(db: &TdPool, attempt: &PhysicalFunctionRunDB) -> Result<AtTime, TdError> {
    let at = AtTime::now();
    let running = PhysicalFunctionRunUpdateDB {
        dispatched_on: attempt.base.dispatched_on,
        started_on: Some(at.into()),
        ended_on: None,
        status: PhysicalFunctionRunStatus::Running,
    };
    db.scd2().update(&running, &attempt.base.id).await?;
    Ok(at)
}

/// The verdicts of a tick that looked at the database at `at` and saw no
/// workload at all.
async fn judged(
    db: &TdPool,
    at: &AtTime,
    observation: &WorkloadObservation,
) -> Result<RecoveryActions, TdError> {
    let snapshot = {
        let mut conn = pools(db).read().await?;
        take_recovery_snapshot(conn.get(grant()), at).await?
    };
    let decisions = classify_divergence(&no_grace(), observation, &snapshot);
    Ok(RecoveryActions {
        fn_actions: decisions.fn_actions,
        ..RecoveryActions::default()
    })
}

async fn apply(
    db: &TdPool,
    project: &ProjectId,
    actions: &RecoveryActions,
) -> Result<RecoveryActions, TdError> {
    apply_project(
        &mut ReqCtx::default(),
        &pools(db),
        grant(),
        project,
        actions,
    )
    .await
}

/// The pod came up and reported its start after recovery judged its
/// workload missing, before the verdict was written: the run goes on.
#[td_test::test(db)]
async fn test_a_start_reported_after_the_judgement_keeps_the_attempt(
    db: TdPool,
) -> Result<(), TdError> {
    let (project, attempt) = dispatched_publisher(&db, COLLECTION, FUNCTION).await?;
    let actions = judged(&db, &AtTime::now(), &WorkloadObservation::default()).await?;
    assert_eq!(actions.fn_actions.len(), 1, "judged missing");

    started(&db, &attempt).await?;
    let committed = apply(&db, &project, &actions).await?;

    assert!(committed.fn_actions.is_empty(), "{committed:?}");
    let after: PhysicalFunctionRunDB = db.timeline().fetch_one(&attempt.base.id).await?;
    assert_eq!(after.base.status, PhysicalFunctionRunStatus::Running);
    let workers: Vec<WorkerDB> = db.fetch_all(&attempt.base.id).await?;
    assert!(
        workers
            .iter()
            .all(|worker| worker.base.status != WorkerStatus::Failed),
        "{workers:?}"
    );
    Ok(())
}

/// A tick judges on what it read when it began; the attempt it fails ends
/// when the verdict is written, never before the attempt started.
#[td_test::test(db)]
async fn test_a_failed_attempt_ends_after_it_started(db: TdPool) -> Result<(), TdError> {
    let (project, attempt) = dispatched_publisher(&db, COLLECTION, FUNCTION).await?;
    let tick = AtTime::now();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let started_on = started(&db, &attempt).await?;
    let actions = judged(&db, &tick, &WorkloadObservation::default()).await?;
    assert_eq!(actions.fn_actions.len(), 1, "judged missing while Running");

    apply(&db, &project, &actions).await?;

    let after: PhysicalFunctionRunDB = db.timeline().fetch_one(&attempt.base.id).await?;
    assert_eq!(after.base.status, PhysicalFunctionRunStatus::Failed);
    let ended_on = after.base.ended_on.expect("a failed attempt has an end");
    assert!(*ended_on >= started_on, "{ended_on:?} < {started_on:?}");
    Ok(())
}

/// A Start the backend still holds is not a missing workload, however
/// long ago the run was dispatched.
#[td_test::test(db)]
async fn test_a_run_still_being_created_is_not_missing(db: TdPool) -> Result<(), TdError> {
    let (_, attempt) = dispatched_publisher(&db, COLLECTION, FUNCTION).await?;
    let workers: Vec<WorkerDB> = db.fetch_all(&attempt.base.id).await?;
    let queued = WorkloadObservation {
        workloads: Vec::new(),
        queued: workers
            .iter()
            .map(|worker| worker.base.id.clone())
            .collect(),
    };
    let actions = judged(&db, &AtTime::now(), &queued).await?;
    assert!(actions.fn_actions.is_empty(), "{actions:?}");

    let gone = WorkloadObservation {
        workloads: Vec::new(),
        queued: HashSet::new(),
    };
    let actions = judged(&db, &AtTime::now(), &gone).await?;
    assert_eq!(
        actions.fn_actions.len(),
        1,
        "missing once the backend let go"
    );
    Ok(())
}
