//
// Copyright 2026 Tabsdata Inc.
//

use crate::execution::cancellation::function_stops;
use crate::execution::update_status::{
    abandon_requested_physical_runs, move_workers, update_function_run_status, update_worker_status,
};
use crate::recovery::attempt_cap::count_recoveries;
use crate::recovery::{
    RecoveryActions, fail_lfrs_from_actions, fail_recovered_attempts,
    fail_running_lfrs_for_recover, failed_worker_ids, finalize_recovery_actions,
    recover_lfrs_from_actions,
};
use crate::worker::retry::after_failure;
use itertools::Itertools;
use ta_services::ctx_service::ReqCtx;
use td_common::execution_status::WorkerCallbackStatus;
use td_database::access::{InProject, LockInstant, WriteMode};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::worker::{WorkerDB, WorkerDBWithNames, WorkerExecutionUpdate, WorkerUpdateDB};
use td_objects::types::basic::LogicalFunctionRunStatus;
use td_objects::types::basic::{AtTime, WorkerId};
use td_queries::Queries;
use tracing::instrument;

/// One short write transaction for the surviving function actions: finalize
/// re-reads run state (a concurrently-terminalized attempt drops out — the
/// callback wins), then worker rows → Failed, attempts → Failed, and the LFRs
/// recover or fail. Returns the actions that survived, whose workloads the
/// caller stops once it commits.
#[instrument(
    name = "recovery.apply",
    skip_all,
    level = "debug",
    err(Debug, level = "debug")
)]
pub async fn apply_recovery_actions(
    msgs: &mut ReqCtx,
    conn: &mut DomainConnection<Apiserver, InProject, impl WriteMode>,
    at: LockInstant,
    actions: &RecoveryActions,
) -> Result<RecoveryActions, TdError> {
    let actions = finalize_recovery_actions(conn, actions).await?;

    // Worker rows → Failed (informational record).
    let worker_ids = failed_worker_ids(&actions)?;
    let workers: Vec<WorkerDB> = conn.chunked().fetch_all(&worker_ids, ()).await?;
    move_workers(conn, &workers, &WorkerUpdateDB::failed_at(at)).await?;
    let unobserved: Vec<WorkerId> = actions
        .fn_actions
        .iter()
        .filter(|action| action.unobserved)
        .filter_map(|action| action.worker_id.clone())
        .unique()
        .collect();
    let unobserved: Vec<WorkerDBWithNames> = conn.chunked().fetch_all(&unobserved, ()).await?;
    let mut worker_stops = function_stops(conn, unobserved).await?;

    // Attempts → Failed; late callbacks drop at the in-flight filter.
    fail_recovered_attempts(conn, at, &actions).await?;

    // Running LFRs with a recover verdict → Failed first; ReScheduled is only
    // reachable from Failed/OnHold/Requested.
    let running = fail_running_lfrs_for_recover(&actions)?;
    update_function_run_status(msgs, conn, &running, LogicalFunctionRunStatus::Failed).await?;

    // Requested and just-failed LFRs with a recover verdict → ReScheduled.
    let to_recover = recover_lfrs_from_actions(&actions)?;
    count_recoveries(conn, &to_recover).await?;
    worker_stops.extend(abandon_requested_physical_runs(conn, at, &to_recover).await?);
    update_function_run_status(
        msgs,
        conn,
        &to_recover,
        LogicalFunctionRunStatus::ReScheduled,
    )
    .await?;

    // The rest → Failed, with the downstream on-hold cascade.
    let to_fail = fail_lfrs_from_actions(&actions)?;
    update_function_run_status(msgs, conn, &to_fail, LogicalFunctionRunStatus::Failed).await?;

    fail_exited_streams(conn, at, &actions.stage_exits).await?;
    let cron_recreates = still_as_read(conn, &actions.cron_recreates).await?;
    let stage_recreates = still_as_read(conn, &actions.stage_recreates).await?;
    Ok(RecoveryActions {
        cron_recreates,
        stage_recreates,
        worker_stops,
        ..actions
    })
}

/// The trigger workers to recreate that nothing moved since recovery read
/// them, locked until the commit, since recovery recreates them after it: a
/// rotation that stopped one since this unit's snapshot fails the unit, one
/// that comes later waits for it, and recovery never starts a worker a newer
/// one displaced.
async fn still_as_read(
    conn: &mut DomainConnection<Apiserver, InProject, impl WriteMode>,
    read: &[WorkerDBWithNames],
) -> Result<Vec<WorkerDBWithNames>, TdError> {
    let mut kept = Vec::with_capacity(read.len());
    for worker in read {
        let current: Option<WorkerDB> = conn
            .for_update()
            .fetch_optional(&worker.base.base.id)
            .await?;
        if current.is_some_and(|current| {
            current.base.status == worker.base.base.status && *current.base.worker_enabled
        }) {
            kept.push(worker.clone());
        }
    }
    Ok(kept)
}

/// A streamer whose workload ended without reporting failed: it is marked so,
/// with its next restart due by the same backoff as a reported failure. A
/// worker a callback or a stop moved meanwhile is left as it is.
async fn fail_exited_streams(
    conn: &mut DomainConnection<Apiserver, InProject, impl WriteMode>,
    at: LockInstant,
    exited: &[WorkerDBWithNames],
) -> Result<(), TdError> {
    let failed = WorkerUpdateDB::failed_at(at);
    let exit = WorkerExecutionUpdate {
        started_on: None,
        ended_on: Some(AtTime::from(at).into()),
        status: WorkerCallbackStatus::Failed,
    };
    for worker in exited {
        let id = &worker.base.base.id;
        let current: WorkerDB = conn.fetch_one(id).await?;
        if current.base.status != worker.base.base.status
            || !update_worker_status(conn, &failed, id).await?
        {
            continue;
        }
        let retry = after_failure(&current.failures, &exit, &AtTime::from(at));
        conn.update(&retry, id).await?;
    }
    Ok(())
}
