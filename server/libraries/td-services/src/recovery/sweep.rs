//
// Copyright 2026 Tabsdata Inc.
//

use crate::execution::cancellation::function_stops;
use crate::execution::update_status::{
    abandon_requested_physical_runs, update_function_run_status,
};
use crate::recovery::attempt_cap::{count_recoveries, gave_up, under_cap};
use itertools::Itertools;
use std::collections::{HashMap, HashSet};
use ta_services::ctx_service::ReqCtx;
use td_bridge::messaging::FunctionStopRequest;
use td_database::access::{
    AccessMode, Holds, InProject, LockInstant, ProjectLock, SeesProjectRows,
};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_execution::RecoveryConfig;
use td_objects::and;
use td_objects::dxo::logical::LogicalFunctionRunDB;
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
use td_objects::dxo::worker::{WorkerDBWithNames, WorkerUpdateDB};
use td_objects::types::basic::{
    AtTime, ExecutionId, FunctionId, LogicalFunctionRunId, LogicalFunctionRunStatus,
    PhysicalFunctionRunId, PhysicalFunctionRunStatus,
};
use td_queries::Queries;
use tracing::{debug, instrument, warn};

/// `stuck_lfrs` get the full recover flow; `abandoned_pfr_ids` (attempts
/// under non-`Requested` LFRs, e.g. replays) are only terminalized.
#[derive(Debug, Clone)]
pub struct StaleDispatchedRuns {
    pub stuck_lfrs: Vec<LogicalFunctionRunDB>,
    pub abandoned_pfr_ids: Vec<PhysicalFunctionRunId>,
}

impl StaleDispatchedRuns {
    fn empty() -> Self {
        Self {
            stuck_lfrs: Vec::new(),
            abandoned_pfr_ids: Vec::new(),
        }
    }
}

/// A PFR still `Dispatched` past the timeout never provisioned its pod;
/// no callback will arrive.
#[instrument(
    name = "recovery.fetch_stale_dispatched_runs",
    skip_all,
    level = "debug",
    err(Debug, level = "debug")
)]
pub async fn fetch_stale_dispatched_runs<S: SeesProjectRows, M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    at: &AtTime,
    config: &RecoveryConfig,
) -> Result<StaleDispatchedRuns, TdError> {
    let timeout = config.stuck_requested_timeout.0;
    let cutoff = AtTime::from(***at - chrono::Duration::seconds(timeout.as_secs() as i64));
    let execution_ids: Vec<ExecutionId> = conn
        .physical_runs()
        .dispatched_before(&cutoff)
        .await?
        .into_iter()
        .map(|pfr| pfr.base.execution_id)
        .unique()
        .collect();
    if execution_ids.is_empty() {
        return Ok(StaleDispatchedRuns::empty());
    }

    let logical_function_runs: Vec<LogicalFunctionRunDB> = conn
        .chunked()
        .fetch_all::<LogicalFunctionRunDB>(&execution_ids, ())
        .await?;
    let stale: Vec<PhysicalFunctionRunDB> = conn.physical_runs().dispatched_before(&cutoff).await?;
    if stale.is_empty() {
        return Ok(StaleDispatchedRuns::empty());
    }
    let lfr_by_function: HashMap<(&ExecutionId, &FunctionId), &LogicalFunctionRunDB> =
        logical_function_runs
            .iter()
            .map(|lfr| ((&lfr.execution_id, &lfr.function_id), lfr))
            .collect();

    let mut seen: HashSet<LogicalFunctionRunId> = HashSet::new();
    let mut stuck_lfrs = Vec::new();
    let mut abandoned_pfr_ids = Vec::new();
    for pfr in &stale {
        match lfr_by_function.get(&(&pfr.execution_id, &pfr.function_id)) {
            Some(lfr) if lfr.status == LogicalFunctionRunStatus::Requested => {
                if seen.insert(lfr.id.clone()) {
                    stuck_lfrs.push((*lfr).clone());
                }
            }
            _ => abandoned_pfr_ids.push(pfr.id.clone()),
        }
    }
    debug!(
        stuck_count = stuck_lfrs.len(),
        abandoned_count = abandoned_pfr_ids.len(),
        "scheduler.fetch_stale_dispatched_runs: stale Dispatched PFR(s) past timeout"
    );
    Ok(StaleDispatchedRuns {
        stuck_lfrs,
        abandoned_pfr_ids,
    })
}

/// Late callbacks from the failed attempts drop at the in-flight filter.
/// Returns the stops for their workers.
#[instrument(
    name = "scheduler.fail_stale_abandoned_attempts",
    skip_all,
    level = "debug",
    err(Debug, level = "debug")
)]
pub async fn fail_stale_abandoned_attempts(
    conn: &mut DomainConnection<Apiserver, InProject, impl Holds<ProjectLock>>,
    at: LockInstant,
    stale: &StaleDispatchedRuns,
) -> Result<Vec<FunctionStopRequest>, TdError> {
    if stale.abandoned_pfr_ids.is_empty() {
        return Ok(Vec::new());
    }
    let workers = conn
        .physical_runs()
        .end(&stale.abandoned_pfr_ids, at)
        .await?;
    debug!(
        failed_count = stale.abandoned_pfr_ids.len(),
        "scheduler.fail_stale_abandoned_attempts: terminalized stale attempt(s)"
    );
    function_stops(conn, workers).await
}

/// The function workers still live although their attempt has ended for
/// longer than the dispatch grace: none of them will report, as their report
/// was lost, or a release that abandoned attempts without failing their
/// workers left them behind.
#[instrument(
    name = "recovery.stray_function_workers",
    skip_all,
    level = "debug",
    err(Debug, level = "debug")
)]
pub async fn stray_function_workers<S: SeesProjectRows, M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    at: &AtTime,
    config: &RecoveryConfig,
) -> Result<Vec<WorkerDBWithNames>, TdError> {
    let live: Vec<WorkerDBWithNames> = conn
        .fetch_all(WorkerDBWithNames::function_runners_in_flight())
        .await?;
    let pfr_ids: Vec<PhysicalFunctionRunId> = live
        .iter()
        .filter_map(|worker| worker.base.physical_function_run_id.clone())
        .unique()
        .collect();
    if pfr_ids.is_empty() {
        return Ok(Vec::new());
    }
    let cutoff = ***at - chrono::Duration::from_std(config.dispatch_grace.0).unwrap_or_default();
    let ended_long_ago: HashSet<PhysicalFunctionRunId> = conn
        .scd2()
        .chunked()
        .fetch_all::<PhysicalFunctionRunDB>(&pfr_ids, ())
        .await?
        .into_iter()
        .filter(|pfr| !PhysicalFunctionRunStatus::IN_FLIGHT.contains(&pfr.status))
        .filter(|pfr| {
            let ended = pfr
                .ended_on
                .map(|ended| *ended)
                .or(pfr.dispatched_on.map(|dispatched| *dispatched));
            ended.is_none_or(|ended| **ended < cutoff)
        })
        .map(|pfr| pfr.id.clone())
        .collect();
    Ok(live
        .into_iter()
        .filter(|worker| {
            worker
                .base
                .physical_function_run_id
                .as_ref()
                .is_some_and(|pfr_id| ended_long_ago.contains(pfr_id))
        })
        .collect())
}

/// Fails the project's [stray function workers](stray_function_workers), and
/// returns the stops for any workload they left.
#[instrument(
    name = "recovery.fail_stray_function_workers",
    skip_all,
    level = "debug",
    err(Debug, level = "debug")
)]
pub async fn fail_stray_function_workers(
    conn: &mut DomainConnection<Apiserver, InProject, impl Holds<ProjectLock>>,
    at: LockInstant,
    config: &RecoveryConfig,
) -> Result<Vec<FunctionStopRequest>, TdError> {
    let stray = stray_function_workers(conn, &AtTime::from(at), config).await?;
    if stray.is_empty() {
        return Ok(Vec::new());
    }
    let failed = WorkerUpdateDB::failed_at(at);
    let keys: Vec<_> = stray
        .iter()
        .map(|worker| and!(worker.base.id.clone(), worker.base.status))
        .collect();
    conn.chunked().update(&failed, &keys).await?;
    warn!(
        workers = stray.len(),
        "recovery: failing function workers whose attempt ended without them"
    );
    function_stops(conn, stray).await
}

/// The time-based sweep, in one write transaction: stale `Dispatched` PFRs →
/// `Failed`, and those under a `Requested` LFR additionally get the
/// operator-recover flow.
#[instrument(
    name = "recovery.sweep",
    skip_all,
    level = "debug",
    err(Debug, level = "debug")
)]
pub async fn sweep_stale_runs(
    msgs: &mut ReqCtx,
    conn: &mut DomainConnection<Apiserver, InProject, impl Holds<ProjectLock>>,
    at: LockInstant,
    config: &RecoveryConfig,
) -> Result<Vec<FunctionStopRequest>, TdError> {
    let stale = fetch_stale_dispatched_runs(conn, &AtTime::from(at), config).await?;
    let mut stops = fail_stale_abandoned_attempts(conn, at, &stale).await?;
    let (stuck_lfrs, given_up): (Vec<_>, Vec<_>) =
        stale.stuck_lfrs.iter().cloned().partition(under_cap);
    count_recoveries(conn, &stuck_lfrs).await?;
    stops.extend(abandon_requested_physical_runs(conn, at, &stuck_lfrs).await?);
    update_function_run_status(
        msgs,
        conn,
        &stuck_lfrs,
        LogicalFunctionRunStatus::ReScheduled,
    )
    .await?;
    if !given_up.is_empty() {
        warn!(
            runs = given_up.len(),
            reason = %gave_up(),
            "recovery.sweep: failing stuck runs"
        );
    }
    stops.extend(abandon_requested_physical_runs(conn, at, &given_up).await?);
    update_function_run_status(msgs, conn, &given_up, LogicalFunctionRunStatus::Failed).await?;
    Ok(stops)
}
