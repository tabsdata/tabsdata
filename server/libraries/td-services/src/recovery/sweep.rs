//
// Copyright 2026 Tabsdata Inc.
//

use crate::execution::update_status::{
    abandon_requested_physical_runs, update_function_run_status,
};
use itertools::Itertools;
use std::collections::{HashMap, HashSet};
use ta_services::ctx_service::ReqCtx;
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_execution::RecoveryConfig;
use td_objects::dxo::logical::{LogicalFunctionRunDB, LogicalFunctionRunUpdateDB};
use td_objects::dxo::physical_function_run::{PhysicalFunctionRunDB, UpdatePhysicalFunctionRunDB};
use td_objects::types::basic::{
    AtTime, ExecutionId, FunctionId, LogicalFunctionRunId, LogicalFunctionRunStatus,
    PhysicalFunctionRunId,
};
use td_queries::{ChunkedQueries, PfrEntry};
use tracing::{debug, instrument};

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
    err(Debug)
)]
pub async fn fetch_stale_dispatched_runs(
    conn: &mut DomainConnection<Apiserver>,
    at: &AtTime,
    config: &RecoveryConfig,
) -> Result<StaleDispatchedRuns, TdError> {
    let timeout = config.stuck_requested_timeout.0;
    let cutoff = AtTime::from(***at - chrono::Duration::seconds(timeout.as_secs() as i64));
    let execution_ids: Vec<ExecutionId> = conn
        .pfr()
        .dispatched_before(&cutoff)
        .await?
        .into_iter()
        .map(|pfr| pfr.execution_id)
        .unique()
        .collect();
    if execution_ids.is_empty() {
        return Ok(StaleDispatchedRuns::empty());
    }

    let logical_function_runs: Vec<LogicalFunctionRunDB> = conn
        .fetch_all_chunked::<LogicalFunctionRunDB, ExecutionId>(&execution_ids)
        .await?;
    let stale: Vec<PhysicalFunctionRunDB> = conn.pfr().dispatched_before(&cutoff).await?;
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

/// Late callbacks from the failed attempts drop at the in-flight filter;
/// no pointer writes, matching operator recover.
#[instrument(
    name = "scheduler.fail_stale_abandoned_attempts",
    skip_all,
    level = "debug",
    err(Debug)
)]
pub async fn fail_stale_abandoned_attempts(
    conn: &mut DomainConnection<Apiserver>,
    at: &AtTime,
    stale: &StaleDispatchedRuns,
) -> Result<(), TdError> {
    if stale.abandoned_pfr_ids.is_empty() {
        return Ok(());
    }
    let update = UpdatePhysicalFunctionRunDB::failed_at(at)?;
    conn.update_chunked::<_, PhysicalFunctionRunDB, _>(&update, &stale.abandoned_pfr_ids)
        .await?;
    debug!(
        failed_count = stale.abandoned_pfr_ids.len(),
        "scheduler.fail_stale_abandoned_attempts: terminalized stale attempt(s)"
    );
    Ok(())
}

pub async fn stuck_lfrs_from_stale(
    stale: &StaleDispatchedRuns,
) -> Result<Vec<LogicalFunctionRunDB>, TdError> {
    Ok(stale.stuck_lfrs.clone())
}

/// The time-based sweep, in one write transaction: stale `Dispatched` PFRs →
/// `Failed`, and those under a `Requested` LFR additionally get the
/// operator-recover flow.
#[instrument(name = "recovery.sweep", skip_all, level = "debug", err(Debug))]
pub async fn sweep_stale_runs(
    msgs: &mut ReqCtx,
    conn: &mut DomainConnection<Apiserver>,
    at: &AtTime,
    config: &RecoveryConfig,
) -> Result<(), TdError> {
    let stale = fetch_stale_dispatched_runs(conn, at, config).await?;
    fail_stale_abandoned_attempts(conn, at, &stale).await?;
    let stuck_lfrs = stuck_lfrs_from_stale(&stale).await?;
    abandon_requested_physical_runs(conn, at, &stuck_lfrs).await?;
    let recover = LogicalFunctionRunUpdateDB::recover();
    update_function_run_status(msgs, conn, &stuck_lfrs, &recover).await?;
    Ok(())
}
