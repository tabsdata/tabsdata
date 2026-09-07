//
// Copyright 2026 Tabsdata Inc.
//

use crate::execution::update_status::{
    abandon_requested_physical_runs, update_function_run_status,
};
use itertools::Itertools;
use std::collections::{HashMap, HashSet};
use ta_services::ctx_service::ReqCtx;
use td_database::access::SeesProjectRows;
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_execution::RecoveryConfig;
use td_objects::dxo::logical::{LogicalFunctionRunDB, LogicalFunctionRunStatusUpdateDB};
use td_objects::dxo::physical_function_run::{PhysicalFunctionRunDB, PhysicalFunctionRunUpdateDB};
use td_objects::types::basic::{
    AtTime, ExecutionId, FunctionId, LogicalFunctionRunId, LogicalFunctionRunStatus,
    PhysicalFunctionRunId,
};
use td_queries::Queries;
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
pub async fn fetch_stale_dispatched_runs<S: SeesProjectRows>(
    conn: &mut DomainConnection<Apiserver, S>,
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
            .map(|lfr| ((&lfr.base.execution_id, &lfr.base.function_id), lfr))
            .collect();

    let mut seen: HashSet<LogicalFunctionRunId> = HashSet::new();
    let mut stuck_lfrs = Vec::new();
    let mut abandoned_pfr_ids = Vec::new();
    for pfr in &stale {
        match lfr_by_function.get(&(&pfr.base.execution_id, &pfr.base.function_id)) {
            Some(lfr) if lfr.base.status == LogicalFunctionRunStatus::Requested => {
                if seen.insert(lfr.base.id.clone()) {
                    stuck_lfrs.push((*lfr).clone());
                }
            }
            _ => abandoned_pfr_ids.push(pfr.base.id.clone()),
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
#[instrument(
    name = "scheduler.fail_stale_abandoned_attempts",
    skip_all,
    level = "debug",
    err(Debug)
)]
pub async fn fail_stale_abandoned_attempts<S: SeesProjectRows>(
    conn: &mut DomainConnection<Apiserver, S>,
    at: &AtTime,
    stale: &StaleDispatchedRuns,
) -> Result<(), TdError> {
    if stale.abandoned_pfr_ids.is_empty() {
        return Ok(());
    }
    let update = PhysicalFunctionRunUpdateDB::failed_at(at)?;
    conn.scd2()
        .chunked()
        .update(&update, &stale.abandoned_pfr_ids)
        .await?;
    debug!(
        failed_count = stale.abandoned_pfr_ids.len(),
        "scheduler.fail_stale_abandoned_attempts: terminalized stale attempt(s)"
    );
    Ok(())
}

/// The time-based sweep, in one write transaction: stale `Dispatched` PFRs →
/// `Failed`, and those under a `Requested` LFR additionally get the
/// operator-recover flow.
#[instrument(name = "recovery.sweep", skip_all, level = "debug", err(Debug))]
pub async fn sweep_stale_runs<S: SeesProjectRows>(
    msgs: &mut ReqCtx,
    conn: &mut DomainConnection<Apiserver, S>,
    at: &AtTime,
    config: &RecoveryConfig,
) -> Result<(), TdError> {
    let stale = fetch_stale_dispatched_runs(conn, at, config).await?;
    fail_stale_abandoned_attempts(conn, at, &stale).await?;
    let stuck_lfrs = stale.stuck_lfrs.clone();
    abandon_requested_physical_runs(conn, at, &stuck_lfrs).await?;
    let recover = LogicalFunctionRunStatusUpdateDB::recover();
    update_function_run_status(msgs, conn, &stuck_lfrs, &recover).await?;
    Ok(())
}
