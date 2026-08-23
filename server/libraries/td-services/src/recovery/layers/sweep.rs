//
// Copyright 2026 Tabsdata Inc.
//

use crate::logical::layers::index_lfrs_by_exec_fn;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use std::collections::HashSet;
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_execution::RecoveryConfig;
use td_objects::dxo::logical::LogicalFunctionRunDB;
use td_objects::dxo::physical_function_run::{PhysicalFunctionRunDB, UpdatePhysicalFunctionRunDB};
use td_objects::types::basic::{
    AtTime, ExecutionId, LogicalFunctionRunId, LogicalFunctionRunStatus, PhysicalFunctionRunId,
};
use td_queries::{ChunkedQueries, PfrEntry};
use td_tower::extractors::{MutRef, Ref};
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
    mut conn: MutRef<DomainConnection<Apiserver>>,
    at: Ref<AtTime>,
    config: Ref<RecoveryConfig>,
) -> Result<StaleDispatchedRuns, TdError> {
    let timeout = config.stuck_requested_timeout.0;
    let cutoff: DateTime<Utc> = ***at - chrono::Duration::seconds(timeout.as_secs() as i64);
    let stale: Vec<PhysicalFunctionRunDB> = conn
        .pfr()
        .dispatched_before(&AtTime::try_from(cutoff)?)
        .await?;
    if stale.is_empty() {
        return Ok(StaleDispatchedRuns::empty());
    }

    let execution_ids: Vec<ExecutionId> = stale
        .iter()
        .map(|pfr| pfr.execution_id.clone())
        .unique()
        .collect();
    let logical_function_runs: Vec<LogicalFunctionRunDB> = conn
        .fetch_all_chunked::<LogicalFunctionRunDB, ExecutionId>(&execution_ids)
        .await?;
    let lfr_by_exec_fn = index_lfrs_by_exec_fn(logical_function_runs);

    let mut seen: HashSet<LogicalFunctionRunId> = HashSet::new();
    let mut stuck_lfrs = Vec::new();
    let mut abandoned_pfr_ids = Vec::new();
    for pfr in &stale {
        let lfr = lfr_by_exec_fn
            .get(&pfr.execution_id)
            .and_then(|by_fn| by_fn.get(&pfr.function_id));
        match lfr {
            Some(lfr) if lfr.status == LogicalFunctionRunStatus::Requested => {
                if seen.insert(lfr.id.clone()) {
                    stuck_lfrs.push(lfr.clone());
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
    mut conn: MutRef<DomainConnection<Apiserver>>,
    at: Ref<AtTime>,
    stale: Ref<StaleDispatchedRuns>,
) -> Result<(), TdError> {
    if stale.abandoned_pfr_ids.is_empty() {
        return Ok(());
    }
    let update = UpdatePhysicalFunctionRunDB::failed_at(&at)?;
    conn.update_chunked::<_, PhysicalFunctionRunDB, _>(&update, &stale.abandoned_pfr_ids)
        .await?;
    debug!(
        failed_count = stale.abandoned_pfr_ids.len(),
        "scheduler.fail_stale_abandoned_attempts: terminalized stale attempt(s)"
    );
    Ok(())
}

pub async fn stuck_lfrs_from_stale(
    stale: Ref<StaleDispatchedRuns>,
) -> Result<Vec<LogicalFunctionRunDB>, TdError> {
    Ok(stale.stuck_lfrs.clone())
}
