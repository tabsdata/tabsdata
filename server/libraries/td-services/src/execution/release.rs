//
// Copyright 2026 Tabsdata Inc.
//

//! Releasing runs held on hold. A run is on hold only while an upstream run is
//! Failed or on hold; once none is, it goes back to the scheduler. A replay
//! that heals a failed run releases its readers at once, and the scheduler
//! tick releases whatever any other path left held.

use crate::execution::update_status::without_attempt_in_flight;
use std::collections::HashSet;
use td_database::access::{InProject, WriteMode};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::logical::LogicalFunctionRunDB;
use td_objects::types::basic::{LogicalFunctionRunId, LogicalFunctionRunStatus};
use td_queries::Queries;
use tracing::debug;

/// Releases the on-hold readers of `healed`, and theirs in turn.
pub async fn release_readers_of(
    conn: &mut DomainConnection<Apiserver, InProject, impl WriteMode>,
    healed: &[LogicalFunctionRunId],
) -> Result<(), TdError> {
    let held = on_hold_readers(conn, healed).await?;
    release(conn, held).await
}

/// Releases every run of the project held on hold with nothing left
/// blocking it.
pub async fn release_unblocked(
    conn: &mut DomainConnection<Apiserver, InProject, impl WriteMode>,
) -> Result<(), TdError> {
    let held: Vec<LogicalFunctionRunId> = conn
        .fetch_all::<LogicalFunctionRunDB>(&LogicalFunctionRunStatus::OnHold)
        .await?
        .into_iter()
        .map(|lfr| lfr.base.id)
        .collect();
    release(conn, held).await
}

async fn release(
    conn: &mut DomainConnection<Apiserver, InProject, impl WriteMode>,
    mut held: Vec<LogicalFunctionRunId>,
) -> Result<(), TdError> {
    while !held.is_empty() {
        let blocked: HashSet<LogicalFunctionRunId> = conn
            .logical_runs()
            .blockers(&held)
            .await?
            .into_iter()
            .map(|(run, _)| run)
            .collect();
        let free: Vec<LogicalFunctionRunId> = held
            .into_iter()
            .filter(|id| !blocked.contains(id))
            .collect();
        let free = without_attempt_in_flight(conn, free).await?;
        if free.is_empty() {
            break;
        }
        conn.logical_runs()
            .set_status(&free, LogicalFunctionRunStatus::ReScheduled)
            .await?;
        debug!(
            released_count = free.len(),
            "execution.release: released on-hold LFR(s)"
        );
        held = on_hold_readers(conn, &free).await?;
    }
    Ok(())
}

async fn on_hold_readers(
    conn: &mut DomainConnection<Apiserver, InProject, impl WriteMode>,
    upstream: &[LogicalFunctionRunId],
) -> Result<Vec<LogicalFunctionRunId>, TdError> {
    if upstream.is_empty() {
        return Ok(Vec::new());
    }
    let readers = conn
        .logical_runs()
        .downstream_one_hop(upstream, upstream)
        .await?;
    if readers.is_empty() {
        return Ok(Vec::new());
    }
    Ok(conn
        .chunked()
        .fetch_all::<LogicalFunctionRunDB>(&readers, ())
        .await?
        .into_iter()
        .filter(|lfr| lfr.base.status == LogicalFunctionRunStatus::OnHold)
        .map(|lfr| lfr.base.id)
        .collect())
}
