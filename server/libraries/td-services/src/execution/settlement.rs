//
// Copyright 2026 Tabsdata Inc.
//

//! Settling transactions: a transaction publishes all its tables or none.
//! One whose runs are all Done commits; Done runs beside a Canceled one are
//! discarded. The Done callback settles its own executions at once, and the
//! scheduler tick settles whatever any other path left behind.

use crate::execution::cancellation::cancel_runs;
use td_bridge::messaging::FunctionStopRequest;
use td_database::access::{InProject, WriteMode};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::types::basic::{AtTime, ExecutionId, LogicalFunctionRunStatus};
use td_queries::Queries;
use tracing::debug;

/// Commits the transactions of `execution_ids` whose runs are all Done and
/// whose inputs come from Committed runs or their own. Committing one can make
/// another eligible, so it repeats until nothing more commits.
pub async fn commit_done_transactions(
    conn: &mut DomainConnection<Apiserver, InProject, impl WriteMode>,
    execution_ids: &[ExecutionId],
) -> Result<(), TdError> {
    loop {
        let to_commit = conn.logical_runs().to_commit(execution_ids).await?;
        if to_commit.is_empty() {
            return Ok(());
        }
        conn.logical_runs()
            .set_status(&to_commit, LogicalFunctionRunStatus::Committed)
            .await?;
        debug!(
            committed_count = to_commit.len(),
            "execution.commit: committed LFR(s)"
        );
    }
}

/// Discards the Done runs of transactions holding a Canceled run, then
/// commits what is left to commit. Returns the stops of the runs the discard
/// reached, to send once the unit of work commits.
pub async fn settle_transactions(
    conn: &mut DomainConnection<Apiserver, InProject, impl WriteMode>,
    at: &AtTime,
) -> Result<Vec<FunctionStopRequest>, TdError> {
    let discarded = conn.logical_runs().done_beside_canceled().await?;
    let stops = cancel_runs(conn, at, &discarded).await?.stops;
    let awaiting = conn.logical_runs().awaiting_commit().await?;
    if !awaiting.is_empty() {
        commit_done_transactions(conn, &awaiting).await?;
    }
    Ok(stops)
}
