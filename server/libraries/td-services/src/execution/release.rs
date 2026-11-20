//
// Copyright 2026 Tabsdata Inc.
//

//! Releasing runs held on hold. A run is on hold only while something holds
//! it ([`HoldEvaluator`]); once nothing does, it goes back to the scheduler.
//! A replay that heals a failed run and a recover release at once, and the
//! project's pass releases whatever any other path left held.

use crate::execution::holds::HoldEvaluator;
use crate::execution::update_status::without_attempt_in_flight;
use std::collections::HashSet;
use td_database::access::{Holds, InProject, ProjectLock};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::logical::LogicalFunctionRunDB;
use td_objects::types::basic::{LogicalFunctionRunId, LogicalFunctionRunStatus};
use td_queries::Queries;
use tracing::debug;

/// Releases every run of the project held on hold with nothing left
/// holding it, round after round: a run released may free others.
pub async fn release_unblocked(
    conn: &mut DomainConnection<Apiserver, InProject, impl Holds<ProjectLock>>,
) -> Result<(), TdError> {
    let mut held: Vec<LogicalFunctionRunId> = conn
        .fetch_all::<LogicalFunctionRunDB>(&LogicalFunctionRunStatus::OnHold)
        .await?
        .into_iter()
        .map(|lfr| lfr.base.id)
        .collect();
    let mut evaluator = HoldEvaluator::default();
    while !held.is_empty() {
        let blocked = evaluator.held(conn, &held, &HashSet::new()).await?;
        let (blocked, free): (Vec<LogicalFunctionRunId>, Vec<LogicalFunctionRunId>) =
            held.into_iter().partition(|run| blocked.contains_key(run));
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
        evaluator.mark_released(&free);
        held = blocked;
    }
    Ok(())
}
