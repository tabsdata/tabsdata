//
// Copyright 2026 Tabsdata Inc.
//

use crate::recovery::{
    RecoverySnapshot, build_recovery_snapshot, cron_candidates, current_trigger_versions,
    fn_candidates, stage_candidates, stage_trigger_functions,
};
use td_database::access::SeesProjectRows;
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::worker::WorkerDBWithNames;
use td_objects::types::basic::AtTime;
use td_queries::Queries;
use tracing::instrument;

/// One read-only pass over the DB: reconciliation candidates plus the stage
/// function/bundle locations the recreates need, so the orchestrator then runs
/// every Kubernetes call with no connection held.
#[instrument(name = "recovery.snapshot", skip_all, level = "debug", err(Debug))]
pub async fn take_recovery_snapshot<S: SeesProjectRows>(
    conn: &mut DomainConnection<Apiserver, S>,
    at: &AtTime,
) -> Result<RecoverySnapshot, TdError> {
    let cron_workers: Vec<WorkerDBWithNames> = conn
        .fetch_all(WorkerDBWithNames::cron_triggers_in_flight())
        .await?;
    let cron = cron_candidates(&cron_workers).await?;
    let stage_workers: Vec<WorkerDBWithNames> = conn
        .fetch_all(WorkerDBWithNames::stage_triggers_in_flight())
        .await?;
    let stage = stage_candidates(&stage_workers).await?;
    let current = current_trigger_versions(conn, &cron, &stage).await?;
    let fn_attempts = fn_candidates(conn).await?;
    let stage_functions = stage_trigger_functions(conn, &stage).await?;
    build_recovery_snapshot(at, &cron, &stage, &fn_attempts, &stage_functions, &current).await
}
