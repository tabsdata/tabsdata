//
// Copyright 2026 Tabsdata Inc.
//

use crate::execution::update_status::{
    abandon_requested_physical_runs, update_function_run_status,
};
use crate::recovery::{
    RecoveryActions, fail_lfrs_from_actions, fail_recovered_attempts,
    fail_running_lfrs_for_recover, failed_worker_ids, failed_worker_update,
    finalize_recovery_actions, recover_lfrs_from_actions,
};
use ta_services::ctx_service::ReqCtx;
use td_database::access::InProject;
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::logical::LogicalFunctionRunStatusUpdateDB;
use td_objects::types::basic::AtTime;
use td_queries::Queries;
use tracing::instrument;

/// One short write transaction for the surviving function actions: finalize
/// re-reads run state (a concurrently-terminalized attempt drops out — the
/// callback wins), then worker rows → Failed, attempts → Failed, and the LFRs
/// recover or fail.
#[instrument(name = "recovery.apply", skip_all, level = "debug", err(Debug))]
pub async fn apply_recovery_actions(
    msgs: &mut ReqCtx,
    conn: &mut DomainConnection<Apiserver, InProject>,
    at: &AtTime,
    actions: &RecoveryActions,
) -> Result<(), TdError> {
    let actions = finalize_recovery_actions(conn, actions).await?;

    // Worker rows → Failed (informational record).
    let worker_update = failed_worker_update(at)?;
    let worker_ids = failed_worker_ids(&actions)?;
    conn.chunked().update(&worker_update, &worker_ids).await?;

    // Attempts → Failed; late callbacks drop at the in-flight filter.
    fail_recovered_attempts(conn, at, &actions).await?;

    // Running LFRs with a recover verdict → Failed first; ReScheduled is only
    // reachable from Failed/OnHold/Requested.
    let running = fail_running_lfrs_for_recover(&actions)?;
    let failed = LogicalFunctionRunStatusUpdateDB::failed();
    update_function_run_status(msgs, conn, &running, &failed).await?;

    // Requested and just-failed LFRs with a recover verdict → ReScheduled.
    let to_recover = recover_lfrs_from_actions(&actions)?;
    abandon_requested_physical_runs(conn, at, &to_recover).await?;
    let recover = LogicalFunctionRunStatusUpdateDB::recover();
    update_function_run_status(msgs, conn, &to_recover, &recover).await?;

    // The rest → Failed, with the downstream on-hold cascade.
    let to_fail = fail_lfrs_from_actions(&actions)?;
    update_function_run_status(msgs, conn, &to_fail, &failed).await?;
    Ok(())
}
