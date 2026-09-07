//
// Copyright 2026 Tabsdata Inc.
//

//! Replay-endpoint helpers. The endpoint mints PFR + PTDV
//! placeholders for the new attempt; the pointer log is untouched
//! until the worker callback. Dispatch readiness lives on the
//! physical side ([`td_queries::Pfr::to_dispatch`]).

use crate::function::topology::{RunnableVersion, latest_patch_or_head};
use crate::logical::mint::{mint_function_run, mint_output_table_data_versions};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::{TdError, td_error};
use td_objects::dxo::logical::{
    LogicalFunctionRunDB, LogicalFunctionRunPointerDB, LogicalFunctionRunReplayResponse,
};
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
use td_objects::types::basic::{
    AtTime, LogicalFunctionRunId, LogicalFunctionRunStatus, LogicalPointerStatus,
    PhysicalFunctionRunId, PhysicalFunctionRunStatus,
};
use td_queries::{PfrEntry, VersionedEntry};
use tracing::{debug, instrument};

#[td_error]
pub enum ReplayError {
    #[error("LFR [{0}] is not Bound to a finished function run; nothing to replay")]
    NotBound(LogicalFunctionRunId) = 0,
    #[error("LFR [{0}] status [{1:?}] is not terminal; cannot replay an in-flight run")]
    NotTerminal(LogicalFunctionRunId, LogicalFunctionRunStatus) = 1,
    #[error("LFR [{0}] pointer is Bound but the bound FR id is missing")]
    BoundWithoutId(LogicalFunctionRunId) = 2,
    #[error(
        "LFR [{0}] has an in-flight attempt [{1}] (status {2:?}); cannot replay until it finishes"
    )]
    AttemptInFlight(
        LogicalFunctionRunId,
        PhysicalFunctionRunId,
        PhysicalFunctionRunStatus,
    ) = 3,
}

/// Read the LFR's current pointer event. Refuse if not Bound — replay
/// only applies to a previously-dispatched LFR. Surfaces FR_old's id
/// for the next step.
#[instrument(
    name = "replay.load_bound_pointer",
    skip_all,
    level = "debug",
    err(Debug)
)]
pub async fn load_bound_function_run_id(
    conn: &mut DomainConnection<Apiserver>,
    lfr: &LogicalFunctionRunDB,
    at: &AtTime,
) -> Result<PhysicalFunctionRunId, TdError> {
    let latest: LogicalFunctionRunPointerDB = conn.versioned(at).fetch_one(&lfr.id).await?;
    if !matches!(latest.status, LogicalPointerStatus::Bound) {
        Err(ReplayError::NotBound(lfr.id.clone()))?
    }
    let fr_id = latest
        .physical_function_run_id
        .clone()
        .ok_or_else(|| ReplayError::BoundWithoutId(lfr.id.clone()))?;
    Ok(fr_id)
}

/// Refuse replay when a prior attempt is still in flight (PFR status
/// in `Requested | Running`). Failed / Done / Canceled prior
/// attempts don't block — they're terminal, and the operator may
/// legitimately want to mint a new replay.
#[instrument(
    name = "replay.assert_no_concurrent_replay",
    skip_all,
    level = "debug",
    err(Debug)
)]
pub async fn assert_no_concurrent_replay(
    conn: &mut DomainConnection<Apiserver>,
    lfr: &LogicalFunctionRunDB,
    bound: &PhysicalFunctionRunId,
) -> Result<(), TdError> {
    let in_flight: Option<PhysicalFunctionRunDB> = conn
        .pfr()
        .in_flight_for(&lfr.execution_id, &lfr.function_id)
        .await?;
    if let Some(pfr) = in_flight
        && pfr.id != *bound
    {
        Err(ReplayError::AttemptInFlight(
            lfr.id.clone(),
            pfr.id.clone(),
            pfr.status,
        ))?
    }
    Ok(())
}

/// Validate the LFR's status is terminal. All five terminals are allowed
/// inputs to replay; the LFR-side effect of a replay differs by status:
///
/// - `Committed` / `Canceled` / `Yanked` are **sticky** (DB-enforced via
///   the `logical_function_runs__sticky_terminals` trigger) — the LFR
///   stays put; the replay's outcome surfaces through a fresh `Bound`
///   pointer event written by `redirect_at_finish`.
/// - `Done` — same shape (idempotent self-transition allowed).
/// - `Failed` — can be **promoted to `Done`** by a successful replay
///   ("replay healed a failed run"); see the `Failed → Done` arm in
///   `update_function_run_status`.
#[instrument(name = "replay.assert_terminal", skip_all, level = "debug", err(Debug))]
pub async fn assert_lfr_terminal(lfr: &LogicalFunctionRunDB) -> Result<(), TdError> {
    match lfr.status {
        LogicalFunctionRunStatus::Done
        | LogicalFunctionRunStatus::Committed
        | LogicalFunctionRunStatus::Failed
        | LogicalFunctionRunStatus::Canceled
        | LogicalFunctionRunStatus::Yanked => Ok(()),
        _ => Err(ReplayError::NotTerminal(lfr.id.clone(), lfr.status))?,
    }
}

/// Resolve the head + optional patch fvid to run on this attempt.
/// Pins to the replay-request `at`, so a patch registered between the
/// replay request and the dispatch tick won't be silently picked up.
#[instrument(name = "replay.resolve_fvid", skip_all, level = "debug", err(Debug))]
pub async fn resolve_replay_function_version_id(
    conn: &mut DomainConnection<Apiserver>,
    lfr: &LogicalFunctionRunDB,
    at: &AtTime,
) -> Result<RunnableVersion, TdError> {
    let runnable = latest_patch_or_head(&mut *conn, &lfr.function_version_id, at).await?;
    debug!(
        lfr = %lfr.id,
        planned = %lfr.function_version_id,
        head = %runnable.function_version_id,
        patch = ?runnable.function_version_patch_id,
        "replay.resolve: head + optional patch"
    );
    Ok(runnable)
}

#[instrument(name = "replay.mint_fr_new", skip_all, level = "debug", err(Debug))]
pub async fn mint_replay_function_run(
    conn: &mut DomainConnection<Apiserver>,
    lfr: &LogicalFunctionRunDB,
    runnable: &RunnableVersion,
    at: &AtTime,
) -> Result<PhysicalFunctionRunDB, TdError> {
    mint_function_run(&mut *conn, lfr, runnable, at).await
}

#[instrument(name = "replay.mint_tdvs_new", skip_all, level = "debug", err(Debug))]
pub async fn mint_replay_table_data_versions(
    conn: &mut DomainConnection<Apiserver>,
    lfr: &LogicalFunctionRunDB,
    runnable: &RunnableVersion,
    fr_new: &PhysicalFunctionRunDB,
) -> Result<(), TdError> {
    mint_output_table_data_versions(&mut *conn, lfr, runnable, fr_new).await?;
    Ok(())
}

#[instrument(name = "replay.response", skip_all, level = "debug")]
pub async fn build_replay_response(
    lfr: &LogicalFunctionRunDB,
    previous_function_run_id: &PhysicalFunctionRunId,
    fr_new: &PhysicalFunctionRunDB,
    runnable: &RunnableVersion,
) -> Result<LogicalFunctionRunReplayResponse, TdError> {
    // Response advertises the effective fvid — the patch if one was
    // picked up, else the head.
    Ok(LogicalFunctionRunReplayResponse {
        logical_function_run_id: lfr.id.clone(),
        previous_function_run_id: previous_function_run_id.clone(),
        new_function_run_id: fr_new.id.clone(),
        function_version_id: runnable.effective().clone(),
    })
}
