//
// Copyright 2026 Tabsdata Inc.
//

//! Replay-endpoint helpers. The endpoint mints PFR + PTDV placeholders for
//! the new attempt; dispatch readiness lives on the physical side
//! ([`td_queries::PhysicalRuns::to_dispatch`]).

use crate::function::topology::{RunnableVersion, latest_patch_or_head};
use crate::logical::mint::{mint_function_run, mint_output_table_data_versions};
use td_database::access::LockInstant;
use td_database::access::{AccessMode, InProject, SeesProjectRows, WriteMode, XW};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::{TdError, td_error};
use td_objects::dxo::logical::{LogicalFunctionRunDB, LogicalFunctionRunReplayResponse};
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
use td_objects::dxo::physical_function_run::PhysicalFunctionRunInsertDB;
use td_objects::types::basic::{
    AtTime, LogicalFunctionRunId, LogicalFunctionRunStatus, PhysicalFunctionRunId,
    PhysicalFunctionRunStatus,
};
use td_queries::Queries;
use tracing::{debug, instrument};

#[td_error]
pub enum ReplayError {
    #[error("LFR [{0}] has no function run attempt; nothing to replay")]
    NoAttempt(LogicalFunctionRunId) = 0,
    #[error("LFR [{0}] status [{1:?}] is not terminal; cannot replay an in-flight run")]
    NotTerminal(LogicalFunctionRunId, LogicalFunctionRunStatus) = 1,
    #[error(
        "LFR [{0}] has an in-flight attempt [{1}] (status {2:?}); cannot replay until it finishes"
    )]
    AttemptInFlight(
        LogicalFunctionRunId,
        PhysicalFunctionRunId,
        PhysicalFunctionRunStatus,
    ) = 3,
}

/// The LFR's current attempt; replay only applies to a dispatched LFR.
#[instrument(
    name = "replay.load_current_attempt",
    skip_all,
    level = "debug",
    err(Debug)
)]
pub async fn load_current_function_run_id<S: SeesProjectRows, M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    lfr: &LogicalFunctionRunDB,
) -> Result<PhysicalFunctionRunId, TdError> {
    let current: Option<PhysicalFunctionRunDB> = conn
        .scd2()
        .fetch_optional::<PhysicalFunctionRunDB>(&lfr.base.id)
        .await?;
    let current = current.ok_or_else(|| ReplayError::NoAttempt(lfr.base.id.clone()))?;
    Ok(current.base.id)
}

/// Refuse replay while the current attempt is in flight.
#[instrument(
    name = "replay.assert_no_concurrent_replay",
    skip_all,
    level = "debug",
    err(Debug)
)]
pub async fn assert_no_concurrent_replay<S: SeesProjectRows, M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    lfr: &LogicalFunctionRunDB,
) -> Result<(), TdError> {
    let current: Option<PhysicalFunctionRunDB> = conn
        .scd2()
        .fetch_optional::<PhysicalFunctionRunDB>(&lfr.base.id)
        .await?;
    if let Some(pfr) = current
        && PhysicalFunctionRunStatus::IN_FLIGHT.contains(&pfr.base.status)
    {
        Err(ReplayError::AttemptInFlight(
            lfr.base.id.clone(),
            pfr.base.id.clone(),
            pfr.base.status,
        ))?
    }
    Ok(())
}

/// Validate the LFR's status is terminal. All five terminals are allowed
/// inputs to replay; the LFR-side effect of a replay differs by status:
///
/// - `Committed` / `Canceled` / `Yanked` are **sticky** (DB-enforced via
///   the `logical_function_runs__sticky_terminals` trigger) — the LFR
///   stays put; the replay's outcome surfaces on the new attempt, which
///   is the run's current version from mint.
/// - `Done` — same shape (idempotent self-transition allowed).
/// - `Failed` — can be **promoted to `Done`** by a successful replay
///   ("replay healed a failed run"); see the `Failed → Done` arm in
///   `update_function_run_status`.
#[instrument(name = "replay.assert_terminal", skip_all, level = "debug", err(Debug))]
pub fn assert_lfr_terminal(lfr: &LogicalFunctionRunDB) -> Result<(), TdError> {
    match lfr.base.status {
        LogicalFunctionRunStatus::Done
        | LogicalFunctionRunStatus::Committed
        | LogicalFunctionRunStatus::Failed
        | LogicalFunctionRunStatus::Canceled
        | LogicalFunctionRunStatus::Yanked => Ok(()),
        _ => Err(ReplayError::NotTerminal(
            lfr.base.id.clone(),
            lfr.base.status,
        ))?,
    }
}

/// Resolve the head + optional patch fvid to run on this attempt.
/// Pins to the replay-request `at`, so a patch registered between the
/// replay request and the dispatch tick won't be silently picked up.
#[instrument(name = "replay.resolve_fvid", skip_all, level = "debug", err(Debug))]
pub async fn resolve_replay_function_version_id<S: SeesProjectRows, M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    lfr: &LogicalFunctionRunDB,
    at: &AtTime,
) -> Result<RunnableVersion, TdError> {
    let runnable = latest_patch_or_head(&mut *conn, &lfr.base.function_version_id, at).await?;
    debug!(
        lfr = %lfr.base.id,
        planned = %lfr.base.function_version_id,
        head = %runnable.function_version_id,
        patch = ?runnable.function_version_patch_id,
        "replay.resolve: head + optional patch"
    );
    Ok(runnable)
}

#[instrument(name = "replay.mint_fr_new", skip_all, level = "debug", err(Debug))]
pub async fn mint_replay_function_run(
    conn: &mut DomainConnection<Apiserver, InProject, XW>,
    lfr: &LogicalFunctionRunDB,
    runnable: &RunnableVersion,
    at: LockInstant,
) -> Result<PhysicalFunctionRunInsertDB, TdError> {
    mint_function_run(&mut *conn, lfr, runnable, at).await
}

#[instrument(name = "replay.mint_tdvs_new", skip_all, level = "debug", err(Debug))]
pub async fn mint_replay_table_data_versions(
    conn: &mut DomainConnection<Apiserver, InProject, impl WriteMode>,
    lfr: &LogicalFunctionRunDB,
    runnable: &RunnableVersion,
    fr_new: &PhysicalFunctionRunInsertDB,
    at: LockInstant,
) -> Result<(), TdError> {
    mint_output_table_data_versions(&mut *conn, lfr, runnable, fr_new, at).await?;
    Ok(())
}

#[instrument(name = "replay.response", skip_all, level = "debug")]
pub fn build_replay_response(
    lfr: &LogicalFunctionRunDB,
    previous_function_run_id: &PhysicalFunctionRunId,
    fr_new: &PhysicalFunctionRunInsertDB,
    runnable: &RunnableVersion,
) -> Result<LogicalFunctionRunReplayResponse, TdError> {
    Ok(LogicalFunctionRunReplayResponse {
        logical_function_run_id: lfr.base.id.clone(),
        previous_function_run_id: previous_function_run_id.clone(),
        new_function_run_id: fr_new.id.clone(),
        function_version_id: runnable.effective().clone(),
    })
}
