//
// Copyright 2025 Tabs Data Inc.
//

use http::StatusCode;
use std::ops::Deref;
use td_database::domain::{Apiserver, DomainConnection};
use td_error::{ApiError, TdError, api_error};
use td_objects::and;
use td_objects::dxo::execution::ExecutionDB;
use td_objects::dxo::function::FunctionDBWithNames;
use td_objects::dxo::logical::LogicalFunctionRunDB;
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
use td_objects::types::basic::{
    ExternalUid, FunctionId, FunctionVersionId, LogicalFunctionRunStatus, SkipIfRunning,
};
use td_queries::{LfrEntry, LiveEntry};
use td_tower::extractors::{MutRef, Ref};
use tracing::{debug, instrument};

pub async fn assert_function_version(
    function_version_id: Ref<FunctionVersionId>,
    function: Ref<FunctionDBWithNames>,
) -> Result<(), TdError> {
    if function.id != *function_version_id {
        Err(api_error!(
            ApiError::InputError,
            r#"Function version missmatch. There is a newer version of the function.
            Requested function version '{}' must be the current version of the function '{}'."#,
            function_version_id,
            function.id
        ))?
    }
    Ok(())
}

/// Trigger gate 1 — `skip_if_running=true` and the trigger function is currently running:
/// short-circuit with `425 Too Early` rather than starting a concurrent run. No-op when the
/// flag is unset, there is no prior execution, or the latest run has ended.
///
/// The "latest attempt" for an (execution, fvid) is whichever FR the LFR's pointer is
/// currently bound to — replays leave the prior FR row in `physical_function_runs`, but the
/// pointer is what consumers (and this gate) should follow.
#[instrument(
    name = "execution.reject_if_running",
    skip_all,
    fields(function_version_id = %*function_version_id),
    level = "debug",
    err(Debug)
)]
pub async fn reject_if_running(
    mut conn: MutRef<DomainConnection<Apiserver>>,
    function_version_id: Ref<FunctionVersionId>,
    skip_if_running: Ref<SkipIfRunning>,
    latest_execution: Ref<Option<ExecutionDB>>,
) -> Result<(), TdError> {
    if !**skip_if_running {
        return Ok(());
    }
    let Some(latest_execution) = latest_execution.as_ref() else {
        debug!("No latest execution found");
        return Ok(());
    };
    let lfr: Option<LogicalFunctionRunDB> = conn
        .live()
        .fetch_optional(and!(&latest_execution.id, function_version_id.deref()))
        .await?;
    let bound_fr_id = if let Some(lfr) = lfr {
        conn.lfr().pointers().concrete(&lfr.id).await?
    } else {
        None
    };
    let physical_function_run: Option<PhysicalFunctionRunDB> = match bound_fr_id {
        Some(id) => conn.live().fetch_optional(&id).await?,
        None => None,
    };
    match physical_function_run {
        Some(fr) if !fr.status.is_ended() => {
            debug!(physical_function_run_id = %fr.id, status = ?fr.status, "Latest function run running");
            Err(api_error!(
                status = StatusCode::TOO_EARLY,
                "Function is currently running (execution '{}'); retry once it finishes",
                latest_execution.id
            ))
        }
        _ => Ok(()),
    }
}

/// Serialize concurrent triggers sharing a `(function_id, external_uid)` before the conflict
/// check, closing the check-then-insert race. Without it two identical tagged triggers can
/// both pass [`reject_if_external_uid_in_use`] under READ COMMITTED and race to INSERT.
///
/// Takes a transaction-scoped advisory lock (Postgres) keyed on the pair, so a racing second
/// trigger blocks until the first commits and then observes the committed row. No-op when
/// `external_uid` is `None` (untagged triggers never dedup) and on non-Postgres backends
/// (single writer). Must run before [`reject_if_external_uid_in_use`], inside the request
/// transaction.
#[instrument(
    name = "execution.dedup_lock",
    skip_all,
    fields(function_id = %*function_id),
    level = "debug",
    err(Debug)
)]
pub async fn dedup_lock(
    mut conn: MutRef<DomainConnection<Apiserver>>,
    function_id: Ref<FunctionId>,
    external_uid: Ref<Option<ExternalUid>>,
) -> Result<(), TdError> {
    if let Some(external_uid) = external_uid.deref() {
        let key = format!("execution_dedup:{}:{}", *function_id, external_uid);
        conn.get()
            .await?
            .advisory_xact_lock_text(&key)
            .await
            .map_err(|e| api_error!(ApiError::InternalError, "dedup advisory lock failed: {e}"))?;
    }
    Ok(())
}

/// Trigger gate 2 — the `external_uid` is already in use by a still-live execution:
/// short-circuit with `409 Conflict`. No-op when `external_uid` is absent (untagged triggers
/// never dedup) or when the existing execution's run is **canceled or yanked** — those
/// statuses *release* the key so a fresh trigger may reuse it. Any other status (or a missing
/// run) keeps the key in use.
///
/// The status is read from the canonical `LogicalFunctionRunDB` keyed by
/// `(execution_id, function_id)`. [`dedup_lock`] must run before this so the
/// check-then-insert cannot race.
#[instrument(
    name = "execution.reject_if_external_uid_in_use",
    skip_all,
    fields(function_id = %*function_id),
    level = "debug",
    err(Debug)
)]
pub async fn reject_if_external_uid_in_use(
    mut conn: MutRef<DomainConnection<Apiserver>>,
    function_id: Ref<FunctionId>,
    external_uid: Ref<Option<ExternalUid>>,
) -> Result<(), TdError> {
    if external_uid.is_none() {
        return Ok(());
    }
    let existing: Option<ExecutionDB> = conn
        .live()
        .fetch_optional(and!(function_id.deref(), external_uid.deref()))
        .await?;
    let Some(existing) = existing else {
        return Ok(());
    };
    // A canceled/yanked run frees the external_uid so a fresh trigger may reuse it.
    let lfr: Option<LogicalFunctionRunDB> = conn
        .live()
        .fetch_optional(and!(&existing.id, function_id.deref()))
        .await?;
    let released = matches!(
        lfr.map(|lfr| lfr.status),
        Some(LogicalFunctionRunStatus::Canceled | LogicalFunctionRunStatus::Yanked)
    );
    if released {
        return Ok(());
    }
    Err(api_error!(
        status = StatusCode::CONFLICT,
        "external_uid already in use by execution '{}'",
        existing.id
    ))
}
