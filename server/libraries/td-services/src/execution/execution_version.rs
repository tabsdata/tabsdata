//
// Copyright 2025 Tabsdata Inc.
//

use http::StatusCode;
use td_database::access::{AccessMode, SeesProjectRows};
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
use td_queries::Queries;
use tracing::{debug, instrument};

pub fn assert_function_version(
    function_version_id: &FunctionVersionId,
    function: &FunctionDBWithNames,
) -> Result<(), TdError> {
    if function.base.base.id != *function_version_id {
        Err(api_error!(
            ApiError::InputError,
            r#"Function version missmatch. There is a newer version of the function.
            Requested function version '{}' must be the current version of the function '{}'."#,
            function_version_id,
            function.base.base.id
        ))?
    }
    Ok(())
}

/// Trigger gate 1 — `skip_if_running=true` and the trigger function is currently running:
/// short-circuit with `425 Too Early` rather than starting a concurrent run. No-op when the
/// flag is unset, there is no prior execution, or the latest run has ended.
///
/// The "latest attempt" for an (execution, fvid) is the LFR's current FR — replays leave
/// the prior FR rows in `physical_function_runs` as closed versions.
#[instrument(
    name = "execution.reject_if_running",
    skip_all,
    fields(function_version_id = %*function_version_id),
    level = "debug",
    err(Debug, level = "debug")
)]
pub async fn reject_if_running<S: SeesProjectRows, M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    skip_if_running: &SkipIfRunning,
    latest_execution: &Option<ExecutionDB>,
) -> Result<(), TdError> {
    if !**skip_if_running {
        return Ok(());
    }
    let Some(latest_execution) = latest_execution.as_ref() else {
        debug!("No latest execution found");
        return Ok(());
    };
    let lfr: Option<LogicalFunctionRunDB> = conn
        .fetch_optional(and!(
            &latest_execution.base.id,
            &latest_execution.base.function_id,
        ))
        .await?;
    let physical_function_run: Option<PhysicalFunctionRunDB> = match lfr {
        Some(lfr) => {
            conn.scd2()
                .fetch_optional::<PhysicalFunctionRunDB>(&lfr.base.id)
                .await?
        }
        None => None,
    };
    match physical_function_run {
        Some(fr) if !fr.base.status.is_ended() => {
            debug!(physical_function_run_id = %fr.base.id, status = ?fr.base.status, "Latest function run running");
            Err(api_error!(
                status = StatusCode::TOO_EARLY,
                "Function is currently running (execution '{}'); retry once it finishes",
                latest_execution.base.id
            ))
        }
        _ => Ok(()),
    }
}

/// Trigger gate 2 — the `external_uid` is already in use by a still-live execution:
/// short-circuit with `409 Conflict`. No-op when `external_uid` is absent (untagged triggers
/// never dedup) or when the existing execution's run is **canceled or yanked** — those
/// statuses *release* the key so a fresh trigger may reuse it. Any other status (or a missing
/// run) keeps the key in use.
///
/// The status is read from the canonical `LogicalFunctionRunDB` keyed by
/// `(execution_id, function_id)`. Check-then-insert cannot race: planning runs
/// on an exclusive connection, which holds the project's lock for the whole
/// transaction, and a function belongs to one project.
#[instrument(
    name = "execution.reject_if_external_uid_in_use",
    skip_all,
    fields(function_id = %*function_id),
    level = "debug",
    err(Debug, level = "debug")
)]
pub async fn reject_if_external_uid_in_use<S: SeesProjectRows, M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    function_id: &FunctionId,
    external_uid: &Option<ExternalUid>,
) -> Result<(), TdError> {
    let Some(external_uid) = external_uid else {
        return Ok(());
    };
    let existing: Option<ExecutionDB> =
        conn.fetch_optional(and!(function_id, external_uid)).await?;
    let Some(existing) = existing else {
        return Ok(());
    };
    // A canceled/yanked run frees the external_uid so a fresh trigger may reuse it.
    let lfr: Option<LogicalFunctionRunDB> = conn
        .fetch_optional(and!(&existing.base.id, function_id))
        .await?;
    let released = matches!(
        lfr.map(|lfr| lfr.base.status),
        Some(LogicalFunctionRunStatus::Canceled | LogicalFunctionRunStatus::Yanked)
    );
    if released {
        return Ok(());
    }
    Err(api_error!(
        status = StatusCode::CONFLICT,
        "external_uid already in use by execution '{}'",
        existing.base.id
    ))
}
