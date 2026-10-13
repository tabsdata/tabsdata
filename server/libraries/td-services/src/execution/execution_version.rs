//
// Copyright 2025 Tabsdata Inc.
//

use http::StatusCode;
use td_database::access::{AccessMode, SeesProjectRows, XW};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::{ApiError, TdError, api_error};
use td_objects::and;
use td_objects::dxo::execution::{ExecutionDB, ExecutionExternalUidReleaseDB, TriggerResponse};
use td_objects::dxo::function::FunctionDBWithNames;
use td_objects::dxo::logical::LogicalFunctionRunDB;
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
use td_objects::types::basic::TriggerOutcome;
use td_objects::types::basic::{
    ExecutionId, ExternalUid, FunctionId, FunctionVersionId, LogicalFunctionRunStatus,
    SkipIfRunning,
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

/// Trigger gate 1: with `skip_if_running`, the function's latest execution
/// whose trigger run has not ended. `None` when the flag is unset, there is no
/// prior execution, or its latest attempt has ended.
///
/// The "latest attempt" for an (execution, fvid) is the LFR's current FR — replays leave
/// the prior FR rows in `physical_function_runs` as closed versions.
#[instrument(
    name = "execution.running_execution",
    skip_all,
    level = "debug",
    err(Debug, level = "debug")
)]
pub async fn running_execution<S: SeesProjectRows, M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    skip_if_running: &SkipIfRunning,
    latest_execution: &Option<ExecutionDB>,
) -> Result<Option<ExecutionId>, TdError> {
    if !**skip_if_running {
        return Ok(None);
    }
    let Some(latest_execution) = latest_execution.as_ref() else {
        debug!("No latest execution found");
        return Ok(None);
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
            Ok(Some(latest_execution.base.id.clone()))
        }
        _ => Ok(None),
    }
}

/// Trigger gate 2: the execution holding the trigger's `external_uid`, as
/// `Consumed` when its trigger run committed and `Held` otherwise. `None`
/// when there is no key (untagged triggers never dedup) or no holder. A
/// canceled or yanked run releases its key: the old execution gives it up
/// here, so the new one can take it under the unique index.
///
/// Check-then-write cannot race: planning runs on an exclusive connection,
/// which holds the project's lock for the whole transaction, and a function
/// belongs to one project.
#[instrument(
    name = "execution.external_uid_holder",
    skip_all,
    fields(function_id = %*function_id),
    level = "debug",
    err(Debug, level = "debug")
)]
pub async fn external_uid_holder<S: SeesProjectRows>(
    conn: &mut DomainConnection<Apiserver, S, XW>,
    function_id: &FunctionId,
    external_uid: &Option<ExternalUid>,
) -> Result<Option<TriggerResponse>, TdError> {
    let Some(external_uid) = external_uid else {
        return Ok(None);
    };
    let existing: Option<ExecutionDB> =
        conn.fetch_optional(and!(function_id, external_uid)).await?;
    let Some(existing) = existing else {
        return Ok(None);
    };
    let lfr: Option<LogicalFunctionRunDB> = conn
        .fetch_optional(and!(&existing.base.id, function_id))
        .await?;
    let outcome = match lfr.map(|lfr| lfr.base.status) {
        Some(LogicalFunctionRunStatus::Canceled | LogicalFunctionRunStatus::Yanked) => {
            conn.update::<ExecutionExternalUidReleaseDB>(
                &ExecutionExternalUidReleaseDB::released(),
                &existing.base.id,
            )
            .await?;
            return Ok(None);
        }
        Some(LogicalFunctionRunStatus::Committed) => TriggerOutcome::Consumed,
        _ => TriggerOutcome::Held,
    };
    Ok(Some(TriggerResponse {
        outcome,
        execution_id: existing.base.id,
    }))
}

/// The public execute answer for a trigger a gate stopped: 425 while the
/// function runs, 409 while its key is held or consumed.
pub fn refusal(refused: &TriggerResponse) -> TdError {
    match refused.outcome {
        TriggerOutcome::Busy => api_error!(
            status = StatusCode::TOO_EARLY,
            "Function is currently running (execution '{}'); retry once it finishes",
            refused.execution_id
        ),
        _ => api_error!(
            status = StatusCode::CONFLICT,
            "external_uid already in use by execution '{}'",
            refused.execution_id
        ),
    }
}
