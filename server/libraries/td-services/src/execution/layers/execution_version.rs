//
// Copyright 2025 Tabs Data Inc.
//

use std::ops::Deref;
use td_database::dynamic::DynamicConnection;
use td_error::{ApiError, TdError, api_error};
use td_objects::and;
use td_objects::dxo::execution::{
    ExecutionDB, ExecutionResponse, ExecutionTriggerResponse,
};
use td_objects::dxo::function::FunctionDBWithNames;
use td_objects::dxo::logical::LogicalFunctionRunDB;
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
use td_objects::types::basic::{ExternalUid, FunctionId, FunctionVersionId, SkipIfRunning};
use td_queries::{LfrEntry, LiveEntry};
use td_tower::default_services::Condition;
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

#[instrument(
    name = "execution.version_skip_check",
    skip_all,
    fields(function_version_id = %*function_version_id),
    level = "debug",
    err(Debug)
)]
pub async fn skip_if_running(
    mut conn: MutRef<DynamicConnection<'static>>,
    function_version_id: Ref<FunctionVersionId>,
    skip_if_running: Ref<SkipIfRunning>,
    latest_execution: Ref<Option<ExecutionDB>>,
) -> Result<Condition, TdError> {
    // The "latest attempt" for an (execution, fvid) is whichever FR the
    // LFR's pointer is currently bound to — replays leave the prior FR
    // row in `physical_function_runs` but the pointer is what consumers
    // (and this gate) should follow.
    let running = match latest_execution.as_ref() {
        Some(latest_execution) => {
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
                Some(fr) if fr.status.is_ended() => {
                    debug!(
                        physical_function_run_id = %fr.id,
                        status = ?fr.status,
                        "Latest function run not running"
                    );
                    false
                }
                Some(fr) => {
                    debug!(
                        physical_function_run_id = %fr.id,
                        status = ?fr.status,
                        "Latest function run running"
                    );
                    true
                }
                None => {
                    debug!("No bound FR for latest execution");
                    false
                }
            }
        }
        None => {
            debug!("No latest function run found");
            false
        }
    };
    Ok(Condition(running && **skip_if_running))
}

pub async fn executed_trigger_response(
    response: Ref<ExecutionResponse>,
) -> Result<ExecutionTriggerResponse, TdError> {
    let response = ExecutionTriggerResponse::Executed(Box::new(response.deref().clone()));
    Ok(response)
}

pub async fn already_running_trigger_response(
    latest_execution: Ref<ExecutionDB>,
) -> Result<ExecutionTriggerResponse, TdError> {
    let response = ExecutionTriggerResponse::AlreadyRunning(latest_execution.id.clone());
    Ok(response)
}

/// Fetch the execution that already used this `(function_id, external_uid)` for
/// deduplication. Returns `None` when `external_uid` is absent (untagged
/// triggers are never deduplicated).
#[instrument(
    name = "execution.dedup_fetch",
    skip_all,
    fields(function_id = %*function_id),
    level = "debug",
    err(Debug)
)]
pub async fn dedup_fetch(
    mut conn: MutRef<DynamicConnection<'static>>,
    function_id: Ref<FunctionId>,
    external_uid: Ref<Option<ExternalUid>>,
) -> Result<Option<ExecutionDB>, TdError> {
    let existing = match external_uid.deref() {
        Some(_) => {
            conn.live()
                .fetch_optional(and!(function_id.deref(), external_uid.deref()))
                .await?
        }
        None => None,
    };
    Ok(existing)
}

pub async fn dedup_condition(existing: Ref<Option<ExecutionDB>>) -> Result<Condition, TdError> {
    Ok(Condition(existing.is_some()))
}
