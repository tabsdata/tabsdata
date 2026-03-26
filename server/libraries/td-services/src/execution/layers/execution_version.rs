//
// Copyright 2025 Tabs Data Inc.
//

use std::ops::Deref;
use td_database::dynamic::DynamicConnection;
use td_error::{ApiError, TdError, api_error};
use td_objects::and;
use td_objects::dxo::execution::{ExecutionDB, ExecutionResponse, ExecutionResponseInternal};
use td_objects::dxo::function::FunctionDBWithNames;
use td_objects::dxo::function_run::FunctionRunDB;
use td_objects::types::basic::{FunctionRunStatus, FunctionVersionId, SkipIfRunning};
use td_queries::BasicQueries;
use td_tower::default_services::Condition;
use td_tower::extractors::{MutRef, Ref};
use tracing::debug;

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

pub async fn skip_if_running(
    mut conn: MutRef<DynamicConnection<'static>>,
    function_version_id: Ref<FunctionVersionId>,
    skip_if_running: Ref<SkipIfRunning>,
    latest_execution: Ref<Option<ExecutionDB>>,
) -> Result<Condition, TdError> {
    let running = match latest_execution.as_ref() {
        Some(latest_execution) => {
            let function_run = conn
                .fetch_one::<FunctionRunDB>(and!(function_version_id.deref(), &latest_execution.id))
                .await?;

            match function_run.status {
                FunctionRunStatus::Committed
                | FunctionRunStatus::Canceled
                | FunctionRunStatus::Yanked => {
                    debug!(
                        "Latest function run {} is not running, with status {}",
                        function_run.id, function_run.status
                    );
                    false
                }
                _ => {
                    debug!(
                        "Latest function run {} is running, with status {}",
                        function_run.id, function_run.status
                    );
                    true
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

pub async fn executed_execute_internal_response(
    response: Ref<ExecutionResponse>,
) -> Result<ExecutionResponseInternal, TdError> {
    let response = ExecutionResponseInternal::Executed(Box::new(response.deref().clone()));
    Ok(response)
}

pub async fn already_running_execute_internal_response(
    latest_execution: Ref<ExecutionDB>,
) -> Result<ExecutionResponseInternal, TdError> {
    let response = ExecutionResponseInternal::AlreadyRunning(latest_execution.id.clone());
    Ok(response)
}
