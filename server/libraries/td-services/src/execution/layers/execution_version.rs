//
// Copyright 2025 Tabs Data Inc.
//

use std::ops::Deref;
use td_error::{ApiError, TdError, api_error};
use td_objects::dxo::crudl::handle_sql_err;
use td_objects::dxo::execution::{ExecutionDB, ExecutionResponse, ExecutionResponseInternal};
use td_objects::dxo::function::FunctionDBWithNames;
use td_objects::dxo::function_run::FunctionRunDB;
use td_objects::sql::{DaoQueries, SelectBy};
use td_objects::types::basic::{FunctionRunStatus, FunctionVersionId, SkipIfRunning};
use td_tower::default_services::Condition;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection, SrvCtx};
use tracing::debug;

pub async fn assert_function_version(
    Input(function_version_id): Input<FunctionVersionId>,
    Input(function): Input<FunctionDBWithNames>,
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
    Connection(conn): Connection,
    SrvCtx(queries): SrvCtx<DaoQueries>,
    Input(function_version_id): Input<FunctionVersionId>,
    Input(skip_if_running): Input<SkipIfRunning>,
    Input(latest_execution): Input<Option<ExecutionDB>>,
) -> Result<Condition, TdError> {
    let running = match latest_execution.as_ref() {
        Some(latest_execution) => {
            let mut conn = conn.lock().await;
            let conn = conn.get_mut_connection()?;

            let function_run: FunctionRunDB = queries
                .select_by::<FunctionRunDB>(&(*function_version_id, latest_execution.id.clone()))?
                .build_query_as()
                .fetch_one(conn)
                .await
                .map_err(handle_sql_err)?;

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
    Input(response): Input<ExecutionResponse>,
) -> Result<ExecutionResponseInternal, TdError> {
    let response = ExecutionResponseInternal::Executed(Box::new(response.deref().clone()));
    Ok(response)
}

pub async fn already_running_execute_internal_response(
    Input(latest_execution): Input<ExecutionDB>,
) -> Result<ExecutionResponseInternal, TdError> {
    let response = ExecutionResponseInternal::AlreadyRunning(latest_execution.id.clone());
    Ok(response)
}
