//
// Copyright 2025 Tabsdata Inc.
//

use ta_execution::PlanScope;
use ta_services::api_message;
use ta_services::ctx_service::ReqCtx;
use td_error::{ApiError, TdError, api_error};
use td_execution::{ExecutionPlan, ExecutionScope, TransactionBy};
use td_objects::dxo::execution::ExecutionDB;
use td_objects::dxo::function::FunctionDBWithNames;
use td_objects::types::basic::{FunctionOnly, FunctionStatus, OverrideDisabledTrigger};
use td_objects::types::recursion::RecursionOptions;

pub(crate) mod response;
pub(crate) mod rows;
pub(crate) mod versions;

pub fn assert_function_status(
    msgs: &mut ReqCtx,
    function: &FunctionDBWithNames,
    override_disabled_trigger: &OverrideDisabledTrigger,
) -> Result<(), TdError> {
    if !matches!(function.base.base.status, FunctionStatus::Active) {
        Err(api_error!(
            ApiError::InputError,
            "Function '{}' must be in an active status to execute. Current status: '{}'",
            function.base.base.name,
            function.base.base.status
        ))?
    }
    if !*function.base.base.enabled {
        if **override_disabled_trigger {
            msgs.notification(api_message!(
                "Trigger function '{}' is disabled but override_disabled_trigger is set to true",
                function.base.base.name
            ));
        } else {
            Err(api_error!(
                ApiError::InputError,
                "Function '{}' must be enabled to be executed",
                function.base.base.name,
            ))?
        }
    }
    Ok(())
}

pub fn validate_execution_plan(
    transaction_by: &TransactionBy,
    execution_plan: &ExecutionPlan,
) -> Result<(), TdError> {
    execution_plan.validate_dag_trigger()?;
    execution_plan.validate_dag_dependency()?;
    execution_plan.validate_dag_transaction(transaction_by)?;
    Ok(())
}

pub fn with_existing_recursion_options(
    execution: &ExecutionDB,
) -> Result<RecursionOptions, TdError> {
    let options = execution
        .base
        .recursion_options
        .as_deref()
        .cloned()
        .unwrap_or_else(|| {
            ExecutionScope::execution_recursion_options(
                FunctionOnly::FALSE,
                OverrideDisabledTrigger::FALSE,
            )
        });
    Ok(options)
}
