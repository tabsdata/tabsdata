//
//  Copyright 2024 Tabs Data Inc.
//

use td_common::error::TdError;
use td_execution::error::ExecutionPlannerError;
use td_execution::execution_planner::ExecutionTemplate;
use td_tower::extractors::Input;

pub async fn unwrap_execution_template(
    Input(execution_template): Input<Option<ExecutionTemplate>>,
) -> Result<ExecutionTemplate, TdError> {
    match &*execution_template {
        Some(execution_template) => Ok(execution_template.clone()),
        None => Err(ExecutionPlannerError::MissingExecutionTemplate.into()),
    }
}
