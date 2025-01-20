//
//  Copyright 2024 Tabs Data Inc.
//

use td_common::error::TdError;
use td_execution::execution_planner::ExecutionTemplate;
use td_tower::default_services::Condition;
use td_tower::extractors::Input;

pub async fn execution_template_exists(
    Input(_execution_template): Input<Option<ExecutionTemplate>>,
) -> Result<Condition, TdError> {
    // TODO for now, we always recalculate the execution template
    Ok(Condition(false))
    // Ok(Condition(execution_template.is_some()))
}
