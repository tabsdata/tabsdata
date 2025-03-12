//
// Copyright 2024 Tabs Data Inc.
//

use td_error::TdError;
use td_execution::execution_planner::ExecutionTemplate;
use td_objects::datasets::dao::DsFunction;
use td_tower::extractors::Input;
use tracing::debug;

pub async fn deserialize_execution_template(
    Input(ds_function): Input<DsFunction>,
) -> Result<Option<ExecutionTemplate>, TdError> {
    let execution_template = match ds_function.execution_template() {
        Some(e) => serde_json::from_str(e).unwrap_or({
            debug!("Failed to parse execution template: {}", e);
            None
        }),
        None => None,
    };
    Ok(execution_template)
}
