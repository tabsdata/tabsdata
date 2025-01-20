//
// Copyright 2025 Tabs Data Inc.
//

use td_common::error::TdError;
use td_execution::error::ExecutionPlannerError;
use td_execution::execution_planner::ExecutionPlan;
use td_objects::datasets::dao::DsExecutionPlan;
use td_tower::extractors::Input;

pub async fn deserialize_execution_plan(
    Input(ds_execution_plan): Input<DsExecutionPlan>,
) -> Result<ExecutionPlan, TdError> {
    let plan = serde_json::from_str(ds_execution_plan.plan()).map_err(|e| {
        ExecutionPlannerError::CouldNotDeserializeExecutionPlan(
            ds_execution_plan.id().to_string(),
            e,
        )
    })?;
    Ok(plan)
}
