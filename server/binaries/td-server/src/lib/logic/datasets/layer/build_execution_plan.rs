//
// Copyright 2024 Tabs Data Inc.
//

use chrono::{DateTime, Utc};
use td_error::TdError;
use td_execution::error::ExecutionPlannerError;
use td_execution::execution_planner::ExecutionPlan;
use td_objects::datasets::dao::{DatasetWithNames, DsExecutionPlan};
use td_objects::datasets::dto::ExecutionPlanWrite;
use td_objects::dlo::{ExecutionPlanId, RequestUserId};
use td_tower::extractors::Input;

pub async fn build_execution_plan(
    Input(dataset): Input<DatasetWithNames>,
    Input(execution_plan_id): Input<ExecutionPlanId>,
    Input(execution_plan_write): Input<ExecutionPlanWrite>,
    Input(user_id): Input<RequestUserId>,
    Input(trigger_time): Input<DateTime<Utc>>,
    Input(execution_plan): Input<ExecutionPlan>,
) -> Result<DsExecutionPlan, TdError> {
    let serialized_plan = serde_json::to_string(&execution_plan).map_err(|e| {
        ExecutionPlannerError::CouldNotSerializeExecutionPlan(execution_plan_id.to_string(), e)
    })?;

    let name = match execution_plan_write.name() {
        Some(name) => name,
        None => &execution_plan_id.to_string(),
    };

    let ds_execution_plan = DsExecutionPlan::builder()
        .id(execution_plan_id.as_str())
        .name(name)
        .collection_id(dataset.collection_id())
        .dataset_id(dataset.id())
        .function_id(dataset.current_function_id())
        .plan(serialized_plan.clone())
        .triggered_by_id(user_id.as_str())
        .triggered_on(*trigger_time)
        .build()
        .unwrap();

    Ok(ds_execution_plan)
}
