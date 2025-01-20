//
//  Copyright 2024 Tabs Data Inc.
//

use td_common::error::TdError;
use td_execution::execution_planner::ExecutionPlanWithNames;
use td_execution::graphs::{groups_into_subgraph, ExecutionDot, ExecutionGraphWithNames};
use td_objects::datasets::dao::DsExecutionPlan;
use td_objects::datasets::dto::ExecutionPlanRead;
use td_tower::extractors::Input;

pub async fn execution_plan_to_api(
    Input(ds_execution_plan): Input<DsExecutionPlan>,
    Input(graph): Input<ExecutionGraphWithNames>,
    Input(execution_plan): Input<ExecutionPlanWithNames>,
) -> Result<ExecutionPlanRead, TdError> {
    let dot = graph.graph().dot();
    let dot = groups_into_subgraph(dot);

    let (triggered_with_ids, triggered_with_names): (Vec<_>, Vec<_>) = execution_plan
        .triggers()
        .iter()
        .map(|dataset| {
            (
                dataset.dataset_uri_with_ids().to_string(),
                dataset.dataset_uri_with_names().to_string(),
            )
        })
        .unzip();

    let response = ExecutionPlanRead::builder()
        .name(ds_execution_plan.name().to_string())
        .triggered_datasets_with_ids(triggered_with_ids)
        .triggered_datasets_with_names(triggered_with_names)
        .dot(dot)
        .build()
        .unwrap();
    Ok(response)
}
