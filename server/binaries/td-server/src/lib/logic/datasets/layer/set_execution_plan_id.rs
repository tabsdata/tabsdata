//
// Copyright 2024 Tabs Data Inc.
//

use td_common::id::id;
use td_error::TdError;
use td_objects::dlo::ExecutionPlanId;

pub async fn set_execution_plan_id() -> Result<ExecutionPlanId, TdError> {
    Ok(ExecutionPlanId(id().to_string()))
}
