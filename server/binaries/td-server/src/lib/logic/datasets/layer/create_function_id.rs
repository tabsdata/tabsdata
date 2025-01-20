//
// Copyright 2024 Tabs Data Inc.
//

use td_common::error::TdError;
use td_common::id;
use td_objects::dlo::FunctionId;

pub async fn create_function_id() -> Result<FunctionId, TdError> {
    Ok(FunctionId::new(id::id()))
}
