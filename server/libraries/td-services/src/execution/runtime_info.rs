//
// Copyright 2025 Tabsdata Inc.
//

use crate::execution::services::runtime_info::RuntimeContext;
use td_error::TdError;
use td_objects::dxo::runtime_info::RuntimeInfo;

pub async fn runtime_info(runtime_context: &RuntimeContext) -> Result<RuntimeInfo, TdError> {
    Ok(runtime_context.info().clone())
}
