//
// Copyright 2025. Tabs Data Inc.
//

use crate::execution::services::runtime_info::RuntimeContext;
use td_error::TdError;
use td_objects::dxo::runtime_info::RuntimeInfo;
use td_tower::extractors::SrvCtx;

pub async fn runtime_info(
    SrvCtx(runtime_context): SrvCtx<RuntimeContext>,
) -> Result<RuntimeInfo, TdError> {
    Ok(runtime_context.info().clone())
}
