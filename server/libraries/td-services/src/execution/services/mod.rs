//
// Copyright 2025 Tabs Data Inc.
//

use crate::execution::services::callback::ExecutionCallbackService;
use crate::execution::services::cancel::ExecutionCancelService;
use crate::execution::services::details::ExecutionDetailsService;
use crate::execution::services::execute::ExecuteFunctionService;
use crate::execution::services::list::ExecutionListService;
use crate::execution::services::read::ExecutionReadService;
use crate::execution::services::recover::ExecutionRecoverService;
use crate::execution::services::runtime_info::RuntimeInfoService;
use ta_services::factory::ServiceFactory;

pub(crate) mod callback;
mod cancel;
mod details;
pub(crate) mod execute;
mod list;
mod read;
mod recover;
pub mod runtime_info;

#[derive(ServiceFactory)]
pub struct ExecutionServices {
    pub callback: ExecutionCallbackService,
    pub cancel: ExecutionCancelService,
    pub details: ExecutionDetailsService,
    pub execute: ExecuteFunctionService,
    pub list: ExecutionListService,
    pub read: ExecutionReadService,
    pub recover: ExecutionRecoverService,
    pub info: RuntimeInfoService,
}
