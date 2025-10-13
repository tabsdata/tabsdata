//
// Copyright 2025 Tabs Data Inc.
//

use crate::execution::services::callback::ExecutionCallbackService;
use crate::execution::services::cancel::ExecutionCancelService;
use crate::execution::services::execute::ExecuteFunctionService;
use crate::execution::services::list::ExecutionListService;
use crate::execution::services::read::ExecutionReadService;
use crate::execution::services::recover::ExecutionRecoverService;
use crate::execution::services::runtime_info::RuntimeInfoService;
use getset::Getters;
use ta_services::factory::ServiceFactory;

pub(crate) mod callback;
mod cancel;
pub(crate) mod execute;
mod list;
mod read;
mod recover;
pub mod runtime_info;

#[derive(ServiceFactory, Getters)]
#[getset(get = "pub")]
pub struct ExecutionServices {
    callback: ExecutionCallbackService,
    cancel: ExecutionCancelService,
    execute: ExecuteFunctionService,
    list: ExecutionListService,
    read: ExecutionReadService,
    recover: ExecutionRecoverService,
    info: RuntimeInfoService,
}
