//
// Copyright 2025 Tabsdata Inc.
//

use crate::execution::services::callback::ExecutionCallbackService;
use crate::execution::services::cancel::ExecutionCancelService;
use crate::execution::services::details::ExecutionDetailsService;
use crate::execution::services::execute::ExecuteFunctionService;
use crate::execution::services::execute_version::ExecuteFunctionVersionService;
use crate::execution::services::list::ExecutionListService;
use crate::execution::services::list_by_function_version::ExecutionListByFunctionVersionService;
use crate::execution::services::list_by_physical_table_data_version::ExecutionListByPhysicalTableDataVersionService;
use crate::execution::services::read::ExecutionReadService;
use crate::execution::services::recover::ExecutionRecoverService;
use crate::execution::services::runtime_info::RuntimeInfoService;
use ta_services::factory::ServiceFactory;
use td_objects::dxo::function::FunctionDB;

pub(crate) mod callback;
mod cancel;
mod details;
pub(crate) mod execute;
pub mod execute_version;
mod list;
mod list_by_function_version;
mod list_by_physical_table_data_version;
mod read;
mod recover;
pub mod runtime_info;

#[derive(ServiceFactory)]
pub struct ExecutionServices {
    pub callback: ExecutionCallbackService,
    pub cancel: ExecutionCancelService,
    pub details: ExecutionDetailsService,
    pub execute: ExecuteFunctionService,
    pub execute_version: ExecuteFunctionVersionService,
    pub list: ExecutionListService,
    pub list_by_function_version: ExecutionListByFunctionVersionService,
    pub list_by_physical_table_data_version: ExecutionListByPhysicalTableDataVersionService,
    pub read: ExecutionReadService<{ FunctionDB::Executable }>,
    pub read_with_disabled: ExecutionReadService<{ FunctionDB::Active }>,
    pub recover: ExecutionRecoverService,
    pub info: RuntimeInfoService,
}
