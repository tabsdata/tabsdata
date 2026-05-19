//
// Copyright 2025 Tabs Data Inc.
//

use crate::function_run::services::list::FunctionRunListService;
use crate::function_run::services::list_logical::LogicalFunctionRunListService;
use crate::function_run::services::read::FunctionRunReadService;
use crate::function_run::services::read_version_execution::FunctionVersionExecutionReadService;
use ta_services::factory::ServiceFactory;

mod list;
mod list_logical;
mod read;
mod read_version_execution;

#[derive(ServiceFactory)]
pub struct PhysicalFunctionRunServices {
    pub list: FunctionRunListService,
    pub list_logical: LogicalFunctionRunListService,
    pub read: FunctionRunReadService,
    pub read_version_execution: FunctionVersionExecutionReadService,
}
