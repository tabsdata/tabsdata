//
// Copyright 2025 Tabs Data Inc.
//

use crate::function_run::services::list::FunctionRunListService;
use crate::function_run::services::read::FunctionRunReadService;
use crate::function_run::services::read_version_execution::FunctionVersionExecutionReadService;
use ta_services::factory::ServiceFactory;

mod list;
mod read;
mod read_version_execution;

#[derive(ServiceFactory)]
pub struct FunctionRunServices {
    pub list: FunctionRunListService,
    pub read: FunctionRunReadService,
    pub read_version_execution: FunctionVersionExecutionReadService,
}
