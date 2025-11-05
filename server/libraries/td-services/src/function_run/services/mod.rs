//
// Copyright 2025 Tabs Data Inc.
//

use crate::function_run::services::list::FunctionRunListService;
use crate::function_run::services::read::FunctionRunReadService;
use ta_services::factory::ServiceFactory;

mod list;
mod read;

#[derive(ServiceFactory)]
pub struct FunctionRunServices {
    pub list: FunctionRunListService,
    pub read: FunctionRunReadService,
}
