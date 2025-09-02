//
// Copyright 2025 Tabs Data Inc.
//

use crate::function_run::services::list::FunctionRunListService;
use crate::function_run::services::read::FunctionRunReadService;
use getset::Getters;
use td_tower::ServiceFactory;

mod list;
mod read;

#[derive(ServiceFactory, Getters)]
#[getset(get = "pub")]
pub struct FunctionRunServices {
    list: FunctionRunListService,
    read: FunctionRunReadService,
}
