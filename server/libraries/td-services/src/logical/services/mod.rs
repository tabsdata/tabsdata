//
// Copyright 2026 Tabs Data Inc.
//

use crate::logical::services::replay::ReplayLogicalFunctionRunService;
use getset::Getters;
use ta_services::factory::ServiceFactory;

pub(crate) mod replay;

#[derive(ServiceFactory, Getters)]
#[getset(get = "pub")]
pub struct LogicalServices {
    pub replay: ReplayLogicalFunctionRunService,
}
