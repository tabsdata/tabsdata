//
// Copyright 2025 Tabs Data Inc.
//

use crate::system::services::status::StatusService;
use getset::Getters;
use td_tower::ServiceFactory;

mod status;

#[derive(ServiceFactory, Getters)]
#[getset(get = "pub")]
pub struct SystemServices {
    status: StatusService,
}
