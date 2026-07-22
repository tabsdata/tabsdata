//
// Copyright 2025 Tabsdata Inc.
//

use crate::system::services::status::StatusService;
use ta_services::factory::ServiceFactory;

mod status;

#[derive(ServiceFactory)]
pub struct SystemServices {
    pub status: StatusService,
}
