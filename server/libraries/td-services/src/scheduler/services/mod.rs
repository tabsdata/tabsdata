//
// Copyright 2025 Tabs Data Inc.
//

use crate::scheduler::services::commit::ScheduleCommitService;
use crate::scheduler::services::request::ScheduleRequestService;
use ta_services::factory::ServiceFactory;

mod commit;
mod request;

#[derive(ServiceFactory)]
pub struct ScheduleServices {
    pub request: ScheduleRequestService,
    pub commit: ScheduleCommitService,
}
