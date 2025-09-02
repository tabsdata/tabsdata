//
// Copyright 2025 Tabs Data Inc.
//

use crate::scheduler::services::commit::ScheduleCommitService;
use crate::scheduler::services::request::ScheduleRequestService;
use getset::Getters;
use td_tower::ServiceFactory;

mod commit;
mod request;

#[derive(ServiceFactory, Getters)]
#[getset(get = "pub")]
pub struct ScheduleServices {
    request: ScheduleRequestService,
    commit: ScheduleCommitService,
}
