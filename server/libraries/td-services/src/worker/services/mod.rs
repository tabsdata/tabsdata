//
// Copyright 2025 Tabs Data Inc.
//

mod list;
mod logs;

use crate::worker::services::list::WorkerListService;
use crate::worker::services::logs::WorkerLogService;
use getset::Getters;
use td_tower::ServiceFactory;

#[derive(ServiceFactory, Getters)]
#[getset(get = "pub")]
pub struct WorkerServices {
    list: WorkerListService,
    logs: WorkerLogService,
}
