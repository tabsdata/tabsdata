//
// Copyright 2025 Tabs Data Inc.
//

mod callback;
mod list;
mod logs;

use crate::worker::services::callback::WorkerCallbackService;
use crate::worker::services::list::WorkerListService;
use crate::worker::services::logs::WorkerLogService;
use ta_services::factory::ServiceFactory;

#[derive(ServiceFactory)]
pub struct WorkerServices {
    pub list: WorkerListService,
    pub logs: WorkerLogService,
    pub callback: WorkerCallbackService,
}
