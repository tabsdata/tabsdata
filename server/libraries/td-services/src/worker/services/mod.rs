//
// Copyright 2025 Tabsdata Inc.
//

mod callback;
mod list;
mod logs;
mod resolve;

use crate::worker::services::callback::WorkerCallbackService;
use crate::worker::services::list::WorkerListService;
use crate::worker::services::logs::{WorkerLogService, WorkerLogTailService};
use crate::worker::services::resolve::ResolveWorkerMessageService;
use ta_services::factory::ServiceFactory;

#[derive(ServiceFactory)]
pub struct WorkerServices {
    pub list: WorkerListService,
    pub logs: WorkerLogService,
    pub logs_tail: WorkerLogTailService,
    pub callback: WorkerCallbackService,
    pub resolve_message: ResolveWorkerMessageService,
}
