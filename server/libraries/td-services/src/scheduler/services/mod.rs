//
// Copyright 2025 Tabs Data Inc.
//

use crate::scheduler::services::dispatch::DispatchService;
use crate::scheduler::services::request::RequestService;
use axum::extract::FromRef;
use std::sync::Arc;
use ta_services::factory::{FieldAccessors, ServiceFactory};
#[cfg(feature = "test-utils")]
use td_common::server::FileSupervisorMessageQueue;
use td_common::server::OrchestratorMessageQueueHandle;
use td_database::pool::TdPool;
use td_execution::SchedulerNotify;
use td_objects::types::addresses::InternalServerAddresses;
use td_storage::Storage;

mod dispatch;
mod request;

#[derive(ServiceFactory)]
pub struct FunctionSchedulerServices {
    pub request: RequestService,
    pub dispatch: DispatchService,
}

#[derive(FieldAccessors, FromRef, Clone)]
pub struct SchedulerContext {
    pub db: TdPool,
    pub storage: Arc<Storage>,
    pub orchestrator_queue: Arc<OrchestratorMessageQueueHandle>,
    pub internal_addresses: Arc<InternalServerAddresses>,
    pub scheduler_notify: Arc<SchedulerNotify>,
}

#[cfg(feature = "test-utils")]
impl SchedulerContext {
    pub fn with_defaults(db: TdPool) -> Self {
        Self {
            db,
            storage: Arc::new(Storage::default()),
            orchestrator_queue: Arc::new(OrchestratorMessageQueueHandle::new(Arc::new(
                FileSupervisorMessageQueue::default(),
            ))),
            internal_addresses: Arc::new(InternalServerAddresses::default()),
            scheduler_notify: Arc::new(SchedulerNotify::new()),
        }
    }
}
