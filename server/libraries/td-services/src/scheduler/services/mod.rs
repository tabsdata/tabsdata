//
// Copyright 2025 Tabs Data Inc.
//

use crate::scheduler::services::dispatch::DispatchService;
use crate::scheduler::services::request::RequestService;
use axum::extract::FromRef;
use std::sync::Arc;
use ta_services::factory::{FieldAccessors, ServiceFactory};
use td_common::server::FileWorkerMessageQueue;
use td_database::pool::TdPool;
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
    pub worker_queue: Arc<FileWorkerMessageQueue>,
    pub internal_addresses: Arc<InternalServerAddresses>,
}

#[cfg(feature = "test-utils")]
impl SchedulerContext {
    pub fn with_defaults(db: TdPool) -> Self {
        Self {
            db,
            storage: Arc::new(Storage::default()),
            worker_queue: Arc::new(FileWorkerMessageQueue::default()),
            internal_addresses: Arc::new(InternalServerAddresses::default()),
        }
    }
}
