//
// Copyright 2025 Tabs Data Inc.
//

use crate::scheduler::v1::{FunctionDispatcherV1, FunctionRequesterV1};
use async_trait::async_trait;
use std::sync::Arc;
use td_common::server::WorkerMessageQueueHandle;
use td_common::server_runner::ServerRunnable;
use td_database::pool::TdPool;
use td_execution::SchedulerNotify;
use td_objects::types::addresses::InternalServerAddresses;
use td_services::scheduler::services::SchedulerContext;
use td_storage::Storage;
use tokio::sync::watch;

pub mod v1;

pub struct SchedulerCtx {
    pub context: SchedulerContext,
}

impl SchedulerCtx {
    pub fn new(
        db: TdPool,
        storage: Arc<Storage>,
        worker_queue: Arc<WorkerMessageQueueHandle>,
        internal_addresses: Arc<InternalServerAddresses>,
        scheduler_notify: SchedulerNotify,
    ) -> Self {
        let scheduler_notify = Arc::new(scheduler_notify);
        let context = SchedulerContext {
            db,
            storage,
            worker_queue,
            internal_addresses,
            scheduler_notify: scheduler_notify.clone(),
        };

        Self { context }
    }
}

pub enum FunctionRequester {
    V1(Box<FunctionRequesterV1>),
}

impl FunctionRequester {
    pub async fn new(context: &SchedulerCtx) -> Self {
        FunctionRequester::V1(Box::new(FunctionRequesterV1::new(context).await))
    }
}

#[async_trait]
impl ServerRunnable for FunctionRequester {
    async fn run(
        self: Box<Self>,
        shutdown: watch::Receiver<()>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match *self {
            FunctionRequester::V1(srv) => srv.run(shutdown).await,
        }
    }
}

pub enum FunctionDispatcher {
    V1(Box<FunctionDispatcherV1>),
}

impl FunctionDispatcher {
    pub async fn new(context: &SchedulerCtx) -> Self {
        FunctionDispatcher::V1(Box::new(FunctionDispatcherV1::new(context).await))
    }
}

#[async_trait]
impl ServerRunnable for FunctionDispatcher {
    async fn run(
        self: Box<Self>,
        shutdown: watch::Receiver<()>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match *self {
            FunctionDispatcher::V1(srv) => srv.run(shutdown).await,
        }
    }
}
