//
// Copyright 2025 Tabs Data Inc.
//

use crate::ServerRunnable;
use crate::scheduler::v1::{CommitSchedulerV1, RequestSchedulerV1};
use async_trait::async_trait;
use std::sync::Arc;
use td_common::server::FileWorkerMessageQueue;
use td_database::sql::DbPool;
use td_objects::sql::DaoQueries;
use td_objects::types::addresses::InternalServerAddresses;
use td_services::SchedulerContext;
use td_storage::Storage;
use tokio::sync::watch;

pub mod v1;

pub struct SchedulerCtx {
    pub context: SchedulerContext,
}

impl SchedulerCtx {
    pub fn new(
        db: DbPool,
        queries: Arc<DaoQueries>,
        storage: Arc<Storage>,
        worker_queue: Arc<FileWorkerMessageQueue>,
        internal_addresses: Arc<InternalServerAddresses>,
    ) -> Self {
        let context = SchedulerContext {
            db,
            queries,
            storage,
            worker_queue,
            internal_addresses,
        };

        Self { context }
    }
}

pub enum RequestScheduler {
    V1(Box<RequestSchedulerV1>),
}

impl RequestScheduler {
    pub async fn new(context: &SchedulerCtx) -> Self {
        RequestScheduler::V1(Box::new(RequestSchedulerV1::new(context).await))
    }
}

#[async_trait]
impl ServerRunnable for RequestScheduler {
    async fn run(
        self: Box<Self>,
        shutdown: watch::Receiver<()>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match *self {
            RequestScheduler::V1(srv) => srv.run(shutdown).await,
        }
    }
}

pub enum CommitScheduler {
    V1(Box<CommitSchedulerV1>),
}

impl CommitScheduler {
    pub async fn new(context: &SchedulerCtx) -> Self {
        CommitScheduler::V1(Box::new(CommitSchedulerV1::new(context).await))
    }
}

#[async_trait]
impl ServerRunnable for CommitScheduler {
    async fn run(
        self: Box<Self>,
        shutdown: watch::Receiver<()>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match *self {
            CommitScheduler::V1(srv) => srv.run(shutdown).await,
        }
    }
}
