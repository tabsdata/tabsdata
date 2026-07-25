//
// Copyright 2025 Tabsdata Inc.
//

use crate::scheduler::v1::{FunctionDispatcherV1, FunctionRequesterV1};
use async_trait::async_trait;
use std::sync::Arc;
use td_bridge::BridgeProviderHandle;
use td_common::jwt::JwtConfig;
use td_common::server_runner::ServerRunnable;
use td_database::pool::TdPool;
use td_execution::{SchedulerConfig, SchedulerNotify};
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
        bridge: Arc<BridgeProviderHandle>,
        internal_addresses: Arc<InternalServerAddresses>,
        scheduler_notify: Arc<SchedulerNotify>,
        jwt_config: Arc<JwtConfig>,
        scheduler_config: Arc<SchedulerConfig>,
    ) -> Self {
        let context = SchedulerContext {
            db,
            storage,
            bridge,
            internal_addresses,
            scheduler_notify,
            jwt_config,
            scheduler_config,
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
