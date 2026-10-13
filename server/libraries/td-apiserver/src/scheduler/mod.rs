//
// Copyright 2025 Tabsdata Inc.
//

use crate::scheduler::v1::FunctionRequesterV1;
use async_trait::async_trait;
use std::error::Error;
use td_common::server_runner::ServerRunnable;
use td_services::Services;
use tokio::sync::watch;

pub mod v1;

pub enum FunctionRequester {
    V1(Box<FunctionRequesterV1>),
}

impl FunctionRequester {
    pub async fn new(services: &Services) -> Self {
        FunctionRequester::V1(Box::new(FunctionRequesterV1::new(services).await))
    }
}

#[async_trait]
impl ServerRunnable for FunctionRequester {
    async fn run(
        self: Box<Self>,
        shutdown: watch::Receiver<()>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        match *self {
            FunctionRequester::V1(server) => server.run(shutdown).await,
        }
    }
}
