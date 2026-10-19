//
// Copyright 2025 Tabsdata Inc.
//

use crate::scheduler::v1::FunctionRequesterV1;
use td_common::task_group::{BoxError, GroupTask};
use td_services::Services;
use tokio_util::sync::CancellationToken;

pub mod v1;

pub enum FunctionRequester {
    V1(Box<FunctionRequesterV1>),
}

impl FunctionRequester {
    pub async fn new(services: &Services) -> Self {
        FunctionRequester::V1(Box::new(FunctionRequesterV1::new(services).await))
    }
}

impl GroupTask for FunctionRequester {
    const NAME: &'static str = "function_requester";

    async fn run(self, shutdown: CancellationToken) -> Result<(), BoxError> {
        match self {
            FunctionRequester::V1(server) => server.run(shutdown).await,
        }
    }
}
