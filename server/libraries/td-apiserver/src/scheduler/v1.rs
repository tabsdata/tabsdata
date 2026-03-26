//
// Copyright 2025 Tabs Data Inc.
//

use crate::scheduler::SchedulerCtx;
use async_trait::async_trait;
use std::error::Error;
use std::time::Duration;
use ta_services::factory::ServiceFactory;
use ta_services::service::TdService;
use td_common::server_runner::ServerRunnable;
use td_services::scheduler::services::FunctionSchedulerServices;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider};
use tokio::sync::watch;
use tower::{BoxError, ServiceBuilder, ServiceExt};
use tracing::{debug, error, trace};

// TODO, bring it back to 5 seconds after adding checks following trigger/callback calls
const CHECK_FREQUENCY: Duration = Duration::from_millis(500);

pub struct FunctionRequesterV1 {
    request_service: ServiceProvider<(), (), BoxError>,
}

impl FunctionRequesterV1 {
    pub async fn new(context: &SchedulerCtx) -> Self {
        let services = FunctionSchedulerServices::build(&context.context);
        let request_service = ServiceBuilder::new()
            .buffer(1)
            .concurrency_limit(1)
            .rate_limit(1, CHECK_FREQUENCY)
            .timeout(Duration::from_secs(10))
            .service(services.request.service())
            .into_service_provider();
        FunctionRequesterV1 { request_service }
    }

    async fn request(&self) -> Result<(), BoxError> {
        let service = self.request_service.make();
        let _ = service.oneshot(()).await?;
        Ok(())
    }
}

#[async_trait]
impl ServerRunnable for FunctionRequesterV1 {
    async fn run(
        self: Box<Self>,
        mut shutdown: watch::Receiver<()>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    debug!("Execution plan request scheduler shutting down...");
                    break;
                }
                res = self.request() => {
                    match res {
                        Ok(_) => trace!("Execution plan scheduler executed successfully"),
                        Err(e) => error!("Error executing execution plan request scheduler: {e}"),
                    }
                }
            }
        }
        Ok(())
    }
}

pub struct FunctionDispatcherV1 {
    commit_service: ServiceProvider<(), (), BoxError>,
}

impl FunctionDispatcherV1 {
    pub async fn new(context: &SchedulerCtx) -> Self {
        let services = FunctionSchedulerServices::build(&context.context);
        let commit_service = ServiceBuilder::new()
            .buffer(1)
            .concurrency_limit(1)
            .rate_limit(1, CHECK_FREQUENCY)
            .timeout(Duration::from_secs(10))
            .service(services.dispatch.service())
            .into_service_provider();
        FunctionDispatcherV1 { commit_service }
    }

    async fn commit(&self) -> Result<(), BoxError> {
        let service = self.commit_service.make();
        let _ = service.oneshot(()).await?;
        Ok(())
    }
}

#[async_trait]
impl ServerRunnable for FunctionDispatcherV1 {
    async fn run(
        self: Box<Self>,
        mut shutdown: watch::Receiver<()>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    debug!("Execution plan commit scheduler shutting down...");
                    break;
                }
                res = self.commit() => {
                    match res {
                        Ok(_) => trace!("Execution plan commit scheduler executed successfully"),
                        Err(e) => error!("Error executing execution plan commit scheduler: {e}"),
                    }
                }
            }
        }
        Ok(())
    }
}
