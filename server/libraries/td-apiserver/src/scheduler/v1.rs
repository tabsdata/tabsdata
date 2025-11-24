//
// Copyright 2025 Tabs Data Inc.
//

use crate::ServerRunnable;
use crate::scheduler::SchedulerCtx;
use async_trait::async_trait;
use std::error::Error;
use std::time::Duration;
use ta_services::factory::ServiceFactory;
use ta_services::service::TdService;
use td_services::scheduler::services::ScheduleServices;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider};
use tokio::sync::watch;
use tower::{BoxError, ServiceBuilder, ServiceExt};
use tracing::{Instrument, Level, debug, error, span, trace};

// TODO, bring it back to 5 seconds after adding checks following trigger/callback calls
const CHECK_FREQUENCY: Duration = Duration::from_millis(500);

pub struct RequestSchedulerV1 {
    request_service: ServiceProvider<(), (), BoxError>,
}

impl RequestSchedulerV1 {
    pub async fn new(context: &SchedulerCtx) -> Self {
        let services = ScheduleServices::build(&context.context);
        let request_service = ServiceBuilder::new()
            .buffer(1)
            .concurrency_limit(1)
            .rate_limit(1, CHECK_FREQUENCY)
            .timeout(Duration::from_secs(10))
            .service(services.request.service().await)
            .into_service_provider();
        RequestSchedulerV1 { request_service }
    }

    async fn request(&self) -> Result<(), BoxError> {
        let service = self.request_service.make().await;
        let _ = service.oneshot(()).await?;
        Ok(())
    }
}

#[async_trait]
impl ServerRunnable for RequestSchedulerV1 {
    async fn run(
        self: Box<Self>,
        shutdown: watch::Receiver<()>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        // TODO make span part of the service, not the futures
        let log_span = span!(Level::INFO, "scheduler");

        let mut shutdown_clone = shutdown.clone();
        async move {
            loop {
                tokio::select! {
                    _ = shutdown_clone.changed() => {
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
        }
        .instrument(log_span.clone())
        .await;
        Ok(())
    }
}

pub struct CommitSchedulerV1 {
    commit_service: ServiceProvider<(), (), BoxError>,
}

impl CommitSchedulerV1 {
    pub async fn new(context: &SchedulerCtx) -> Self {
        let services = ScheduleServices::build(&context.context);
        let commit_service = ServiceBuilder::new()
            .buffer(1)
            .concurrency_limit(1)
            .rate_limit(1, CHECK_FREQUENCY)
            .timeout(Duration::from_secs(10))
            .service(services.commit.service().await)
            .into_service_provider();
        CommitSchedulerV1 { commit_service }
    }

    async fn commit(&self) -> Result<(), BoxError> {
        let service = self.commit_service.make().await;
        let _ = service.oneshot(()).await?;
        Ok(())
    }
}

#[async_trait]
impl ServerRunnable for CommitSchedulerV1 {
    async fn run(
        self: Box<Self>,
        shutdown: watch::Receiver<()>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let log_span = span!(Level::INFO, "scheduler");
        let mut shutdown_clone = shutdown.clone();
        async move {
            loop {
                tokio::select! {
                    _ = shutdown_clone.changed() => {
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
        }.instrument(log_span.clone()).await;
        Ok(())
    }
}
