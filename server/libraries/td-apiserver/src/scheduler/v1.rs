//
// Copyright 2025 Tabsdata Inc.
//

use crate::scheduler::SchedulerCtx;
use async_trait::async_trait;
use std::error::Error;
use std::future::Future;
use std::time::{Duration, Instant};
use ta_services::factory::ServiceFactory;
use ta_services::service::TdService;
use td_common::server_runner::ServerRunnable;
use td_execution::SchedulerNotify;
use td_services::scheduler::services::FunctionSchedulerServices;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider};
use tokio::sync::watch;
use tower::{BoxError, ServiceBuilder, ServiceExt};
use tracing::{debug, error, trace, warn};

pub struct FunctionRequesterV1 {
    request_service: ServiceProvider<(), (), BoxError>,
    notify: SchedulerNotify,
    // Failover safety net — `SchedulerNotify` wakes the loop on status
    // changes; the timer also bounds the stuck-run sweep cadence.
    check_frequency: Duration,
    tick_timeout: Duration,
}

impl FunctionRequesterV1 {
    pub async fn new(context: &SchedulerCtx) -> Self {
        let services = FunctionSchedulerServices::build(&context.context);
        let tick_timeout = context.context.scheduler_config.tick_timeout.0;
        let request_service = ServiceBuilder::new()
            .buffer(1)
            .concurrency_limit(1)
            .timeout(tick_timeout)
            .service(services.request.service())
            .into_service_provider();
        FunctionRequesterV1 {
            request_service,
            notify: (*context.context.scheduler_notify).clone(),
            check_frequency: context.context.scheduler_config.check_frequency.0,
            tick_timeout,
        }
    }

    async fn request(&self) -> Result<(), BoxError> {
        let service = self.request_service.make();
        let result = timed_tick("request", self.tick_timeout, service.oneshot(())).await;
        let _ = result?;
        Ok(())
    }
}

/// Runs one tick, warning when it consumes half its `tick_timeout` budget —
/// the early signal that the scheduler is heading for `Elapsed` cancellations
/// (e.g. a starved connection pool or a slow orchestrator backend).
pub(crate) async fn timed_tick<T>(
    tick: &str,
    tick_timeout: Duration,
    fut: impl Future<Output = Result<T, BoxError>>,
) -> Result<T, BoxError> {
    let started = Instant::now();
    let result = fut.await;
    let elapsed = started.elapsed();
    if elapsed >= tick_timeout / 2 {
        warn!(
            tick,
            elapsed_ms = elapsed.as_millis() as u64,
            tick_timeout_ms = tick_timeout.as_millis() as u64,
            "slow scheduler tick"
        );
    }
    result
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
                _ = self.notify.notified_scheduler() => debug!("Request scheduler: tick (notify)"),
                _ = tokio::time::sleep(self.check_frequency) => debug!("Request scheduler: tick (timer)"),
            }
            match self.request().await {
                Ok(_) => trace!("Execution plan scheduler executed successfully"),
                Err(e) => error!("Error executing execution plan request scheduler: {e:#?}"),
            }
        }
        Ok(())
    }
}

pub struct FunctionDispatcherV1 {
    commit_service: ServiceProvider<(), (), BoxError>,
    notify: SchedulerNotify,
    check_frequency: Duration,
    tick_timeout: Duration,
}

impl FunctionDispatcherV1 {
    pub async fn new(context: &SchedulerCtx) -> Self {
        let services = FunctionSchedulerServices::build(&context.context);
        let tick_timeout = context.context.scheduler_config.tick_timeout.0;
        let commit_service = ServiceBuilder::new()
            .buffer(1)
            .concurrency_limit(1)
            .timeout(tick_timeout)
            .service(services.dispatch.service())
            .into_service_provider();
        FunctionDispatcherV1 {
            commit_service,
            notify: (*context.context.scheduler_notify).clone(),
            check_frequency: context.context.scheduler_config.check_frequency.0,
            tick_timeout,
        }
    }

    async fn commit(&self) -> Result<(), BoxError> {
        let service = self.commit_service.make();
        let result = timed_tick("dispatch", self.tick_timeout, service.oneshot(())).await;
        let _ = result?;
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
                _ = self.notify.notified_dispatcher() => debug!("Dispatch scheduler: tick (notify)"),
                _ = tokio::time::sleep(self.check_frequency) => debug!("Dispatch scheduler: tick (timer)"),
            }
            match self.commit().await {
                Ok(_) => trace!("Execution plan commit scheduler executed successfully"),
                Err(e) => error!("Error executing execution plan commit scheduler: {e:#?}"),
            }
        }
        Ok(())
    }
}
