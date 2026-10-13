//
// Copyright 2025 Tabsdata Inc.
//

use async_trait::async_trait;
use std::error::Error;
use std::future::Future;
use std::time::{Duration, Instant};
use td_common::server_runner::ServerRunnable;
use td_execution::SchedulerNotify;
use td_services::Services;
use tokio::sync::watch;
use tracing::{debug, error, trace, warn};

type BoxError = Box<dyn Error + Send + Sync>;

pub struct FunctionRequesterV1 {
    services: Services,
    notify: SchedulerNotify,
    // Failover safety net — `SchedulerNotify` wakes the loop on status
    // changes; the timer also bounds the stuck-run sweep cadence.
    check_frequency: Duration,
    tick_timeout: Duration,
}

impl FunctionRequesterV1 {
    pub async fn new(services: &Services) -> Self {
        FunctionRequesterV1 {
            services: services.clone(),
            notify: (*services.scheduler_notify).clone(),
            check_frequency: services.scheduler_config.check_frequency.0,
            tick_timeout: services.scheduler_config.tick_timeout.0,
        }
    }

    async fn request(&self) -> Result<(), BoxError> {
        timed_tick("request", self.tick_timeout, self.services.request(())).await?;
        Ok(())
    }
}

/// Runs one tick under `tick_timeout` (the loop is sequential, so a pass that
/// overruns is abandoned and the next tick retries), warning when it consumes
/// half its budget — the early signal that the scheduler is heading for
/// timeout cancellations (e.g. a starved connection pool or a slow
/// orchestrator backend).
pub(crate) async fn timed_tick<T, E: Into<BoxError>>(
    tick: &str,
    tick_timeout: Duration,
    fut: impl Future<Output = Result<T, E>>,
) -> Result<T, BoxError> {
    let started = Instant::now();
    let result = match tokio::time::timeout(tick_timeout, fut).await {
        Ok(res) => res.map_err(Into::into),
        Err(_elapsed) => Err(format!("scheduler {tick} tick timed out").into()),
    };
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
