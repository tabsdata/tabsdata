//
// Copyright 2026 Tabsdata Inc.
//

use crate::scheduler::v1::timed_tick;
use async_trait::async_trait;
use std::error::Error;
use std::time::Duration;
use td_common::server_runner::ServerRunnable;
use td_services::Services;
use tokio::sync::watch;
use tracing::{debug, error, trace};

type BoxError = Box<dyn Error + Send + Sync>;

pub enum WorkerRecoverer {
    V1(Box<WorkerRecovererV1>),
}

impl WorkerRecoverer {
    pub async fn new(services: &Services) -> Self {
        WorkerRecoverer::V1(Box::new(WorkerRecovererV1::new(services).await))
    }
}

#[async_trait]
impl ServerRunnable for WorkerRecoverer {
    async fn run(
        self: Box<Self>,
        shutdown: watch::Receiver<()>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match *self {
            WorkerRecoverer::V1(srv) => srv.run(shutdown).await,
        }
    }
}

/// Timer-only (no notify): recovery reacts to divergence, not to
/// scheduling events.
pub struct WorkerRecovererV1 {
    services: Services,
    check_frequency: Duration,
    tick_timeout: Duration,
}

impl WorkerRecovererV1 {
    pub async fn new(services: &Services) -> Self {
        WorkerRecovererV1 {
            services: services.clone(),
            check_frequency: services.recovery_config.check_frequency.0,
            tick_timeout: services.recovery_config.tick_timeout.0,
        }
    }

    async fn recover(&self) -> Result<(), BoxError> {
        timed_tick("recovery", self.tick_timeout, self.services.recovery(())).await?;
        Ok(())
    }
}

#[async_trait]
impl ServerRunnable for WorkerRecovererV1 {
    async fn run(
        self: Box<Self>,
        mut shutdown: watch::Receiver<()>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        // First tick immediately: cold-start convergence must not wait out
        // a full check period.
        match self.recover().await {
            Ok(_) => trace!("Worker recovery executed successfully"),
            Err(e) => error!("Error executing worker recovery: {e:#?}"),
        }
        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    debug!("Worker recovery shutting down...");
                    break;
                }
                _ = tokio::time::sleep(self.check_frequency) => debug!("Worker recovery: tick (timer)"),
            }
            match self.recover().await {
                Ok(_) => trace!("Worker recovery executed successfully"),
                Err(e) => error!("Error executing worker recovery: {e:#?}"),
            }
        }
        Ok(())
    }
}
