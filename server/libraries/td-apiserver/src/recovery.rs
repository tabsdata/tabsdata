//
// Copyright 2026 Tabsdata Inc.
//

use crate::scheduler::TickBudget;
use std::time::Duration;
use td_common::task_group::{BoxError, GroupTask};
use td_services::Services;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, trace};

/// Timer-only (no notify): recovery reacts to divergence, not to
/// scheduling events.
pub struct WorkerRecoverer {
    services: Services,
    check_frequency: Duration,
    budget: TickBudget,
}

impl WorkerRecoverer {
    pub async fn new(services: &Services) -> Self {
        WorkerRecoverer {
            services: services.clone(),
            check_frequency: services.recovery_config.check_frequency.0,
            budget: TickBudget::new("recovery", services.recovery_config.tick_timeout.0),
        }
    }

    async fn recover(&mut self) -> Result<(), BoxError> {
        self.budget.run(self.services.recovery(())).await?;
        Ok(())
    }
}

impl GroupTask for WorkerRecoverer {
    const NAME: &'static str = "worker_recoverer";

    async fn run(mut self, shutdown: CancellationToken) -> Result<(), BoxError> {
        // First tick immediately: cold-start convergence must not wait out
        // a full check period.
        match self.recover().await {
            Ok(_) => trace!("Worker recovery executed successfully"),
            Err(e) => error!("Error executing worker recovery: {e:#?}"),
        }
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
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
