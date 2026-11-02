//
// Copyright 2025 Tabsdata Inc.
//

use std::future::Future;
use std::time::Duration;
use ta_services::service::Ctx;
use td_common::task_group::BoxError;
use td_execution::SchedulerNotify;
use td_services::Services;
use td_services::connection_catalog::ConnectionCatalog;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, trace, warn};

pub struct FunctionRequesterV1 {
    services: Services,
    notify: SchedulerNotify,
    // Failover safety net — `SchedulerNotify` wakes the loop on status
    // changes; the timer also bounds the stuck-run sweep cadence.
    check_frequency: Duration,
    budget: TickBudget,
}

impl FunctionRequesterV1 {
    pub async fn new(services: &Services) -> Self {
        FunctionRequesterV1 {
            services: services.clone(),
            notify: (*services.scheduler_notify).clone(),
            check_frequency: services.scheduler_config.check_frequency.0,
            budget: TickBudget::new("request", services.scheduler_config.tick_timeout.0),
        }
    }

    async fn request(&mut self) -> Result<(), BoxError> {
        self.budget.run(self.services.request(())).await?;
        Ok(())
    }

    pub async fn run(mut self, shutdown: CancellationToken) -> Result<(), BoxError> {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
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

/// Ticks overrunning the budget a tick may take, which follows how long ticks
/// actually take. Under load a tick's work outgrows the configured budget, and
/// a cancelled tick leaves what it did not reach for the next one, which a
/// fixed budget cancels the same way. So a timeout raises the budget by the
/// configured amount, and a tick that finishes sets it to twice what it took
/// while lowering it by at most the configured amount: a budget falling
/// straight back after one fast tick would cancel the next slow one. It stays
/// between the configured budget and `TICK_BUDGET_CAP` times it.
pub(crate) struct TickBudget {
    tick: &'static str,
    base: Duration,
    current: Duration,
}

const TICK_BUDGET_CAP: u32 = 10;

impl TickBudget {
    pub(crate) fn new(tick: &'static str, base: Duration) -> Self {
        TickBudget {
            tick,
            base,
            current: base,
        }
    }

    #[cfg(test)]
    fn current(&self) -> Duration {
        self.current
    }

    fn settle(&mut self, elapsed: Duration, timed_out: bool) {
        let next = if timed_out {
            self.current + self.base
        } else {
            (elapsed * 2).max(self.current.saturating_sub(self.base))
        };
        self.current = next.clamp(self.base, self.base * TICK_BUDGET_CAP);
    }

    /// Runs one tick under the current budget (the loop is sequential, so a
    /// pass that overruns is abandoned and the next tick retries), warning when
    /// it consumes half of it — the early signal that the scheduler is heading
    /// for timeout cancellations (e.g. a starved connection pool or a slow
    /// orchestrator backend).
    pub(crate) async fn run<T, E: Into<BoxError>>(
        &mut self,
        fut: impl Future<Output = Result<T, E>>,
    ) -> Result<T, BoxError> {
        let tick = self.tick;
        let budget = self.current;
        let started = tokio::time::Instant::now();
        let (result, timed_out) = match tokio::time::timeout(budget, fut).await {
            Ok(res) => (res.map_err(Into::into), false),
            Err(_elapsed) => (Err(format!("scheduler {tick} tick timed out").into()), true),
        };
        let elapsed = started.elapsed();
        if elapsed >= budget / 2 {
            warn!(
                tick,
                elapsed_ms = elapsed.as_millis() as u64,
                tick_timeout_ms = budget.as_millis() as u64,
                "slow scheduler tick"
            );
        }
        self.settle(elapsed, timed_out);
        if self.current > budget {
            warn!(
                tick,
                from_ms = budget.as_millis() as u64,
                to_ms = self.current.as_millis() as u64,
                "scheduler tick budget raised"
            );
        } else if self.current < budget {
            debug!(
                tick,
                from_ms = budget.as_millis() as u64,
                to_ms = self.current.as_millis() as u64,
                "scheduler tick budget lowered"
            );
        }
        result
    }
}

/// Claims queued connection probes and sends their requests; also expires
/// sample rows and times out probes that never reported.
pub struct ProbeDispatcherV1 {
    services: Services,
    catalog: Ctx<ConnectionCatalog>,
    check_frequency: Duration,
    budget: TickBudget,
}

impl ProbeDispatcherV1 {
    pub async fn new(services: &Services) -> Self {
        ProbeDispatcherV1 {
            services: services.clone(),
            catalog: services.connection_catalog.clone(),
            check_frequency: services.scheduler_config.check_frequency.0,
            budget: TickBudget::new("probe dispatch", services.scheduler_config.tick_timeout.0),
        }
    }

    async fn dispatch(&mut self) -> Result<(), BoxError> {
        self.budget
            .run(self.services.dispatch_connection_probes(()))
            .await?;
        Ok(())
    }

    pub async fn run(mut self, shutdown: CancellationToken) -> Result<(), BoxError> {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    debug!("Connection probe dispatcher shutting down...");
                    break;
                }
                _ = self.catalog.submitted.notified() => debug!("Probe dispatcher: tick (notify)"),
                _ = tokio::time::sleep(self.check_frequency) => debug!("Probe dispatcher: tick (timer)"),
            }
            if let Err(e) = self.dispatch().await {
                error!("Error executing the connection probe dispatcher: {e:#?}");
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: Duration = Duration::from_secs(10);

    #[test]
    fn test_a_timeout_raises_the_budget_by_its_base_up_to_the_cap() {
        let mut budget = TickBudget::new("t", BASE);
        let raised: Vec<u64> = (0..12)
            .map(|_| {
                budget.settle(budget.current(), true);
                budget.current().as_secs()
            })
            .collect();
        assert_eq!(raised, [20, 30, 40, 50, 60, 70, 80, 90, 100, 100, 100, 100]);
    }

    #[test]
    fn test_a_fast_tick_lowers_the_budget_one_base_at_a_time_down_to_it() {
        let mut budget = TickBudget::new("t", BASE);
        for _ in 0..5 {
            budget.settle(budget.current(), true);
        }
        let lowered: Vec<u64> = (0..7)
            .map(|_| {
                budget.settle(Duration::from_millis(100), false);
                budget.current().as_secs()
            })
            .collect();
        assert_eq!(lowered, [50, 40, 30, 20, 10, 10, 10]);
    }

    #[test]
    fn test_a_slow_tick_that_finishes_keeps_twice_its_time() {
        let mut budget = TickBudget::new("t", BASE);
        budget.settle(Duration::from_secs(7), false);
        assert_eq!(budget.current(), Duration::from_secs(14));
        budget.settle(Duration::from_secs(60), false);
        assert_eq!(budget.current(), BASE * TICK_BUDGET_CAP);
        budget.settle(Duration::from_secs(40), false);
        assert_eq!(budget.current(), Duration::from_secs(90));
    }

    #[tokio::test(start_paused = true)]
    async fn test_a_tick_that_overruns_is_cancelled_and_the_next_gets_longer() {
        let mut budget = TickBudget::new("t", BASE);
        let slow = || async {
            tokio::time::sleep(Duration::from_secs(15)).await;
            Ok::<_, BoxError>(())
        };
        let err = budget.run(slow()).await.unwrap_err();
        assert_eq!(err.to_string(), "scheduler t tick timed out");
        assert_eq!(budget.current(), Duration::from_secs(20));
        budget.run(slow()).await.unwrap();
        assert_eq!(budget.current(), Duration::from_secs(30));
    }
}
