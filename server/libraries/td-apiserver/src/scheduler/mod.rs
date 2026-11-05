//
// Copyright 2025 Tabsdata Inc.
//

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;
use ta_services::service::Ctx;
use td_common::task_group::{BoxError, GroupTask};
use td_database::domain::Apiserver;
use td_database::pool::PoolKind;
use td_execution::{Concerns, ProjectWakes};
use td_objects::types::basic::ProjectId;
use td_services::Services;
use td_services::connection_catalog::ConnectionCatalog;
use td_services::scheduler::services::ProjectPass;
use td_services::scheduler::stats;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, error, info_span, warn};

/// Runs one pass per runnable project, at most `slots` at once, and sweeps
/// every project at the check frequency.
pub struct FunctionRequester {
    services: Services,
    wakes: Ctx<ProjectWakes>,
    check_frequency: Duration,
    budgets: PassBudgets,
    slots: usize,
}

impl FunctionRequester {
    pub async fn new(services: &Services) -> Self {
        FunctionRequester {
            services: services.clone(),
            wakes: services.wakes.clone(),
            check_frequency: services.scheduler_config.check_frequency.0,
            budgets: PassBudgets::new(services.scheduler_config.tick_timeout.0),
            slots: pass_slots(services),
        }
    }

    async fn sweep(&self) {
        if let Err(e) = self.services.sweep(()).await {
            error!("scheduler.sweep: failed: {e:#?}");
        }
    }

    fn start_runnable(&self, slots: &Arc<Semaphore>, passes: &mut JoinSet<()>) {
        while let Ok(slot) = slots.clone().try_acquire_owned() {
            let Some((project, concerns)) = self.wakes.take() else {
                break;
            };
            passes.spawn(run_pass(
                self.services.clone(),
                project,
                concerns,
                self.budgets.clone(),
                slot,
            ));
        }
    }
}

impl GroupTask for FunctionRequester {
    const NAME: &'static str = "function_requester";

    async fn run(self, shutdown: CancellationToken) -> Result<(), BoxError> {
        let slots = Arc::new(Semaphore::new(self.slots));
        let mut passes = JoinSet::new();
        let mut sweeps = tokio::time::interval(self.check_frequency);
        sweeps.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            self.start_runnable(&slots, &mut passes);
            tokio::select! {
                _ = shutdown.cancelled() => {
                    debug!("scheduler: shutting down");
                    break;
                }
                _ = self.wakes.runnable() => {}
                Some(_) = passes.join_next() => {}
                _ = sweeps.tick() => self.sweep().await,
            }
        }
        while passes.join_next().await.is_some() {}
        Ok(())
    }
}

/// Half the pass pool: callbacks, recovery and the probe dispatcher share it.
fn pass_slots(services: &Services) -> usize {
    services
        .pools
        .max_connections::<Apiserver>(PoolKind::System)
        .map_or(1, |max| (max as usize / 2).max(1))
}

async fn run_pass(
    services: Services,
    project: ProjectId,
    concerns: Concerns,
    budgets: PassBudgets,
    _slot: OwnedSemaphorePermit,
) {
    let mut budget = budgets.take(&project);
    let pass = services
        .pass(ProjectPass { project, concerns })
        .instrument(info_span!("scheduler.pass", project_id = %project));
    let again = match budget.within(pass).await {
        Some(Ok(again)) => again.data,
        Some(Err(e)) => {
            error!(project_id = %project, "scheduler.pass: failed: {e:#?}");
            Concerns::NONE
        }
        None => {
            warn!(project_id = %project, "scheduler.pass: timed out, running again");
            stats::pass_timed_out();
            concerns
        }
    };
    budgets.keep(project, budget);
    services.wakes.finish(&project, again);
}

/// The raised [`TickBudget`] of each project; a pass takes it and puts it back.
#[derive(Clone)]
struct PassBudgets {
    base: Duration,
    raised: Arc<Mutex<BTreeMap<ProjectId, TickBudget>>>,
}

impl PassBudgets {
    fn new(base: Duration) -> Self {
        PassBudgets {
            base,
            raised: Arc::default(),
        }
    }

    fn take(&self, project: &ProjectId) -> TickBudget {
        self.raised
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(project)
            .unwrap_or_else(|| TickBudget::new("pass", self.base))
    }

    fn keep(&self, project: ProjectId, budget: TickBudget) {
        if budget.current > self.base {
            self.raised
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(project, budget);
        }
    }
}

/// A tick budget that follows how long ticks take, between the configured one
/// and `TICK_BUDGET_CAP` times it: a fixed budget cancels a slow tick's retry
/// the same way. It falls one base at a time, so one fast tick cannot drop it.
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

    /// Runs one tick under the current budget, warning once it uses half of it.
    pub(crate) async fn run<T, E: Into<BoxError>>(
        &mut self,
        fut: impl Future<Output = Result<T, E>>,
    ) -> Result<T, BoxError> {
        match self.within(fut).await {
            Some(result) => result.map_err(Into::into),
            None => Err(format!("scheduler {} tick timed out", self.tick).into()),
        }
    }

    /// [`run`](Self::run) for any output: `None` when it overran the budget.
    async fn within<T>(&mut self, fut: impl Future<Output = T>) -> Option<T> {
        let tick = self.tick;
        let budget = self.current;
        let started = tokio::time::Instant::now();
        let result = tokio::time::timeout(budget, fut).await.ok();
        let timed_out = result.is_none();
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
pub struct ProbeDispatcher {
    services: Services,
    catalog: Ctx<ConnectionCatalog>,
    check_frequency: Duration,
    budget: TickBudget,
}

impl ProbeDispatcher {
    pub async fn new(services: &Services) -> Self {
        ProbeDispatcher {
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
}

impl GroupTask for ProbeDispatcher {
    const NAME: &'static str = "probe_dispatcher";

    async fn run(mut self, shutdown: CancellationToken) -> Result<(), BoxError> {
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

    #[test]
    fn test_a_project_keeps_a_raised_pass_budget_and_drops_a_base_one() {
        let budgets = PassBudgets::new(BASE);
        let (raised, base) = (ProjectId::default(), ProjectId::default());
        let mut budget = budgets.take(&raised);
        budget.settle(BASE, true);
        budgets.keep(raised, budget);
        budgets.keep(base, budgets.take(&base));
        assert_eq!(budgets.take(&raised).current(), BASE * 2);
        assert_eq!(budgets.take(&base).current(), BASE);
        assert!(budgets.raised.lock().unwrap().is_empty());
    }
}
