//
// Copyright 2026 Tabsdata Inc.
//

//! The long-lived tasks of a process, run as one group: the first task to
//! end, or the signal, cancels every task, and one deadline bounds the
//! shutdown that follows. A task that must not end on its own reports a
//! fault by returning `Err`; the process then exits and its orchestrator
//! restarts it.

use crate::status::ExitStatus;
use std::collections::HashMap;
use std::error::Error;
use std::pin::Pin;
use std::time::Duration;
use tokio::task::{Id, JoinError, JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Level, debug, error, info, span, warn};

pub type BoxError = Box<dyn Error + Send + Sync>;

type TaskFuture = Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send>>;

/// A task that runs in a [`TaskGroup`] until `shutdown` is cancelled.
pub trait GroupTask: Send + 'static {
    const NAME: &'static str;

    fn run(self, shutdown: CancellationToken) -> impl Future<Output = Result<(), BoxError>> + Send;
}

pub struct TaskGroup {
    grace: Duration,
    shutdown: CancellationToken,
    tasks: Vec<(&'static str, TaskFuture)>,
}

impl TaskGroup {
    /// `grace` bounds the shutdown, from the moment it starts until every
    /// task has ended; past it the rest are aborted.
    pub fn new(grace: Duration) -> Self {
        Self {
            grace,
            shutdown: CancellationToken::new(),
            tasks: vec![],
        }
    }

    pub fn task<T: GroupTask>(mut self, task: T) -> Self {
        let shutdown = self.shutdown.clone();
        self.tasks.push((T::NAME, Box::pin(task.run(shutdown))));
        self
    }

    /// Runs every task until `signal` resolves or a task ends; `Err` when a
    /// task failed or panicked, or the shutdown outlived the grace.
    pub async fn run_until(self, signal: impl Future) -> Result<ExitStatus, ExitStatus> {
        let mut set = JoinSet::new();
        let mut names = HashMap::new();
        for (name, task) in self.tasks {
            let span = span!(Level::INFO, "task", name);
            let handle = set.spawn(task.instrument(span));
            names.insert(handle.id(), name);
        }
        if set.is_empty() {
            return Ok(ExitStatus::Success);
        }

        let mut failed = false;
        tokio::select! {
            _ = signal => info!("shutdown signal received; stopping every task"),
            Some(ended) = set.join_next_with_id() => {
                failed |= ended_while_running(&names, ended);
            }
        }
        self.shutdown.cancel();

        let drained = tokio::time::timeout(self.grace, async {
            let mut failed = false;
            while let Some(ended) = set.join_next_with_id().await {
                failed |= ended_on_shutdown(&names, ended);
            }
            failed
        })
        .await;
        match drained {
            Ok(drain_failed) => failed |= drain_failed,
            Err(_) => {
                failed = true;
                error!(grace = ?self.grace, "shutdown outlived its grace; aborting the remaining tasks");
                set.abort_all();
                while let Some(ended) = set.join_next_with_id().await {
                    ended_on_shutdown(&names, ended);
                }
            }
        }

        if failed {
            error!("task group finished with errors");
            Err(ExitStatus::GeneralError)
        } else {
            info!("task group finished");
            Ok(ExitStatus::Success)
        }
    }
}

type Ended = Result<(Id, Result<(), BoxError>), JoinError>;

/// Whether the first task to end failed. One that ends cleanly before
/// shutdown is not a fault — a bounded task ends on purpose — but it stops
/// the group all the same.
fn ended_while_running(names: &HashMap<Id, &'static str>, ended: Ended) -> bool {
    match ended {
        Ok((id, Ok(()))) => {
            warn!(
                task = names[&id],
                "task finished before shutdown; stopping every task"
            );
            false
        }
        Ok((id, Err(e))) => {
            error!(task = names[&id], "task failed: {e:#?}");
            true
        }
        Err(e) => {
            error!(task = names[&e.id()], "task panicked: {e}");
            true
        }
    }
}

fn ended_on_shutdown(names: &HashMap<Id, &'static str>, ended: Ended) -> bool {
    match ended {
        Ok((id, Ok(()))) => {
            debug!(task = names[&id], "task stopped");
            false
        }
        Ok((id, Err(e))) => {
            error!(task = names[&id], "task failed while stopping: {e:#?}");
            true
        }
        Err(e) if e.is_cancelled() => {
            warn!(task = names[&e.id()], "task aborted");
            true
        }
        Err(e) => {
            error!(task = names[&e.id()], "task panicked while stopping: {e}");
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::{pending, ready};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    enum Probe {
        UntilShutdown(Arc<AtomicBool>),
        IgnoresShutdown,
        Ends,
        Fails,
        Panics,
    }

    impl GroupTask for Probe {
        const NAME: &'static str = "probe";

        async fn run(self, shutdown: CancellationToken) -> Result<(), BoxError> {
            match self {
                Probe::UntilShutdown(stopped) => {
                    shutdown.cancelled().await;
                    stopped.store(true, Ordering::SeqCst);
                    Ok(())
                }
                Probe::IgnoresShutdown => {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    Ok(())
                }
                Probe::Ends => Ok(()),
                Probe::Fails => Err("failed".into()),
                Probe::Panics => panic!("probe panicked"),
            }
        }
    }

    fn until_shutdown() -> (Probe, Arc<AtomicBool>) {
        let stopped = Arc::new(AtomicBool::new(false));
        (Probe::UntilShutdown(stopped.clone()), stopped)
    }

    #[tokio::test]
    async fn test_the_signal_stops_every_task() {
        let (a, a_stopped) = until_shutdown();
        let (b, b_stopped) = until_shutdown();
        let status = TaskGroup::new(Duration::from_secs(5))
            .task(a)
            .task(b)
            .run_until(ready(()))
            .await;
        assert!(matches!(status, Ok(ExitStatus::Success)));
        assert!(a_stopped.load(Ordering::SeqCst));
        assert!(b_stopped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_a_task_ignoring_shutdown_is_aborted_at_the_grace() {
        let status = TaskGroup::new(Duration::from_millis(100))
            .task(Probe::IgnoresShutdown)
            .run_until(ready(()))
            .await;
        assert!(matches!(status, Err(ExitStatus::GeneralError)));
    }

    #[tokio::test]
    async fn test_a_task_ending_first_stops_the_rest() {
        let (waiting, stopped) = until_shutdown();
        let status = TaskGroup::new(Duration::from_secs(5))
            .task(Probe::Ends)
            .task(waiting)
            .run_until(pending::<()>())
            .await;
        assert!(matches!(status, Ok(ExitStatus::Success)));
        assert!(stopped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_a_failing_task_stops_the_rest_and_fails_the_group() {
        let (waiting, stopped) = until_shutdown();
        let status = TaskGroup::new(Duration::from_secs(5))
            .task(Probe::Fails)
            .task(waiting)
            .run_until(pending::<()>())
            .await;
        assert!(matches!(status, Err(ExitStatus::GeneralError)));
        assert!(stopped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_a_panicking_task_stops_the_rest_and_fails_the_group() {
        let (waiting, stopped) = until_shutdown();
        let status = TaskGroup::new(Duration::from_secs(5))
            .task(Probe::Panics)
            .task(waiting)
            .run_until(pending::<()>())
            .await;
        assert!(matches!(status, Err(ExitStatus::GeneralError)));
        assert!(stopped.load(Ordering::SeqCst));
    }
}
