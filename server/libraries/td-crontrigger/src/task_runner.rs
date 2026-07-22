//
// Copyright 2026 Tabsdata Inc.
//

//! A generic task runner for executing tasks with optional start/end times and heartbeats.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::any::type_name;
use std::convert::Infallible;
use std::error::Error;
use std::fmt;
use std::fmt::Debug;
use td_common::server_runner::ServerRunnable;
use td_error::td_error;
use tokio::sync::watch;
use tokio::time::{Duration, sleep};
use tracing::{debug, error, info, warn};

/// Errors that can occur within the `TaskRunner`.
#[td_error]
pub enum TaskRunnerError {
    /// The main task failed to execute.
    #[error("Failed to run task: {0}")]
    TaskFailure(#[source] Box<dyn Error + Send + Sync>),
    /// The heartbeat task unexpectedly terminated.
    #[error("Heartbeat task should not end")]
    HeartbeatEnded,
}

/// A trait for a runnable task.
#[async_trait]
pub trait Task: Send + Sync + fmt::Display {
    /// The error type for the task.
    type Error: Error + Send + Sync;
    /// Executes the task.
    async fn run(&self, runner_shutdown_rx: watch::Receiver<()>) -> Result<(), Self::Error>;
}

/// A dummy struct to represent the absence of a heartbeat task.
pub struct NoHeartbeat;

#[async_trait]
impl Task for NoHeartbeat {
    type Error = Infallible;
    async fn run(&self, _runner_shutdown_rx: watch::Receiver<()>) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl fmt::Display for NoHeartbeat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", type_name::<Self>())
    }
}

/// A runner for a `Task` that can be scheduled to start and end at specific times,
/// and can run a periodic heartbeat task.
pub struct TaskRunner<T, E, H, HE>
where
    T: Task<Error = E> + 'static,
    E: Error + Send + Sync,
    H: Task<Error = HE> + 'static,
    HE: Error + Send + Sync,
{
    task: T,
    start: Option<DateTime<Utc>>,
    end: Option<DateTime<Utc>>,
    heartbeat: Option<(H, Duration)>,
}

impl<T, E> TaskRunner<T, E, NoHeartbeat, Infallible>
where
    T: Task<Error = E> + 'static,
    E: Error + Send + Sync,
{
    /// Creates a new `TaskRunner` without a heartbeat.
    #[allow(unused)]
    pub fn new(task: T, start: Option<DateTime<Utc>>, end: Option<DateTime<Utc>>) -> Self {
        Self {
            task,
            start,
            end,
            heartbeat: None,
        }
    }
}

impl<T, E, H, HE> TaskRunner<T, E, H, HE>
where
    T: Task<Error = E> + 'static,
    E: Error + Send + Sync,
    H: Task<Error = HE> + 'static,
    HE: Error + Send + Sync,
{
    /// Creates a new `TaskRunner` with a heartbeat.
    pub fn new_with_heartbeat(
        task: T,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
        heartbeat: H,
        heartbeat_interval: Duration,
    ) -> Self {
        debug!(
            "Creating, start: {start:?}, end: {end:?}, task: ({task}), heartbeat task: ({heartbeat}), heartbeat interval: {heartbeat_interval:?}.",
        );
        Self {
            task,
            start,
            end,
            heartbeat: Some((heartbeat, heartbeat_interval)),
        }
    }
}

/// Sleeps until the specified `DateTime<Utc>`.
async fn sleep_until(time: DateTime<Utc>) {
    let now = Utc::now();
    if time > now {
        let delay = (time - now).to_std().unwrap_or(Duration::ZERO);
        debug!("Waiting until {time}. Current is {now}. Time to wait: {delay:?}.");
        sleep(delay).await;
    }
}

/// Waits until the start time, if provided.
/// Returns `true` if the task should proceed, `false` if shutdown is signaled.
async fn wait_for_start(
    start: Option<DateTime<Utc>>,
    mut runner_shutdown_rx: watch::Receiver<()>,
) -> bool {
    if let Some(start_time) = start {
        info!("Task will start running at: {start_time}.");
        tokio::select! {
            _ = runner_shutdown_rx.changed() => {
                debug!("Task runner shutdown while waiting to start.");
                false
            },
            _ = sleep_until(start_time) => {
                debug!("Task starting now.");
                true
            }
        }
    } else {
        debug!("No start time provided, task starting now.");
        true
    }
}

/// A future that completes at the specified end time.
async fn finish_on_end(end: Option<DateTime<Utc>>) {
    if let Some(end_time) = end {
        info!("Task will end runnint at: {end_time}.");
        sleep_until(end_time).await
    } else {
        debug!("No end time provided, task will run until completion.");
        std::future::pending().await
    }
}

/// Runs the heartbeat task at the specified interval.
async fn run_heartbeat<H: Task>(
    heartbeat_fn_interval: Option<(H, Duration)>,
    mut runner_shutdown_rx: watch::Receiver<()>,
) {
    if let Some((heartbeat, heartbeat_interval)) = heartbeat_fn_interval {
        info!("Starting heartbeat loop");
        loop {
            tokio::select! {
                _ = sleep(heartbeat_interval) => {
                    debug!("Running heartbeat task.");
                    match heartbeat.run(runner_shutdown_rx.clone()).await {
                        Ok(_) => {},
                        Err(err) => warn!("Heartbeat task run failed: {err}.")
                    }
                }
                _ = runner_shutdown_rx.changed() => {
                    debug!("Heartbeat task received shutdown signal, it will no longer run.");
                    std::future::pending::<H>().await;
                }
            }
        }
    } else {
        info!("Running without heartbeat.");
        runner_shutdown_rx.changed().await.ok();
        std::future::pending::<H>().await;
    }
}

#[async_trait]
impl<T, E, H, HE> ServerRunnable for TaskRunner<T, E, H, HE>
where
    T: Task<Error = E> + 'static,
    E: Error + Send + Sync + 'static,
    H: Task<Error = HE> + 'static,
    HE: Error + Send + Sync,
{
    async fn run(
        self: Box<Self>,
        mut runner_shutdown_rx: watch::Receiver<()>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        if !wait_for_start(self.start, runner_shutdown_rx.clone()).await {
            return Ok(());
        }

        let end_task = finish_on_end(self.end);
        tokio::pin!(end_task);

        let main_task = self.task.run(runner_shutdown_rx.clone());
        let heartbeat_task = run_heartbeat(self.heartbeat, runner_shutdown_rx.clone());

        info!(start = ?self.start, end = ?self.end, "crontrigger.task_runner: starting");
        tokio::select! {
           res = main_task => {
               if let Err(e) = res {
                   error!("Task failed: {:#?}", e);
                   return Err(Box::new(TaskRunnerError::TaskFailure(Box::new(e))))
               }
           },
           _ = heartbeat_task => {
               // we should never get here
               error!("Heartbeat loop finished, it should not happen.");
               return Err(Box::new(TaskRunnerError::HeartbeatEnded))
           },
           _ = &mut end_task => {
               warn!("End time reached, stopping task. Ok if task is endless.");
           },
           _ = runner_shutdown_rx.changed() => {
               info!("Shutdown signal received.");
           }

        }
        info!("Shutdown complete.");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeDelta;
    use std::fmt;
    use std::sync::{Arc, Mutex};
    use tokio::time::Instant;

    #[derive(Debug, thiserror::Error)]
    struct TestErr;

    impl fmt::Display for TestErr {
        fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
            Ok(())
        }
    }
    struct TestTask {
        run_count: Arc<Mutex<u32>>,
        should_block: bool,
        should_fail: bool,
    }

    #[async_trait::async_trait]
    impl Task for TestTask {
        type Error = TestErr;
        async fn run(
            &self,
            mut runner_shutdown_rx: watch::Receiver<()>,
        ) -> Result<(), Self::Error> {
            if self.should_fail {
                return Err(TestErr);
            }
            {
                let mut run_count = self.run_count.lock().unwrap();
                *run_count += 1;
            }

            if self.should_block {
                runner_shutdown_rx.changed().await.ok();
            }

            Ok(())
        }
    }

    impl fmt::Display for TestTask {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", type_name::<Self>())
        }
    }

    async fn wait_for_run_count(run_count: Arc<Mutex<u32>>, count: u32, timeout: Duration) {
        let start = Instant::now();
        while start.elapsed() < timeout {
            if *run_count.lock().unwrap() >= count {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    }

    #[tokio::test]
    async fn test_task_runner_runs_task_successfully() {
        let run_count = Arc::new(Mutex::new(0));
        let task = TestTask {
            should_fail: false,
            run_count: run_count.clone(),
            should_block: false,
        };

        let runner = TaskRunner::new(task, None, None);
        let (shutdown_tx, shutdown_rx) = watch::channel(());

        Box::new(runner).run(shutdown_rx).await.unwrap();

        drop(shutdown_tx);

        assert_eq!(*run_count.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn test_task_runner_runs_task_erroring() {
        let run_count = Arc::new(Mutex::new(0));
        let task = TestTask {
            should_fail: true,
            run_count: run_count.clone(),
            should_block: false,
        };

        let runner = TaskRunner::new(task, None, None);
        let (shutdown_tx, shutdown_rx) = watch::channel(());

        let res = Box::new(runner).run(shutdown_rx).await;

        assert!(res.is_err());
        let err = res.unwrap_err();
        let err = err.downcast_ref::<TaskRunnerError>().unwrap();
        assert!(matches!(err, &TaskRunnerError::TaskFailure(_)));

        drop(shutdown_tx);
    }

    #[tokio::test]
    async fn test_shutdown_cancellation() {
        let run_count = Arc::new(Mutex::new(0));
        let task = TestTask {
            should_fail: false,
            run_count: run_count.clone(),
            should_block: true,
        };

        let start_time = Utc::now() + TimeDelta::try_seconds(5).unwrap();
        let runner = TaskRunner::new(task, Some(start_time), None);

        let (shutdown_tx, shutdown_rx) = watch::channel(());

        let runner_handle = tokio::spawn(async move {
            Box::new(runner).run(shutdown_rx).await.unwrap();
        });

        let _ = shutdown_tx.send(());
        runner_handle.await.unwrap();

        assert_eq!(*run_count.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn test_end_stops_task() {
        let run_count = Arc::new(Mutex::new(0));
        let task = TestTask {
            should_fail: false,
            run_count: run_count.clone(),
            should_block: true,
        };

        let end_time = Utc::now() + TimeDelta::try_seconds(1).unwrap();
        let runner = TaskRunner::new(task, None, Some(end_time));

        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let res = Box::new(runner).run(shutdown_rx).await;

        assert!(res.is_ok());
        assert_eq!(*run_count.lock().unwrap(), 1);
        drop(shutdown_tx);
    }

    #[tokio::test]
    async fn test_task_with_heartbeat() {
        let task_run_count = Arc::new(Mutex::new(0));
        let task = TestTask {
            should_fail: false,
            run_count: task_run_count.clone(),
            should_block: true,
        };

        let heartbeat_run_count = Arc::new(Mutex::new(0));
        let heartbeat_task = TestTask {
            should_fail: false,
            run_count: heartbeat_run_count.clone(),
            should_block: false,
        };

        let heartbeat_interval = Duration::from_millis(200);
        let runner =
            TaskRunner::new_with_heartbeat(task, None, None, heartbeat_task, heartbeat_interval);

        let (shutdown_tx, shutdown_rx) = watch::channel(());

        let runner_handle = tokio::spawn(async move {
            Box::new(runner).run(shutdown_rx).await.unwrap();
        });

        wait_for_run_count(heartbeat_run_count.clone(), 3, Duration::from_secs(1)).await;

        let _ = shutdown_tx.send(());
        runner_handle.await.unwrap();

        assert_eq!(*task_run_count.lock().unwrap(), 1);
        assert!(*heartbeat_run_count.lock().unwrap() >= 3);
    }
}
