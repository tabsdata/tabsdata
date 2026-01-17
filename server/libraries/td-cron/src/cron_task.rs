//
// Copyright 2026. Tabs Data Inc.
//

//! A `Task` implementation that runs another `Task` on a cron schedule.
//
// Copyright 2026. Tabs Data Inc.
//

use crate::Task;
use async_trait::async_trait;
use croner::parser::CronParser;
use croner::{errors, parser};
use std::error::Error;
use std::fmt;
use std::fmt::{Debug, Display};
use std::sync::Arc;
use td_error::td_error;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio_cron_scheduler::{Job, JobScheduler, JobSchedulerError};
use tracing::{debug, error, info, warn};

/// Errors that can occur within the `CronTask`.
#[td_error]
pub enum CronTaskError {
    /// The cron mask string is invalid.
    #[error("Failed to parse '{0}' cron mask: {1}")]
    MaskParseError(String, #[source] errors::CronError),
    /// The underlying `JobScheduler` could not be created.
    #[error("Failed to create scheduler: {0}")]
    SchedulerCreationError(#[source] JobSchedulerError),
    /// The cron job could not be scheduled.
    #[error("Failed to schedule cron job: {0}")]
    SchedulingJobError(#[source] JobSchedulerError),
    /// The `JobScheduler` failed to shut down gracefully.
    #[error("Failed to shutdown scheduler: {0}")]
    SchedulerShutdownError(#[source] JobSchedulerError),
    /// The `JobScheduler` failed to start.
    #[error("Scheduler failed to start: {0}")]
    SchedulerStartError(#[source] Box<dyn Error + Send + Sync>),
    /// The scheduled task failed during execution.
    #[error("Task execution failed: {0}")]
    TaskFailure(#[source] Box<dyn Error + Send + Sync>),
    /// Could not determine the next scheduled tick for a job.
    #[error("Could not get next tick")]
    CouldNotGetNextTick,
}

/// A `Task` that runs another `Task` based on a cron schedule.
pub struct CronTask<T: Task + 'static> {
    mask: String,
    task: Arc<T>,
}

impl<T: Task + 'static> CronTask<T> {
    /// Creates a new `CronTask`.
    ///
    /// The `mask` is a standard cron expression, but does not support the seconds field.
    pub fn new(mask: &str, task: T) -> Result<Self, CronTaskError> {
        let mask = Self::normalize_mask(mask, false)?;
        let task = Self {
            mask,
            task: Arc::new(task),
        };
        debug!("Creating, {task}.");
        Ok(task)
    }

    /// (For testing only). Creates a new `CronTask` with a cron expression that includes seconds.
    #[cfg(test)]
    fn new_using_seconds(mask: &str, task: T) -> Result<Self, CronTaskError> {
        let mask = Self::normalize_mask(mask, true)?;
        let task = Self {
            mask,
            task: Arc::new(task),
        };
        debug!("Creating, {task}. USING SECONDS (for unit tests only)");
        Ok(task)
    }

    /// Parses the cron mask string.
    fn normalize_mask(mask: &str, handle_seconds: bool) -> Result<String, CronTaskError> {
        let (mask_with_secs, parser) = if handle_seconds {
            (
                mask.to_string(),
                CronParser::builder()
                    .seconds(parser::Seconds::Required)
                    .build(),
            )
        } else {
            (
                format!("0 {mask}"),
                CronParser::builder()
                    .seconds(parser::Seconds::Disallowed)
                    .build(),
            )
        };
        match parser.parse(mask) {
            Ok(_) => Ok(mask_with_secs),
            Err(e) => Err(CronTaskError::MaskParseError(mask.into(), e)),
        }
    }

    /// Creates and configures the `JobScheduler`.
    async fn create_scheduler(
        &self,
        runner_shutdown_rx: watch::Receiver<()>,
        task_error_tx: mpsc::Sender<Box<dyn Error + Send + Sync>>,
    ) -> Result<JobScheduler, CronTaskError> {
        let scheduler = JobScheduler::new()
            .await
            .map_err(CronTaskError::SchedulerCreationError)?;

        let task = self.task.clone();
        let self_to_string = self.to_string();

        scheduler
            .add(
                Job::new_async(&self.mask, move |uuid, mut l| {
                    Box::pin({
                        let task = task.clone();
                        let self_to_string = self_to_string.clone();

                        let runner_shutdown_rx = runner_shutdown_rx.clone();
                        let task_error_tx = task_error_tx.clone();

                        async move {
                            info!("Ticking, {self_to_string}.");
                            match task.run(runner_shutdown_rx).await {
                                Ok(_) => {
                                    debug!("Executed successfully, {self_to_string}, cron job {uuid}.")
                                }
                                Err(err) => {
                                    error!("Error executing, {self_to_string}, cron job {uuid}: {err}.");
                                    if task_error_tx.send(Box::new(err)).await.is_err() {
                                        error!("Could not send error upstream, {self_to_string}, cron job {uuid}. Receiver likely dropped.");
                                    }
                                }
                            };

                            // Check when next cron tick is.
                            match l.next_tick_for_job(uuid).await {
                                Ok(Some(ts)) => {
                                    debug!("Next tick, {self_to_string}, cron job:{uuid}, at: {ts}.");
                                }
                                _ => {
                                    error!("Could not get next cron tick, {self_to_string}, cron job {uuid}");
                                    if task_error_tx
                                        .send(Box::new(CronTaskError::CouldNotGetNextTick))
                                        .await
                                        .is_err()
                                    {
                                        error!("Could not send error upstream, {self_to_string}, cron job {uuid}. Receiver likely dropped.");
                                    }
                                }
                            }
                        }
                    })
                })
                .map_err(CronTaskError::SchedulingJobError)?,
            )
            .await
            .map_err(CronTaskError::SchedulingJobError)?;

        Ok(scheduler)
    }
}

#[async_trait]
impl<T: Task + 'static> Task for CronTask<T> {
    type Error = CronTaskError;

    async fn run(&self, runner_shutdown_rx: watch::Receiver<()>) -> Result<(), Self::Error> {
        let mut shutdown_task = runner_shutdown_rx.clone();
        let (task_err_tx, mut task_err_rx) = mpsc::channel(1);

        // 1. Create the scheduler
        let mut scheduler = self
            .create_scheduler(runner_shutdown_rx.clone(), task_err_tx)
            .await?;

        // We await the result immediately to ensure the background task spawned successfully.
        scheduler
            .start()
            .await
            .map_err(|err| CronTaskError::SchedulerStartError(Box::new(err)))?;

        info!("Running, {self}.");

        tokio::select! {
            _ = shutdown_task.changed() => {
                info!("Received shutdown signal, {self}.");

                if let Err(e) = scheduler.shutdown().await {
                    warn!("Error shutting down, {self}. Error: {e}");
                } else {
                    debug!("Shutdown complete, {self}.");
                }
                Ok(())
            }
            Some(err) = task_err_rx.recv() => {
                // If the task fails while executing during a cron tick, we stop the cron scheduler and fail.
                error!("Shutting down because critical error, {self}. Error: {err}");

                if let Err(e) = scheduler.shutdown().await {
                    warn!("Error shutting down because of critical error, {self}. Error: {e}");
                }

                Err(CronTaskError::TaskFailure(err))
            }
        }
    }
}

impl<T: Task + 'static> Display for CronTask<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "mask: {}, task: {}", self.mask, self.task)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task_runner::{Task, TaskRunner, TaskRunnerError};
    use std::any::type_name;
    use std::convert::Infallible;
    use std::error::Error;
    use std::fmt;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use td_common::server_runner::ServerRunnable;
    use tokio::sync::watch;
    use tokio::time::sleep;

    struct TestTask {
        run_count: Arc<Mutex<u32>>,
    }

    #[async_trait::async_trait]
    impl Task for TestTask {
        type Error = Infallible;

        async fn run(&self, _shutdown_receiver: watch::Receiver<()>) -> Result<(), Self::Error> {
            let mut run_count = self.run_count.lock().unwrap();
            *run_count += 1;
            Ok(())
        }
    }

    impl Display for TestTask {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", type_name::<Self>())
        }
    }

    struct FailingTask {
        run_count: Arc<Mutex<u32>>,
    }

    #[derive(Debug)]
    struct TaskError;

    impl Display for TaskError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", type_name::<Self>())
        }
    }

    impl Error for TaskError {}

    #[async_trait::async_trait]
    impl Task for FailingTask {
        type Error = TaskError;

        async fn run(&self, _shutdown_receiver: watch::Receiver<()>) -> Result<(), Self::Error> {
            let mut run_count = self.run_count.lock().unwrap();
            *run_count += 1;
            Err(TaskError)
        }
    }

    impl Display for FailingTask {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", type_name::<Self>())
        }
    }

    #[tokio::test]
    async fn test_cron_task_run() {
        let run_count = Arc::new(Mutex::new(0));
        let task = TestTask {
            run_count: run_count.clone(),
        };

        let mask = "*/1 * * * * *";

        let task = CronTask::new_using_seconds(mask, task).unwrap();

        let runner = TaskRunner::new(task, None, None);

        let (shutdown_tx, shutdown_rx) = watch::channel(());

        let scheduler_handle = tokio::spawn(async move {
            Box::new(runner).run(shutdown_rx).await.unwrap();
        });

        sleep(Duration::from_millis(2500)).await;

        shutdown_tx.send(()).unwrap();

        scheduler_handle.await.unwrap();

        // Assert that the task ran at least twice.
        assert!(*run_count.lock().unwrap() > 1);
    }

    #[tokio::test]
    async fn test_mask_parsing_error() {
        let task = TestTask {
            run_count: Arc::new(Mutex::new(0)),
        };
        let result = CronTask::new("invalid mask", task);
        assert!(result.is_err());
        let error = result.err().unwrap();
        assert!(matches!(error, CronTaskError::MaskParseError(_, _)));
    }

    #[tokio::test]
    async fn test_mask_parsing_success() {
        let task = TestTask {
            run_count: Arc::new(Mutex::new(0)),
        };
        let result = CronTask::new("* * * * *", task);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_cron_task_does_not_run_before_time() {
        let run_count = Arc::new(Mutex::new(0));
        let task = TestTask {
            run_count: run_count.clone(),
        };

        let mask = "*/2 * * * * *"; // Every 2 seconds

        let task = CronTask::new_using_seconds(mask, task).unwrap();

        let runner = TaskRunner::new(task, None, None);

        let (shutdown_tx, shutdown_rx) = watch::channel(());

        let scheduler_handle = tokio::spawn(async move {
            Box::new(runner).run(shutdown_rx).await.unwrap();
        });

        sleep(Duration::from_millis(500)).await;
        assert_eq!(*run_count.lock().unwrap(), 0);

        sleep(Duration::from_millis(2000)).await;
        assert!(*run_count.lock().unwrap() >= 1);

        shutdown_tx.send(()).unwrap();

        scheduler_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_cron_task_runner_fails_on_error() {
        let run_count = Arc::new(Mutex::new(0));
        let task = FailingTask {
            run_count: run_count.clone(),
        };

        let mask = "*/1 * * * * *";

        let task = CronTask::new_using_seconds(mask, task).unwrap();

        let runner = TaskRunner::new(task, None, None);

        let (_shutdown_tx, shutdown_rx) = watch::channel(());

        let scheduler_handle = tokio::spawn(async move {
            let result = Box::new(runner).run(shutdown_rx).await;
            assert!(result.is_err());
            let err = result.err().unwrap();

            // The error from TaskRunner is a boxed TaskRunnerError
            let task_runner_error = err.downcast_ref::<TaskRunnerError>().unwrap();

            // We expect a TaskFailure variant from the TaskRunner
            if let TaskRunnerError::TaskFailure(cron_task_err) = task_runner_error {
                // The inner error should be our CronTaskError
                let cron_task_error = cron_task_err.downcast_ref::<CronTaskError>().unwrap();

                // We expect a TaskFailure variant from the CronTask
                if let CronTaskError::TaskFailure(task_err) = cron_task_error {
                    // The inner error should be our TaskError
                    assert!(task_err.is::<TaskError>());
                } else {
                    panic!(
                        "Expected CronTaskError::TaskFailure, but got {:?}",
                        cron_task_error
                    );
                }
            } else {
                panic!(
                    "Expected TaskRunnerError::TaskFailure, but got {:?}",
                    task_runner_error
                );
            }
        });

        scheduler_handle.await.unwrap();

        // Assert that the task ran once.
        assert_eq!(*run_count.lock().unwrap(), 1);
    }

    #[test]
    fn test_create_cron_handles_seconds() {
        // Test case 1: handle_seconds = true, 6-element mask
        let mask_6_elements = "* * * * * *";
        let cron = CronTask::<TestTask>::normalize_mask(mask_6_elements, true).unwrap();
        assert_eq!(cron, "* * * * * *");

        // Test case 2: handle_seconds = false, 5-element mask
        let mask_5_elements = "* * * * *";
        let cron = CronTask::<TestTask>::normalize_mask(mask_5_elements, false).unwrap();
        assert_eq!(cron, "0 * * * * *");

        // Test case 3: handle_seconds = false, 6-element mask (should fail)
        let result = CronTask::<TestTask>::normalize_mask(mask_6_elements, false);
        assert!(result.is_err());
        let error = result.err().unwrap();
        assert!(matches!(error, CronTaskError::MaskParseError(_, _)));

        // Test case 4: handle_seconds = true, 5-element mask (should fail)
        let result = CronTask::<TestTask>::normalize_mask(mask_5_elements, true);
        assert!(result.is_err());
        let error = result.err().unwrap();
        assert!(matches!(error, CronTaskError::MaskParseError(_, _)));
        assert!(
            error
                .to_string()
                .contains("Failed to parse '* * * * *' cron mask")
        );
    }
}
