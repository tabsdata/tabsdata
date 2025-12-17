//
// Copyright 2025 Tabs Data Inc.
//

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::{FutureExt, future};
use std::error::Error;
use std::sync::Arc;
use td_common::datetime::IntoDateTimeUtc;
use td_common::server_runner::ServerRunnable;
use tokio::sync::watch;
use tokio::time::{Duration, sleep};
use tokio_cron_scheduler::{Job, JobScheduler, JobSchedulerError};
use tracing::{debug, error, info, warn};

#[derive(Debug, thiserror::Error)]
pub enum CronJobSchedulerError {
    #[error("Failed to create scheduler: {0}")]
    SchedulerCreation(#[source] JobSchedulerError),
    #[error("Failed to schedule cron job: {0}")]
    ScheduleJob(#[source] JobSchedulerError),
    #[error("Failed to shutdown scheduler: {0}")]
    Shutdown(#[source] JobSchedulerError),
    #[error("Failed to run cron job: {0}")]
    JobFailure(#[source] Box<dyn Error + Send + Sync>),
}

#[async_trait]
pub trait CronJobTask: Send + Sync {
    async fn run(&self, shutdown: watch::Receiver<()>) -> Result<(), CronJobSchedulerError>;
}

pub(crate) struct CronScheduler {
    mask: croner::Cron,
    start: Option<i64>, // in milliseconds
    end: Option<i64>,   // in milliseconds
    task: Box<dyn CronJobTask>,
}

impl CronScheduler {
    pub fn new(
        mask: croner::Cron,
        start: Option<i64>,
        end: Option<i64>,
        task: Box<dyn CronJobTask>,
    ) -> Self {
        Self {
            mask,
            start,
            end,
            task,
        }
    }
}

async fn sleep_until(time: DateTime<Utc>) {
    let now = Utc::now();
    if time > now {
        let delay = (time - now).to_std().unwrap_or(Duration::ZERO);
        info!("Waiting until {time}. Current is {now}. Time to wait: {delay:?}.");
        sleep(delay).await;
    }
}

#[async_trait]
impl ServerRunnable for CronScheduler {
    async fn run(
        self: Box<Self>,
        mut shutdown: watch::Receiver<()>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mask = self.mask;
        let start = self.start;
        let end = self.end;
        let task = Arc::new(self.task);

        let mut scheduler = JobScheduler::new()
            .await
            .map_err(CronJobSchedulerError::SchedulerCreation)?;

        let shutdown_task = shutdown.clone();
        scheduler
            .add(
                Job::new_async(mask, move |uuid, mut l| {
                    Box::pin({
                        let task = task.clone();
                        let shutdown_task = shutdown_task.clone();
                        async move {
                            match task.run(shutdown_task).await {
                                Ok(_) => {
                                    debug!("Cron job executed successfully.")
                                }
                                Err(err) => {
                                    error!("Error executing cron job: {err}.");
                                }
                            };
                            match l.next_tick_for_job(uuid).await {
                                Ok(Some(ts)) => debug!("Next cron tick at {ts}."),
                                _ => error!("Could not get next cron tick"),
                            }
                        }
                    })
                })
                .map_err(CronJobSchedulerError::ScheduleJob)?,
            )
            .await
            .map_err(CronJobSchedulerError::ScheduleJob)?;

        // Wait until start (none means start immediately)
        match start.map(|t| t.datetime_utc()).transpose() {
            Ok(Some(start)) => {
                info!("Cron job scheduler start time is: {start}.");
                tokio::select! {
                    _ = shutdown.changed() => {
                        debug!("Cron job shutdown while starting.");
                        return Ok(()); // do not even start the scheduler
                    },
                    _ = sleep_until(start) => {
                        debug!("Cron job scheduler finished waiting to start.");
                    }
                }
            }
            Ok(None) => {
                debug!("No cron job scheduler start time provided. Proceeding without waiting...");
            }
            Err(err) => {
                warn!("Cron job scheduler start time error: {err}. Proceeding without waiting...");
            }
        };
        scheduler
            .start()
            .await
            .map_err(CronJobSchedulerError::SchedulerCreation)?;
        info!("Cron job scheduler started.");

        let end_fut = match end.map(|t| t.datetime_utc()).transpose() {
            Ok(Some(end_time)) => {
                info!("Cron job scheduler shut down time is: {end_time}.");
                sleep_until(end_time).boxed()
            }
            Ok(None) => {
                debug!("No cron job scheduler end time provided. Proceeding without waiting...");
                future::pending().boxed()
            }
            Err(err) => {
                warn!("Cron job scheduler end time error: {err}. Proceeding without waiting...");
                future::pending().boxed()
            }
        };

        tokio::select! {
            _ = shutdown.changed() => {
                debug!("Cron job scheduler signal shutdown.");
            },
            _ = end_fut => {
                debug!("Cron job scheduler finished waiting to shutdown.");
            }
        }

        scheduler
            .shutdown()
            .await
            .map_err(CronJobSchedulerError::Shutdown)?;
        info!("Cron job scheduler shutdown.");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scheduler::tasks::heartbeat::Heartbeat;
    use tokio::sync::watch;

    #[tokio::test]
    async fn test_cron_scheduler_run() {
        let mask = "0/1 * * * * *".parse().unwrap();
        let task = Box::new(Heartbeat::new("tick".to_string()));

        let scheduler = CronScheduler::new(mask, None, None, task);
        let (tx, rx) = watch::channel(());

        // Run the scheduler in a separate task
        let handle = tokio::spawn(async move {
            let boxed_scheduler = Box::new(scheduler);
            boxed_scheduler.run(rx).await
        });

        // Let it run for a bit
        sleep(Duration::from_millis(1500)).await;

        // Shutdown
        tx.send(()).unwrap();

        // Wait for the scheduler to finish
        let result = handle.await.unwrap();
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_cron_scheduler_start_end() {
        let now = Utc::now();
        let start = now + chrono::Duration::milliseconds(100);
        let end = now + chrono::Duration::milliseconds(2000);

        let mask = "0/1 * * * * *".parse().unwrap();
        let task = Box::new(Heartbeat::new("tick".to_string()));

        let scheduler = CronScheduler::new(
            mask,
            Some(start.timestamp_millis()),
            Some(end.timestamp_millis()),
            task,
        );
        let (_tx, rx) = watch::channel(());

        let boxed_scheduler = Box::new(scheduler);
        let result = boxed_scheduler.run(rx).await;
        assert!(result.is_ok());
    }

    struct MockTask {
        tx: tokio::sync::mpsc::UnboundedSender<()>,
    }

    #[async_trait]
    impl CronJobTask for MockTask {
        async fn run(&self, _shutdown: watch::Receiver<()>) -> Result<(), CronJobSchedulerError> {
            let _ = self.tx.send(());
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_cron_scheduler_ticks() {
        let mask = "0/1 * * * * *".parse().unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let task = Box::new(MockTask { tx });

        let scheduler = CronScheduler::new(mask, None, None, task);
        let (shutdown_tx, shutdown_rx) = watch::channel(());

        let handle = tokio::spawn(async move {
            let boxed_scheduler = Box::new(scheduler);
            boxed_scheduler.run(shutdown_rx).await
        });

        // Wait for 3 ticks
        for _ in 0..3 {
            let received = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await;
            assert!(received.is_ok(), "Timed out waiting for tick");
            assert!(received.unwrap().is_some(), "Channel closed unexpectedly");
        }

        shutdown_tx.send(()).unwrap();
        let result = handle.await.unwrap();
        assert!(result.is_ok());
    }
}
