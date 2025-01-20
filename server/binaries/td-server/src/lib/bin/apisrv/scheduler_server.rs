//
//   Copyright 2024 Tabs Data Inc.
//

use crate::common::signal::terminate;
use crate::logic::datasets::service::execution::schedule::ScheduleServices;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use td_common::error::TdError;
use td_common::server::WorkerMessageQueue;
use td_database::sql::DbPool;
use td_error::td_error;
use td_storage::Storage;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider};
use tokio::select;
use tower::{BoxError, ServiceBuilder, ServiceExt};
use tower_service::Service;
use tracing::{error, trace};

pub struct Scheduler {
    scheduler_service: ServiceProvider<(), (), SchedulerError>,
    commit_service: ServiceProvider<(), (), SchedulerError>,
}

impl Scheduler {
    async fn schedule(&self) -> Result<(), SchedulerError> {
        let service = self.scheduler_service.make().await;
        service.oneshot(()).await
    }

    async fn commit(&self) -> Result<(), SchedulerError> {
        let service = self.commit_service.make().await;
        service.oneshot(()).await
    }

    pub async fn run(self) {
        let this = Arc::new(self);

        let scheduler = this.clone();
        tokio::spawn(async move {
            loop {
                match scheduler.schedule().await {
                    Ok(_) => trace!("Execution plan scheduler executed successfully"),
                    Err(e) => {
                        error!("Error executing execution plan scheduler: {}", e);
                    }
                };
            }
        });

        let scheduler = this.clone();
        tokio::spawn(async move {
            loop {
                match scheduler.commit().await {
                    Ok(_) => trace!("Execution plan scheduler commit executed successfully"),
                    Err(e) => {
                        error!("Error executing execution plan scheduler commit: {}", e);
                    }
                };
            }
        });

        select! {
            _ = terminate() => {
                trace!("Stopping Scheduler");
            }
        }
    }
}

pub struct SchedulerBuilder<Q> {
    db: DbPool,
    storage: Arc<Storage>,
    worker_message_queue: Arc<Q>,
    server_url: Arc<SocketAddr>,
}

impl<Q> SchedulerBuilder<Q>
where
    Q: WorkerMessageQueue,
{
    pub fn new(
        db: DbPool,
        storage: Arc<Storage>,
        worker_message_queue: Arc<Q>,
        server_url: Arc<SocketAddr>,
    ) -> Self {
        Self {
            db,
            storage,
            worker_message_queue,
            server_url,
        }
    }

    pub fn build(self) -> Scheduler {
        let services = ScheduleServices::new(
            self.db,
            self.storage,
            self.worker_message_queue,
            self.server_url,
        );
        let services = Arc::new(services);

        // TODO, bring it back to 5 seconds after adding checks following trigger/callback calls
        const CHECK_FREQUENCY: Duration = Duration::from_millis(500);

        let scheduler_service = ServiceBuilder::new()
            .map_err(SchedulerError::ServiceError)
            .buffer(1)
            .concurrency_limit(1)
            .rate_limit(1, CHECK_FREQUENCY)
            .timeout(Duration::from_secs(10))
            .service(SchedulerService::new(services.clone()))
            .into_service_provider();

        let commit_service = ServiceBuilder::new()
            .map_err(SchedulerError::ServiceError)
            .buffer(1)
            .concurrency_limit(1)
            .rate_limit(1, CHECK_FREQUENCY)
            .timeout(Duration::from_secs(10))
            .service(CommitService::new(services.clone()))
            .into_service_provider();

        Scheduler {
            scheduler_service,
            commit_service,
        }
    }
}

#[td_error]
pub enum SchedulerError {
    #[error("Service error: {0}")]
    ServiceError(#[from] BoxError) = 5000,
}

macro_rules! scheduler_service {
    ($name:ident, $func:ident) => {
        pub struct $name<Q> {
            services: Arc<ScheduleServices<Q>>,
        }

        impl<Q> $name<Q> {
            fn new(services: Arc<ScheduleServices<Q>>) -> Self {
                Self { services }
            }
        }

        impl<Q> Clone for $name<Q> {
            fn clone(&self) -> Self {
                Self {
                    services: self.services.clone(),
                }
            }
        }

        impl<Q> Service<()> for $name<Q>
        where
            Q: WorkerMessageQueue,
        {
            type Response = ();
            type Error = TdError;
            type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

            fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
                Poll::Ready(Ok(()))
            }

            fn call(&mut self, _req: ()) -> Self::Future {
                let services = self.services.clone();

                Box::pin(async move {
                    $func(services).await?;
                    Ok(())
                })
            }
        }
    };
}

scheduler_service!(SchedulerService, scheduler);
scheduler_service!(CommitService, commit);

async fn scheduler<Q>(services: Arc<ScheduleServices<Q>>) -> Result<(), TdError>
where
    Q: WorkerMessageQueue,
{
    let datasets = services.poll().await.oneshot(()).await?;
    trace!(
        "Found {} functions ready to execute: {:#?}",
        datasets.len(),
        datasets
    );

    // We do not error out on single message errors
    for ds in datasets.into_iter() {
        if let Err(e) = services.create().await.oneshot(ds).await {
            error!("Error creating worker message: {}", e);
        }
    }
    Ok(())
}

async fn commit<Q>(services: Arc<ScheduleServices<Q>>) -> Result<(), TdError>
where
    Q: WorkerMessageQueue,
{
    let locked = services.list().await.oneshot(()).await?;
    trace!(
        "Found {} locked messages in the queue: {:#?}",
        locked.len(),
        locked
    );

    // We do not error out on single message errors
    for message in locked.into_iter() {
        if let Err(e) = services.commit().await.oneshot(message).await {
            error!("Error committing worker message: {}", e);
        }
    }
    Ok(())
}
