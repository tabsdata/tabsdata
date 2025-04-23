//
// Copyright 2025 Tabs Data Inc.
//

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use td_common::server::WorkerMessageQueue;
use td_common::signal::terminate;
use td_database::sql::DbPool;
use td_services::execution::services::SchedulerServices;
use td_storage::Storage;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider};
use tokio::select;
use tower::{BoxError, ServiceBuilder, ServiceExt};
use tracing::{error, trace};

pub struct Scheduler {
    request_service: ServiceProvider<(), (), BoxError>,
    commit_service: ServiceProvider<(), (), BoxError>,
}

impl Scheduler {
    async fn request(&self) -> Result<(), BoxError> {
        let service = self.request_service.make().await;
        let _ = service.oneshot(()).await?;
        Ok(())
    }

    async fn commit(&self) -> Result<(), BoxError> {
        let service = self.commit_service.make().await;
        let _ = service.oneshot(()).await?;
        Ok(())
    }

    pub async fn run(self) {
        let this = Arc::new(self);

        let scheduler = this.clone();
        tokio::spawn(async move {
            loop {
                match scheduler.request().await {
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

    pub async fn build(self) -> Scheduler {
        let services = SchedulerServices::new(
            self.db,
            self.storage,
            self.worker_message_queue,
            self.server_url,
        );

        // TODO, bring it back to 5 seconds after adding checks following trigger/callback calls
        const CHECK_FREQUENCY: Duration = Duration::from_millis(500);

        let request_service = ServiceBuilder::new()
            .buffer(1)
            .concurrency_limit(1)
            .rate_limit(1, CHECK_FREQUENCY)
            .timeout(Duration::from_secs(10))
            .service(services.request().await)
            .into_service_provider();

        let commit_service = ServiceBuilder::new()
            .buffer(1)
            .concurrency_limit(1)
            .rate_limit(1, CHECK_FREQUENCY)
            .timeout(Duration::from_secs(10))
            .service(services.commit().await)
            .into_service_provider();

        Scheduler {
            request_service,
            commit_service,
        }
    }
}
