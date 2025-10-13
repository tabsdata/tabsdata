//
// Copyright 2025 Tabs Data Inc.
//

use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use ta_services::factory::ServiceFactory;
use ta_services::service::TdService;
use td_common::server::FileWorkerMessageQueue;
use td_common::signal::terminate;
use td_database::sql::DbPool;
use td_objects::sql::DaoQueries;
use td_objects::types::basic::InternalServerAddresses;
use td_services::SchedulerContext;
use td_services::scheduler::services::ScheduleServices;
use td_storage::Storage;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider};
use tokio::select;
use tower::{BoxError, ServiceBuilder, ServiceExt};
use tracing::{Instrument, Level, error, span, trace};

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

    pub async fn run(self) -> Result<(), Box<dyn Error>> {
        let this = Arc::new(self);
        // TODO make span part of the service, not the futures
        let log_span = span!(Level::INFO, "scheduler");

        let scheduler = this.clone();
        let request_future = async move {
            loop {
                match scheduler.request().await {
                    Ok(_) => trace!("Execution plan scheduler executed successfully"),
                    Err(e) => {
                        error!("Error executing execution plan scheduler: {}", e);
                    }
                };
            }
        }
        .instrument(log_span.clone());
        tokio::spawn(request_future);

        let scheduler = this.clone();
        let commit_future = async move {
            loop {
                match scheduler.commit().await {
                    Ok(_) => trace!("Execution plan scheduler commit executed successfully"),
                    Err(e) => {
                        error!("Error executing execution plan scheduler commit: {}", e);
                    }
                };
            }
        }
        .instrument(log_span.clone());
        tokio::spawn(commit_future);

        select! {
            _ = terminate() => {
                trace!("Stopping Scheduler Server");
                Ok(())
            }
        }
    }
}

#[derive(ServiceFactory)]
pub struct SchedulerBuilder {
    services: ScheduleServices,
}

impl SchedulerBuilder {
    pub fn new(
        db: DbPool,
        queries: Arc<DaoQueries>,
        storage: Arc<Storage>,
        worker_queue: Arc<FileWorkerMessageQueue>,
        internal_addresses: Arc<InternalServerAddresses>,
    ) -> Self {
        let context = SchedulerContext {
            db,
            queries,
            storage,
            worker_queue,
            internal_addresses,
        };

        let services = ScheduleServices::build(&context);
        Self { services }
    }

    pub async fn build(self) -> Scheduler {
        // TODO, bring it back to 5 seconds after adding checks following trigger/callback calls
        const CHECK_FREQUENCY: Duration = Duration::from_millis(500);

        let request_service = ServiceBuilder::new()
            .buffer(1)
            .concurrency_limit(1)
            .rate_limit(1, CHECK_FREQUENCY)
            .timeout(Duration::from_secs(10))
            .service(self.services.request().service().await)
            .into_service_provider();

        let commit_service = ServiceBuilder::new()
            .buffer(1)
            .concurrency_limit(1)
            .rate_limit(1, CHECK_FREQUENCY)
            .timeout(Duration::from_secs(10))
            .service(self.services.commit().service().await)
            .into_service_provider();

        Scheduler {
            request_service,
            commit_service,
        }
    }
}
