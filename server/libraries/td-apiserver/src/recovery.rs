//
// Copyright 2026 Tabsdata Inc.
//

use crate::scheduler::v1::timed_tick;
use async_trait::async_trait;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use ta_services::factory::ServiceFactory;
use ta_services::service::TdService;
use td_bridge::BridgeProviderHandle;
use td_common::jwt::JwtConfig;
use td_common::server_runner::ServerRunnable;
use td_database::pool::TdPool;
use td_execution::{RecoveryConfig, SchedulerNotify};
use td_objects::types::addresses::InternalServerEndpoint;
use td_services::recovery::services::{RecoveryContext, RecoveryServices};
use td_storage::Storage;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider};
use tokio::sync::watch;
use tower::{BoxError, ServiceBuilder, ServiceExt};
use tracing::{debug, error, trace};

pub struct RecoveryCtx {
    pub context: RecoveryContext,
}

impl RecoveryCtx {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        db: TdPool,
        storage: Arc<Storage>,
        bridge: Arc<BridgeProviderHandle>,
        internal_endpoint: Arc<InternalServerEndpoint>,
        scheduler_notify: Arc<SchedulerNotify>,
        jwt_config: Arc<JwtConfig>,
        recovery_config: Arc<RecoveryConfig>,
    ) -> Self {
        let context = RecoveryContext {
            db,
            storage,
            bridge,
            internal_endpoint,
            scheduler_notify,
            jwt_config,
            recovery_config,
        };
        Self { context }
    }
}

pub enum WorkerRecoverer {
    V1(Box<WorkerRecovererV1>),
}

impl WorkerRecoverer {
    pub async fn new(context: &RecoveryCtx) -> Self {
        WorkerRecoverer::V1(Box::new(WorkerRecovererV1::new(context).await))
    }
}

#[async_trait]
impl ServerRunnable for WorkerRecoverer {
    async fn run(
        self: Box<Self>,
        shutdown: watch::Receiver<()>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match *self {
            WorkerRecoverer::V1(srv) => srv.run(shutdown).await,
        }
    }
}

/// Timer-only (no notify): recovery reacts to divergence, not to
/// scheduling events.
pub struct WorkerRecovererV1 {
    recovery_service: ServiceProvider<(), (), BoxError>,
    check_frequency: Duration,
    tick_timeout: Duration,
}

impl WorkerRecovererV1 {
    pub async fn new(context: &RecoveryCtx) -> Self {
        let services = RecoveryServices::build(&context.context);
        let tick_timeout = context.context.recovery_config.tick_timeout.0;
        let recovery_service = ServiceBuilder::new()
            .buffer(1)
            .concurrency_limit(1)
            .timeout(tick_timeout)
            .service(services.recovery.service())
            .into_service_provider();
        WorkerRecovererV1 {
            recovery_service,
            check_frequency: context.context.recovery_config.check_frequency.0,
            tick_timeout,
        }
    }

    async fn recover(&self) -> Result<(), BoxError> {
        let service = self.recovery_service.make();
        let result = timed_tick("recovery", self.tick_timeout, service.oneshot(())).await;
        let _ = result?;
        Ok(())
    }
}

#[async_trait]
impl ServerRunnable for WorkerRecovererV1 {
    async fn run(
        self: Box<Self>,
        mut shutdown: watch::Receiver<()>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        // First tick immediately: cold-start convergence must not wait out
        // a full check period.
        match self.recover().await {
            Ok(_) => trace!("Worker recovery executed successfully"),
            Err(e) => error!("Error executing worker recovery: {e:#?}"),
        }
        loop {
            tokio::select! {
                _ = shutdown.changed() => {
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
