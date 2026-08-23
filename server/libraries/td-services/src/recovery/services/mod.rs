//
// Copyright 2026 Tabsdata Inc.
//

use axum::extract::FromRef;
use std::sync::Arc;
use ta_services::factory::{FieldAccessors, ServiceFactory};
use td_bridge::BridgeProviderHandle;
use td_common::jwt::JwtConfig;
use td_database::pool::TdPool;
use td_execution::{RecoveryConfig, SchedulerNotify};
use td_objects::types::addresses::InternalServerEndpoint;
use td_storage::Storage;

mod apply;
mod service;
mod snapshot;
mod sweep;

pub use service::RecoveryService;

#[derive(ServiceFactory)]
pub struct RecoveryServices {
    pub recovery: RecoveryService,
}

#[derive(FieldAccessors, FromRef, Clone)]
pub struct RecoveryContext {
    pub db: TdPool,
    pub storage: Arc<Storage>,
    pub bridge: Arc<BridgeProviderHandle>,
    pub internal_endpoint: Arc<InternalServerEndpoint>,
    pub scheduler_notify: Arc<SchedulerNotify>,
    /// Signing key for the worker tokens embedded in the recreated trigger
    /// requests; validated by the internal router on the way back.
    pub jwt_config: Arc<JwtConfig>,
    pub recovery_config: Arc<RecoveryConfig>,
}

#[cfg(feature = "test-utils")]
impl RecoveryContext {
    pub fn with_defaults(db: TdPool) -> Self {
        Self {
            db,
            storage: Arc::new(Storage::default()),
            bridge: Arc::new(BridgeProviderHandle::default()),
            internal_endpoint: Arc::new(InternalServerEndpoint::default()),
            scheduler_notify: Arc::new(SchedulerNotify::new()),
            jwt_config: Arc::new(JwtConfig::default()),
            recovery_config: Arc::new(RecoveryConfig::default()),
        }
    }
}
