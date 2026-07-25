//
// Copyright 2025 Tabsdata Inc.
//

use crate::scheduler::services::dispatch::DispatchService;
use crate::scheduler::services::request::RequestService;
use axum::extract::FromRef;
use std::sync::Arc;
use ta_services::factory::{FieldAccessors, ServiceFactory};
use td_bridge::BridgeProviderHandle;
use td_common::jwt::JwtConfig;
use td_database::pool::TdPool;
use td_execution::{SchedulerConfig, SchedulerNotify};
use td_objects::types::addresses::InternalServerAddresses;
use td_storage::Storage;

mod dispatch;
mod request;

#[derive(ServiceFactory)]
pub struct FunctionSchedulerServices {
    pub request: RequestService,
    pub dispatch: DispatchService,
}

#[derive(FieldAccessors, FromRef, Clone)]
pub struct SchedulerContext {
    pub db: TdPool,
    pub storage: Arc<Storage>,
    pub bridge: Arc<BridgeProviderHandle>,
    pub internal_addresses: Arc<InternalServerAddresses>,
    pub scheduler_notify: Arc<SchedulerNotify>,
    /// Signing key for the worker tokens embedded in callback/resolver
    /// HttpRequests; validated by the internal router on the way back.
    pub jwt_config: Arc<JwtConfig>,
    pub scheduler_config: Arc<SchedulerConfig>,
}

#[cfg(feature = "test-utils")]
impl SchedulerContext {
    pub fn with_defaults(db: TdPool) -> Self {
        Self {
            db,
            storage: Arc::new(Storage::default()),
            bridge: Arc::new(BridgeProviderHandle::default()),
            internal_addresses: Arc::new(InternalServerAddresses::default()),
            scheduler_notify: Arc::new(SchedulerNotify::new()),
            jwt_config: Arc::new(JwtConfig::default()),
            scheduler_config: Arc::new(SchedulerConfig::default()),
        }
    }
}
