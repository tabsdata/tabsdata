//
// Copyright 2025 Tabsdata Inc.
//

use crate::execution::services::runtime_info::RuntimeContext;
use crate::internal_api::InternalApi;
use axum::extract::FromRef;
use std::path::PathBuf;
use ta_services::factory::FieldAccessors;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::session_provider::Sessions;
use td_bridge::BridgeProviderHandle;
use td_common::jwt::JwtConfig;
use td_database::access::{DbPool, Global};
use td_database::domain::DomainPools;
use td_execution::{RecoveryConfig, SchedulerConfig, SchedulerNotify, TransactionBy};
use td_objects::types::addresses::ApiServerAddresses;
use td_security::config::PasswordHashingConfig;
use td_storage::Storage;

pub mod auth;
pub mod collection;
pub mod execution;
pub mod function;
pub mod function_run;
pub mod graph;
pub mod group;
pub mod internal_api;
pub mod logical;
pub mod metrics;
pub mod permission;
pub mod project;
pub mod recovery;
pub mod role;
pub mod scheduler;
pub mod simple_permission;
pub mod system;
pub mod table;
#[cfg(test)]
mod tests;
pub mod transaction;
pub mod user;
pub mod user_role;
pub mod worker;

/// The single service state: the dependency record every `#[service_factory]`
/// fn's entry method is generated onto. Handlers hold one `State<Services>`;
/// tests build one with [`Services::with_defaults`].
#[derive(FieldAccessors, FromRef, Clone)]
pub struct Services {
    pub pools: DomainPools,
    pub server_addresses: Ctx<ApiServerAddresses>,
    pub jwt_config: Ctx<JwtConfig>,
    pub auth_context: Ctx<AuthzContext>,
    pub sessions: Ctx<Sessions>,
    pub password_settings: Ctx<PasswordHashingConfig>,
    pub ssl_folder: Ctx<PathBuf>,
    /// Root served by `GET /resources/{*path}`; `None` 404s every request.
    pub resources_dir: Ctx<Option<PathBuf>>,
    pub storage: Ctx<Storage>,
    pub runtime_context: Ctx<RuntimeContext>,
    pub transaction_by: Ctx<TransactionBy>,
    pub scheduler_notify: Ctx<SchedulerNotify>,
    pub scheduler_config: Ctx<SchedulerConfig>,
    pub recovery_config: Ctx<RecoveryConfig>,
    pub bridge: Ctx<BridgeProviderHandle>,
    pub internal_api: Ctx<InternalApi>,
}

/// The same handle the `#[service_factory]` `Extract` impls build, for tower
/// layers running before any request context exists.
impl FromRef<Services> for DbPool<Global> {
    fn from_ref(services: &Services) -> Self {
        DbPool::new(services.pools.clone(), Global)
    }
}

#[cfg(feature = "test-utils")]
use crate::internal_api::InternalRoutes;
#[cfg(feature = "test-utils")]
use td_database::pool::TdPool;

#[cfg(feature = "test-utils")]
impl Default for Services {
    fn default() -> Self {
        let jwt_config = Ctx::new(JwtConfig::default());
        Self {
            pools: DomainPools::default(),
            server_addresses: Ctx::new(ApiServerAddresses::default()),
            jwt_config: jwt_config.clone(),
            auth_context: Ctx::new(AuthzContext::default()),
            sessions: Ctx::new(Sessions::default()),
            password_settings: Ctx::new(PasswordHashingConfig::default()),
            ssl_folder: Ctx::new(PathBuf::default()),
            resources_dir: Ctx::new(None),
            storage: Ctx::new(Storage::default()),
            runtime_context: Ctx::new(RuntimeContext::default()),
            transaction_by: Ctx::new(TransactionBy::default()),
            scheduler_notify: Ctx::new(SchedulerNotify::new()),
            scheduler_config: Ctx::new(SchedulerConfig::default()),
            recovery_config: Ctx::new(RecoveryConfig::default()),
            bridge: Ctx::new(BridgeProviderHandle::default()),
            internal_api: Ctx::new(InternalApi::new(
                InternalRoutes::default(),
                jwt_config,
                std::sync::Arc::default(),
            )),
        }
    }
}

#[cfg(feature = "test-utils")]
impl Services {
    pub fn with_defaults(db: TdPool) -> Self {
        Self {
            pools: DomainPools::of_apiserver(db),
            ..Self::default()
        }
    }
}
