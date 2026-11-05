//
// Copyright 2025 Tabsdata Inc.
//

#![cfg_attr(test, allow(linker_messages))]

use crate::connection_catalog::ConnectionCatalog;
use crate::execution::services::runtime_info::RuntimeContext;
use crate::internal_api::InternalApi;
use crate::table::sample::TableSampler;
use axum::extract::FromRef;
use std::path::PathBuf;
use ta_services::factory::FieldAccessors;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::token_provider::ServiceTokens;
use td_bridge::BridgeProviderHandle;
use td_common::jwt::JwtConfig;
use td_database::access::{DbPool, Global};
use td_database::domain::DomainPools;
use td_execution::{ProjectWakes, RecoveryConfig, SchedulerConfig, TransactionBy};
use td_objects::types::addresses::ApiServerAddresses;
use td_objects::types::basic::InstanceId;
use td_objects::types::catalog::CatalogConfig;
use td_security::config::PasswordHashingConfig;
#[cfg(feature = "test-utils")]
use td_security::token::config::MaxTokenDays;
use td_security::token::config::ServiceTokenConfig;
use td_storage::Storage;

pub mod auth;
pub mod collection;
pub mod connection_catalog;
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
pub mod tag;
#[cfg(test)]
mod tests;
pub mod token;
pub mod topology;
pub mod transaction;
pub mod ui_data;
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
    pub tokens: Ctx<ServiceTokens>,
    pub token_settings: Ctx<ServiceTokenConfig>,
    pub password_settings: Ctx<PasswordHashingConfig>,
    pub ssl_folder: Ctx<PathBuf>,
    /// Root served by `GET /resources/{*path}`; `None` 404s every request.
    pub resources_dir: Ctx<Option<PathBuf>>,
    pub storage: Ctx<Storage>,
    pub table_sampler: Ctx<TableSampler>,
    pub runtime_context: Ctx<RuntimeContext>,
    pub instance_id: Ctx<InstanceId>,
    pub transaction_by: Ctx<TransactionBy>,
    pub wakes: Ctx<ProjectWakes>,
    pub scheduler_config: Ctx<SchedulerConfig>,
    pub recovery_config: Ctx<RecoveryConfig>,
    pub bridge: Ctx<BridgeProviderHandle>,
    pub internal_api: Ctx<InternalApi>,
    pub connection_catalog: Ctx<ConnectionCatalog>,
    pub catalog_config: Ctx<CatalogConfig>,
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
use crate::scheduler::services::ProjectPass;
#[cfg(feature = "test-utils")]
use td_database::pool::TdPool;
#[cfg(feature = "test-utils")]
use td_error::TdError;

#[cfg(feature = "test-utils")]
impl Default for Services {
    fn default() -> Self {
        let jwt_config = Ctx::new(JwtConfig::default());
        Self {
            pools: DomainPools::default(),
            server_addresses: Ctx::new(ApiServerAddresses::default()),
            jwt_config: jwt_config.clone(),
            auth_context: Ctx::new(AuthzContext::default()),
            tokens: Ctx::new(ServiceTokens::default()),
            token_settings: Ctx::new(ServiceTokenConfig {
                max_days: Some(MaxTokenDays::Unbounded),
            }),
            password_settings: Ctx::new(PasswordHashingConfig::default()),
            ssl_folder: Ctx::new(PathBuf::default()),
            resources_dir: Ctx::new(None),
            storage: Ctx::new(Storage::default()),
            table_sampler: Ctx::new(TableSampler::default()),
            runtime_context: Ctx::new(RuntimeContext::default()),
            instance_id: Ctx::new(
                InstanceId::try_from("00000000-0000-0000-0000-000000000000").unwrap(),
            ),
            transaction_by: Ctx::new(TransactionBy::default()),
            wakes: Ctx::new(ProjectWakes::default()),
            scheduler_config: Ctx::new(SchedulerConfig::default()),
            recovery_config: Ctx::new(RecoveryConfig::default()),
            bridge: Ctx::new(BridgeProviderHandle::default()),
            internal_api: Ctx::new(InternalApi::new(
                InternalRoutes::default(),
                jwt_config,
                std::sync::Arc::default(),
            )),
            connection_catalog: Ctx::new(ConnectionCatalog::default()),
            catalog_config: Ctx::new(CatalogConfig::default()),
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

    /// The sweep, then one pass of every runnable project, inline.
    pub async fn tick(&self) -> Result<(), TdError> {
        self.sweep(()).await?;
        let runnable: Vec<_> = std::iter::from_fn(|| self.wakes.take()).collect();
        for (project, concerns) in runnable {
            let again = self
                .pass(ProjectPass { project, concerns })
                .await
                .map(|again| again.data);
            self.wakes
                .finish(&project, again.as_ref().copied().unwrap_or_default());
            again?;
        }
        Ok(())
    }
}
