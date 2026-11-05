//
// Copyright 2025 Tabsdata Inc.
//

use crate::config::Config;
use crate::router::internal::internal_routes;
use std::sync::Arc;
use ta_apiserver::router::Routes;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::AuthzRegistry;
use td_authz::token_provider::ServiceTokens;
use td_bridge::BridgeProviderHandle;
use td_database::components::ServingPools;
use td_error::TdError;
use td_execution::{ProjectWakes, RecoveryConfig, SchedulerConfig};
use td_objects::types::addresses::InternalServerEndpoint;
use td_objects::types::basic::InstanceId;
use td_services::Services;
use td_services::connection_catalog::ConnectionCatalog;
use td_services::execution::services::runtime_info::RuntimeContext;
use td_services::internal_api::InternalApi;
use td_services::table::sample::TableSampler;
use td_storage::Storage;
use te_services::{ContextExt, ExtendedServices};

pub mod internal;
mod public;
mod server;

pub use public::ApiServer;

/// Where the API mounts. Nested in two steps so `/api/<anything else>` still
/// answers with an API-shaped 404 while non-`/api` paths fall through.
pub(crate) const BASE_API_URL: &str = "/api";
pub(crate) const V1: &str = "/v1";
/// The prefix a nested route's matched path carries, and so the prefix an
/// internal token's scope is built from.
pub const BASE_URL: &str = constcat::concat!(BASE_API_URL, V1);

#[cfg(feature = "api-docs")]
pub(crate) const DOCS_URL: &str = constcat::concat!(BASE_API_URL, "/docs");
#[cfg(feature = "api-docs")]
pub(crate) const OPENAPI_JSON_URL: &str = constcat::concat!(BASE_API_URL, "/api-docs/openapi.json");

/// axum catch-all segments (`{*name}`) are invalid OpenAPI path templating —
/// Swagger substitutes parameters by name, finds no `{name}`, and leaves the
/// literal `{*name}` in the request URL. Rewrite them to `{name}` so the
/// placeholder matches the declared parameter. Routing is untouched: the axum
/// Router is built from the raw `{*name}` path before the doc is split off
/// (`OpenApiRouter::split_for_parts`), so only the served spec changes.
#[cfg(any(feature = "api-docs", test))]
pub(crate) fn normalize_catch_all_paths(openapi: &mut utoipa::openapi::OpenApi) {
    let paths = std::mem::take(&mut openapi.paths.paths);
    openapi.paths.paths = paths
        .into_iter()
        .map(|(path, item)| (path.replace("{*", "{"), item))
        .collect();
}

pub struct ApiServerCtx {
    pub config: Config,
    pub services: Services,
    pub extended_services: ExtendedServices,
    pub routes: Arc<Routes>,
}

impl ApiServerCtx {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        config: Config,
        serving_pools: ServingPools,
        storage: Ctx<Storage>,
        runtime_context: Ctx<RuntimeContext>,
        instance_id: Ctx<InstanceId>,
        bridge: Ctx<BridgeProviderHandle>,
        authz_registry: Arc<AuthzRegistry>,
        internal_endpoint: Arc<InternalServerEndpoint>,
        routes: Arc<Routes>,
    ) -> Result<Self, TdError> {
        let jwt_config = Ctx::new(config.jwt.clone());
        let services = Services {
            pools: serving_pools.domain_pools(),
            server_addresses: Ctx::new(config.addresses.clone()),
            internal_api: Ctx::new(InternalApi::new(
                internal_routes(),
                jwt_config.clone(),
                internal_endpoint,
            )),
            jwt_config,
            // Built-in authz kinds plus extensions (e.g. te-authz's `ai`).
            auth_context: Ctx::new(AuthzContext::with_registry(authz_registry)),
            tokens: Ctx::new(ServiceTokens::default()),
            token_settings: Ctx::new(config.service_tokens.clone()),
            password_settings: {
                // to verify up front configuration is OK.
                let password_hash_config = config.password.clone();
                let _ = password_hash_config.password_hasher();
                Ctx::new(password_hash_config)
            },
            ssl_folder: Ctx::new(config.ssl_folder.clone()),
            resources_dir: Ctx::new(config.resources_folder.clone()),
            storage: storage.clone(),
            table_sampler: Ctx::new(TableSampler::default()),
            runtime_context: runtime_context.clone(),
            instance_id,
            transaction_by: Ctx::new(config.transaction_by.clone()),
            wakes: Ctx::new(ProjectWakes::default()),
            scheduler_config: Ctx::new(SchedulerConfig {
                check_frequency: config.check_frequency,
                max_functions_per_tick: config.max_functions_per_tick,
                tick_timeout: config.tick_timeout,
            }),
            recovery_config: Ctx::new(RecoveryConfig {
                check_frequency: config.check_frequency,
                tick_timeout: config.recovery_tick_timeout,
                stuck_requested_timeout: config.stuck_requested_timeout,
                dispatch_grace: config.dispatch_grace,
                max_pre_running_time: config.max_pre_running_time,
                max_running_time: config.max_running_time,
                max_completion_time: config.max_completion_time,
                max_recoveries_per_tick: config.max_recoveries_per_tick,
            }),
            bridge,
            connection_catalog: Ctx::new(ConnectionCatalog::default()),
            catalog_config: Ctx::new(config.catalog.clone()),
        };
        let extended_services = services
            .extend(&serving_pools, &config.extended_config)
            .await?;

        Ok(Self {
            config,
            services,
            extended_services,
            routes,
        })
    }
}

#[cfg(test)]
mod catch_all_tests {
    use super::normalize_catch_all_paths;
    use utoipa::openapi::{OpenApiBuilder, PathItem, PathsBuilder};

    #[test]
    fn rewrites_catch_all_to_openapi_template() {
        let paths = PathsBuilder::new()
            .path("/api/v1/resources/{*path}", PathItem::default())
            .path(
                "/projects/{project}/collections/{collection}",
                PathItem::default(),
            )
            .build();
        let mut openapi = OpenApiBuilder::new().paths(paths).build();

        normalize_catch_all_paths(&mut openapi);

        let keys: Vec<&str> = openapi.paths.paths.keys().map(String::as_str).collect();
        // Catch-all sigil stripped so Swagger can substitute the `path` param.
        assert!(
            keys.contains(&"/api/v1/resources/{path}"),
            "catch-all not normalized, got: {keys:?}"
        );
        // Ordinary single-segment params are left alone.
        assert!(
            keys.contains(&"/projects/{project}/collections/{collection}"),
            "normal params altered, got: {keys:?}"
        );
    }
}
