//
// Copyright 2025 Tabs Data Inc.
//

use crate::apiserver::server::ServerError;
use crate::apiserver::v1::ApiServerV1;
use crate::config::Config;
use async_trait::async_trait;
use std::error::Error;
use std::sync::Arc;
use ta_services::factory::ServiceFactory;
use td_authz::AuthzContext;
use td_authz::session_provider::Sessions;
use td_bridge::BridgeProviderHandle;
use td_common::server_runner::ServerRunnable;
use td_database::system::{APISERVER_COMPONENT_KEY, ServingPools};
use td_error::TdError;
use td_execution::SchedulerNotify;
use td_services::execution::services::runtime_info::RuntimeContext;
use td_services::{Context, Services};
use td_storage::Storage;
use te_services::{ContextExt, ExtendedContext, ExtendedServices};
use tokio::sync::watch;

pub mod internal;
mod server;
pub mod v1;

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

#[allow(dead_code)]
pub struct ApiServerCtx {
    pub config: Config,
    pub context: Context,
    pub services: Services,
    pub extended_context: ExtendedContext,
    pub extended_services: ExtendedServices,
}

impl ApiServerCtx {
    pub async fn new(
        config: Config,
        serving_pools: ServingPools,
        storage: Arc<Storage>,
        runtime_context: Arc<RuntimeContext>,
        scheduler_notify: Arc<SchedulerNotify>,
        bridge: Arc<BridgeProviderHandle>,
        authz_registry: Arc<td_authz::rbac::AuthzRegistry>,
    ) -> Result<Self, TdError> {
        let apiserver_pool = serving_pools.get(APISERVER_COMPONENT_KEY)?.clone();
        let context = Context {
            apiserver_pool,
            database_descriptor: Arc::new(config.database.clone()),
            server_addresses: Arc::new(config.addresses.clone()),
            jwt_config: Arc::new(config.jwt.clone()),
            // Authorization source built from the wired registry (built-in kinds
            // plus extensions, e.g. te-authz's `ai` / `manage_ai`).
            auth_context: Arc::new(AuthzContext::with_registry(authz_registry)),
            sessions: Arc::new(Sessions::default()),
            password_settings: {
                // to verify up front configuration is OK.
                let password_hash_config = config.password.clone();
                let _ = password_hash_config.password_hasher();
                Arc::new(password_hash_config)
            },
            ssl_folder: Arc::new(config.ssl_folder.clone()),
            storage: storage.clone(),
            runtime_context: runtime_context.clone(),
            transaction_by: Arc::new(config.transaction_by.clone()),
            scheduler_notify: scheduler_notify.clone(),
            bridge,
        };
        let services = Services::build(&context);
        // Edition extensions extend the base context with the serving pools (each
        // grabs the component pool it owns) and the edition's resolved config; the
        // pools are consumed here, not carried in the base context.
        let extended_context = context
            .extend(&serving_pools, &config.extended_config)
            .await?;
        let extended_services = ExtendedServices::build(&extended_context);

        Ok(Self {
            config,
            context,
            services,
            extended_context,
            extended_services,
        })
    }
}

pub enum ApiServer {
    V1(ApiServerV1),
}

impl ApiServer {
    pub async fn new(context: &ApiServerCtx) -> Result<Self, ServerError> {
        Ok(ApiServer::V1(ApiServerV1::new(context).await?))
    }
}

#[async_trait]
impl ServerRunnable for ApiServer {
    async fn run(
        self: Box<Self>,
        shutdown: watch::Receiver<()>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        match *self {
            ApiServer::V1(server) => server.inner.run(shutdown).await,
        }
    }
}
