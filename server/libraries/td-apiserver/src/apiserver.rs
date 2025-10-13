//
// Copyright 2025 Tabs Data Inc.
//

//! API Server generator. Any number of routers might be added, with any number of layer per
//! router. Specifics of each router are defined in their respective modules.
//!
//! Layers go from general to specific. Following axum middleware documentation, for the layer
//! we use in [`users`]:
//! ```json
//!                    requests
//!                       |
//!                       v
//!         --------- TraceService ---------
//!          -------- CorsService ---------
//!           ------- ............ -------
//!            ---- JwtDecoderService ----   <--- RequestContext
//!                  users.router()
//!              ----- AdminOnly -----
//!
//!                   list_users
//!
//!              ----- AdminOnly -----
//!                 users.router()
//!            ---- JwtDecoderService ----
//!           ------- ............ -------
//!          -------- CorsService ---------
//!         --------- TraceService ---------
//!                       |
//!                       v
//!                    responses
//! ```

use crate::config::Config;
use crate::layers::authorization::authorization_layer;
use crate::layers::cors::CorsService;
use crate::layers::tracing::TraceService;
use crate::layers::uri_filter::LoopbackIpFilterService;
use crate::router::auth::{SecureAuthRouter, UnsecureAuthRouter};
use crate::router::collections::CollectionsRouter;
use crate::router::executions::ExecutionsRouter;
use crate::router::function_runs::FunctionRunsRouter;
use crate::router::functions::FunctionsRouter;
use crate::router::inter_collection_permissions::InterCollectionPermissionsRouter;
use crate::router::internal::InternalRouter;
use crate::router::permissions::PermissionsRouter;
use crate::router::roles::RolesRouter;
use crate::router::server_status::ServerStatusRouter;
use crate::router::tables::TablesRouter;
use crate::router::transactions::TransactionsRouter;
use crate::router::user_roles::UserRolesRouter;
use crate::router::users::UsersRouter;
use crate::router::workers::WorkersRouter;
use crate::{Server, ServerBuilder, ServerError};
use axum::middleware::{from_fn, from_fn_with_state};
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use ta_apiserver::router::RouterExtension;
use ta_apiserver::status::error_status::ErrorStatus;
use ta_services::extension::ContextExt;
use ta_services::factory::ServiceFactory;
use td_authz::AuthzContext;
use td_database::sql::DbPool;
use td_error::{ApiError, api_error};
use td_objects::sql::DaoQueries;
use td_objects::types::basic::{ApiServerAddresses, InternalServerAddresses, NonEmptyAddresses};
use td_services::auth::session::Sessions;
use td_services::execution::services::runtime_info::RuntimeContext;
use td_services::{Context, Services};
use td_storage::Storage;
use te_apiserver::{AuthenticatedExtendedRouter, UnauthenticatedExtendedRouter};
use te_services::{ExtendedContext, ExtendedServices};
use tower_http::timeout::TimeoutLayer;

pub struct ApiServerInstance {
    internal: Box<dyn Server>,
    api_v1: Box<dyn Server>,
}

impl ApiServerInstance {
    pub async fn api_v1_addresses(&self) -> Result<ApiServerAddresses, Box<dyn Error>> {
        let addr = self
            .api_v1
            .listeners()
            .iter()
            .map(|listener| listener.local_addr())
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ApiServerAddresses(NonEmptyAddresses::from_vec(addr)?))
    }

    pub async fn internal_addresses(&self) -> Result<InternalServerAddresses, Box<dyn Error>> {
        let addr = self
            .internal
            .listeners()
            .iter()
            .map(|listener| listener.local_addr())
            .collect::<Result<Vec<_>, _>>()?;
        Ok(InternalServerAddresses(NonEmptyAddresses::from_vec(addr)?))
    }

    pub async fn run(self) -> Result<(), Box<dyn Error>> {
        tokio::try_join!(self.internal.run(), self.api_v1.run()).map(|_| ())
    }
}

#[allow(dead_code)]
pub struct ApiServerInstanceBuilder {
    config: Config,
    context: Context,
    services: Services,
    extended_context: ExtendedContext,
    extended_services: ExtendedServices,
}

impl ApiServerInstanceBuilder {
    pub fn new(
        config: Config,
        db: DbPool,
        queries: Arc<DaoQueries>,
        storage: Arc<Storage>,
        runtime_context: Arc<RuntimeContext>,
    ) -> Self {
        // to verify up front configuration is OK.
        let password_hash_config = config.password();
        password_hash_config.password_hasher();

        let context = Context {
            db: db.clone(),
            queries: queries.clone(),
            server_addresses: Arc::new(config.addresses().clone()),
            jwt_config: Arc::new(config.jwt().clone()),
            auth_context: Arc::new(AuthzContext::default()),
            sessions: Arc::new(Sessions::default()),
            password_settings: Arc::new(password_hash_config.clone()),
            ssl_folder: Arc::new(config.ssl_folder().clone()),
            storage: storage.clone(),
            runtime_context: runtime_context.clone(),
            transaction_by: Arc::new(config.transaction_by().clone()),
        };
        let services = Services::build(&context);
        let extended_context = ExtendedContext::build(&context, config.extended_config());
        let extended_services = ExtendedServices::build(&extended_context);

        Self {
            config,
            context,
            services,
            extended_context,
            extended_services,
        }
    }

    pub async fn build(&self) -> Result<ApiServerInstance, ServerError> {
        async fn api_not_found_handler() -> ErrorStatus {
            api_error!(ApiError::NotFound, "API endpoint not found").into()
        }

        let api_v1 = {
            // API router
            let router = utoipa_axum::router::OpenApiRouter::default()
                // unsecure endpoints
                .merge(
                    utoipa_axum::router::OpenApiRouter::default()
                        .merge(UnsecureAuthRouter::router(self.services.clone())),
                )
                // secure endpoints
                .merge(
                    utoipa_axum::router::OpenApiRouter::default()
                        .merge(SecureAuthRouter::router(self.services.clone()))
                        .merge(CollectionsRouter::router(self.services.clone()))
                        .merge(ExecutionsRouter::router(self.services.clone()))
                        .merge(FunctionsRouter::router(self.services.clone()))
                        .merge(FunctionRunsRouter::router(self.services.clone()))
                        .merge(InterCollectionPermissionsRouter::router(
                            self.services.clone(),
                        ))
                        .merge(PermissionsRouter::router(self.services.clone()))
                        .merge(RolesRouter::router(self.services.clone()))
                        .merge(ServerStatusRouter::router(self.services.clone()))
                        .merge(UserRolesRouter::router(self.services.clone()))
                        .merge(UsersRouter::router(self.services.clone()))
                        .merge(TablesRouter::router(self.services.clone()))
                        .merge(TransactionsRouter::router(self.services.clone()))
                        .merge(WorkersRouter::router(self.services.clone()))
                        .merge(AuthenticatedExtendedRouter::router(
                            self.extended_services.clone(),
                        ))
                        // authorization layer
                        .layer(from_fn_with_state(
                            self.context.clone(),
                            authorization_layer,
                        )),
                );

            // Nest the router in the V1 address.
            let router = utoipa_axum::router::OpenApiRouter::default()
                .nest(td_objects::rest_urls::V1, router)
                // Everything going to /api and is not found, is a not found. Only non /api calls get through.
                .fallback(api_not_found_handler);
            // Nest the router in the base API URL.
            let router = utoipa_axum::router::OpenApiRouter::default()
                .nest(td_objects::rest_urls::BASE_API_URL, router);

            // Add any router extensions.
            let router = router.merge(UnauthenticatedExtendedRouter::router(
                self.extended_services.clone(),
            ));

            // Add docs endpoints if the feature is enabled.
            #[allow(unused_mut, unused_variables)]
            let (mut router, openapi) = router.split_for_parts();

            #[cfg(feature = "api-docs")]
            {
                use td_build::version::TABSDATA_VERSION;
                use td_objects::rest_urls::{DOCS_URL, OPENAPI_JSON_URL};

                use utoipa_swagger_ui::{SwaggerUi, Url};

                let mut openapi = openapi;
                openapi.info.title = "Tabsdata API".to_string();
                openapi.info.version = TABSDATA_VERSION.to_string();
                let components: &mut utoipa::openapi::Components =
                    openapi.components.as_mut().unwrap();
                components.add_security_scheme(
                    "Token",
                    utoipa::openapi::security::SecurityScheme::Http(
                        utoipa::openapi::security::Http::new(
                            utoipa::openapi::security::HttpAuthScheme::Bearer,
                        ),
                    ),
                );
                router = router.merge(axum::Router::from(
                    SwaggerUi::new(DOCS_URL)
                        .url(Url::new("API V1 Docs", OPENAPI_JSON_URL), openapi),
                ));
            }

            // Default layers
            let router = router
                .layer(TimeoutLayer::new(Duration::from_secs(
                    *self.config.request_timeout() as u64,
                )))
                .layer(CorsService::layer())
                .layer(TraceService::layer());

            ServerBuilder::new(self.config.addresses().clone(), router)
                .tls(self.config.ssl_folder())
                .build()
                .await
        }?;

        let internal = {
            // Internal router, only accessible from loopback IPs
            let router = utoipa_axum::router::OpenApiRouter::default().merge(
                utoipa_axum::router::OpenApiRouter::default()
                    .merge(InternalRouter::router(self.services.clone()))
                    // internal authorization layer
                    .layer(from_fn(LoopbackIpFilterService::layer)),
            );

            // Nest the router in the V1 address.
            let router = utoipa_axum::router::OpenApiRouter::default()
                .nest(td_objects::rest_urls::V1, router)
                // Everything going to /api and is not found, is a not found. Only non /api calls get through.
                .fallback(api_not_found_handler);
            // Nest the router in the base API URL.
            let router = utoipa_axum::router::OpenApiRouter::default()
                .nest(td_objects::rest_urls::BASE_API_URL, router);

            // Default layers
            let router = router
                .layer(TimeoutLayer::new(Duration::from_secs(
                    *self.config.request_timeout() as u64,
                )))
                .layer(CorsService::layer())
                .layer(TraceService::layer());

            ServerBuilder::new(self.config.internal_addresses().clone(), router.into())
                .build()
                .await
        }?;

        Ok(ApiServerInstance { internal, api_v1 })
    }
}
