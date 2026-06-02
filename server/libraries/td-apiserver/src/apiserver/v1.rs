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

use crate::apiserver::ApiServerCtx;
use crate::apiserver::server::{Server, ServerBuilder, ServerError};
use crate::layers::authorization::authorization_layer;
use crate::layers::compression::CompressionService;
use crate::layers::cors::CorsService;
use crate::layers::tracing::TraceService;
use crate::router::auth::{SecureAuthRouter, UnsecureAuthRouter};
use crate::router::collections::CollectionsRouter;
use crate::router::executions::ExecutionsRouter;
use crate::router::function_runs::FunctionRunsRouter;
use crate::router::functions::FunctionsRouter;
use crate::router::groups::GroupsRouter;
use crate::router::inter_collection_permissions::InterCollectionPermissionsRouter;
use crate::router::logical_versions::LogicalVersionsRouter;
use crate::router::metrics::MetricsRouter;
use crate::router::permissions::PermissionsRouter;
use crate::router::ping::ping_router;
use crate::router::roles::RolesRouter;
use crate::router::server_status::ServerStatusRouter;
use crate::router::tables::TablesRouter;
use crate::router::transactions::TransactionsRouter;
use crate::router::user_roles::UserRolesRouter;
use crate::router::users::UsersRouter;
use crate::router::workers::WorkersRouter;
use axum::middleware::from_fn_with_state;
use http::StatusCode;
use std::time::Duration;
use ta_apiserver::router::RouterExtension;
use ta_apiserver::status::error_status::ErrorStatus;
use td_error::{ApiError, api_error};
use te_apiserver::{AuthenticatedExtendedRouter, UnauthenticatedExtendedRouter};
use tower_http::timeout::TimeoutLayer;

pub struct ApiServerV1 {
    pub inner: Box<dyn Server>,
}

impl ApiServerV1 {
    pub async fn new(context: &ApiServerCtx) -> Result<Self, ServerError> {
        async fn api_not_found_handler() -> ErrorStatus {
            api_error!(ApiError::NotFound, "API endpoint not found").into()
        }

        let inner = {
            // API router
            let router = utoipa_axum::router::OpenApiRouter::default()
                // unsecure endpoints
                .merge(
                    utoipa_axum::router::OpenApiRouter::default()
                        .merge(UnsecureAuthRouter::router(context.services.clone()))
                        .merge(ping_router()),
                )
                // secure endpoints
                .merge(
                    utoipa_axum::router::OpenApiRouter::default()
                        .merge(SecureAuthRouter::router(context.services.clone()))
                        .merge(CollectionsRouter::router(context.services.clone()))
                        .merge(ExecutionsRouter::router(context.services.clone()))
                        .merge(FunctionsRouter::router(context.services.clone()))
                        .merge(FunctionRunsRouter::router(context.services.clone()))
                        .merge(GroupsRouter::router(context.services.clone()))
                        .merge(MetricsRouter::router(context.services.clone()))
                        .merge(LogicalVersionsRouter::router(context.services.clone()))
                        .merge(InterCollectionPermissionsRouter::router(
                            context.services.clone(),
                        ))
                        .merge(PermissionsRouter::router(context.services.clone()))
                        .merge(RolesRouter::router(context.services.clone()))
                        .merge(ServerStatusRouter::router(context.services.clone()))
                        .merge(UserRolesRouter::router(context.services.clone()))
                        .merge(UsersRouter::router(context.services.clone()))
                        .merge(TablesRouter::router(context.services.clone()))
                        .merge(TransactionsRouter::router(context.services.clone()))
                        .merge(WorkersRouter::router(context.services.clone()))
                        .merge(AuthenticatedExtendedRouter::router(
                            context.extended_services.clone(),
                        ))
                        // authorization layer
                        .layer(from_fn_with_state(
                            context.context.clone(),
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
                context.extended_services.clone(),
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
                .layer(TimeoutLayer::with_status_code(
                    StatusCode::REQUEST_TIMEOUT,
                    Duration::from_secs(context.config.request_timeout as u64),
                ))
                .layer(CorsService::layer())
                .layer(TraceService::layer())
                .layer(CompressionService::layer());

            ServerBuilder::new(context.config.addresses.clone(), router)
                .tls(&context.config.ssl_folder)
                .build()
                .await
        }?;

        Ok(ApiServerV1 { inner })
    }
}
