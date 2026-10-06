//
// Copyright 2025 Tabsdata Inc.
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

use crate::apiserver::server::{Server, ServerBuilder, ServerError};
use crate::apiserver::{ApiServerCtx, BASE_API_URL, V1};
use crate::layers::authorization::authorization_layer;
use crate::layers::compression::CompressionService;
use crate::layers::cors::CorsService;
use crate::layers::project_resolver::project_resolver_layer;
use crate::layers::tracing::TraceService;
use axum::middleware::from_fn_with_state;
use http::StatusCode;
use std::time::Duration;
use ta_apiserver::router::{
    AuthClass, Position, RouterStates, Scope, Server as Listener, Version, mount_router,
    require_registered,
};
use ta_apiserver::status::error_status::ErrorStatus;
use td_error::{ApiError, api_error};
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
            // Routes come from the inventory registry; only the topology is
            // built here.
            require_registered()?;
            let states = RouterStates::default()
                .with(context.services.clone())
                .with(context.extended_services.clone());
            let mode = context.config.security_mode;
            let at = |scope: Scope, class: AuthClass| {
                mount_router(
                    Position {
                        server: Listener::Public,
                        version: Some(Version::V1),
                        scope,
                        mode,
                        class,
                    },
                    &states,
                )
            };

            let router = utoipa_axum::router::OpenApiRouter::default()
                .merge(at(Scope::Flat, AuthClass::Open))
                .merge({
                    let flat = at(Scope::Flat, AuthClass::Bearer);
                    // The resolver runs after auth and 404s unknown projects
                    // before any RLS-scoped table is touched.
                    let project = at(Scope::Project, AuthClass::Bearer).layer(from_fn_with_state(
                        context.services.clone(),
                        project_resolver_layer,
                    ));
                    flat.merge(project).layer(from_fn_with_state(
                        context.services.clone(),
                        authorization_layer,
                    ))
                });

            let router = utoipa_axum::router::OpenApiRouter::default()
                .nest(V1, router)
                // Everything going to /api and is not found, is a not found. Only non /api calls get through.
                .fallback(api_not_found_handler);
            let router = utoipa_axum::router::OpenApiRouter::default().nest(BASE_API_URL, router);

            let router = router.merge(mount_router(
                Position {
                    server: Listener::Public,
                    version: None,
                    scope: Scope::Flat,
                    mode,
                    class: AuthClass::Open,
                },
                &states,
            ));

            #[allow(unused_mut, unused_variables)]
            let (mut router, openapi) = router.split_for_parts();

            #[cfg(feature = "api-docs")]
            {
                use crate::apiserver::{DOCS_URL, OPENAPI_JSON_URL, normalize_catch_all_paths};
                use td_build::version::TABSDATA_VERSION;

                use utoipa_swagger_ui::{SwaggerUi, Url};

                let mut openapi = openapi;
                openapi.info.title = "Tabsdata API".to_string();
                openapi.info.version = TABSDATA_VERSION.to_string();
                normalize_catch_all_paths(&mut openapi);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::router::collections::LIST_COLLECTIONS_PATH;
    use crate::router::server_status::STATUS_PATH;
    use crate::router::users::LIST_USERS_PATH;
    use ta_apiserver::router::SecurityMode;
    use td_services::Services;
    use te_services::ExtendedServices;

    /// Folding a registry position yields the routes AND their utoipa docs.
    #[test]
    fn flat_position_carries_routes_and_docs() {
        let states = RouterStates::default()
            .with(Services::default())
            .with(ExtendedServices::default());
        let flat = mount_router(
            Position {
                server: Listener::Public,
                version: Some(Version::V1),
                scope: Scope::Flat,
                mode: SecurityMode::default(),
                class: AuthClass::Bearer,
            },
            &states,
        );
        let (_, openapi) = flat.split_for_parts();
        for path in [LIST_USERS_PATH, LIST_COLLECTIONS_PATH, STATUS_PATH] {
            assert!(
                openapi.paths.paths.contains_key(path),
                "expected {path} in the OpenAPI doc, got {:?}",
                openapi.paths.paths.keys().collect::<Vec<_>>()
            );
        }
    }
}
