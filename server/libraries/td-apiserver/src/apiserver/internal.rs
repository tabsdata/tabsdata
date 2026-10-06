//
// Copyright 2025 Tabsdata Inc.
//

use crate::apiserver::server::{self, Server, ServerBuilder, ServerError};
use crate::apiserver::{ApiServerCtx, BASE_API_URL, V1};
use crate::config::Config;
use crate::layers::cors::CorsService;
use crate::layers::internal_token::internal_token_layer;
use crate::layers::tracing::TraceService;
use async_trait::async_trait;
use axum::middleware::from_fn_with_state;
use http::StatusCode;
use std::error::Error;
use std::time::Duration;
use ta_apiserver::router::{
    AuthClass, Position, RouterStates, Scope, Server as Listener, Version, mount_router,
    require_registered,
};
use ta_apiserver::status::error_status::ErrorStatus;
use td_common::server_runner::ServerRunnable;
use td_error::{ApiError, api_error};
use td_objects::types::addresses::{
    InternalServerAddresses, InternalServerEndpoint, NonEmptyAddresses,
};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tower_http::timeout::TimeoutLayer;

pub struct InternalServer {
    inner: Box<dyn Server>,
}

impl InternalServer {
    /// Binds the internal listeners up front, so the callback endpoint they
    /// determine exists before `Services` is assembled.
    pub async fn bind(
        config: &Config,
    ) -> Result<(Vec<TcpListener>, InternalServerEndpoint), Box<dyn Error>> {
        let listeners = server::bind(&config.internal_addresses).await?;
        let bound = listeners
            .iter()
            .map(|listener| listener.local_addr())
            .collect::<Result<Vec<_>, _>>()?;
        let addresses = InternalServerAddresses(NonEmptyAddresses::from_vec(bound)?);
        let endpoint =
            InternalServerEndpoint::new(addresses, config.advertised_internal_host.as_ref())?;
        Ok((listeners, endpoint))
    }

    pub async fn new(
        context: &ApiServerCtx,
        listeners: Vec<TcpListener>,
    ) -> Result<Self, ServerError> {
        async fn api_not_found_handler() -> ErrorStatus {
            api_error!(ApiError::NotFound, "Internal API endpoint not found").into()
        }

        let inner = {
            // Routes are keyed by globally-unique ids; the services resolve
            // the owning project from the row itself, so no project
            // resolution happens here.
            require_registered()?;
            let states = RouterStates::default()
                .with(context.services.clone())
                .with(context.extended_services.clone());
            let internal = mount_router(
                Position {
                    server: Listener::Internal,
                    version: Some(Version::V1),
                    scope: Scope::Flat,
                    mode: context.config.security_mode,
                    class: AuthClass::Internal,
                },
                &states,
            );
            let router = utoipa_axum::router::OpenApiRouter::default().merge(
                internal
                    // Internal authorization gate: validate the per-request
                    // internal-endpoint token (replaces the loopback-IP gate)
                    // and inject the internal RequestContext (system user,
                    // internal provenance). `route_layer` so routing runs first
                    // and the gate can read the matched route + path params it
                    // validates against.
                    .route_layer(from_fn_with_state(
                        context.services.clone(),
                        internal_token_layer,
                    )),
            );

            let router = utoipa_axum::router::OpenApiRouter::default()
                .nest(V1, router)
                // Everything going to /api and is not found, is a not found. Only non /api calls get through.
                .fallback(api_not_found_handler);
            let router = utoipa_axum::router::OpenApiRouter::default().nest(BASE_API_URL, router);

            #[allow(unused_mut, unused_variables)]
            let (mut router, openapi) = router.split_for_parts();

            #[cfg(feature = "api-docs")]
            {
                use crate::apiserver::{DOCS_URL, OPENAPI_JSON_URL, normalize_catch_all_paths};
                use td_build::version::TABSDATA_VERSION;

                use utoipa_swagger_ui::{SwaggerUi, Url};

                let mut openapi = openapi;
                openapi.info.title = "Tabsdata Internal API".to_string();
                openapi.info.version = TABSDATA_VERSION.to_string();
                normalize_catch_all_paths(&mut openapi);
                router = router.merge(axum::Router::from(
                    SwaggerUi::new(DOCS_URL)
                        .url(Url::new("Internal API V1 Docs", OPENAPI_JSON_URL), openapi),
                ));
            }

            let router = router
                .layer(TimeoutLayer::with_status_code(
                    StatusCode::REQUEST_TIMEOUT,
                    Duration::from_secs(context.config.request_timeout as u64),
                ))
                .layer(CorsService::layer())
                .layer(TraceService::layer());

            ServerBuilder::bound(listeners, router).build().await
        }?;

        Ok(InternalServer { inner })
    }
}

#[async_trait]
impl ServerRunnable for InternalServer {
    async fn run(
        self: Box<Self>,
        shutdown: watch::Receiver<()>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.inner.run(shutdown).await
    }
}
