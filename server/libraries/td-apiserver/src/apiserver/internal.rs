//
// Copyright 2025 Tabs Data Inc.
//

use crate::apiserver::ApiServerCtx;
use crate::apiserver::server::{Server, ServerBuilder, ServerError};
use crate::layers::cors::CorsService;
use crate::layers::internal_token::internal_token_layer;
use crate::layers::tracing::TraceService;
use crate::router::internal::InternalRouter;
use async_trait::async_trait;
use axum::middleware::from_fn_with_state;
use http::StatusCode;
use std::error::Error;
use std::time::Duration;
use ta_apiserver::router::RouterExtension;
use ta_apiserver::status::error_status::ErrorStatus;
use td_common::server_runner::ServerRunnable;
use td_error::{ApiError, api_error};
use td_objects::types::addresses::{InternalServerAddresses, NonEmptyAddresses};
use tokio::sync::watch;
use tower_http::timeout::TimeoutLayer;

pub struct InternalServer {
    inner: Box<dyn Server>,
}

impl InternalServer {
    pub async fn addresses(&self) -> Result<InternalServerAddresses, Box<dyn Error>> {
        let addr = self
            .inner
            .listeners()
            .iter()
            .map(|listener| listener.local_addr())
            .collect::<Result<Vec<_>, _>>()?;
        Ok(InternalServerAddresses(NonEmptyAddresses::from_vec(addr)?))
    }

    pub async fn new(context: &ApiServerCtx) -> Result<Self, ServerError> {
        async fn api_not_found_handler() -> ErrorStatus {
            api_error!(ApiError::NotFound, "Internal API endpoint not found").into()
        }

        let inner = {
            // Internal router, only accessible from loopback IPs
            let router = utoipa_axum::router::OpenApiRouter::default().merge(
                utoipa_axum::router::OpenApiRouter::default()
                    .merge(InternalRouter::router(context.services.clone()))
                    // internal authorization layer: validate the per-request
                    // internal-endpoint token (replaces the loopback-IP gate).
                    .layer(from_fn_with_state(
                        context.context.clone(),
                        internal_token_layer,
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

            // Add docs endpoints if the feature is enabled.
            #[allow(unused_mut, unused_variables)]
            let (mut router, openapi) = router.split_for_parts();

            #[cfg(feature = "api-docs")]
            {
                use td_build::version::TABSDATA_VERSION;
                use td_objects::rest_urls::{DOCS_URL, OPENAPI_JSON_URL};

                use utoipa_swagger_ui::{SwaggerUi, Url};

                let mut openapi = openapi;
                openapi.info.title = "Tabsdata Internal API".to_string();
                openapi.info.version = TABSDATA_VERSION.to_string();
                router = router.merge(axum::Router::from(
                    SwaggerUi::new(DOCS_URL)
                        .url(Url::new("Internal API V1 Docs", OPENAPI_JSON_URL), openapi),
                ));
            }

            // Default layers
            let router = router
                .layer(TimeoutLayer::with_status_code(
                    StatusCode::REQUEST_TIMEOUT,
                    Duration::from_secs(context.config.request_timeout as u64),
                ))
                .layer(CorsService::layer())
                .layer(TraceService::layer());

            ServerBuilder::new(context.config.internal_addresses.clone(), router)
                .build()
                .await
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
