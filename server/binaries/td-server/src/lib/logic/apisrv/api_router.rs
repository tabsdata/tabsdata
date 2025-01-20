//
// Copyright 2024 Tabs Data Inc.
//

use axum::Router;
use derive_builder::Builder;
use utoipa::openapi::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

/// Utoipa OpenApi Wrapper to provide a single entrypoint to create this service.
///
/// # Examples
///
/// ```rust
/// use utoipa::OpenApi;
/// use utoipa_swagger_ui::SwaggerUi;
/// use tabsdatalib::logic::apisrv::api_router::{OpenApiRouter, OpenApiRouterBuilder};
///
/// #[derive(OpenApi)]
/// #[openapi()]
/// struct ApiDoc;
///
/// OpenApiRouterBuilder::default().openapi(ApiDoc::openapi()).build().unwrap();
/// ```
#[derive(Builder, Default)]
pub struct OpenApiRouter {
    #[builder(default = "self.default_docs_base_url()")]
    docs_base_url: String,
    #[builder(default = "self.default_openapi_base_url()")]
    openapi_base_url: String,
    openapi: OpenApi,
}

const DOCS_BASE_URL: &str = "/docs";
const OPENAPI_BASE_URL: &str = "/api-docs/openapi.json";

impl OpenApiRouterBuilder {
    fn default_docs_base_url(&self) -> String {
        String::from(DOCS_BASE_URL)
    }

    fn default_openapi_base_url(&self) -> String {
        String::from(OPENAPI_BASE_URL)
    }
}

impl From<OpenApiRouter> for Router {
    fn from(open_api_router: OpenApiRouter) -> Self {
        Router::new().merge(
            SwaggerUi::new(open_api_router.docs_base_url)
                .url(open_api_router.openapi_base_url, open_api_router.openapi),
        )
    }
}

#[cfg(test)]
mod tests {
    use axum::body::to_bytes;
    use axum::extract::Path;
    use axum::{body::Body, http::Request, routing::get, Router};
    use tower::ServiceExt;

    use super::*;

    #[tokio::test]
    async fn test_router_route() {
        let router = Router::new();
        let router = router.route("/test", get(|| async { "test" }));

        let response = router
            .oneshot(Request::builder().uri("/test").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), 200);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(body, "test");
    }

    #[tokio::test]
    async fn test_router_route_parametrized() {
        let router = Router::new();
        let router = router.route(
            "/test/{param}",
            get(|Path(param): Path<String>| async move { format!("test - {param}") }),
        );

        let response = router
            .oneshot(
                Request::builder()
                    .uri("/test/joaquin")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), 200);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(body, "test - joaquin");
    }

    #[tokio::test]
    async fn test_router_apply_service() {
        let router = Router::new().route("/test", get(|| async { "test" }));
        let app = Router::new();
        let app = router.merge(app);

        let response = app
            .oneshot(Request::builder().uri("/test").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), 200);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(body, "test");
    }

    #[tokio::test]
    async fn test_open_api_service_router_service() {
        fn router() -> Router {
            OpenApiRouterBuilder::default()
                .openapi(OpenApi::default())
                .build()
                .unwrap()
                .into()
        }

        let response = router()
            .oneshot(
                Request::builder()
                    .uri("/docs/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
    }
}
