//
//  Copyright 2024 Tabs Data Inc.
//

use axum::extract::Request;
use axum::middleware::Next;
use axum::response::Response;
use axum::Extension;

use crate::logic::apisrv::status::error_status::AuthorizeErrorStatus;
use td_objects::crudl::RequestContext;

#[derive(Default)]
pub struct AdminOnly;

impl AdminOnly {
    /// This method filters requests to ensure that only admin users can access a given API.
    pub async fn layer(
        Extension(context): Extension<RequestContext>,
        request: Request,
        next: Next,
    ) -> Result<Response, AuthorizeErrorStatus> {
        // Check if the user is an admin
        context.assert_sys_admin()?;

        // Let the request continue
        Ok(next.run(request).await)
    }
}

#[cfg(test)]
mod tests {
    use crate::logic::apisrv::jwt::admin_only::AdminOnly;
    use axum::body::{to_bytes, Body};
    use axum::http::{Request, StatusCode};
    use axum::routing::get;
    use axum::Router;
    use td_objects::crudl::RequestContext;
    use tower::ServiceExt;

    async fn test_handler() -> &'static str {
        "test"
    }

    fn create_router() -> Router {
        Router::new().route(
            "/test",
            get(test_handler).layer(axum::middleware::from_fn(AdminOnly::layer)),
        )
    }

    #[tokio::test]
    async fn test_admin_only_allows_admin() {
        let app = create_router();

        let mut req = Request::builder().uri("/test").body(Body::empty()).unwrap();
        let context = RequestContext::with("user", "admin", true).await;
        req.extensions_mut().insert(context);

        let response = app.oneshot(req).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(body, "test");
    }

    #[tokio::test]
    async fn test_admin_only_denies_non_admin() {
        let app = create_router();

        let mut req = Request::builder().uri("/test").body(Body::empty()).unwrap();
        let context = RequestContext::with("user", "admin", false).await;
        req.extensions_mut().insert(context);

        let response = app.oneshot(req).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
}
