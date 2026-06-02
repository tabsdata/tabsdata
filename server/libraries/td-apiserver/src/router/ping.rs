//
// Copyright 2026 Tabs Data Inc.
//

//! Unsecured ping probe. Answers a single question — "is the process up and
//! serving requests?" — with a bare `200 OK`. It deliberately does no auth, no
//! database access, and no service work, so it stays cheap and can't be turned
//! into a back door: there is nothing behind it to reach. For dependency-aware
//! health (database latency, etc.) use the secured server-status endpoint.

use axum::routing::get;
use http::StatusCode;
use td_objects::rest_urls::PING;
use utoipa_axum::router::OpenApiRouter;

/// Always `200 OK`, empty body.
async fn ping() -> StatusCode {
    StatusCode::OK
}

/// The unsecured ping route, to be merged outside the authorization layer.
pub(crate) fn ping_router() -> OpenApiRouter {
    OpenApiRouter::default().route(PING, get(ping))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::extract::Request;
    use axum::Router;
    use http::Method;
    use td_objects::rest_urls::{BASE_API_URL, V1};
    use tower::ServiceExt;

    /// Reachable at the production path, with no auth layer in front, returning
    /// a bare 200.
    #[tokio::test]
    async fn ping_returns_200_unauthenticated() {
        let app: Router = OpenApiRouter::default()
            .nest(
                BASE_API_URL,
                OpenApiRouter::default().nest(V1, ping_router()),
            )
            .into();

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/api/v1/ping")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }
}
