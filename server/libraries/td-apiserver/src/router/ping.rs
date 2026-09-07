//
// Copyright 2026 Tabsdata Inc.
//

//! Unsecured ping probe. Answers a single question — "is the process up and
//! serving requests?" — with a bare `200 OK`. It deliberately does no auth, no
//! database access, and no service work, so it stays cheap and can't be turned
//! into a back door: there is nothing behind it to reach. For dependency-aware
//! health (database latency, etc.) use the secured server-status endpoint.

use axum::routing::get;
use http::StatusCode;
use ta_apiserver::router::{Mount, RouterRegistration, RouterStates, inventory};
use utoipa_axum::router::OpenApiRouter;

pub(crate) const PING: &str = "/ping";

/// Always `200 OK`, empty body.
async fn ping() -> StatusCode {
    StatusCode::OK
}

/// The unsecured ping route; deliberately absent from the OpenAPI doc.
pub(crate) fn ping_router() -> OpenApiRouter {
    OpenApiRouter::default().route(PING, get(ping))
}

fn registry_build(_states: &RouterStates) -> OpenApiRouter {
    ping_router()
}

inventory::submit! {
    RouterRegistration {
        name: "ping",
        mount: Mount::Unauthenticated,
        build: registry_build,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apiserver::{BASE_API_URL, V1};
    use axum::Router;
    use axum::body::Body;
    use axum::extract::Request;
    use http::Method;
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
