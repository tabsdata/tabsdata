//
// Copyright 2026 Tabsdata Inc.
//

//! Liveness probe: is the process up and serving requests. No auth, no
//! database, no service work, so there is nothing behind it to reach; the
//! secured status endpoint is the dependency-aware check.

use axum::extract::State;
use ta_apiserver::status::error_status::ErrorStatus;
use ta_apiserver::status::ok_status::RawStatus;
use td_apiforge::apiserver_path;
use td_objects::dxo::system::Pong;
use td_services::Services;

const SYSTEM_TAG: &str = "System";

#[apiserver_path(
    version(v1),
    auth(none),
    method(get),
    path = "/ping",
    tag(SYSTEM_TAG),
    doc = "Liveness probe"
)]
pub async fn ping(State(_state): State<Services>) -> Result<RawStatus<Pong>, ErrorStatus> {
    Ok(RawStatus::OK(Pong {}))
}

#[cfg(test)]
mod tests {
    use super::PING_PATH;
    use axum::Router;
    use axum::body::Body;
    use axum::extract::Request;
    use http::{Method, StatusCode};
    use ta_apiserver::router::{RouterStates, named_router};
    use td_services::Services;
    use tower::ServiceExt;

    #[tokio::test]
    async fn ping_returns_200_without_a_request_context() {
        let states = RouterStates::default().with(Services::default());
        let app: Router = named_router(&["ping"], &states).into();
        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(PING_PATH)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}
