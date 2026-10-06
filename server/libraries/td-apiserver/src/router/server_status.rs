//
// Copyright 2025 Tabsdata Inc.
//

use axum::extract::State;
use ta_apiserver::status::error_status::ErrorStatus;
use ta_apiserver::status::ok_status::GetStatus;
use td_apiforge::apiserver_path;
use td_objects::dxo::runtime_info::RuntimeInfo;
use td_objects::dxo::system::ApiStatus;
use td_services::Services;

const SYSTEM_TAG: &str = "System";

#[apiserver_path(
    version(v1),
    auth(session),
    method(get),
    path = "/status",
    tag(SYSTEM_TAG),
    doc = "API Server Status"
)]
pub async fn status(State(state): State<Services>) -> Result<GetStatus<ApiStatus>, ErrorStatus> {
    let response = state.status(()).await?;
    Ok(GetStatus::OK(response))
}

#[apiserver_path(
    version(v1),
    auth(session),
    method(get),
    path = "/runtime-info",
    tag(SYSTEM_TAG),
    doc = "Runtime information"
)]
pub async fn info(State(state): State<Services>) -> Result<GetStatus<RuntimeInfo>, ErrorStatus> {
    let response = state.runtime_info(()).await?;
    Ok(GetStatus::OK(response))
}

#[cfg(test)]
mod tests {
    use super::STATUS_PATH;
    use axum::body::{Body, to_bytes};
    use axum::extract::Request;
    use axum::{Extension, Router};
    use http::{Method, StatusCode};
    use ta_apiserver::router::{RouterStates, named_router};
    use td_database::pool::TdPool;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::system::{ApiStatus, HealthStatus};
    use td_objects::types::basic::{AccessTokenId, UserId};
    use td_services::Services;
    use tower::ServiceExt;

    async fn to_route<R: Into<Router> + Clone>(router: &R) -> Router {
        let context = RequestContext::with(AccessTokenId::default(), UserId::admin());
        let router = router.clone().into();
        router.layer(Extension(context))
    }

    #[td_test::test(db)]
    async fn test_status(db: TdPool) {
        let states = RouterStates::default().with(Services::with_defaults(db));
        let router = named_router(&["status", "info"], &states);

        let response = to_route(&router)
            .await
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(STATUS_PATH)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let database_status: ApiStatus =
            serde_json::from_value(body["data"].clone()).expect("Failed to parse database status");
        assert!(matches!(database_status.status, HealthStatus::OK));
        assert!(database_status.latency_as_nanos > 0);
    }
}
