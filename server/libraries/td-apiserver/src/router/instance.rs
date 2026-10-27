//
// Copyright 2026 Tabsdata Inc.
//

use axum::extract::State;
use ta_apiserver::status::error_status::ErrorStatus;
use ta_apiserver::status::ok_status::GetStatus;
use td_apiforge::apiserver_path;
use td_objects::dxo::system::Instance;
use td_services::Services;

const SYSTEM_TAG: &str = "System";

#[apiserver_path(
    version(v1),
    auth(session, service_token),
    method(get),
    path = "/instance",
    tag(SYSTEM_TAG),
    doc = "Identity of this Tabsdata instance"
)]
pub async fn instance(State(state): State<Services>) -> Result<GetStatus<Instance>, ErrorStatus> {
    let response = state.instance(()).await?;
    Ok(GetStatus::OK(response))
}

#[cfg(test)]
mod tests {
    use super::INSTANCE_PATH;
    use axum::body::{Body, to_bytes};
    use axum::extract::Request;
    use axum::{Extension, Router};
    use http::{Method, StatusCode};
    use ta_apiserver::router::{RouterStates, named_router};
    use ta_services::service::Ctx;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::system::Instance;
    use td_objects::types::basic::{AccessTokenId, InstanceId, UserId};
    use td_services::Services;
    use tower::ServiceExt;

    #[tokio::test]
    async fn instance_returns_the_instance_id() {
        let id = InstanceId::try_from("0b5c3f0e-8a52-4d2b-9c41-6f1e2d3c4b5a").unwrap();
        let services = Services {
            instance_id: Ctx::new(id.clone()),
            ..Services::default()
        };
        let states = RouterStates::default().with(services);
        let app: Router = named_router(&["instance"], &states).into();
        let response = app
            .layer(Extension(RequestContext::with(
                AccessTokenId::default(),
                UserId::admin(),
            )))
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(INSTANCE_PATH)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let instance: Instance = serde_json::from_value(body["data"].clone()).unwrap();
        assert_eq!(instance.instance_id, id);
    }
}
