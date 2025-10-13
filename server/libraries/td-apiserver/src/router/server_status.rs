//
// Copyright 2025 Tabs Data Inc.
//

use td_apiforge::router_ext;

#[router_ext(ServerStatusRouter)]
mod routes {
    use axum::Extension;
    use axum::extract::State;
    use std::sync::Arc;
    use ta_apiserver::status::error_status::ErrorStatus;
    use ta_apiserver::status::ok_status::GetStatus;
    use ta_services::service::TdService;
    use td_apiforge::apiserver_path;
    use td_objects::crudl::RequestContext;
    use td_objects::rest_urls::{RUNTIME_INFO, SERVER_STATUS};
    use td_objects::types::runtime_info::RuntimeInfo;
    use td_objects::types::system::ApiStatus;
    use td_services::execution::services::ExecutionServices;
    use td_services::system::services::SystemServices;
    use tower::ServiceExt;

    const STATUS_TAG: &str = "Status";

    #[apiserver_path(method = get, path = SERVER_STATUS, tag = STATUS_TAG)]
    #[doc = "API Server Status"]
    pub async fn status(
        State(status_state): State<Arc<SystemServices>>,
    ) -> Result<GetStatus<ApiStatus>, ErrorStatus> {
        let response = status_state.status().service().await.oneshot(()).await?;
        Ok(GetStatus::OK(response))
    }

    #[apiserver_path(method = get, path = RUNTIME_INFO, tag = STATUS_TAG)]
    #[doc = "Runtime information"]
    pub async fn info(
        State(executions): State<Arc<ExecutionServices>>,
        Extension(context): Extension<RequestContext>,
    ) -> Result<GetStatus<RuntimeInfo>, ErrorStatus> {
        let request = context.read(());
        let response = executions.info().service().await.oneshot(request).await?;
        Ok(GetStatus::OK(response))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::extract::Request;
    use axum::{Extension, Router};
    use http::{Method, StatusCode};
    use ta_apiserver::router::RouterExtension;
    use ta_services::factory::ServiceFactory;
    use td_database::sql::DbPool;
    use td_objects::crudl::RequestContext;
    use td_objects::rest_urls::SERVER_STATUS;
    use td_objects::types::basic::{AccessTokenId, RoleId, UserId};
    use td_objects::types::system::{ApiStatus, HealthStatus};
    use td_services::{Context, Services};
    use tower::ServiceExt;

    async fn to_route<R: Into<Router> + Clone>(router: &R) -> Router {
        let context =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user());
        let router = router.clone().into();
        router.layer(Extension(context.clone()))
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_status(db: DbPool) {
        let router = ServerStatusRouter::router(Services::build(&Context::with_defaults(db)));

        // Retrieve the status
        let response = to_route(&router)
            .await
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(SERVER_STATUS)
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
        assert!(matches!(database_status.status(), HealthStatus::OK));
        assert!(*database_status.latency_as_nanos() > 0);
    }
}
