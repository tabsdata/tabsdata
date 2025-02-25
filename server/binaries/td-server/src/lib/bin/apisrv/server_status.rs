//
// Copyright 2024 Tabs Data Inc.
//

//! Users API Service for API Server.

#![allow(clippy::upper_case_acronyms)]

use crate::bin::apisrv::api_server::StatusState;
use crate::logic::apisrv::jwt::admin_only::AdminOnly;
use crate::logic::apisrv::status::error_status::ServerErrorStatus;
use crate::logic::server_status::ApiStatus;
use crate::router;
use axum::extract::State;
use axum::middleware::from_fn;
use axum::routing::get;
use derive_builder::Builder;
use getset::Getters;
use serde::Serialize;
use std::fmt::Debug;
use td_apiforge::{api_server_path, api_server_tag, get_status};
use td_tower::ctx_service::{CtxMap, CtxResponse, CtxResponseBuilder};
use tower::ServiceExt;

pub const STATUS: &str = "/status";

api_server_tag!(name = "Status", description = "Server Status API");

router! {
    state => { StatusState },
    paths => {
        {
            STATUS => get(status),
        }
        .layer => |_| from_fn(AdminOnly::layer),
    }
}

get_status!(ApiStatus);

#[api_server_path(method = get, path = STATUS, tag = STATUS_TAG)]
#[doc = "API Server Status"]
async fn status(State(status_state): State<StatusState>) -> Result<GetStatus, ServerErrorStatus> {
    let response = status_state.status_service().await.oneshot(()).await?;
    Ok(GetStatus::OK(response.into()))
}

#[cfg(test)]
mod tests {
    use crate::bin::apisrv::api_server::StatusState;
    use crate::bin::apisrv::server_status::STATUS;
    use crate::logic::server_status::{ApiStatus, HealthStatus, StatusLogic};
    use axum::body::{to_bytes, Body};
    use axum::extract::Request;
    use axum::{Extension, Router};
    use http::{Method, StatusCode};
    use std::sync::Arc;
    use td_objects::crudl::RequestContext;
    use tower::ServiceExt;

    async fn users_state() -> StatusState {
        let db = td_database::test_utils::db().await.unwrap();
        let logic = StatusLogic::new(db);
        Arc::new(logic)
    }

    async fn to_route(router: &Router) -> Router {
        let context = RequestContext::with("", "", true).await;
        router.clone().layer(Extension(context.clone()))
    }

    #[tokio::test]
    async fn test_status() {
        let users_state = users_state().await;
        let router = super::router(users_state);

        // Retrieve the status
        let response = to_route(&router)
            .await
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(STATUS)
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
