//
// Copyright 2025 Tabs Data Inc.
//

//! Users API Service for API Server.

#![allow(clippy::upper_case_acronyms)]

use crate::router;
use crate::router::state::System;
use crate::status::error_status::ServerErrorStatus;
use axum::extract::State;
use derive_builder::Builder;
use getset::Getters;
use serde::Serialize;
use std::fmt::Debug;
use td_apiforge::{apiserver_path, apiserver_tag, get_status};
use td_objects::types::system::ApiStatus;
use td_tower::ctx_service::{CtxMap, CtxResponse, CtxResponseBuilder};
use tower::ServiceExt;

pub const STATUS: &str = "/status";

apiserver_tag!(name = "Status", description = "Server Status API");

router! {
    state => { System },
    routes => { status }
}

get_status!(ApiStatus);

#[apiserver_path(method = get, path = STATUS, tag = STATUS_TAG)]
#[doc = "API Server Status"]
async fn status(State(status_state): State<System>) -> Result<GetStatus, ServerErrorStatus> {
    let response = status_state.status().await.oneshot(()).await?;
    Ok(GetStatus::OK(response.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::router::server_status::ApiStatus;
    use axum::body::{to_bytes, Body};
    use axum::extract::Request;
    use axum::{Extension, Router};
    use http::{Method, StatusCode};
    use std::sync::Arc;
    use td_database::sql::DbPool;
    use td_objects::crudl::RequestContext;
    use td_objects::types::basic::{AccessTokenId, RoleId, UserId};
    use td_objects::types::system::HealthStatus;
    use td_services::system::services::SystemServices;
    use tower::ServiceExt;

    async fn to_route<R: Into<Router> + Clone>(router: &R) -> Router {
        let context =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user());
        let router = router.clone().into();
        router.layer(Extension(context.clone()))
    }

    #[td_test::test(sqlx)]
    async fn test_status(db: DbPool) {
        let router = super::router(Arc::new(SystemServices::new(db)));

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
