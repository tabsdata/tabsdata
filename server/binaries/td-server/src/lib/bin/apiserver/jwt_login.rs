//
// Copyright 2024 Tabs Data Inc.
//

//! Login API Service for API Server. Implements JWT authentication and authorization.

use crate::bin::apiserver::UsersState;
use crate::logic::apiserver::jwt::jwt_logic::{AccessRequest, RefreshRequest, TokenResponse};
use crate::logic::apiserver::status::error_status::AuthorizeErrorStatus;
use crate::logic::apiserver::status::extractors::Json;
use crate::router;
use axum::extract::State;
use td_apiforge::{apiserver_path, apiserver_tag, auth_status_raw};
use td_objects::users::dto::AuthenticateRequest;
use td_tower::ctx_service::RawOneshot;

pub const ACCESS: &str = "/auth/access";
pub const REFRESH: &str = "/auth/refresh";

apiserver_tag!(name = "Authentication", description = "Authentication API");

router! {
    state => { UsersState },
    // state => { JwtState, UsersState, RolesState },
    routes => { access, refresh }
}

auth_status_raw!(TokenResponse);

#[apiserver_path(method = post, path = ACCESS, tag = AUTHENTICATION_TAG)]
pub async fn access(
    State(users_state): State<UsersState>,
    Json(request): Json<AccessRequest>,
) -> Result<AuthStatusRaw, AuthorizeErrorStatus> {
    let request =
        AuthenticateRequest::new(request.name().to_string(), request.password().to_string());
    // Authenticate user
    let token = users_state
        .authenticate_user()
        .await
        .raw_oneshot(request)
        .await?;
    Ok(AuthStatusRaw::OK(token))
}

#[apiserver_path(method = post, path = REFRESH, tag = AUTHENTICATION_TAG)]
pub async fn refresh(
    State(_users_state): State<UsersState>,
    // State((jwt_state, _users_state)): State<(JwtState, UsersState)>,
    Json(_request): Json<RefreshRequest>,
) -> Result<AuthStatusRaw, AuthorizeErrorStatus> {
    todo!()
    // let refresh_token = EncodedToken::new(request.refresh_token());
    // let _refresh_claim = jwt_state.authenticate_refresh(refresh_token);
    // // do something with the refresh claim, and get user and role
    // // let user = users_state
    // //     .authenticate(request.name(), request.password())
    // //     .await?;
    // // TODO(TD-273) Roles and permissions in token
    // let token = jwt_state.authorize_access("user.name()", "request.role()")?;
    // Ok(AuthStatusRaw::OK(token))
}

#[cfg(test)]
mod tests {
    // use std::sync::Arc;
    //
    // use axum::body::to_bytes;
    // use axum::routing::post;
    // use axum::{body::Body, http::Request, Router};
    // use http::StatusCode;
    // use serde_json::json;
    // use crate::common::xsql::DbPool;
    // use tower::ServiceExt;
    //
    // use super::*;
    // use td_database::test::test_config;
    // use crate::logic::apiserver::jwt::jwt_logic::tests::test_jwt_logic;
    // use td_schema;
    // use crate::logic::users::{PasswordHashingConfig, UsersLogic};

    #[ignore] // TODO(TD-273) Roles and permissions in token
    #[tokio::test]
    async fn test_successful_login() {
        // let jwt_logic = test_jwt_logic();
        // let db: &'static DbPool =
        //     Box::leak(Box::new(tabsdata::db().await.unwrap()));
        // let users_logic = UsersLogic::new(&db, Arc::new(PasswordHashingConfig::default()));
        // let roles_logic = RolesLogic::new(&db);
        // let app = Router::new().route(ACCESS, post(access)).with_state((
        //     Arc::new(jwt_logic),
        //     Arc::new(users_logic),
        //     Arc::new(roles_logic),
        // ));
        // let request = Request::builder()
        //     .uri(ACCESS)
        //     .method("POST")
        //     .header("content-type", "application/json")
        //     .body(Body::from(
        //         json!({
        //             "name": "admin",
        //             "password": "tabsdata",
        //             "role": "User"
        //         })
        //         .to_string(),
        //     ))
        //     .unwrap();
        //
        // let response = app.oneshot(request).await.unwrap();
        // assert_eq!(response.status(), StatusCode::OK);
    }

    #[ignore] // TODO(TD-273) Roles and permissions in token
    #[tokio::test]
    async fn test_login_wrong_credentials() {
        // let jwt_logic = test_jwt_logic();
        // let db: &'static DbPool =
        //     Box::leak(Box::new(tabsdata::db().await.unwrap()));
        // let users_logic = UsersLogic::new(&db, Arc::new(PasswordHashingConfig::default()));
        // let roles_logic = RolesLogic::new(&db);
        // let app = Router::new().route(ACCESS, post(access)).with_state((
        //     Arc::new(jwt_logic),
        //     Arc::new(users_logic),
        //     Arc::new(roles_logic),
        // ));
        // let request = Request::builder()
        //     .uri(ACCESS)
        //     .method("POST")
        //     .header("content-type", "application/json")
        //     .body(Body::from(
        //         json!({
        //             "name": "wrong_user",
        //             "password": "wrong_password",
        //             "role": "User"
        //         })
        //         .to_string(),
        //     ))
        //     .unwrap();
        //
        // let response = app.oneshot(request).await.unwrap();
        // assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        //
        // let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        // assert_eq!(
        //     body,
        //     json!(
        //         {
        //             "error":"unauthorized",
        //             "error_description":"Invalid credentials"
        //         }
        //     )
        //     .to_string()
        // );
    }

    #[ignore] // TODO(TD-273) Roles and permissions in token
    #[tokio::test]
    async fn test_login_invalid_role() {
        // let jwt_logic = test_jwt_logic();
        // let db: &'static DbPool =
        //     Box::leak(Box::new(tabsdata::db().await.unwrap()));
        // let users_logic = UsersLogic::new(&db, Arc::new(PasswordHashingConfig::default()));
        // let roles_logic = RolesLogic::new(&db);
        // let app = Router::new().route(ACCESS, post(access)).with_state((
        //     Arc::new(jwt_logic),
        //     Arc::new(users_logic),
        //     Arc::new(roles_logic),
        // ));
        // let request = Request::builder()
        //     .uri(ACCESS)
        //     .method("POST")
        //     .header("content-type", "application/json")
        //     .body(Body::from(
        //         json!({
        //             "name": "user",
        //             "password": "user",
        //             "role": "invalid_role"
        //         })
        //         .to_string(),
        //     ))
        //     .unwrap();
        //
        // let response = app.oneshot(request).await.unwrap();
        // assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        //
        // let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        // assert_eq!(
        //     body,
        //     json!(
        //         {"error":"unauthorized",
        //         "error_description":"Invalid credentials"}
        //     )
        //     .to_string()
        // );
    }
}
