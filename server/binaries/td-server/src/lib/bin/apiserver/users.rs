//
// Copyright 2024 Tabs Data Inc.
//

//! Users API Service for API Server.

#![allow(clippy::upper_case_acronyms)]

use crate::bin::apiserver::UsersState;
use crate::logic::apiserver::status::error_status::{
    CreateErrorStatus, DeleteErrorStatus, GetErrorStatus, ListErrorStatus, UpdateErrorStatus,
};
use crate::logic::apiserver::status::extractors::Json;
use crate::logic::apiserver::status::DeleteStatus;
use crate::router;
use axum::extract::{Path, Query, State};
use axum::Extension;
use derive_builder::Builder;
use getset::Getters;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use td_apiforge::{
    apiserver_path, apiserver_tag, create_status, get_status, list_status, update_status,
};
use td_objects::crudl::{ListParams, ListResponse, ListResponseBuilder, RequestContext};
use td_objects::users::dto::{UserCreate, UserRead, UserUpdate};
use td_tower::ctx_service::{CtxMap, CtxResponse, CtxResponseBuilder};
use tower::ServiceExt;
use utoipa::IntoParams;

pub const USERS: &str = "/users";
pub const USER: &str = "/users/{user}";

apiserver_tag!(name = "User", description = "Users API");

router! {
    state => { UsersState },
    routes => { list_users, get_user, create_user, update_user, delete_user }
}

#[derive(Deserialize, IntoParams, Getters)]
#[getset(get = "pub")]
pub struct UserUriParams {
    /// User name
    user: String,
}

list_status!(UserRead);

const LIST_USERS: &str = USERS;
#[apiserver_path(method = get, path = LIST_USERS, tag = USER_TAG)]
#[doc = "Lists users"]
async fn list_users(
    State(users_state): State<UsersState>,
    Extension(context): Extension<RequestContext>,
    Query(query_params): Query<ListParams>,
) -> Result<ListStatus, ListErrorStatus> {
    let request = context.list((), query_params);
    let response = users_state.list_users().await.oneshot(request).await?;
    Ok(ListStatus::OK(response.into()))
}

get_status!(UserRead);

const GET_USER: &str = USER;
#[apiserver_path(method = get, path = GET_USER, tag = USER_TAG)]
#[doc = "Get a user"]
pub async fn get_user(
    State(users_state): State<UsersState>,
    Extension(context): Extension<RequestContext>,
    Path(path_params): Path<UserUriParams>,
) -> Result<GetStatus, GetErrorStatus> {
    let request = context.read(path_params.user());
    let response = users_state.read_user().await.oneshot(request).await?;
    Ok(GetStatus::OK(response.into()))
}

create_status!(UserRead);

const CREATE_USER: &str = USERS;
#[apiserver_path(method = post, path = CREATE_USER, tag = USER_TAG)]
#[doc = "Create a user"]
pub async fn create_user(
    State(users_state): State<UsersState>,
    Extension(context): Extension<RequestContext>,
    Json(request): Json<UserCreate>,
) -> Result<CreateStatus, CreateErrorStatus> {
    let request = context.create((), request);
    let response = users_state.create_user().await.oneshot(request).await?;
    Ok(CreateStatus::CREATED(response.into()))
}

update_status!(UserRead);

const UPDATE_USER: &str = USER;
#[apiserver_path(method = post, path = UPDATE_USER, tag = USER_TAG)]
#[doc = "Update a user"]
pub async fn update_user(
    State(users_state): State<UsersState>,
    Extension(context): Extension<RequestContext>,
    Path(path_params): Path<UserUriParams>,
    Json(request): Json<UserUpdate>,
) -> Result<UpdateStatus, UpdateErrorStatus> {
    let request = context.update::<String, _>(path_params.user(), request);
    let response = users_state.update_user().await.oneshot(request).await?;
    Ok(UpdateStatus::OK(response.into()))
}

const DELETE_USER: &str = USER;
#[apiserver_path(method = delete, path = DELETE_USER, tag = USER_TAG)]
#[doc = "Delete a user"]
pub async fn delete_user(
    State(users_state): State<UsersState>,
    Extension(context): Extension<RequestContext>,
    Path(path_params): Path<UserUriParams>,
) -> Result<DeleteStatus, DeleteErrorStatus> {
    let request = context.delete(path_params.user());
    let response = users_state.delete_user().await.oneshot(request).await?;
    Ok(DeleteStatus::OK(response.into()))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::logic::apiserver::jwt::jwt_logic::JwtLogic;
    use crate::logic::users::service::UserServices;
    use axum::body::{to_bytes, Body};
    use axum::http::{Request, StatusCode};
    use axum::Router;
    use chrono::Duration;
    use http::method::Method;
    use serde_json::json;
    use td_database::sql::DbPool;
    use td_objects::types::basic::AccessTokenId;
    use td_objects::types::basic::RoleId;
    use td_objects::types::basic::UserId;
    use td_security::config::PasswordHashingConfig;
    use tower::ServiceExt;

    async fn users_state() -> UsersState {
        let db: &'static DbPool = Box::leak(Box::new(td_database::test_utils::db().await.unwrap()));
        let jwt_logic = Arc::new(JwtLogic::new(
            "SECRET",
            Duration::seconds(3600),
            Duration::seconds(7200),
        ));
        let logic = UserServices::new(
            db.clone(),
            Arc::new(PasswordHashingConfig::default()),
            jwt_logic,
        );
        Arc::new(logic)
    }

    async fn to_route<R: Into<Router> + Clone>(router: &R) -> Router {
        let context = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            true,
        );
        let router = router.clone().into();
        router.layer(Extension(context.clone()))
    }

    #[tokio::test]
    async fn test_users_lifecycle() {
        let users_state = users_state().await;
        let router = super::router(users_state);

        // List empty users (only sysadmin will be there)
        let response = to_route(&router)
            .await
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(LIST_USERS)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["data"]["len"], 1);

        // Create a new user
        let user_create = json!(
            {
                "name": "joaquin",
                "full_name": "Joaquin",
                "password": "this is a real password",
                "email": "joaquin@tabsdata.com"
            }
        );

        let response = to_route(&router)
            .await
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(CREATE_USER)
                    .header("content-type", "application/json")
                    .body(serde_json::to_string(&user_create).unwrap())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["data"]["name"], "joaquin");
        assert_eq!(body["data"]["full_name"], "Joaquin");
        assert_eq!(body["data"]["email"], "joaquin@tabsdata.com");

        // List again and assert we have 2 users
        let response = to_route(&router)
            .await
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(LIST_USERS)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["data"]["len"], 2);

        // Get the new user
        let response = to_route(&router)
            .await
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(GET_USER.replace("{user}", "joaquin"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["data"]["name"], "joaquin");
        assert_eq!(body["data"]["full_name"], "Joaquin");
        assert_eq!(body["data"]["email"], "joaquin@tabsdata.com");

        // Update the new user
        let user_update = json!(
            {
                "full_name": "Mister Duck",
                "email": "quack@tabsdata.com"
            }
        );

        let response = to_route(&router)
            .await
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(UPDATE_USER.replace("{user}", "joaquin"))
                    .header("content-type", "application/json")
                    .body(serde_json::to_string(&user_update).unwrap())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["data"]["name"], "joaquin");
        assert_eq!(body["data"]["full_name"], "Mister Duck");
        assert_eq!(body["data"]["email"], "quack@tabsdata.com");

        // Delete the new user
        let response = to_route(&router)
            .await
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri(DELETE_USER.replace("{user}", "joaquin"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // List again and assert we are back to 1 user
        let response = to_route(&router)
            .await
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(LIST_USERS)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["data"]["len"], 1);
    }
}
