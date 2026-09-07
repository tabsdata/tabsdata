//
// Copyright 2025 Tabsdata Inc.
//

use axum::Extension;
use axum::extract::{Path, State};
use axum_extra::extract::Query;
use ta_apiserver::status::error_status::ErrorStatus;
use ta_apiserver::status::extractors::Json;
use ta_apiserver::status::ok_status::{
    CreateStatus, DeleteStatus, GetStatus, ListStatus, NoContent, UpdateStatus,
};
use td_apiforge::apiserver_path;
use td_objects::dxo::crudl::{ListParams, RequestContext};
use td_objects::dxo::user::{UserCreate, UserParam, UserRead, UserUpdate};
use td_services::Services;

const USERS_TAG: &str = "Users";

#[apiserver_path(mount = global, method = get, path = "/users", tag = USERS_TAG, doc = "Lists users")]
async fn list_users(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Query(list_params): Query<ListParams>,
) -> Result<ListStatus<UserRead>, ErrorStatus> {
    let request = context.list((), list_params);
    let response = state.list_users(request).await?;
    Ok(ListStatus::OK(response))
}

#[apiserver_path(mount = global, method = get, path = "/users/{user}", tag = USERS_TAG, doc = "Get a user")]
pub async fn get_user(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Path(user): Path<UserParam>,
) -> Result<GetStatus<UserRead>, ErrorStatus> {
    let request = context.read(user);
    let response = state.read_user(request).await?;
    Ok(GetStatus::OK(response))
}

#[apiserver_path(mount = global, method = post, path = "/users", tag = USERS_TAG, doc = "Create a user")]
pub async fn create_user(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Json(request): Json<UserCreate>,
) -> Result<CreateStatus<UserRead>, ErrorStatus> {
    let request = context.create((), request);
    let response = state.create_user(request).await?;
    Ok(CreateStatus::CREATED(response))
}

#[apiserver_path(mount = global, method = post, path = "/users/{user}", tag = USERS_TAG, doc = "Update a user")]
pub async fn update_user(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Path(user): Path<UserParam>,
    Json(request): Json<UserUpdate>,
) -> Result<UpdateStatus<UserRead>, ErrorStatus> {
    let request = context.update(user, request);
    let response = state.update_user(request).await?;
    Ok(UpdateStatus::OK(response))
}

#[apiserver_path(mount = global, method = delete, path = "/users/{user}", tag = USERS_TAG, doc = "Delete a user")]
pub async fn delete_user(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Path(user): Path<UserParam>,
) -> Result<DeleteStatus<NoContent>, ErrorStatus> {
    let request = context.delete(user);
    let response = state.delete_user(request).await?;
    Ok(DeleteStatus::OK(response))
}

#[cfg(test)]
mod tests {
    use super::{GET_USER_PATH, LIST_USERS_PATH};
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use axum::{Extension, Router};
    use http::method::Method;
    use serde_json::json;
    use ta_apiserver::router::{RouterStates, named_router};
    use td_database::pool::TdPool;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::types::basic::{AccessTokenId, UserId};
    use td_services::Services;
    use tower::ServiceExt;

    async fn to_route<R: Into<Router> + Clone>(router: &R) -> Router {
        let context = RequestContext::with(AccessTokenId::default(), UserId::admin());
        let router = router.clone().into();
        router.layer(Extension(context.clone()))
    }

    #[td_test::test(db)]
    async fn test_users_lifecycle(db: TdPool) {
        let states = RouterStates::default().with(Services::with_defaults(db));
        let router = named_router(
            &[
                "list_users",
                "get_user",
                "create_user",
                "update_user",
                "delete_user",
            ],
            &states,
        );

        // List users — only the seeded `admin` row (`system` is excluded).
        let response = to_route(&router)
            .await
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(LIST_USERS_PATH)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["data"]["len"], 1);

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
                    .uri(LIST_USERS_PATH)
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

        // List again and assert we have 2 users (admin + joaquin; system excluded).
        let response = to_route(&router)
            .await
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(LIST_USERS_PATH)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["data"]["len"], 2);

        let response = to_route(&router)
            .await
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(GET_USER_PATH.replace("{user}", "joaquin"))
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
                    .uri(GET_USER_PATH.replace("{user}", "joaquin"))
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

        let response = to_route(&router)
            .await
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri(GET_USER_PATH.replace("{user}", "joaquin"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // List again and assert we are back to 1 (admin; system excluded).
        let response = to_route(&router)
            .await
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(LIST_USERS_PATH)
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
