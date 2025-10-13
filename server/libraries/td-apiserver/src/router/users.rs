//
// Copyright 2025 Tabs Data Inc.
//

use td_apiforge::router_ext;

#[router_ext(UsersRouter)]
mod routes {
    use axum::Extension;
    use axum::extract::{Path, State};
    use axum_extra::extract::Query;
    use std::sync::Arc;
    use ta_apiserver::status::error_status::ErrorStatus;
    use ta_apiserver::status::extractors::Json;
    use ta_apiserver::status::ok_status::{
        CreateStatus, DeleteStatus, GetStatus, ListStatus, NoContent, UpdateStatus,
    };
    use ta_services::service::TdService;
    use td_apiforge::apiserver_path;
    use td_objects::crudl::{ListParams, RequestContext};
    use td_objects::rest_urls::{
        CREATE_USER, DELETE_USER, GET_USER, LIST_USERS, UPDATE_USER, UserParam,
    };
    use td_objects::types::user::{UserCreate, UserRead, UserUpdate};
    use td_services::user::service::UserServices;
    use tower::ServiceExt;

    const USERS_TAG: &str = "Users";

    #[apiserver_path(method = get, path = LIST_USERS, tag = USERS_TAG)]
    #[doc = "Lists users"]
    async fn list(
        State(users_state): State<Arc<UserServices>>,
        Extension(context): Extension<RequestContext>,
        Query(query_params): Query<ListParams>,
    ) -> Result<ListStatus<UserRead>, ErrorStatus> {
        let request = context.list((), query_params);
        let response = users_state.list().service().await.oneshot(request).await?;
        Ok(ListStatus::OK(response))
    }

    #[apiserver_path(method = get, path = GET_USER, tag = USERS_TAG)]
    #[doc = "Get a user"]
    pub async fn get(
        State(users_state): State<Arc<UserServices>>,
        Extension(context): Extension<RequestContext>,
        Path(path_params): Path<UserParam>,
    ) -> Result<GetStatus<UserRead>, ErrorStatus> {
        let request = context.read(path_params);
        let response = users_state.read().service().await.oneshot(request).await?;
        Ok(GetStatus::OK(response))
    }

    #[apiserver_path(method = post, path = CREATE_USER, tag = USERS_TAG)]
    #[doc = "Create a user"]
    pub async fn create(
        State(users_state): State<Arc<UserServices>>,
        Extension(context): Extension<RequestContext>,
        Json(request): Json<UserCreate>,
    ) -> Result<CreateStatus<UserRead>, ErrorStatus> {
        let request = context.create((), request);
        let response = users_state
            .create()
            .service()
            .await
            .oneshot(request)
            .await?;
        Ok(CreateStatus::CREATED(response))
    }

    #[apiserver_path(method = post, path = UPDATE_USER, tag = USERS_TAG)]
    #[doc = "Update a user"]
    pub async fn update(
        State(users_state): State<Arc<UserServices>>,
        Extension(context): Extension<RequestContext>,
        Path(path_params): Path<UserParam>,
        Json(request): Json<UserUpdate>,
    ) -> Result<UpdateStatus<UserRead>, ErrorStatus> {
        let request = context.update(path_params, request);
        let response = users_state
            .update()
            .service()
            .await
            .oneshot(request)
            .await?;
        Ok(UpdateStatus::OK(response))
    }

    #[apiserver_path(method = delete, path = DELETE_USER, tag = USERS_TAG)]
    #[doc = "Delete a user"]
    pub async fn delete(
        State(users_state): State<Arc<UserServices>>,
        Extension(context): Extension<RequestContext>,
        Path(path_params): Path<UserParam>,
    ) -> Result<DeleteStatus<NoContent>, ErrorStatus> {
        let request = context.delete(path_params);
        let response = users_state
            .delete()
            .service()
            .await
            .oneshot(request)
            .await?;
        Ok(DeleteStatus::OK(response))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use axum::{Extension, Router};
    use http::method::Method;
    use serde_json::json;
    use ta_apiserver::router::RouterExtension;
    use ta_services::factory::ServiceFactory;
    use td_database::sql::DbPool;
    use td_objects::crudl::RequestContext;
    use td_objects::rest_urls::{CREATE_USER, DELETE_USER, GET_USER, LIST_USERS, UPDATE_USER};
    use td_objects::types::basic::AccessTokenId;
    use td_objects::types::basic::RoleId;
    use td_objects::types::basic::UserId;
    use td_services::{Context, Services};
    use tower::ServiceExt;

    async fn to_route<R: Into<Router> + Clone>(router: &R) -> Router {
        let context = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sec_admin(),
        );
        let router = router.clone().into();
        router.layer(Extension(context.clone()))
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_users_lifecycle(db: DbPool) {
        let router = UsersRouter::router(Services::build(&Context::with_defaults(db)));

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
