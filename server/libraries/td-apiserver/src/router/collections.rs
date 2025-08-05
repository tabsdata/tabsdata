//
// Copyright 2025 Tabs Data Inc.
//

//! Collections API Service for API Server.

#![allow(clippy::upper_case_acronyms)]

use crate::router;
use crate::router::state::Collections;
use crate::status::error_status::{
    CreateErrorStatus, DeleteErrorStatus, GetErrorStatus, ListErrorStatus, UpdateErrorStatus,
};
use crate::status::extractors::Json;
use crate::status::DeleteStatus;
use axum::extract::{Path, State};
use axum::Extension;
use axum_extra::extract::Query;
use derive_builder::Builder;
use getset::Getters;
use serde::{Deserialize, Serialize};
use td_apiforge::{
    apiserver_path, apiserver_tag, create_status, get_status, list_status, update_status,
};
use td_objects::crudl::ListResponse;
use td_objects::crudl::ListResponseBuilder;
use td_objects::crudl::{ListParams, RequestContext};
use td_objects::rest_urls::{
    CollectionParam, CREATE_COLLECTION, DELETE_COLLECTION, GET_COLLECTION, LIST_COLLECTIONS,
    UPDATE_COLLECTION,
};
use td_objects::types::collection::{CollectionCreate, CollectionRead, CollectionUpdate};
use td_tower::ctx_service::{CtxMap, CtxResponse, CtxResponseBuilder};
use tower::ServiceExt;

apiserver_tag!(name = "Collections", description = "Collections API");

router! {
    state => { Collections },
    routes => { list_collections, get_collection, create_collection, update_collection, delete_collection }
}

list_status!(CollectionRead);

#[apiserver_path(method = get, path = LIST_COLLECTIONS, tag = COLLECTIONS_TAG)]
#[doc = "Lists collections"]
pub async fn list_collections(
    State(collection_state): State<Collections>,
    Extension(context): Extension<RequestContext>,
    Query(query_params): Query<ListParams>,
) -> Result<ListStatus, ListErrorStatus> {
    let request = context.list((), query_params);
    let response = collection_state
        .list_collections()
        .await
        .oneshot(request)
        .await?;
    Ok(ListStatus::OK(response.into()))
}

get_status!(CollectionRead);

#[apiserver_path(method = get, path = GET_COLLECTION, tag = COLLECTIONS_TAG)]
#[doc = "Get a collection"]
pub async fn get_collection(
    State(collection_state): State<Collections>,
    Extension(context): Extension<RequestContext>,
    Path(collection_param): Path<CollectionParam>,
) -> Result<GetStatus, GetErrorStatus> {
    let request = context.read(collection_param);
    let response = collection_state
        .read_collection()
        .await
        .oneshot(request)
        .await?;
    Ok(GetStatus::OK(response.into()))
}

create_status!(CollectionRead);

#[apiserver_path(method = post, path = CREATE_COLLECTION, tag = COLLECTIONS_TAG)]
#[doc = "Create a collection"]
pub async fn create_collection(
    State(collection_state): State<Collections>,
    Extension(context): Extension<RequestContext>,
    Json(request): Json<CollectionCreate>,
) -> Result<CreateStatus, CreateErrorStatus> {
    let request = context.create((), request);
    let response = collection_state
        .create_collection()
        .await
        .oneshot(request)
        .await?;
    Ok(CreateStatus::CREATED(response.into()))
}

update_status!(CollectionRead);

#[apiserver_path(method = post, path = UPDATE_COLLECTION, tag = COLLECTIONS_TAG)]
#[doc = "Update a collection"]
pub async fn update_collection(
    State(collection_state): State<Collections>,
    Extension(context): Extension<RequestContext>,
    Path(collection_param): Path<CollectionParam>,
    Json(request): Json<CollectionUpdate>,
) -> Result<UpdateStatus, UpdateErrorStatus> {
    let request = context.update(collection_param, request);
    let response = collection_state
        .update_collection()
        .await
        .oneshot(request)
        .await?;
    Ok(UpdateStatus::OK(response.into()))
}

#[apiserver_path(method = delete, path = DELETE_COLLECTION, tag = COLLECTIONS_TAG)]
#[doc = "Delete a collection"]
pub async fn delete_collection(
    State(collections_state): State<Collections>,
    Extension(context): Extension<RequestContext>,
    Path(collection_param): Path<CollectionParam>,
) -> Result<DeleteStatus, DeleteErrorStatus> {
    let request = context.delete(collection_param);
    let response = collections_state
        .delete_collection()
        .await
        .oneshot(request)
        .await?;
    Ok(DeleteStatus::OK(response.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{to_bytes, Body};
    use axum::http::{Request, StatusCode};
    use axum::Router;
    use http::method::Method;
    use serde_json::json;
    use std::sync::Arc;
    use td_authz::AuthzContext;
    use td_database::sql::DbPool;
    use td_objects::types::basic::{AccessTokenId, RoleId, UserId};
    use td_services::collection::service::CollectionServices;
    use tower::ServiceExt;

    async fn to_route<R: Into<Router> + Clone>(router: &R) -> Router {
        let context = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sys_admin(),
        );
        let router = router.clone().into();
        router.layer(Extension(context.clone()))
    }

    #[td_test::test(sqlx)]
    async fn test_collections_lifecycle(db: DbPool) {
        let collections_state = Arc::new(CollectionServices::new(
            db.clone(),
            Arc::new(AuthzContext::default()),
        ));
        let router = super::router(collections_state);

        // List empty collections
        let response = to_route(&router)
            .await
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(LIST_COLLECTIONS)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["data"]["len"], 0);

        // Create a new collection
        let collection_create = json!(
            {
                "name": "joaquin_collection",
                "description": "mock collection",
                "security_level": 5
            }
        );

        let response = to_route(&router)
            .await
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(CREATE_COLLECTION)
                    .header("content-type", "application/json")
                    .body(serde_json::to_string(&collection_create).unwrap())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["data"]["name"], "joaquin_collection");
        assert_eq!(body["data"]["description"], "mock collection");

        // List again and assert we have 2 collections
        let response = to_route(&router)
            .await
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(LIST_COLLECTIONS)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["data"]["len"], 1);

        // Get the new collection
        let response = to_route(&router)
            .await
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(GET_COLLECTION.replace("{collection}", "joaquin_collection"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["data"]["name"], "joaquin_collection");
        assert_eq!(body["data"]["description"], "mock collection");

        // Update the new collection
        let collection_update = json!(
            {
                "description": "not a mock anymore",
                "security_level": 2
            }
        );

        let response = to_route(&router)
            .await
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(UPDATE_COLLECTION.replace("{collection}", "joaquin_collection"))
                    .header("content-type", "application/json")
                    .body(serde_json::to_string(&collection_update).unwrap())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["data"]["name"], "joaquin_collection");
        assert_eq!(body["data"]["description"], "not a mock anymore");

        // Delete the new collection
        let response = to_route(&router)
            .await
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri(DELETE_COLLECTION.replace("{collection}", "joaquin_collection"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // List again and assert we are back to 1 collection
        let response = to_route(&router)
            .await
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(LIST_COLLECTIONS)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["data"]["len"], 0);
    }
}
