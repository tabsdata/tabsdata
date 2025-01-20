//
// Copyright 2024 Tabs Data Inc.
//

//! Collections API Service for API Server.

#![allow(clippy::upper_case_acronyms)]

use crate::logic::apisrv::status::extractors::Json;
use axum::extract::{Path, Query, State};
use axum::routing::{delete, get, post};
use axum::Extension;
use derive_builder::Builder;
use getset::Getters;
use serde::{Deserialize, Serialize};

use crate::bin::apisrv::api_server::CollectionsState;
use crate::logic::apisrv::jwt::admin_only::AdminOnly;
use crate::logic::apisrv::status::error_status::{
    CreateErrorStatus, DeleteErrorStatus, GetErrorStatus, ListErrorStatus, UpdateErrorStatus,
};
use crate::logic::apisrv::status::status_macros::DeleteStatus;
use crate::{create_status, get_status, list_status, router, update_status};
use axum::middleware::from_fn;
use td_concrete::concrete;
use td_objects::collections::dto::{
    CollectionCreate, CollectionList, CollectionRead, CollectionUpdate,
};
use td_objects::crudl::ListResponse;
use td_objects::crudl::ListResponseBuilder;
use td_objects::crudl::{ListParams, RequestContext};
use td_objects::rest_urls::{
    CollectionParam, CREATE_COLLECTION, DELETE_COLLECTION, GET_COLLECTION, LIST_COLLECTIONS,
    UPDATE_COLLECTION,
};
use td_utoipa::{api_server_path, api_server_schema, api_server_tag};
use tower::ServiceExt;

api_server_tag!(name = "Collection", description = "Collections API");

router! {
    state => { CollectionsState },
    paths => {
        {
            LIST_COLLECTIONS => get(list_collections),
            GET_COLLECTION => get(get_collection),
            CREATE_COLLECTION => post(create_collection),
            UPDATE_COLLECTION => post(update_collection),
            DELETE_COLLECTION => delete(delete_collection),
        }
        .layer => |_| from_fn(AdminOnly::layer),
    }
}

#[concrete]
#[api_server_schema]
type ListResponseCollection = ListResponse<CollectionList>;

list_status!(ListResponseCollection);

#[api_server_path(method = get, path = LIST_COLLECTIONS, tag = COLLECTION_TAG)]
#[doc = "Lists collections"]
pub async fn list_collections(
    State(collection_state): State<CollectionsState>,
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

#[api_server_path(method = get, path = GET_COLLECTION, tag = COLLECTION_TAG)]
#[doc = "Get a collection"]
pub async fn get_collection(
    State(collection_state): State<CollectionsState>,
    Extension(context): Extension<RequestContext>,
    Path(collection_param): Path<CollectionParam>,
) -> Result<GetStatus, GetErrorStatus> {
    let request = context.read(collection_param);
    let response = collection_state
        .read_collection()
        .await
        .oneshot(request)
        .await?;
    Ok(GetStatus::OK(response))
}

create_status!(CollectionRead);

#[api_server_path(method = post, path = CREATE_COLLECTION, tag = COLLECTION_TAG)]
#[doc = "Create a collection"]
pub async fn create_collection(
    State(collection_state): State<CollectionsState>,
    Extension(context): Extension<RequestContext>,
    Json(request): Json<CollectionCreate>,
) -> Result<CreateStatus, CreateErrorStatus> {
    let request = context.create((), request);
    let response = collection_state
        .create_collection()
        .await
        .oneshot(request)
        .await?;
    Ok(CreateStatus::CREATED(response))
}

update_status!(CollectionRead);

#[api_server_path(method = post, path = UPDATE_COLLECTION, tag = COLLECTION_TAG)]
#[doc = "Update a collection"]
pub async fn update_collection(
    State(collection_state): State<CollectionsState>,
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
    Ok(UpdateStatus::OK(response))
}

#[api_server_path(method = delete, path = DELETE_COLLECTION, tag = COLLECTION_TAG)]
#[doc = "Delete a collection"]
pub async fn delete_collection(
    State(collections_state): State<CollectionsState>,
    Extension(context): Extension<RequestContext>,
    Path(collection_param): Path<CollectionParam>,
) -> Result<DeleteStatus, DeleteErrorStatus> {
    let request = context.delete(collection_param);
    collections_state
        .delete_collection()
        .await
        .oneshot(request)
        .await?;
    Ok(DeleteStatus::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logic::collections::service::CollectionServices;
    use axum::body::{to_bytes, Body};
    use axum::http::{Request, StatusCode};
    use axum::Router;
    use http::method::Method;
    use serde_json::json;
    use std::sync::Arc;
    use td_database::sql::DbPool;
    use tower::ServiceExt;

    async fn collections_state() -> CollectionsState {
        let db: &'static DbPool = Box::leak(Box::new(td_database::test_utils::db().await.unwrap()));
        let logic = CollectionServices::new(db.clone());
        Arc::new(logic)
    }

    async fn to_route(router: &Router) -> Router {
        let context = RequestContext::with("", "", true).await;
        router.clone().layer(Extension(context.clone()))
    }

    #[tokio::test]
    async fn test_collections_lifecycle() {
        let collections_state = collections_state().await;
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
        assert_eq!(body["len"], 0);

        // Create a new collection
        let collection_create = json!(
            {
                "name": "joaquin's collection",
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
        assert_eq!(body["name"], "joaquin's collection");
        assert_eq!(body["description"], "mock collection");

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
        assert_eq!(body["len"], 1);

        // Get the new collection
        let response = to_route(&router)
            .await
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(GET_COLLECTION.replace("{collection}", "joaquin%27s%20collection"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["name"], "joaquin's collection");
        assert_eq!(body["description"], "mock collection");

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
                    .uri(UPDATE_COLLECTION.replace("{collection}", "joaquin%27s%20collection"))
                    .header("content-type", "application/json")
                    .body(serde_json::to_string(&collection_update).unwrap())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["name"], "joaquin's collection");
        assert_eq!(body["description"], "not a mock anymore");

        // Delete the new collection
        let response = to_route(&router)
            .await
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri(DELETE_COLLECTION.replace("{collection}", "joaquin%27s%20collection"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

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
        assert_eq!(body["len"], 0);
    }
}
