//
// Copyright 2025 Tabs Data Inc.
//

use td_apiforge::router_ext;

#[router_ext(CollectionsRouter)]
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
    use td_apiforge::apiserver_path;
    use td_objects::crudl::{ListParams, RequestContext};
    use td_objects::rest_urls::{
        CREATE_COLLECTION, CollectionParam, DELETE_COLLECTION, GET_COLLECTION, LIST_COLLECTIONS,
        UPDATE_COLLECTION,
    };
    use td_objects::types::collection::{CollectionCreate, CollectionRead, CollectionUpdate};
    use td_services::collection::service::CollectionServices;
    use td_tower::td_service::TdService;
    use tower::ServiceExt;

    const COLLECTIONS_TAG: &str = "Collections";

    #[apiserver_path(method = get, path = LIST_COLLECTIONS, tag = COLLECTIONS_TAG)]
    #[doc = "Lists collections"]
    pub async fn list_collections(
        State(collection_state): State<Arc<CollectionServices>>,
        Extension(context): Extension<RequestContext>,
        Query(query_params): Query<ListParams>,
    ) -> Result<ListStatus<CollectionRead>, ErrorStatus> {
        let request = context.list((), query_params);
        let response = collection_state
            .list()
            .service()
            .await
            .oneshot(request)
            .await?;
        Ok(ListStatus::OK(response))
    }

    #[apiserver_path(method = get, path = GET_COLLECTION, tag = COLLECTIONS_TAG)]
    #[doc = "Get a collection"]
    pub async fn get_collection(
        State(collection_state): State<Arc<CollectionServices>>,
        Extension(context): Extension<RequestContext>,
        Path(collection_param): Path<CollectionParam>,
    ) -> Result<GetStatus<CollectionRead>, ErrorStatus> {
        let request = context.read(collection_param);
        let response = collection_state
            .read()
            .service()
            .await
            .oneshot(request)
            .await?;
        Ok(GetStatus::OK(response))
    }

    #[apiserver_path(method = post, path = CREATE_COLLECTION, tag = COLLECTIONS_TAG)]
    #[doc = "Create a collection"]
    pub async fn create_collection(
        State(collection_state): State<Arc<CollectionServices>>,
        Extension(context): Extension<RequestContext>,
        Json(request): Json<CollectionCreate>,
    ) -> Result<CreateStatus<CollectionRead>, ErrorStatus> {
        let request = context.create((), request);
        let response = collection_state
            .create()
            .service()
            .await
            .oneshot(request)
            .await?;
        Ok(CreateStatus::CREATED(response))
    }

    #[apiserver_path(method = post, path = UPDATE_COLLECTION, tag = COLLECTIONS_TAG)]
    #[doc = "Update a collection"]
    pub async fn update_collection(
        State(collection_state): State<Arc<CollectionServices>>,
        Extension(context): Extension<RequestContext>,
        Path(collection_param): Path<CollectionParam>,
        Json(request): Json<CollectionUpdate>,
    ) -> Result<UpdateStatus<CollectionRead>, ErrorStatus> {
        let request = context.update(collection_param, request);
        let response = collection_state
            .update()
            .service()
            .await
            .oneshot(request)
            .await?;
        Ok(UpdateStatus::OK(response))
    }

    #[apiserver_path(method = delete, path = DELETE_COLLECTION, tag = COLLECTIONS_TAG)]
    #[doc = "Delete a collection"]
    pub async fn delete_collection(
        State(collection_state): State<Arc<CollectionServices>>,
        Extension(context): Extension<RequestContext>,
        Path(collection_param): Path<CollectionParam>,
    ) -> Result<DeleteStatus<NoContent>, ErrorStatus> {
        let request = context.delete(collection_param);
        let response = collection_state
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
    use crate::router::collections::CollectionsRouter;
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use axum::{Extension, Router};
    use http::method::Method;
    use serde_json::json;
    use std::sync::Arc;
    use ta_apiserver::router::RouterExtension;
    use td_database::sql::DbPool;
    use td_objects::crudl::RequestContext;
    use td_objects::rest_urls::{
        CREATE_COLLECTION, DELETE_COLLECTION, GET_COLLECTION, LIST_COLLECTIONS, UPDATE_COLLECTION,
    };
    use td_objects::types::basic::{AccessTokenId, RoleId, UserId};
    use td_services::Context;
    use td_services::collection::service::CollectionServices;
    use td_tower::factory::ServiceFactory;
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
    #[tokio::test]
    async fn test_collections_lifecycle(db: DbPool) {
        let router = CollectionsRouter::router(Arc::new(CollectionServices::build(
            &Context::with_defaults(db),
        )));

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
