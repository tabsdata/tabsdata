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
    use ta_services::service::TdService;
    use td_apiforge::apiserver_path;
    use td_database::pool::scoped::InProject;
    use td_objects::dxo::collection::{CollectionCreate, CollectionRead, CollectionUpdate};
    use td_objects::dxo::connection::{ConnectionRead, ConnectionUpsert};
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::rest_urls::{
        CREATE_COLLECTION, CollectionParam, DELETE_COLLECTION, GET_COLLECTION,
        GET_COLLECTION_CONNECTION, LIST_COLLECTIONS, UPDATE_COLLECTION,
        UPSERT_COLLECTION_CONNECTION,
    };
    use td_objects::types::basic::ProjectId;
    use td_services::collection::service::CollectionServices;
    use tower::ServiceExt;

    const COLLECTIONS_TAG: &str = "Collections";

    #[apiserver_path(method = get, path = LIST_COLLECTIONS, tag = COLLECTIONS_TAG)]
    #[doc = "Lists collections"]
    pub async fn list_collections(
        State(collection_state): State<Arc<CollectionServices>>,
        Extension(context): Extension<RequestContext>,
        Extension(project_id): Extension<ProjectId>,
        Query(query_params): Query<ListParams>,
    ) -> Result<ListStatus<CollectionRead>, ErrorStatus> {
        let request = context.list((), query_params).in_project(project_id);
        let response = collection_state.list.service().oneshot(request).await?;
        Ok(ListStatus::OK(response))
    }

    #[apiserver_path(method = get, path = GET_COLLECTION, tag = COLLECTIONS_TAG)]
    #[doc = "Get a collection"]
    pub async fn get_collection(
        State(collection_state): State<Arc<CollectionServices>>,
        Extension(context): Extension<RequestContext>,
        Extension(project_id): Extension<ProjectId>,
        Path(collection_param): Path<CollectionParam>,
    ) -> Result<GetStatus<CollectionRead>, ErrorStatus> {
        let request = context.read(collection_param).in_project(project_id);
        let response = collection_state.read.service().oneshot(request).await?;
        Ok(GetStatus::OK(response))
    }

    #[apiserver_path(method = post, path = CREATE_COLLECTION, tag = COLLECTIONS_TAG)]
    #[doc = "Create a collection"]
    pub async fn create_collection(
        State(collection_state): State<Arc<CollectionServices>>,
        Extension(context): Extension<RequestContext>,
        Extension(project_id): Extension<ProjectId>,
        Json(request): Json<CollectionCreate>,
    ) -> Result<CreateStatus<CollectionRead>, ErrorStatus> {
        let request = context.create((), request).in_project(project_id);
        let response = collection_state.create.service().oneshot(request).await?;
        Ok(CreateStatus::CREATED(response))
    }

    #[apiserver_path(method = post, path = UPDATE_COLLECTION, tag = COLLECTIONS_TAG)]
    #[doc = "Update a collection"]
    pub async fn update_collection(
        State(collection_state): State<Arc<CollectionServices>>,
        Extension(context): Extension<RequestContext>,
        Extension(project_id): Extension<ProjectId>,
        Path(collection_param): Path<CollectionParam>,
        Json(request): Json<CollectionUpdate>,
    ) -> Result<UpdateStatus<CollectionRead>, ErrorStatus> {
        let request = context
            .update(collection_param, request)
            .in_project(project_id);
        let response = collection_state.update.service().oneshot(request).await?;
        Ok(UpdateStatus::OK(response))
    }

    #[apiserver_path(method = delete, path = DELETE_COLLECTION, tag = COLLECTIONS_TAG)]
    #[doc = "Delete a collection"]
    pub async fn delete_collection(
        State(collection_state): State<Arc<CollectionServices>>,
        Extension(context): Extension<RequestContext>,
        Extension(project_id): Extension<ProjectId>,
        Path(collection_param): Path<CollectionParam>,
    ) -> Result<DeleteStatus<NoContent>, ErrorStatus> {
        // The delete service also drops the connection from the store (its
        // best-effort cleanup layer) for source/destination collections.
        let request = context.delete(collection_param).in_project(project_id);
        let response = collection_state.delete.service().oneshot(request).await?;
        Ok(DeleteStatus::OK(response))
    }

    #[apiserver_path(method = post, path = UPSERT_COLLECTION_CONNECTION, tag = COLLECTIONS_TAG)]
    #[doc = "Create or update the connection info of a source/destination collection"]
    pub async fn upsert_collection_connection(
        State(collection_state): State<Arc<CollectionServices>>,
        Extension(context): Extension<RequestContext>,
        Extension(project_id): Extension<ProjectId>,
        Path(collection_param): Path<CollectionParam>,
        Json(request): Json<ConnectionUpsert>,
    ) -> Result<CreateStatus<ConnectionRead>, ErrorStatus> {
        let request = context
            .update(collection_param, request)
            .in_project(project_id);
        let response = collection_state
            .connection_upsert
            .service()
            .oneshot(request)
            .await?;
        Ok(CreateStatus::CREATED(response))
    }

    #[apiserver_path(method = get, path = GET_COLLECTION_CONNECTION, tag = COLLECTIONS_TAG)]
    #[doc = "Read the connection info of a source/destination collection (names only, no values)"]
    pub async fn get_collection_connection(
        State(collection_state): State<Arc<CollectionServices>>,
        Extension(context): Extension<RequestContext>,
        Extension(project_id): Extension<ProjectId>,
        Path(collection_param): Path<CollectionParam>,
    ) -> Result<GetStatus<ConnectionRead>, ErrorStatus> {
        let request = context.read(collection_param).in_project(project_id);
        let response = collection_state
            .connection_read
            .service()
            .oneshot(request)
            .await?;
        Ok(GetStatus::OK(response))
    }
}

#[cfg(test)]
mod tests {
    use crate::layers::project_resolver::project_resolver_layer;
    use crate::router::collections::CollectionsRouter;
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use axum::middleware::from_fn_with_state;
    use axum::{Extension, Router};
    use http::method::Method;
    use serde_json::json;
    use std::sync::Arc;
    use ta_apiserver::router::RouterExtension;
    use ta_services::factory::ServiceFactory;
    use td_database::pool::TdPool;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::rest_urls::{
        CREATE_COLLECTION, DELETE_COLLECTION, GET_COLLECTION, GET_COLLECTION_CONNECTION,
        LIST_COLLECTIONS, UPDATE_COLLECTION, UPSERT_COLLECTION_CONNECTION,
    };
    use td_objects::types::basic::{AccessTokenId, UserId};
    use td_services::Context;
    use td_services::collection::service::CollectionServices;
    use tower::ServiceExt;

    /// Mounts the same layers the real apiserver wraps tenant routes with:
    ///   - `project_resolver_layer` to resolve the `{project}` URL segment
    ///     into a real `ProjectId` and stamp it onto `RequestContext`.
    ///   - A pre-populated `RequestContext` extension (bypassing the auth
    ///     layer — these tests don't issue real JWTs).
    async fn to_route<R: Into<Router> + Clone>(router: &R, db: TdPool) -> Router {
        let context = RequestContext::with(AccessTokenId::default(), UserId::admin());
        let router = router.clone().into();
        router
            .layer(from_fn_with_state(db, project_resolver_layer))
            .layer(Extension(context))
    }

    /// Substitute the `{project}` segment with the bootstrap-seeded sandbox.
    fn url(template: &str) -> String {
        template.replace("{project}", "sandbox")
    }

    /// 404 the request if the `{project}` segment doesn't resolve. The
    /// resolver runs ahead of every tenant-scoped service so the connection's
    /// RLS GUC is set from a real registry id, never from a raw URL string.
    #[td_test::test(db)]
    async fn test_unknown_project_segment_yields_404(db: TdPool) {
        let router = CollectionsRouter::router(Arc::new(CollectionServices::build(
            &Context::with_defaults(db.clone()),
        )));
        let response = to_route(&router, db)
            .await
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(LIST_COLLECTIONS.replace("{project}", "does-not-exist"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[td_test::test(db)]
    async fn test_collections_lifecycle(db: TdPool) {
        let router = CollectionsRouter::router(Arc::new(CollectionServices::build(
            &Context::with_defaults(db.clone()),
        )));

        // List empty collections
        let response = to_route(&router, db.clone())
            .await
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(url(LIST_COLLECTIONS))
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

        let response = to_route(&router, db.clone())
            .await
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(url(CREATE_COLLECTION))
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
        let response = to_route(&router, db.clone())
            .await
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(url(LIST_COLLECTIONS))
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
        let response = to_route(&router, db.clone())
            .await
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(url(GET_COLLECTION).replace("{collection}", "joaquin_collection"))
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

        let response = to_route(&router, db.clone())
            .await
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(url(UPDATE_COLLECTION).replace("{collection}", "joaquin_collection"))
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
        let response = to_route(&router, db.clone())
            .await
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri(url(DELETE_COLLECTION).replace("{collection}", "joaquin_collection"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // List again and assert we are back to 1 collection
        let response = to_route(&router, db.clone())
            .await
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(url(LIST_COLLECTIONS))
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

    async fn create_collection_in<R: Into<Router> + Clone>(
        router: &R,
        db: TdPool,
        name: &str,
        group: Option<&str>,
    ) {
        let mut body = json!({"name": name, "description": "d"});
        if let Some(group) = group {
            body["group"] = json!(group);
            if group == "source" || group == "destination" {
                let suffix = if group == "source" { "Src" } else { "Dst" };
                body["connection"] = json!({
                    "kind": format!("mysql{suffix}Conn"),
                    "name": name,
                    "conn_configs": {"env:password": "CREDENTIALS_PASSWORD"},
                });
                body["secrets"] = json!([{"key": "CREDENTIALS_PASSWORD", "value": "hunter2"}]);
                body["vars"] = json!([]);
            }
        }
        let response = to_route(router, db)
            .await
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(url(CREATE_COLLECTION))
                    .header("content-type", "application/json")
                    .body(serde_json::to_string(&body).unwrap())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
    }

    #[td_test::test(db)]
    async fn test_collection_connection_endpoints(db: TdPool) {
        let router = CollectionsRouter::router(Arc::new(CollectionServices::build(
            &Context::with_defaults(db.clone()),
        )));

        // Source collection (group `source` → CollectionType::Source).
        create_collection_in(&router, db.clone(), "src_coll", Some("source")).await;

        let conn_body = json!({
            "connection": {
                "kind": "mysqlSrcConn",
                "name": "src_coll",
                "conn_configs": {"env:password": "CREDENTIALS_PASSWORD"},
            },
            "secrets": [{"key": "CREDENTIALS_PASSWORD", "value": "hunter2"}],
            "vars": []
        });

        // Upsert persists through the bridge's connection store; the response
        // echoes the stored names — never values.
        let response = to_route(&router, db.clone())
            .await
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(url(UPSERT_COLLECTION_CONNECTION).replace("{collection}", "src_coll"))
                    .header("content-type", "application/json")
                    .body(serde_json::to_string(&conn_body).unwrap())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["data"]["secret_names"][0], "CREDENTIALS_PASSWORD");
        assert!(!body.to_string().contains("hunter2"));

        // GET reads the connection (names only) back.
        let response = to_route(&router, db.clone())
            .await
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(url(GET_COLLECTION_CONNECTION).replace("{collection}", "src_coll"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["data"]["secret_names"][0], "CREDENTIALS_PASSWORD");
        assert!(!body.to_string().contains("hunter2"));

        // A DataFactory collection (default group) cannot carry a connection.
        create_collection_in(&router, db.clone(), "fac_coll", None).await;
        let response = to_route(&router, db.clone())
            .await
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(url(UPSERT_COLLECTION_CONNECTION).replace("{collection}", "fac_coll"))
                    .header("content-type", "application/json")
                    .body(serde_json::to_string(&conn_body).unwrap())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(response.status().is_client_error());
    }
}
