//
// Copyright 2025 Tabsdata Inc.
//

use crate::extractors::Project;
use axum::Extension;
use axum::extract::{Path, State};
use axum_extra::extract::Query;
use ta_apiserver::status::error_status::ErrorStatus;
use ta_apiserver::status::extractors::Json;
use ta_apiserver::status::ok_status::ListStatus;
use ta_apiserver::status::ok_status::{
    CreateStatus, DeleteStatus, GetStatus, NoContent, UpdateStatus,
};
use td_apiforge::apiserver_path;
use td_database::pool::scoped::ScopedExt;
use td_objects::dxo::collection::{
    CollectionCreate, CollectionParam, CollectionRead, CollectionUpdate,
};
use td_objects::dxo::connection::{ConnectionRead, ConnectionUpsert};
use td_objects::dxo::crudl::{ListParams, RequestContext};
use td_services::Services;

const COLLECTIONS_TAG: &str = "Collections";

// Spans every project the caller can view; narrow with the `project`
// list filter.
#[apiserver_path(mount = global, method = get, path = "/collections", tag = COLLECTIONS_TAG, doc = "Lists collections")]
pub async fn list_collections(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Query(list_params): Query<ListParams>,
) -> Result<ListStatus<CollectionRead>, ErrorStatus> {
    let request = context.list((), list_params);
    let response = state.list_collections(request).await?;
    Ok(ListStatus::OK(response))
}

#[apiserver_path(mount = project, method = get, path = "/projects/{project}/collections/{collection}", tag = COLLECTIONS_TAG, doc = "Get a collection")]
pub async fn get_collection(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Path(collection): Path<CollectionParam>,
) -> Result<GetStatus<CollectionRead>, ErrorStatus> {
    let request = context.read(collection).in_project(project_id);
    let response = state.read_collection(request).await?;
    Ok(GetStatus::OK(response))
}

#[apiserver_path(mount = project, method = post, path = "/projects/{project}/collections", tag = COLLECTIONS_TAG, doc = "Create a collection")]
pub async fn create_collection(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Json(request): Json<CollectionCreate>,
) -> Result<CreateStatus<CollectionRead>, ErrorStatus> {
    let request = context.create((), request).in_project(project_id);
    let response = state.create_collection(request).await?;
    Ok(CreateStatus::CREATED(response))
}

#[apiserver_path(mount = project, method = post, path = "/projects/{project}/collections/{collection}", tag = COLLECTIONS_TAG, doc = "Update a collection")]
pub async fn update_collection(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Path(collection): Path<CollectionParam>,
    Json(request): Json<CollectionUpdate>,
) -> Result<UpdateStatus<CollectionRead>, ErrorStatus> {
    let request = context.update(collection, request).in_project(project_id);
    let response = state.update_collection(request).await?;
    Ok(UpdateStatus::OK(response))
}

#[apiserver_path(mount = project, method = delete, path = "/projects/{project}/collections/{collection}", tag = COLLECTIONS_TAG, doc = "Delete a collection")]
pub async fn delete_collection(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Path(collection): Path<CollectionParam>,
) -> Result<DeleteStatus<NoContent>, ErrorStatus> {
    // The delete service also drops the connection from the store (its
    // best-effort cleanup layer) for source/destination collections.
    let request = context.delete(collection).in_project(project_id);
    let response = state.delete_collection(request).await?;
    Ok(DeleteStatus::OK(response))
}

// Connection info of a source/destination collection (1:1 — created with the
// collection, named by it, deleted with it). The apiserver forwards the
// dehydrated connection + secrets + vars to the opaque store, keyed by
// connection id. POST updates an existing connection; GET reads back names
// only. Both require manage_project ("CRUD sources and destinations").
#[apiserver_path(mount = project, method = post, path = "/projects/{project}/collections/{collection}/connection", tag = COLLECTIONS_TAG, doc = "Create or update the connection info of a source/destination collection")]
pub async fn upsert_collection_connection(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Path(collection): Path<CollectionParam>,
    Json(request): Json<ConnectionUpsert>,
) -> Result<CreateStatus<ConnectionRead>, ErrorStatus> {
    let request = context.update(collection, request).in_project(project_id);
    let response = state.connection_upsert(request).await?;
    Ok(CreateStatus::CREATED(response))
}

#[apiserver_path(mount = project, method = get, path = "/projects/{project}/collections/{collection}/connection", tag = COLLECTIONS_TAG, doc = "Read the connection info of a source/destination collection (names only, no values)")]
pub async fn get_collection_connection(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Path(collection): Path<CollectionParam>,
) -> Result<GetStatus<ConnectionRead>, ErrorStatus> {
    let request = context.read(collection).in_project(project_id);
    let response = state.connection_read(request).await?;
    Ok(GetStatus::OK(response))
}

#[cfg(test)]
mod tests {
    use super::{
        CREATE_COLLECTION_PATH, GET_COLLECTION_PATH, LIST_COLLECTIONS_PATH,
        UPSERT_COLLECTION_CONNECTION_PATH,
    };
    use crate::layers::project_resolver::project_resolver_layer;
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use axum::middleware::from_fn_with_state;
    use axum::{Extension, Router};
    use http::method::Method;
    use serde_json::json;
    use ta_apiserver::router::{RouterStates, named_router};
    use td_database::access::{DbPool, Global};
    use td_database::domain::{Apiserver, DomainPools};
    use td_database::pool::TdPool;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::types::basic::{AccessTokenId, UserId};
    use td_queries::test_utils::SeedEntry;
    use td_security::{DESTINATION_GROUP, SOURCE_GROUP};
    use td_services::Services;
    use tower::ServiceExt;
    use utoipa::openapi::path::{Operation, ParameterIn};

    /// Mounts the same layers the real apiserver wraps project routes with:
    ///   - `project_resolver_layer` to resolve the `{project}` URL segment
    ///     into a real `ProjectId` and stamp it onto `RequestContext`.
    ///   - A pre-populated `RequestContext` extension (bypassing the auth
    ///     layer — these tests don't issue real JWTs).
    async fn to_route<R: Into<Router> + Clone>(router: &R, db: TdPool) -> Router {
        let context = RequestContext::with(AccessTokenId::default(), UserId::admin());
        let router = router.clone().into();
        router
            .layer(from_fn_with_state(
                DbPool::new(DomainPools::default().with::<Apiserver>(db), Global),
                project_resolver_layer,
            ))
            .layer(Extension(context))
    }

    /// Mounts the flat (non-project) list router with only the
    /// `RequestContext` extension — no resolver runs for top-level routes.
    fn to_list_route<R: Into<Router> + Clone>(router: &R) -> Router {
        let context = RequestContext::with(AccessTokenId::default(), UserId::admin());
        router.clone().into().layer(Extension(context))
    }

    /// Substitute the `{project}` segment with the per-test seeded `test` project.
    fn url(template: &str) -> String {
        template.replace("{project}", "test")
    }

    /// 404 the request if the `{project}` segment doesn't resolve. The
    /// resolver runs ahead of every project-scoped service so the connection's
    /// RLS GUC is set from a real registry id, never from a raw URL string.
    #[td_test::test(db)]
    async fn test_unknown_project_segment_yields_404(db: TdPool) {
        let states = RouterStates::default().with(Services::with_defaults(db.clone()));
        let router = named_router(
            &[
                "get_collection",
                "create_collection",
                "update_collection",
                "delete_collection",
                "upsert_collection_connection",
                "get_collection_connection",
            ],
            &states,
        );
        let response = to_route(&router, db)
            .await
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(
                        GET_COLLECTION_PATH
                            .replace("{project}", "does-not-exist")
                            .replace("{collection}", "whatever"),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    /// The `{project}` segment is resolved in middleware (read via the `Project`
    /// extractor, never a `Path<_>`), so the `#[apiserver_path]` macro injects
    /// it into the OpenAPI `params`. Assert every collections operation
    /// documents `project` as a path parameter — and that the per-route params
    /// (e.g. `collection`) still come through alongside it.
    #[td_test::test(db)]
    async fn test_openapi_documents_project_path_param(db: TdPool) {
        let states = RouterStates::default().with(Services::with_defaults(db.clone()));
        let api = named_router(
            &[
                "get_collection",
                "create_collection",
                "update_collection",
                "delete_collection",
                "upsert_collection_connection",
                "get_collection_connection",
            ],
            &states,
        )
        .into_openapi();

        let has_path_param = |op: &Operation, name: &str| {
            op.parameters.as_ref().is_some_and(|ps| {
                ps.iter()
                    .any(|p| p.name == name && matches!(p.parameter_in, ParameterIn::Path))
            })
        };

        let mut ops = 0;
        for (path, item) in api.paths.paths.iter() {
            assert!(
                path.contains("{project}"),
                "every collections route is project-scoped, got: {path}"
            );
            for op in [&item.get, &item.put, &item.post, &item.delete]
                .into_iter()
                .flatten()
            {
                assert!(
                    has_path_param(op, "project"),
                    "missing `project` path parameter on {path}"
                );
                ops += 1;
            }
        }
        assert!(
            ops >= 6,
            "expected the collections operations, counted {ops}"
        );

        // GET_COLLECTION_PATH = /projects/{project}/collections/{collection}: both
        // the injected `project` and the handler's own `collection` are present.
        let get_collection = api
            .paths
            .paths
            .get(GET_COLLECTION_PATH)
            .and_then(|p| p.get.as_ref())
            .expect("GET collection operation present");
        assert!(has_path_param(get_collection, "project"));
        assert!(has_path_param(get_collection, "collection"));
    }

    #[td_test::test(db)]
    async fn test_collections_lifecycle(db: TdPool) {
        db.seed().test_project().await.unwrap();
        let services = Services::with_defaults(db.clone());
        let states = RouterStates::default().with(services);
        let router = named_router(
            &[
                "get_collection",
                "create_collection",
                "update_collection",
                "delete_collection",
                "upsert_collection_connection",
                "get_collection_connection",
            ],
            &states,
        );
        let list_router = named_router(&["list_collections"], &states);

        let response = to_list_route(&list_router)
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(LIST_COLLECTIONS_PATH)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["data"]["len"], 0);

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
                    .uri(url(CREATE_COLLECTION_PATH))
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

        let response = to_list_route(&list_router)
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(LIST_COLLECTIONS_PATH)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["data"]["len"], 1);

        let response = to_route(&router, db.clone())
            .await
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(url(GET_COLLECTION_PATH).replace("{collection}", "joaquin_collection"))
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
                    .uri(url(GET_COLLECTION_PATH).replace("{collection}", "joaquin_collection"))
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

        let response = to_route(&router, db.clone())
            .await
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri(url(GET_COLLECTION_PATH).replace("{collection}", "joaquin_collection"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = to_list_route(&list_router)
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(LIST_COLLECTIONS_PATH)
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
            if group == SOURCE_GROUP || group == DESTINATION_GROUP {
                let suffix = if group == SOURCE_GROUP { "Src" } else { "Dest" };
                // The create endpoint takes the connection as a dehydrated YAML
                // string; the server reads only `type` off it (the rest is opaque
                // and forwarded to the store/worker).
                body["connection"] =
                    json!(format!("kind: connectionDef\ntype: mysql{suffix}Conn\n"));
                body["secrets"] = json!([{"key": "CREDENTIALS_PASSWORD", "value": "hunter2"}]);
                body["vars"] = json!([]);
            }
        }
        let response = to_route(router, db)
            .await
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(url(CREATE_COLLECTION_PATH))
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
        db.seed().test_project().await.unwrap();
        let states = RouterStates::default().with(Services::with_defaults(db.clone()));
        let router = named_router(
            &[
                "get_collection",
                "create_collection",
                "update_collection",
                "delete_collection",
                "upsert_collection_connection",
                "get_collection_connection",
            ],
            &states,
        );

        // Source collection (group `source` → CollectionType::Source).
        create_collection_in(&router, db.clone(), "src_coll", Some(SOURCE_GROUP)).await;

        // The upsert endpoint takes the connection as a dehydrated YAML string
        // (same wire form as create).
        let conn_body = json!({
            "connection": "type: mysqlSrcConn\nname: src_coll\n\
                conn_configs:\n  \"env:password\": CREDENTIALS_PASSWORD\n",
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
                    .uri(url(UPSERT_COLLECTION_CONNECTION_PATH).replace("{collection}", "src_coll"))
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
                    .uri(url(UPSERT_COLLECTION_CONNECTION_PATH).replace("{collection}", "src_coll"))
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
                    .uri(url(UPSERT_COLLECTION_CONNECTION_PATH).replace("{collection}", "fac_coll"))
                    .header("content-type", "application/json")
                    .body(serde_json::to_string(&conn_body).unwrap())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(response.status().is_client_error());
    }
}
