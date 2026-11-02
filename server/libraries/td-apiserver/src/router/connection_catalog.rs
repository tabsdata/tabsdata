//
// Copyright 2026 Tabsdata Inc.
//

//! The connection catalog: listing spans every project the caller views
//! (narrow with the `project` filter); entities and probes need
//! `develop_project`. The definition itself stays behind
//! `/collections/{collection}/connection`.
//!
//! The probe result callback carries a metadata probe's entities, so it reads
//! its body up to `MAX_OUTCOME_BYTES` instead of axum's 2 MiB default.

use axum::Extension;
use axum::body::Body;
use axum::extract::{Path, State};
use axum_extra::extract::Query;
use futures::StreamExt;
use ta_apiserver::project::Project;
use ta_apiserver::status::error_status::ErrorStatus;
use ta_apiserver::status::extractors::Json;
use ta_apiserver::status::ok_status::{
    AcceptedStatus, GetStatus, ListStatus, NoContent, UpdateStatus,
};
use td_apiforge::apiserver_path;
use td_database::pool::scoped::ScopedExt;
use td_error::{ApiError, TdError, api_error};
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::connection_catalog::{
    ConnectionCatalogRead, ConnectionSummary, EntityListParams, EntityRead, ProbeCallback,
    ProbeParam, ProbeRead, ProbeSubmit, ProbeWaitParams,
};
use td_objects::dxo::crudl::{ListParams, RequestContext};
use td_services::Services;
use td_services::connection_catalog::MAX_OUTCOME_BYTES;

const CONNECTIONS_TAG: &str = "Connections";
const INTERNAL_TAG: &str = "Internal";

#[apiserver_path(
    version(v1),
    auth(session, service_token),
    method(get),
    path = "/connections",
    tag(CONNECTIONS_TAG),
    doc = "Lists the connections of every project the caller can view"
)]
pub async fn list_connections(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Query(list_params): Query<ListParams>,
) -> Result<ListStatus<ConnectionSummary>, ErrorStatus> {
    let request = context.list((), list_params);
    let response = state.list_connections(request).await?;
    Ok(ListStatus::OK(response))
}

#[apiserver_path(
    version(v1),
    scope(project),
    auth(session, service_token),
    method(get),
    path = "/projects/{project}/collections/{collection}/connection/catalog",
    tag(CONNECTIONS_TAG),
    doc = "Reads the catalog of a collection's connection: probe status and fingerprint"
)]
pub async fn read_connection_catalog(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Path(collection): Path<CollectionParam>,
) -> Result<GetStatus<ConnectionCatalogRead>, ErrorStatus> {
    let request = context.read(collection).in_project(project_id);
    let response = state.read_connection_catalog(request).await?;
    Ok(GetStatus::OK(response))
}

#[apiserver_path(
    version(v1),
    scope(project),
    auth(session, service_token),
    method(get),
    path = "/projects/{project}/collections/{collection}/connection/entities",
    tag(CONNECTIONS_TAG),
    doc = "Lists the entities a metadata probe found; `fields=true` adds each entity's document"
)]
pub async fn list_connection_entities(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Path(collection): Path<CollectionParam>,
    Query(list_params): Query<ListParams>,
    Query(entity_params): Query<EntityListParams>,
) -> Result<ListStatus<EntityRead>, ErrorStatus> {
    let request = context
        .list((collection, entity_params), list_params)
        .in_project(project_id);
    let response = state.list_connection_entities(request).await?;
    Ok(ListStatus::OK(response))
}

#[apiserver_path(
    version(v1),
    scope(project),
    auth(session, service_token),
    method(post),
    path = "/projects/{project}/collections/{collection}/connection/probes",
    tag(CONNECTIONS_TAG),
    doc = "Submits a check, metadata or sample probe; an identical active probe is joined, a sample only by its own requester"
)]
pub async fn submit_connection_probe(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Path(collection): Path<CollectionParam>,
    Json(request): Json<ProbeSubmit>,
) -> Result<AcceptedStatus<ProbeRead>, ErrorStatus> {
    let request = context.create(collection, request).in_project(project_id);
    let response = state.submit_connection_probe(request).await?;
    Ok(AcceptedStatus::ACCEPTED(response))
}

#[apiserver_path(
    version(v1),
    scope(project),
    auth(session, service_token),
    method(get),
    path = "/projects/{project}/connection-probes/{probe}",
    tag(CONNECTIONS_TAG),
    doc = "Reads a probe, a sample's rows only for its requester; `wait` (seconds, at most 55) holds the request until it ends"
)]
pub async fn read_connection_probe(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Path(probe): Path<ProbeParam>,
    Query(wait): Query<ProbeWaitParams>,
) -> Result<GetStatus<ProbeRead>, ErrorStatus> {
    let request = context.read((probe, wait)).in_project(project_id);
    let response = state.read_connection_probe(request).await?;
    Ok(GetStatus::OK(response))
}

#[apiserver_path(
    version(v1),
    scope(project),
    auth(session, service_token),
    method(post),
    path = "/projects/{project}/connection-probes/{probe}/cancel",
    tag(CONNECTIONS_TAG),
    doc = "Cancels an active probe: the submitter's own, or any with manage_project"
)]
pub async fn cancel_connection_probe(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Path(probe): Path<ProbeParam>,
) -> Result<UpdateStatus<ProbeRead>, ErrorStatus> {
    let request = context.update(probe, ()).in_project(project_id);
    let response = state.cancel_connection_probe(request).await?;
    Ok(UpdateStatus::OK(response))
}

#[apiserver_path(
    internal,
    version(v1),
    auth(internal),
    method(post),
    path = "/internal/connection-probes/{probe}/result",
    tag(INTERNAL_TAG),
    doc = "Callback endpoint for connection probes"
)]
pub async fn connection_probe_result(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Path(probe): Path<ProbeParam>,
    body: Body,
) -> Result<UpdateStatus<NoContent>, ErrorStatus> {
    let request = context.update(probe, bounded_json(body, MAX_OUTCOME_BYTES).await?);
    let response = state.connection_probe_result(request).await?;
    Ok(UpdateStatus::OK(response))
}

async fn bounded_json(body: Body, limit: usize) -> Result<ProbeCallback, TdError> {
    let mut stream = body.into_data_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk
            .map_err(|e| api_error!(ApiError::InputError, "cannot read the request body: {e}"))?;
        if bytes.len() + chunk.len() > limit {
            return Err(api_error!(
                ApiError::Any(413),
                "the probe outcome is over {limit} bytes"
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes)
        .map_err(|e| api_error!(ApiError::InputError, "not a probe callback: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apiserver::{BASE_API_URL, V1};
    use crate::layers::internal_token::internal_token_layer;
    use crate::layers::project_resolver::project_resolver_layer;
    use crate::router::internal::internal_routes;
    use axum::Router;
    use axum::body::to_bytes;
    use axum::http::{Request, StatusCode};
    use axum::middleware::from_fn_with_state;
    use http::method::Method;
    use serde_json::{Value, json};
    use std::sync::Arc;
    use ta_apiserver::router::{RouterStates, Routes};
    use ta_services::service::Ctx;
    use td_bridge::test::TestBridgeProvider;
    use td_bridge::{BridgeProviderHandle, Orchestrator};
    use td_common::execution_status::WorkerCallbackStatus;
    use td_common::worker::Callback;
    use td_database::pool::TdPool;
    use td_objects::dxo::collection::CollectionCreate;
    use td_objects::dxo::connection::KeyValue;
    use td_objects::types::basic::{
        AccessTokenId, CollectionName, Description, GroupName, ProbeId, ProbeKind, ProjectId,
        UserId,
    };
    use td_objects::types::basic::{UserEnabled, UserName};
    use td_queries::test_utils::{SeedEntry, seed_project_viewer};
    use td_services::internal_api::InternalApi;
    use tower::ServiceExt;

    const FLAT: &[&str] = &["list_connections"];
    const PROJECT: &[&str] = &[
        "read_connection_catalog",
        "list_connection_entities",
        "submit_connection_probe",
        "read_connection_probe",
        "cancel_connection_probe",
    ];

    /// Past axum's 2 MiB default.
    const ENTITIES: usize = 600;

    struct Fixture {
        db: TdPool,
        services: Services,
        project: ProjectId,
    }

    impl Fixture {
        async fn new(db: TdPool) -> Self {
            let project = db.seed().test_project().await.unwrap().project_id;
            let mut services = Services::with_defaults(db.clone());
            services.bridge = Ctx::new(BridgeProviderHandle::new(Arc::new(
                TestBridgeProvider::default().with_orchestrator(Orchestrator::Kubernetes),
            )));
            services.internal_api = Ctx::new(InternalApi::new(
                internal_routes(),
                services.jwt_config.clone(),
                Arc::default(),
            ));
            services
                .create_collection(
                    admin()
                        .create(
                            (),
                            CollectionCreate {
                                name: CollectionName::try_from("crm").unwrap(),
                                description: Description::try_from("d").unwrap(),
                                group: GroupName::source(),
                                base_path: None,
                                connection: Some(json!({
                                    "kind": "connectionDef", "apiVersion": "1.0", "name": "crm",
                                    "type": "mysqlSrcConn",
                                    "conn_configs": {"host": "h", "env:password": "PASSWORD"},
                                })),
                                secrets: Some(vec![KeyValue {
                                    key: "PASSWORD".to_string(),
                                    value: Some("hunter2".to_string()),
                                }]),
                            },
                        )
                        .in_project(project),
                )
                .await
                .unwrap();
            Self {
                db,
                services,
                project,
            }
        }

        fn flat(&self) -> Router {
            let states = RouterStates::default().with(self.services.clone());
            let router: Router = Routes::new(&[]).named_router(FLAT, &states).into();
            router.layer(Extension(admin()))
        }

        fn public(&self) -> Router {
            self.public_as(admin())
        }

        fn public_as(&self, context: RequestContext) -> Router {
            let states = RouterStates::default().with(self.services.clone());
            let router: Router = Routes::new(&[]).named_router(PROJECT, &states).into();
            router
                .layer(from_fn_with_state(
                    self.services.clone(),
                    project_resolver_layer,
                ))
                .layer(Extension(context))
        }

        fn internal(&self) -> Router {
            let states = RouterStates::default().with(self.services.clone());
            let inner: Router = Routes::new(&[])
                .named_router(&["connection_probe_result"], &states)
                .into();
            let inner = inner.route_layer(from_fn_with_state(
                self.services.clone(),
                internal_token_layer,
            ));
            Router::new().nest(BASE_API_URL, Router::new().nest(V1, inner))
        }

        async fn submit(&self, kind: ProbeKind) -> ProbeId {
            self.services
                .submit_connection_probe(
                    admin()
                        .create(
                            CollectionParam {
                                collection: "crm".parse().unwrap(),
                            },
                            ProbeSubmit {
                                kind,
                                entity: None,
                                entities: None,
                                limit: None,
                            },
                        )
                        .in_project(self.project),
                )
                .await
                .unwrap()
                .data
                .probe_id
        }

        /// The path and bearer the dispatcher hands `probe`'s runner.
        fn responder(&self, probe: ProbeId) -> (String, String) {
            let Callback::Http(http) = self
                .services
                .internal_api
                .probe_responder(&probe, std::time::Duration::from_secs(60))
                .unwrap();
            (
                http.url.path().to_owned(),
                http.headers["authorization"].clone(),
            )
        }
    }

    fn admin() -> RequestContext {
        RequestContext::with(AccessTokenId::default(), UserId::admin())
    }

    fn at(template: &str) -> String {
        template
            .replace("{project}", "test")
            .replace("{collection}", "crm")
    }

    async fn call(
        router: Router,
        method: Method,
        uri: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let mut request = Request::builder().method(method).uri(uri);
        let body = match body {
            Some(body) => {
                request = request.header("content-type", "application/json");
                Body::from(serde_json::to_vec(&body).unwrap())
            }
            None => Body::empty(),
        };
        let response = router.oneshot(request.body(body).unwrap()).await.unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    async fn post_outcome(router: Router, path: &str, bearer: &str, body: Vec<u8>) -> StatusCode {
        let request = Request::builder()
            .method(Method::POST)
            .uri(path)
            .header("authorization", bearer)
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap();
        router.oneshot(request).await.unwrap().status()
    }

    fn done(output: Value) -> Vec<u8> {
        serde_json::to_vec(&ProbeCallback {
            status: WorkerCallbackStatus::Done,
            error_message: None,
            output: Some(output),
        })
        .unwrap()
    }

    /// A metadata outcome of `entities` entities of about 3.6 KB each.
    fn metadata_outcome(entities: usize) -> Vec<u8> {
        let rows: Vec<Value> = (0..entities)
            .map(|i| {
                json!({"fqn": format!("crm.public.t{i:05}"), "entity_kind": "table",
                       "field_names": "id", "field_count": 1,
                       "doc": {"pad": "p".repeat(3600)}})
            })
            .collect();
        done(json!({"status": "succeeded",
                    "result": {"schema_hash": "h", "truncated": false, "entities": rows}}))
    }

    #[td_test::test(db)]
    async fn the_public_routes_answer_with_their_status_codes(db: TdPool) {
        let fixture = Fixture::new(db).await;
        let public = || fixture.public();

        let flat = || fixture.flat();
        let (status, body) = call(flat(), Method::GET, LIST_CONNECTIONS_PATH, None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["data"]["len"], 1);

        let catalog = at(READ_CONNECTION_CATALOG_PATH);
        let (status, body) = call(public(), Method::GET, &catalog, None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["data"]["check_status"], "unknown");
        let entities = format!("{}?fields=true", at(LIST_CONNECTION_ENTITIES_PATH));
        let (status, body) = call(public(), Method::GET, &entities, None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["data"]["len"], 0);

        let probes = at(SUBMIT_CONNECTION_PROBE_PATH);
        let submit = |body: Value| call(public(), Method::POST, &probes, Some(body));
        let (status, body) = submit(json!({"kind": "check"})).await;
        assert_eq!(status, StatusCode::ACCEPTED);
        let probe = body["data"]["probe_id"].as_str().unwrap().to_owned();
        let (status, _) = submit(json!({"kind": "sample"})).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (status, _) = submit(json!({"kind": "sample", "entity": "crm.public.t"})).await;
        assert_eq!(
            status,
            StatusCode::NOT_FOUND,
            "the entity is not in the catalog"
        );
        let read = at(READ_CONNECTION_PROBE_PATH).replace("{probe}", &probe);
        let (status, body) = call(public(), Method::GET, &format!("{read}?wait=0"), None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["data"]["status"], "queued");
        let cancel = at(CANCEL_CONNECTION_PROBE_PATH).replace("{probe}", &probe);
        let (status, body) = call(public(), Method::POST, &cancel, None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["data"]["status"], "cancelled");
        let (status, _) = call(public(), Method::POST, &cancel, None).await;
        assert_eq!(status, StatusCode::CONFLICT);

        let metadata = fixture.submit(ProbeKind::Metadata).await;
        let (path, bearer) = fixture.responder(metadata);
        assert_eq!(
            post_outcome(fixture.internal(), &path, &bearer, metadata_outcome(1)).await,
            StatusCode::OK
        );
        let (status, body) = call(public(), Method::GET, &catalog, None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["data"]["entity_count"], 1);
    }

    #[td_test::test(db)]
    async fn a_hidden_project_answers_as_a_missing_one(db: TdPool) {
        let fixture = Fixture::new(db).await;
        let viewer = seed_project_viewer(&fixture.db, fixture.project)
            .await
            .unwrap()
            .id;
        let stranger = fixture
            .db
            .seed()
            .user(&UserName::try_from("stranger").unwrap(), &UserEnabled::TRUE)
            .await
            .unwrap()
            .id;
        let as_user =
            |user| fixture.public_as(RequestContext::with(AccessTokenId::default(), user));
        let path = |template: &str, project: &str, collection: &str| {
            template
                .replace("{project}", project)
                .replace("{collection}", collection)
        };
        let check = || Some(json!({"kind": "check"}));

        for (method, template, body) in [
            (Method::GET, READ_CONNECTION_CATALOG_PATH, None),
            (Method::POST, SUBMIT_CONNECTION_PROBE_PATH, check()),
        ] {
            let missing = call(
                as_user(stranger),
                method.clone(),
                &path(template, "nope", "crm"),
                body.clone(),
            )
            .await;
            assert_eq!(missing.0, StatusCode::NOT_FOUND);
            for collection in ["crm", "ghost"] {
                let hidden = call(
                    as_user(stranger),
                    method.clone(),
                    &path(template, "test", collection),
                    body.clone(),
                )
                .await;
                assert_eq!(hidden, missing, "{method} {template} on {collection}");
            }
        }
        let missing = call(
            fixture.public(),
            Method::GET,
            &path(READ_CONNECTION_CATALOG_PATH, "nope", "crm"),
            None,
        )
        .await;
        let hidden = call(
            as_user(stranger),
            Method::GET,
            &path(READ_CONNECTION_CATALOG_PATH, "test", "crm"),
            None,
        )
        .await;
        assert_eq!(hidden, missing, "as an admin asking for a missing project");
        assert!(!hidden.1.to_string().contains(&fixture.project.to_string()));

        let (status, _) = call(
            as_user(viewer),
            Method::GET,
            &at(READ_CONNECTION_CATALOG_PATH),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let (status, _) = call(
            as_user(viewer),
            Method::POST,
            &at(SUBMIT_CONNECTION_PROBE_PATH),
            check(),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::FORBIDDEN,
            "a viewer knows the project exists"
        );
    }

    #[td_test::test(db)]
    async fn a_probe_token_posts_only_its_own_outcome(db: TdPool) {
        let fixture = Fixture::new(db).await;
        let a = fixture.submit(ProbeKind::Check).await;
        let b = fixture.submit(ProbeKind::Metadata).await;
        let (a_path, a_bearer) = fixture.responder(a);
        let (b_path, _) = fixture.responder(b);
        let outcome = done(json!({"status": "succeeded", "result": {"ok": true}}));

        assert_eq!(
            post_outcome(fixture.internal(), &b_path, &a_bearer, outcome.clone()).await,
            StatusCode::UNAUTHORIZED,
            "probe A's token cannot report for probe B"
        );
        assert_eq!(
            post_outcome(fixture.internal(), &a_path, &a_bearer, outcome).await,
            StatusCode::OK
        );
        let (_, body) = call(
            fixture.public(),
            Method::GET,
            &at(READ_CONNECTION_PROBE_PATH).replace("{probe}", &a.to_string()),
            None,
        )
        .await;
        assert_eq!(body["data"]["status"], "succeeded");
    }

    #[td_test::test(db)]
    async fn a_metadata_outcome_past_2_mib_lands(db: TdPool) {
        let fixture = Fixture::new(db).await;
        let big = fixture.submit(ProbeKind::Metadata).await;
        let (path, bearer) = fixture.responder(big);
        let body = metadata_outcome(ENTITIES);
        assert!(body.len() > 2 * 1024 * 1024 && body.len() <= MAX_OUTCOME_BYTES);
        assert_eq!(
            post_outcome(fixture.internal(), &path, &bearer, body).await,
            StatusCode::OK
        );
        let (_, catalog) = call(
            fixture.public(),
            Method::GET,
            &at(READ_CONNECTION_CATALOG_PATH),
            None,
        )
        .await;
        assert_eq!(catalog["data"]["entity_count"], ENTITIES);
    }

    #[tokio::test]
    async fn a_body_past_the_bound_is_413() {
        let body = done(json!({"status": "succeeded", "result": {"ok": true}}));
        let error = bounded_json(Body::from(body.clone()), body.len() - 1)
            .await
            .unwrap_err();
        assert_eq!(error.api_error(), ApiError::Any(413));
        assert!(
            bounded_json(Body::from(body.clone()), body.len())
                .await
                .is_ok()
        );
    }
}
