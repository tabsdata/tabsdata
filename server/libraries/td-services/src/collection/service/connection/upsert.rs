//
// Copyright 2026 Tabsdata Inc.
//

//! Update a source/destination collection's connection. ManageProject-gated
//! ("CRUD sources and destinations" in the permission model). The connection
//! exists by construction — it was created WITH the collection (1:1) — so
//! this is a pure store update: validate
//! against the existing row (kind immutable, name = the collection's), then
//! write the store. No DB write happens at all, and no DB connection is held
//! across the store's network call (the resolve phase has its own).

use super::layer::{
    ConnectionResolution, ensure_connectable, resolve_connection, validate_document,
    validate_upsert,
};
use std::sync::Arc;
use ta_services::factory::{FieldAccessor, ServiceFactory, service_factory};
use ta_services::service::TdService;
use td_authz::AuthzContext;
use td_authz::rbac::{ManageProject, Project, Rbac, Scope};
use td_bridge::BridgeProviderHandle;
use td_bridge::connection::ConnectionStore;
use td_database::pool::TdPool;
use td_database::pool::scoped::Scoped;
use td_error::{ApiError, TdError, api_error};
use td_objects::dxo::collection::ActiveCollectionDB;
use td_objects::dxo::connection::{ConnectionRead, ConnectionUpsert};
use td_objects::dxo::crudl::{IntoData, RequestContext, UpdateRequest};
use td_objects::rest_urls::CollectionParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{CollectionIdName, ProjectId};
use td_queries::By;
use td_tower::ctx_service::{CtxResponse, RawOneshot, ReqCtx};
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;
use td_tower::service_provider::TdBoxService;
use tower::service_fn;
use tower::util::BoxCloneSyncService;

#[service_factory(
    name = ConnectionResolveService,
    request = UpdateRequest<CollectionParam, ConnectionUpsert>,
    response = ConnectionResolution,
    connection = ConnectionProvider,
    scope = "Project",
    context = AuthzContext,
)]
fn resolve() {
    layers!(
        from_fn(
            With::<UpdateRequest<CollectionParam, ConnectionUpsert>>::extract::<RequestContext>
        ),
        from_fn(Scope::<Project>::set),
        from_fn(Rbac::<Project, ManageProject>::check),
        from_fn(
            With::<UpdateRequest<CollectionParam, ConnectionUpsert>>::extract_name::<CollectionParam>
        ),
        from_fn(With::<CollectionParam>::extract::<CollectionIdName>),
        from_fn(By::<CollectionIdName>::fetch_one::<ActiveCollectionDB>),
        from_fn(ensure_connectable),
        from_fn(
            With::<UpdateRequest<CollectionParam, ConnectionUpsert>>::extract_data::<
                ConnectionUpsert,
            >
        ),
        from_fn(validate_upsert),
        from_fn(validate_document),
        from_fn(resolve_connection),
    )
}

/// The routed update service: resolve (own short connection), then the store
/// write with no DB connection held.
pub struct ConnectionUpsertService {
    resolve: ConnectionResolveService,
    bridge: Arc<BridgeProviderHandle>,
}

impl ConnectionUpsertService {
    pub fn new(
        db: TdPool,
        authz_context: Arc<AuthzContext>,
        bridge: Arc<BridgeProviderHandle>,
    ) -> Self {
        Self {
            resolve: ConnectionResolveService::new(db, authz_context),
            bridge,
        }
    }

    #[cfg(feature = "test-utils")]
    pub fn with_defaults(db: TdPool) -> Self {
        Self::new(
            db,
            Arc::new(AuthzContext::default()),
            Arc::new(BridgeProviderHandle::default()),
        )
    }
}

impl<C> ServiceFactory<C> for ConnectionUpsertService
where
    TdPool: FieldAccessor<C>,
    Arc<AuthzContext>: FieldAccessor<C>,
    Arc<BridgeProviderHandle>: FieldAccessor<C>,
{
    type Service = Self;

    fn build(ctx: &C) -> Self {
        Self::new(
            TdPool::get_field(ctx),
            Arc::<AuthzContext>::get_field(ctx),
            Arc::<BridgeProviderHandle>::get_field(ctx),
        )
    }
}

impl TdService for ConnectionUpsertService {
    type Request = Scoped<UpdateRequest<CollectionParam, ConnectionUpsert>, ProjectId>;
    type Response = ConnectionRead;
    type Error = TdError;

    fn service(&self) -> TdBoxService<Self::Request, Self::Response, Self::Error> {
        let resolve = self.resolve.service();
        let bridge = self.bridge.clone();
        BoxCloneSyncService::new(service_fn(move |request: Self::Request| {
            let resolve = resolve.clone();
            let bridge = bridge.clone();
            async move {
                let project = request.project;
                let payload = request.inner.data.clone().into_data();
                let resolution = resolve.raw_oneshot(request).await?;
                let store = bridge.connection_store();
                verify_kept_keys(store, &project, &resolution).await?;
                let mut read = store
                    .upsert(
                        &project,
                        &resolution.row.id,
                        &resolution.collection,
                        &resolution.collection_id,
                        &payload,
                    )
                    .await?;
                read.id = Some(resolution.row.id);
                Ok(CtxResponse::new(read, ReqCtx::default()))
            }
        }))
    }

    #[cfg(feature = "test_tower_metadata")]
    async fn metadata(&self) -> td_tower::metadata::Metadata {
        self.resolve.metadata().await
    }
}

/// Keys carried with `value: None` mean "keep the stored value" — only usable
/// if the store actually holds them, otherwise hydration would come up empty.
async fn verify_kept_keys(
    store: &dyn ConnectionStore,
    project: &ProjectId,
    resolution: &ConnectionResolution,
) -> Result<(), TdError> {
    let document = &resolution.document;
    if document.kept_secrets.is_empty() && document.kept_vars.is_empty() {
        return Ok(());
    }
    let stored = store.fetch(project, &resolution.row.id).await?;
    let missing: Vec<&String> = document
        .kept_secrets
        .iter()
        .filter(|k| !stored.secret_names.contains(k))
        .chain(
            document
                .kept_vars
                .iter()
                .filter(|k| !stored.var_names.contains(k)),
        )
        .collect();
    if !missing.is_empty() {
        return Err(api_error!(
            ApiError::InputError,
            "secrets/vars marked 'keep the stored value' are not in the store: {}",
            missing
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::service::connection::ConnectionReadService;
    use crate::collection::service::connection::test_support::{
        KeepStore, MockStore, collection_param, create_collection, document, linked_connection,
        read_request, secrets, seed_dev_user, upsert_request, upsert_request_body,
        upsert_request_kind, upsert_service,
    };
    use std::sync::{Arc, Mutex};
    use td_bridge::connection::noop::NoopConnectionStore;
    use td_bridge::test::TestBridgeProvider;
    use td_database::pool::scoped::InProject;
    use td_objects::Buildable;
    use td_objects::dxo::connection::KeyValue;
    use td_objects::types::basic::{AccessTokenId, GroupName, UserEnabled, UserName};
    use td_queries::test_utils::SeedEntry;

    // The orchestrator's metadata is its resolve pipeline (the store phase
    // is hand-rolled around it).
    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_connection_upsert(db: TdPool) {
        use td_tower::metadata::type_of_val;

        ConnectionUpsertService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<Scoped<UpdateRequest<CollectionParam, ConnectionUpsert>, ProjectId>, ConnectionResolution>(&[
                type_of_val(
                    &With::<UpdateRequest<CollectionParam, ConnectionUpsert>>::extract::<
                        RequestContext,
                    >,
                ),
                type_of_val(&Scope::<Project>::set),
                type_of_val(&Rbac::<Project, ManageProject>::check),
                type_of_val(
                    &With::<UpdateRequest<CollectionParam, ConnectionUpsert>>::extract_name::<
                        CollectionParam,
                    >,
                ),
                type_of_val(&With::<CollectionParam>::extract::<CollectionIdName>),
                type_of_val(&By::<CollectionIdName>::fetch_one::<ActiveCollectionDB>),
                type_of_val(&ensure_connectable),
                type_of_val(
                    &With::<UpdateRequest<CollectionParam, ConnectionUpsert>>::extract_data::<
                        ConnectionUpsert,
                    >,
                ),
                type_of_val(&validate_upsert),
                type_of_val(&validate_document),
                type_of_val(&resolve_connection),
            ]);
    }

    #[td_test::test(db)]
    async fn test_upsert_forwards_to_store(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        create_collection(&db, p.id, "src", Some(GroupName::source())).await;
        let upserts = Arc::new(Mutex::new(Vec::new()));

        let response = upsert_service(
            &db,
            MockStore {
                upserts: upserts.clone(),
            },
        )
        .service()
        .raw_oneshot(upsert_request(p.id, "src"))
        .await
        .unwrap();

        let row = linked_connection(&db, p.id, "src").await.unwrap();
        let recorded = upserts.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].0, p.id);
        assert_eq!(
            recorded[0].1, row.id,
            "store keyed by the row minted at create"
        );
        assert_eq!(
            response.secret_names,
            vec!["CREDENTIALS_PASSWORD".to_string()]
        );
        assert_eq!(response.id, Some(row.id));
    }

    #[td_test::test(db)]
    async fn test_upsert_rejects_datafactory(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        create_collection(&db, p.id, "fac", None).await; // default group -> DataFactory
        let result = upsert_service(&db, MockStore::default())
            .service()
            .raw_oneshot(upsert_request(p.id, "fac"))
            .await;
        assert!(result.is_err(), "DataFactory must be rejected");
    }

    #[td_test::test(db)]
    async fn test_upsert_rejects_missing_connection(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        create_collection(&db, p.id, "src3", Some(GroupName::source())).await;
        // Secrets without the dehydrated connection — rejected (no subset
        // updates until fetch-and-align lands).
        let body = ConnectionUpsert::builder()
            .connection(None)
            .secrets(Some(secrets()))
            .vars(None)
            .build()
            .unwrap();
        let result = upsert_service(&db, MockStore::default())
            .service()
            .raw_oneshot(upsert_request_body(p.id, "src3", body))
            .await;
        assert!(result.is_err(), "connection-less upsert must be rejected");
    }

    #[td_test::test(db)]
    async fn test_upsert_requires_project_role(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        create_collection(&db, p.id, "src2", Some(GroupName::source())).await;
        // A user with no role in the project fails the DevelopProject check.
        let outsider = db
            .seed()
            .user(
                &UserName::try_from("outsider").unwrap(),
                &UserEnabled::from(true),
            )
            .await
            .unwrap();
        let body = ConnectionUpsert::builder()
            .connection(Some(document("src2", "mysqlSrcConn")))
            .secrets(Some(secrets()))
            .vars(Some(vec![]))
            .build()
            .unwrap();
        let request = RequestContext::with(AccessTokenId::default(), outsider.id)
            .update(collection_param("src2"), body)
            .in_project(p.id);
        let result = upsert_service(&db, MockStore::default())
            .service()
            .raw_oneshot(request)
            .await;
        assert!(
            result.is_err(),
            "a user without a project role must be rejected"
        );
    }

    #[td_test::test(db)]
    async fn test_upsert_requires_manage_project(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        create_collection(&db, p.id, "srcm", Some(GroupName::source())).await;
        let dev = seed_dev_user(&db, p.id).await;
        let body = ConnectionUpsert::builder()
            .connection(Some(document("srcm", "mysqlSrcConn")))
            .secrets(Some(secrets()))
            .vars(Some(vec![]))
            .build()
            .unwrap();
        let request = RequestContext::with(AccessTokenId::default(), dev)
            .update(collection_param("srcm"), body)
            .in_project(p.id);
        let err = upsert_service(&db, MockStore::default())
            .service()
            .raw_oneshot(request)
            .await
            .expect_err("develop_project must not suffice for a connection upsert");
        assert_eq!(err.api_error(), ApiError::Forbidden);
    }

    #[td_test::test(db)]
    async fn test_read_requires_manage_project(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        create_collection(&db, p.id, "srcr", Some(GroupName::source())).await;
        let dev = seed_dev_user(&db, p.id).await;
        let request = RequestContext::with(AccessTokenId::default(), dev)
            .read(collection_param("srcr"))
            .in_project(p.id);
        let err = ConnectionReadService::with_defaults(db.clone())
            .service()
            .raw_oneshot(request)
            .await
            .expect_err("develop_project must not suffice for a connection read");
        assert_eq!(err.api_error(), ApiError::Forbidden);
    }

    #[td_test::test(db)]
    async fn test_upsert_rejects_kind_change(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        create_collection(&db, p.id, "srck", Some(GroupName::source())).await; // mysqlSrcConn
        let result = upsert_service(&db, MockStore::default())
            .service()
            .raw_oneshot(upsert_request_kind(p.id, "srck", "postgresSrcConn"))
            .await;
        let err = result.expect_err("connector kind must be immutable");
        assert!(err.to_string().contains("cannot change"), "got: {err}");
    }

    #[td_test::test(db)]
    async fn test_upsert_rejects_duplicate_keys(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        create_collection(&db, p.id, "srcd", Some(GroupName::source())).await;
        // Same key twice (here once in secrets, once in vars) — which value
        // applies would be backend-dependent.
        let body = ConnectionUpsert::builder()
            .connection(Some(document("srcd", "mysqlSrcConn")))
            .secrets(Some(secrets()))
            .vars(Some(vec![KeyValue {
                key: "CREDENTIALS_PASSWORD".to_string(),
                value: Some("other".to_string()),
            }]))
            .build()
            .unwrap();
        let err = upsert_service(&db, MockStore::default())
            .service()
            .raw_oneshot(upsert_request_body(p.id, "srcd", body))
            .await
            .expect_err("duplicate keys must be rejected");
        assert!(err.to_string().contains("duplicate"), "got: {err}");
    }

    #[td_test::test(db)]
    async fn test_upsert_kept_value_checked_against_store(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        create_collection(&db, p.id, "srcv", Some(GroupName::source())).await;
        let service = upsert_service(
            &db,
            KeepStore {
                stored_secret_names: vec!["CREDENTIALS_PASSWORD".to_string()],
            },
        );

        // Keeping a stored secret is fine.
        let keep_stored = ConnectionUpsert::builder()
            .connection(Some(document("srcv", "mysqlSrcConn")))
            .secrets(Some(vec![KeyValue {
                key: "CREDENTIALS_PASSWORD".to_string(),
                value: None,
            }]))
            .vars(Some(vec![]))
            .build()
            .unwrap();
        service
            .service()
            .raw_oneshot(upsert_request_body(p.id, "srcv", keep_stored))
            .await
            .expect("keeping a stored secret must be accepted");

        // Keeping a never-stored secret would hydrate to nothing.
        let keep_missing = ConnectionUpsert::builder()
            .connection(Some(document("srcv", "mysqlSrcConn")))
            .secrets(Some(vec![
                KeyValue {
                    key: "CREDENTIALS_PASSWORD".to_string(),
                    value: Some("hunter2".to_string()),
                },
                KeyValue {
                    key: "NEVER_STORED".to_string(),
                    value: None,
                },
            ]))
            .vars(Some(vec![]))
            .build()
            .unwrap();
        let err = service
            .service()
            .raw_oneshot(upsert_request_body(p.id, "srcv", keep_missing))
            .await
            .expect_err("keeping a never-stored secret must be rejected");
        assert!(err.to_string().contains("NEVER_STORED"), "got: {err}");
    }

    #[td_test::test(db)]
    async fn test_upsert_with_unconfigured_store_fails(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        create_collection(&db, p.id, "srcq", Some(GroupName::source())).await;
        let err = upsert_service(&db, NoopConnectionStore)
            .service()
            .raw_oneshot(upsert_request(p.id, "srcq"))
            .await
            .expect_err("unconfigured store must error");
        assert!(err.to_string().contains("not configured"));
    }

    /// The "supervisor mode works" pin: collection creation and the
    /// upsert/read pipelines against the real repository-file store, worker
    /// artifact included.
    #[td_test::test(db)]
    async fn test_supervisor_file_store_end_to_end(db: TdPool) {
        use crate::collection::service::connection::test_support::create_request;
        use crate::collection::service::create::CreateCollectionService;
        use td_bridge::connection::file::FileConnectionStore;

        let p = db.seed().test_project().await.unwrap();
        let dir = td_common::tdtestdir!();
        let bridge = || {
            Arc::new(BridgeProviderHandle::new(Arc::new(
                TestBridgeProvider::default()
                    .with_connection_store(FileConnectionStore::with_location(dir.clone())),
            )))
        };

        CreateCollectionService::new(db.clone(), Arc::new(AuthzContext::default()), bridge())
            .service()
            .raw_oneshot(create_request(p.id, "srcw", Some(GroupName::source())))
            .await
            .expect("supervisor-mode create must work");

        // The worker artifact is in place for `NamedConn("srcw")` resolution,
        // under the run's project.
        assert!(
            dir.join("connections")
                .join(p.id.to_string())
                .join("srcw.json")
                .is_file()
        );

        // The update pipeline overwrites it...
        let response =
            ConnectionUpsertService::new(db.clone(), Arc::new(AuthzContext::default()), bridge())
                .service()
                .raw_oneshot(upsert_request(p.id, "srcw"))
                .await
                .expect("supervisor-mode upsert must work");
        assert_eq!(response.secret_names, vec!["CREDENTIALS_PASSWORD"]);

        // ...and the read pipeline serves it back, names only.
        let read =
            ConnectionReadService::new(db.clone(), Arc::new(AuthzContext::default()), bridge())
                .service()
                .raw_oneshot(read_request(p.id, "srcw"))
                .await
                .expect("supervisor-mode read must work");
        assert_eq!(read.secret_names, vec!["CREDENTIALS_PASSWORD"]);
        assert!(!serde_json::to_string(&read).unwrap().contains("hunter2"));
    }
}
