//
// Copyright 2024 Tabsdata Inc.
//

//! Create a collection. A source/destination collection is created WITH its
//! connection (1:1 — it cannot exist without one), store-first: the
//! connection content goes to the store before anything is persisted, so the
//! DB never references content the store doesn't have and no DB connection is
//! held across the store's network call:
//!
//! 1. resolve (read-only): authz, group → collection type, validate the
//!    connection payload and build the rows to insert — nothing persisted.
//! 2. store upsert under the minted connection id (source/destination only).
//! 3. create (short TX): insert the connection row + the collection pointing
//!    at it. If this fails, the store content is unreferenced — drop it
//!    best-effort and surface the error.

use crate::collection::service::connection::{
    CollectionCreatePlan, drop_connection_best_effort, escalate_for_connected,
    insert_collection_with_connection, plan_collection_create,
};
use crate::collection::service::layer::create::collection_type_from_group;
use std::sync::Arc;
use ta_services::factory::{FieldAccessor, ServiceFactory, service_factory};
use ta_services::service::TdService;
use td_authz::AuthzContext;
use td_authz::rbac::{DevelopProject, Project, Rbac, Scope};
use td_bridge::BridgeProviderHandle;
use td_database::pool::TdPool;
use td_database::pool::scoped::{InProject, Scoped};
use td_error::{ApiError, TdError, api_error};
use td_objects::dxo::collection::{CollectionCreate, CollectionRead};
use td_objects::dxo::connection::ConnectionUpsert;
use td_objects::dxo::crudl::{CreateRequest, RequestContext};
use td_objects::dxo::group::GroupDB;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{GroupId, GroupName, ProjectId};
use td_queries::By;
use td_tower::ctx_service::{CtxResponse, RawOneshot, ReqCtx};
use td_tower::default_services::{ConnectionProvider, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::layers;
use td_tower::service_provider::TdBoxService;
use tower::service_fn;
use tower::util::BoxCloneSyncService;

#[service_factory(
    name = CollectionCreateResolveService,
    request = CreateRequest<(), CollectionCreate>,
    response = CollectionCreatePlan,
    connection = ConnectionProvider,
    scope = "Project",
    context = AuthzContext,
)]
fn resolve() {
    layers!(
        from_fn(With::<CreateRequest<(), CollectionCreate>>::extract::<RequestContext>),
        from_fn(Scope::<Project>::set),
        from_fn(Rbac::<Project, DevelopProject>::check),
        from_fn(With::<CreateRequest<(), CollectionCreate>>::extract_data::<CollectionCreate>),
        // resolve the group name to its GroupId
        from_fn(With::<CollectionCreate>::extract::<GroupName>),
        from_fn(By::<GroupName>::fetch_one::<GroupDB>),
        from_fn(With::<GroupDB>::extract::<GroupId>),
        // infer the collection flavour from the resolved group
        from_fn(collection_type_from_group),
        // source/destination collections carry connection config: manage_project
        from_fn(escalate_for_connected),
        // validate the connection payload and build the rows to insert
        from_fn(plan_collection_create),
    )
}

// Internal only (never routed; the orchestrator below is the routed surface),
// so no authz layers — the resolve already gated the request.
#[service_factory(
    name = CollectionCreateTxService,
    request = CreateRequest<(), CollectionCreatePlan>,
    response = CollectionRead,
    connection = TransactionProvider,
    scope = "Project",
)]
fn create() {
    layers!(
        from_fn(With::<CreateRequest<(), CollectionCreatePlan>>::extract::<RequestContext>),
        from_fn(
            With::<CreateRequest<(), CollectionCreatePlan>>::extract_data::<CollectionCreatePlan>
        ),
        from_fn(insert_collection_with_connection),
    )
}

/// The routed create service: resolve → store write → create, each DB phase a
/// factory-built sub-service with its own short, project-scoped connection.
pub struct CreateCollectionService {
    resolve: CollectionCreateResolveService,
    create: CollectionCreateTxService,
    bridge: Arc<BridgeProviderHandle>,
}

impl CreateCollectionService {
    pub fn new(
        db: TdPool,
        authz_context: Arc<AuthzContext>,
        bridge: Arc<BridgeProviderHandle>,
    ) -> Self {
        Self {
            resolve: CollectionCreateResolveService::new(db.clone(), authz_context),
            create: CollectionCreateTxService::new(db),
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

impl<C> ServiceFactory<C> for CreateCollectionService
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

impl TdService for CreateCollectionService {
    type Request = Scoped<CreateRequest<(), CollectionCreate>, ProjectId>;
    type Response = CollectionRead;
    type Error = TdError;

    fn service(&self) -> TdBoxService<Self::Request, Self::Response, Self::Error> {
        let resolve = self.resolve.service();
        let create = self.create.service();
        let bridge = self.bridge.clone();
        BoxCloneSyncService::new(service_fn(move |request: Self::Request| {
            let resolve = resolve.clone();
            let create = create.clone();
            let bridge = bridge.clone();
            async move {
                let project = request.project;
                let context = request.inner.context.clone();
                let plan = resolve.raw_oneshot(request).await?;
                let store = bridge.connection_store();
                let connection_id = plan.connection_row.as_ref().map(|row| row.id);
                if let Some(connection_id) = &connection_id {
                    let payload = connection_payload(&plan)?;
                    store
                        .upsert(
                            &project,
                            connection_id,
                            &plan.create.name,
                            &plan.collection_id,
                            &payload,
                        )
                        .await?;
                }
                let created = create
                    .raw_oneshot(context.create((), plan).in_project(project))
                    .await;
                match created {
                    Ok(read) => Ok(CtxResponse::new(read, ReqCtx::default())),
                    Err(e) => {
                        // Nothing landed; the store content is unreferenced —
                        // drop it best-effort (a leftover is invisible and a
                        // retry overwrites it).
                        if let Some(connection_id) = &connection_id {
                            drop_connection_best_effort(store, &project, connection_id).await;
                        }
                        Err(e)
                    }
                }
            }
        }))
    }

    #[cfg(feature = "test_tower_metadata")]
    async fn metadata(&self) -> td_tower::metadata::Metadata {
        self.resolve.metadata().await
    }
}

/// The connection triple of the create payload, as the store consumes it.
///
/// The store keys the connection (and its worker artifact) by the document's
/// top-level `name`. The connection is 1:1 with its collection and named by
/// it, so stamp the collection name onto the dehydrated document the client
/// sent (which carries none) — mirroring [`rename_stored_connection`].
///
/// [`rename_stored_connection`]: crate::collection::service::connection::rename_stored_connection
fn connection_payload(plan: &CollectionCreatePlan) -> Result<ConnectionUpsert, TdError> {
    let mut connection = plan.create.connection.clone();
    if let Some(document) = connection.as_mut() {
        let object = document.as_object_mut().ok_or_else(|| {
            api_error!(
                ApiError::InternalError,
                "the connection document is not a JSON object"
            )
        })?;
        object.insert(
            "name".to_string(),
            serde_json::Value::String(plan.create.name.to_string()),
        );
    }
    Ok(ConnectionUpsert {
        connection,
        secrets: plan.create.secrets.clone(),
        vars: plan.create.vars.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::service::connection::test_support::{
        FailingStore, bridge, create_collection, create_request, document, linked_connection,
        secrets, seed_dev_user,
    };
    use crate::collection::service::list::ListCollectionsService;
    use td_bridge::connection::memory::InMemoryConnectionStore;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::InProject;
    use td_database::sea_query::kind::{DatabaseBackend, DatabaseKind};
    use td_error::ApiError;
    use td_objects::Buildable;
    use td_objects::dxo::collection::CollectionCreateDB;
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::types::basic::{
        AccessTokenId, AtTime, CollectionName, CollectionType, Description, GroupName, ProjectId,
        ProjectName, UserId, UserName,
    };
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;

    // The orchestrator's metadata is its resolve pipeline (the store/tx
    // phases are hand-rolled around it).
    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_create_collection(db: TdPool) {
        use td_database::pool::scoped::Scoped;
        use td_objects::dxo::group::GroupDB;
        use td_tower::metadata::type_of_val;

        CreateCollectionService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<Scoped<CreateRequest<(), CollectionCreate>, ProjectId>, CollectionCreatePlan>(&[
                type_of_val(
                    &With::<CreateRequest<(), CollectionCreate>>::extract::<RequestContext>,
                ),
                type_of_val(&Scope::<Project>::set),
                type_of_val(&Rbac::<Project, DevelopProject>::check),
                type_of_val(
                    &With::<CreateRequest<(), CollectionCreate>>::extract_data::<CollectionCreate>,
                ),
                type_of_val(&With::<CollectionCreate>::extract::<GroupName>),
                type_of_val(&By::<GroupName>::fetch_one::<GroupDB>),
                type_of_val(&With::<GroupDB>::extract::<GroupId>),
                type_of_val(&collection_type_from_group),
                type_of_val(&escalate_for_connected),
                type_of_val(&plan_collection_create),
            ]);
    }

    fn request(
        project: ProjectId,
        create: CollectionCreate,
    ) -> Scoped<CreateRequest<(), CollectionCreate>, ProjectId> {
        RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create((), create)
            .in_project(project)
    }

    #[td_test::test(db)]
    async fn test_create_collection(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        let name = CollectionName::try_from("ds0").unwrap();
        let description = Description::try_from("DS0").unwrap();

        let create = CollectionCreate::builder()
            .name(&name)
            .description(&description)
            .build()
            .unwrap();

        let before = AtTime::now();
        let service = CreateCollectionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request(p.id, create)).await;
        assert!(response.is_ok());
        let created = response.unwrap();

        assert_eq!(created.name, name);
        assert_eq!(created.description, description);
        assert_eq!(created.group, GroupName::default_group());
        // base_path was omitted on create: it is stored NULL, meaning the collection
        // inherits its owning project's base_path (resolved at stamp time).
        assert_eq!(created.base_path, None);
        // No group specified → default group → DataFactory flavour.
        assert_eq!(created.collection_type, CollectionType::DataFactory);
        assert!(created.created_on >= before);
        assert_eq!(created.created_by_id, UserId::admin());
        assert_eq!(created.created_by, UserName::admin());
        assert_eq!(created.modified_on, created.created_on);
        assert_eq!(created.modified_by_id, UserId::admin());
        assert_eq!(created.modified_by, UserName::admin());

        let found: Vec<CollectionCreateDB> = db
            .live()
            .fetch_all::<CollectionCreateDB>(&name)
            .await
            .unwrap();
        assert_eq!(found.len(), 1);
    }

    #[td_test::test(db)]
    async fn test_create_collection_resolves_group_by_name(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        let group = db
            .scoped(p.id)
            .seed()
            .group(
                GroupName::try_from("analytics").unwrap(),
                Description::try_from("Analytics group").unwrap(),
            )
            .await
            .unwrap();

        let name = CollectionName::try_from("ds_grp").unwrap();
        let create = CollectionCreate::builder()
            .name(&name)
            .description(Description::try_from("DS").unwrap())
            .group(GroupName::try_from("analytics").unwrap())
            .build()
            .unwrap();

        let service = CreateCollectionService::with_defaults(db.clone()).service();
        service.raw_oneshot(request(p.id, create)).await.unwrap();

        let found = db
            .live()
            .fetch_one::<td_objects::dxo::collection::ActiveCollectionDBWithNames>(&name)
            .await
            .unwrap();
        assert_eq!(found.group_id, group.id);
        assert_eq!(found.group, group.name);
    }

    #[td_test::test(db)]
    async fn test_create_collection_type_inferred_from_group(db: TdPool) {
        // The seeded project carries the system groups; a user group is not a
        // source/destination, so it falls through to DataFactory. The
        // source/destination cases go through `create_collection`, which
        // attaches the (now mandatory) connection payload.
        let p = db.seed().test_project().await.unwrap();
        db.scoped(p.id)
            .seed()
            .group(
                GroupName::try_from("analytics").unwrap(),
                Description::try_from("Analytics group").unwrap(),
            )
            .await
            .unwrap();

        create_collection(&db, p.id, "c_src", Some(GroupName::source())).await;
        create_collection(&db, p.id, "c_dst", Some(GroupName::destination())).await;
        create_collection(
            &db,
            p.id,
            "c_fac",
            Some(GroupName::try_from("analytics").unwrap()),
        )
        .await;

        for (name, expected) in [
            ("c_src", CollectionType::Source),
            ("c_dst", CollectionType::Destination),
            ("c_fac", CollectionType::DataFactory),
        ] {
            let found = db
                .live()
                .fetch_one::<td_objects::dxo::collection::ActiveCollectionDBWithNames>(
                    &CollectionName::try_from(name).unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(found.collection_type, expected, "collection {name}");
        }
    }

    #[td_test::test(db)]
    async fn test_create_duplicate_name_does_not_clobber_the_connection(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        let bridge = bridge(InMemoryConnectionStore::default());
        let authz = Arc::new(AuthzContext::default());
        let create = CreateCollectionService::new(db.clone(), authz, bridge.clone());

        create
            .service()
            .raw_oneshot(create_request(p.id, "dup", Some(GroupName::source())))
            .await
            .unwrap();
        // The store is written before the TX, so a taken name must be
        // rejected up front — otherwise this create would overwrite the
        // existing collection's worker artifact and then delete it on
        // rollback.
        let err = create
            .service()
            .raw_oneshot(create_request(p.id, "dup", Some(GroupName::source())))
            .await
            .expect_err("duplicate name must be rejected before the store is touched");
        assert!(err.to_string().contains("already exists"), "got: {err}");

        let connection = linked_connection(&db, p.id, "dup").await.unwrap();
        let stored = bridge
            .connection_store()
            .fetch(&p.id, &connection.id)
            .await
            .unwrap();
        assert_eq!(stored.connection["name"], "dup");
    }

    #[td_test::test(db)]
    async fn test_create_stamps_collection_name_onto_nameless_connection(db: TdPool) {
        // The client sends a dehydrated connection that carries NO name (it is
        // 1:1 with its collection and named by it). The create flow must stamp
        // the collection name onto the document so the store can key the worker
        // artifact — mirroring the rename flow.
        let p = db.seed().test_project().await.unwrap();
        let bridge = bridge(InMemoryConnectionStore::default());
        let authz = Arc::new(AuthzContext::default());
        let create = CreateCollectionService::new(db.clone(), authz, bridge.clone());

        let payload = CollectionCreate::builder()
            .name(CollectionName::try_from("nameless").unwrap())
            .description(Description::try_from("d").unwrap())
            .group(GroupName::source())
            .connection(Some(serde_json::json!({"type": "mysqlSrcConn"})))
            .secrets(Some(vec![]))
            .vars(Some(vec![]))
            .build()
            .unwrap();

        create
            .service()
            .raw_oneshot(request(p.id, payload))
            .await
            .unwrap();

        let connection = linked_connection(&db, p.id, "nameless").await.unwrap();
        let stored = bridge
            .connection_store()
            .fetch(&p.id, &connection.id)
            .await
            .unwrap();
        assert_eq!(stored.connection["name"], "nameless");
    }

    #[td_test::test(db)]
    async fn test_create_source_requires_connection(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        let create = CollectionCreate::builder()
            .name(CollectionName::try_from("bare_src").unwrap())
            .description(Description::try_from("d").unwrap())
            .group(GroupName::source())
            .build()
            .unwrap();
        let err = CreateCollectionService::with_defaults(db.clone())
            .service()
            .raw_oneshot(request(p.id, create))
            .await
            .expect_err("a source collection without a connection must be rejected");
        assert!(
            err.to_string().contains("created WITH its connection"),
            "got: {err}"
        );
    }

    #[td_test::test(db)]
    async fn test_create_datafactory_rejects_connection(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        let create = CollectionCreate::builder()
            .name(CollectionName::try_from("fac_conn").unwrap())
            .description(Description::try_from("d").unwrap())
            .connection(Some(document("fac_conn", "mysqlSrcConn")))
            .secrets(Some(secrets()))
            .vars(Some(vec![]))
            .build()
            .unwrap();
        let err = CreateCollectionService::with_defaults(db.clone())
            .service()
            .raw_oneshot(request(p.id, create))
            .await
            .expect_err("a DataFactory with a connection must be rejected");
        assert!(err.to_string().contains("cannot carry"), "got: {err}");
    }

    #[td_test::test(db)]
    async fn test_create_source_links_connection(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        create_collection(&db, p.id, "src_new", Some(GroupName::source())).await;
        let row = linked_connection(&db, p.id, "src_new")
            .await
            .expect("the collection must be born linked");
        assert_eq!(row.kind.to_string(), "mysqlSrcConn");
    }

    #[td_test::test(db)]
    async fn test_create_with_failing_store_persists_nothing(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        let create = CollectionCreate::builder()
            .name(CollectionName::try_from("src_fail").unwrap())
            .description(Description::try_from("d").unwrap())
            .group(GroupName::source())
            .connection(Some(document("src_fail", "mysqlSrcConn")))
            .secrets(Some(secrets()))
            .vars(Some(vec![]))
            .build()
            .unwrap();
        let bridge = Arc::new(BridgeProviderHandle::new(Arc::new(
            td_bridge::test::TestBridgeProvider::default().with_connection_store(FailingStore),
        )));
        let err =
            CreateCollectionService::new(db.clone(), Arc::new(AuthzContext::default()), bridge)
                .service()
                .raw_oneshot(request(p.id, create))
                .await
                .expect_err("store failure must surface");
        assert!(err.to_string().contains("store unreachable"), "got: {err}");
        let found: Vec<CollectionCreateDB> = db
            .live()
            .fetch_all::<CollectionCreateDB>(&CollectionName::try_from("src_fail").unwrap())
            .await
            .unwrap();
        assert!(found.is_empty(), "store-first: nothing persisted");
    }

    /// End-to-end: a request scoped to a project (as `project_resolver_layer`
    /// scopes it from the `{project}` URL segment) is isolated to that project
    /// through the real service stack — the connection switches to the app role
    /// and `SET LOCAL app.project_id`, so RLS tags writes and filters reads.
    /// PostgreSQL only (sqlite/mysql have no RLS); the body is skipped on other
    /// backends.
    #[td_test::test(db)]
    async fn test_collection_rls_scoped_by_project(db: TdPool) {
        if !matches!(db.kind(), DatabaseBackend::Postgres) {
            return;
        }

        // Two distinct projects to prove isolation, each seeded with its own
        // system groups. A collection resolves its default group within its
        // own project, so both projects must carry one.
        let project_a = db.seed().test_project().await.unwrap().id;
        let project_b = db
            .seed()
            .project(ProjectName::try_from("project_b").unwrap())
            .await
            .unwrap()
            .id;

        // Create one collection in each of two projects, scoping the request to
        // each project.
        for (project, name) in [(&project_a, "ca"), (&project_b, "cb")] {
            let create = CollectionCreate::builder()
                .name(CollectionName::try_from(name).unwrap())
                .description(Description::try_from("d").unwrap())
                .build()
                .unwrap();
            let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
                .create((), create)
                .in_project(*project);
            CreateCollectionService::with_defaults(db.clone())
                .service()
                .raw_oneshot(request)
                .await
                .unwrap();
        }

        // The flat list narrowed to each project sees only that project's
        // collection.
        for (project, name) in [("test", "ca"), ("project_b", "cb")] {
            let params = ListParams::builder()
                .filter(vec![format!("project:eq:{project}")])
                .build()
                .unwrap();
            let request =
                RequestContext::with(AccessTokenId::default(), UserId::admin()).list((), params);
            let list = ListCollectionsService::with_defaults(db.clone())
                .service()
                .raw_oneshot(request)
                .await
                .unwrap();
            assert_eq!(
                list.len, 1,
                "project {project} must see exactly its own collection"
            );
            assert_eq!(list.data[0].name, CollectionName::try_from(name).unwrap());
        }
    }
    /// The DevelopProject floor admits plain collections; a source collection
    /// carries connection config and so requires ManageProject.
    #[td_test::test(db)]
    async fn test_source_create_requires_manage_project(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        let dev = seed_dev_user(&db, p.id).await;

        let create = CollectionCreate::builder()
            .name(CollectionName::try_from("dev_df").unwrap())
            .description(Description::try_from("d").unwrap())
            .build()
            .unwrap();
        let request = RequestContext::with(AccessTokenId::default(), dev)
            .create((), create)
            .in_project(p.id);
        CreateCollectionService::with_defaults(db.clone())
            .service()
            .raw_oneshot(request)
            .await
            .expect("develop_project suffices for a DataFactory collection");

        let create = CollectionCreate::builder()
            .name(CollectionName::try_from("dev_src").unwrap())
            .description(Description::try_from("d").unwrap())
            .group(GroupName::source())
            .connection(Some(document("dev_src", "mysqlSrcConn")))
            .secrets(Some(secrets()))
            .vars(Some(vec![]))
            .build()
            .unwrap();
        let request = RequestContext::with(AccessTokenId::default(), dev)
            .create((), create)
            .in_project(p.id);
        let err = CreateCollectionService::with_defaults(db.clone())
            .service()
            .raw_oneshot(request)
            .await
            .expect_err("develop_project must not suffice for a source collection");
        assert_eq!(err.api_error(), ApiError::Forbidden);
    }
}
