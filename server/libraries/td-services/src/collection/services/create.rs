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

use crate::collection::services::connection::{
    CollectionCreatePlan, drop_connection_best_effort, escalate_for_connected,
    insert_collection_with_connection, plan_collection_create,
};
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{DevelopProject, Project};
use td_bridge::BridgeProviderHandle;
use td_database::access::{DbPool, InProject};
use td_error::{ApiError, TdError, api_error};
use td_objects::dxo::collection::{CollectionCreate, CollectionRead};
use td_objects::dxo::connection::ConnectionUpsert;
use td_objects::dxo::crudl::{CreateRequest, WithinProject};
use td_objects::dxo::group::GroupDB;
use td_objects::dxo::project::ActiveProjectDB;
use td_objects::types::basic::CollectionType;
use td_queries::Queries;

#[service_factory]
async fn create_collection(
    authz: Ctx<AuthzContext>,
    bridge: Ctx<BridgeProviderHandle>,
    db: DbPool<InProject>,
    request: WithinProject<CreateRequest<CollectionCreate>>,
) -> Result<CollectionRead, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, _, create) = request.into_parts();

    // 1. resolve (read-only): authz, group → collection type, plan the insert.
    let (plan, grant) = {
        let mut conn = db.read().await?;
        let grant = authz
            .require::<Project, DevelopProject>(&mut conn, &ctx, &project)
            .await?;
        let conn = conn.get(grant);
        let group: GroupDB = conn.fetch_one(&create.group).await?;
        let collection_type = CollectionType::from_group_kind(&group.base.kind);
        // source/destination collections carry connection config: manage_project
        escalate_for_connected(&authz, conn, &ctx, &project, &collection_type).await?;
        let plan =
            plan_collection_create(conn, &ctx, &create, &group.base.id, &collection_type).await?;
        (plan, grant)
    };

    // 2. store upsert under the minted connection id (source/destination only).
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

    // 3. create (short TX): insert the connection row + the collection.
    let created = async {
        let mut trx = db.begin().await?;
        let conn = trx.get(grant);
        // FOR UPDATE + liveness check against project delete's emptiness guard.
        let _ = conn
            .for_update()
            .fetch_one::<ActiveProjectDB>(&project)
            .await?;
        let read = insert_collection_with_connection(conn, &ctx, &plan).await?;
        trx.commit().await?;
        Ok::<_, TdError>(read)
    }
    .await;
    match created {
        Ok(read) => Ok(read),
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

/// The connection triple of the create payload, as the store consumes it.
///
/// The store keys the connection (and its worker artifact) by the document's
/// top-level `name`. The connection is 1:1 with its collection and named by
/// it, so stamp the collection name onto the dehydrated document the client
/// sent (which carries none) — mirroring [`rename_stored_connection`].
///
/// [`rename_stored_connection`]: crate::collection::services::connection::rename_stored_connection
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
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use crate::collection::services::connection::test_support::{
        FailingStore, bridge, create_collection, create_request, document, linked_connection,
        secrets, seed_dev_user,
    };
    use std::sync::Arc;
    use td_bridge::connection::memory::InMemoryConnectionStore;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt as _;
    use td_error::ApiError;
    use td_objects::dxo::collection::CollectionDB;
    use td_objects::dxo::crudl::{ListParams, RequestContext, WithinProject};
    use td_objects::dxo::project::ProjectParam;
    use td_objects::types::basic::{
        AccessTokenId, AtTime, CollectionName, CollectionType, Description, GroupName, ProjectId,
        ProjectIdName, ProjectName, UserId, UserName,
    };
    use td_queries::Queries;
    use td_queries::test_utils::SeedEntry;

    // The orchestrator's metadata is its resolve pipeline (the store/tx
    // phases are hand-rolled around it).

    fn request(
        project: ProjectId,
        create: CollectionCreate,
    ) -> WithinProject<CreateRequest<CollectionCreate>> {
        RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create((), create)
            .in_project(project)
    }

    #[td_test::test(db)]
    async fn test_create_collection(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        let name = CollectionName::try_from("ds0").unwrap();
        let description = Description::try_from("DS0").unwrap();

        let create = CollectionCreate {
            name: name.clone(),
            description: description.clone(),
            group: GroupName::default_group(),
            base_path: None,
            connection: None,
            secrets: None,
        };

        let before = AtTime::now();
        let service = Services::with_defaults(db.clone());
        let response = service.create_collection(request(p.id, create)).await;
        assert!(response.is_ok());
        let created = response.unwrap().data;

        assert_eq!(created.name, name);
        assert_eq!(created.description, description);
        assert_eq!(created.group, GroupName::default_group());
        // base_path was omitted on create: it is stored NULL, meaning the collection
        // inherits its owning project's base_path (resolved at stamp time).
        assert_eq!(created.base_path, None);
        // No group specified → default group → DataFactory flavour.
        assert_eq!(created.collection_type, CollectionType::DataFactory);
        assert!(*created.created_on >= before);
        assert_eq!(*created.created_by_id, UserId::admin());
        assert_eq!(*created.created_by, UserName::admin());
        assert_eq!(*created.modified_on, *created.created_on);
        assert_eq!(*created.modified_by_id, UserId::admin());
        assert_eq!(*created.modified_by, UserName::admin());

        let found: Vec<CollectionDB> = db.fetch_all::<CollectionDB>(&name).await.unwrap();
        assert_eq!(found.len(), 1);
    }

    /// The create trx re-checks project liveness under its row lock, so a
    /// create racing project delete cannot strand a collection.
    #[td_test::test(db)]
    async fn test_create_into_deleted_project_refused(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        Services::with_defaults(db.clone())
            .delete_project(
                RequestContext::with(AccessTokenId::default(), UserId::admin()).delete(
                    ProjectParam {
                        project: ProjectIdName::Name(ProjectName::try_from("test")?),
                    },
                ),
            )
            .await?;

        let create = CollectionCreate {
            name: CollectionName::try_from("ds0")?,
            description: Description::try_from("DS0")?,
            group: GroupName::default_group(),
            base_path: None,
            connection: None,
            secrets: None,
        };
        let response = Services::with_defaults(db.clone())
            .create_collection(request(p.id, create))
            .await;
        assert!(
            response.is_err(),
            "create into a deleted project must fail, got {response:?}"
        );
        Ok(())
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
        let create = CollectionCreate {
            name: name.clone(),
            description: Description::try_from("DS").unwrap(),
            group: GroupName::try_from("analytics").unwrap(),
            base_path: None,
            connection: None,
            secrets: None,
        };

        let service = Services::with_defaults(db.clone());
        service
            .create_collection(request(p.id, create))
            .await
            .unwrap();

        let found = db
            .fetch_one::<td_objects::dxo::collection::ActiveCollectionDBWithNames>(&name)
            .await
            .unwrap();
        assert_eq!(found.base.group_id, group.base.id);
        assert_eq!(found.group, group.base.name);
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
                .fetch_one::<td_objects::dxo::collection::ActiveCollectionDBWithNames>(
                    &CollectionName::try_from(name).unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(found.base.collection_type, expected, "collection {name}");
        }
    }

    #[td_test::test(db)]
    async fn test_create_duplicate_name_does_not_clobber_the_connection(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        let bridge = bridge(InMemoryConnectionStore::default());
        let create = Services {
            bridge: bridge.clone(),
            ..Services::with_defaults(db.clone())
        };

        create
            .create_collection(create_request(p.id, "dup", Some(GroupName::source())))
            .await
            .unwrap();
        // The store is written before the TX, so a taken name must be
        // rejected up front — otherwise this create would overwrite the
        // existing collection's worker artifact and then delete it on
        // rollback.
        let err = create
            .create_collection(create_request(p.id, "dup", Some(GroupName::source())))
            .await
            .expect_err("duplicate name must be rejected before the store is touched");
        assert!(err.to_string().contains("already exists"), "got: {err}");

        let connection = linked_connection(&db, p.id, "dup").await.unwrap();
        let stored = bridge
            .connection_store()
            .fetch(&p.id, &connection.base.id)
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
        let create = Services {
            bridge: bridge.clone(),
            ..Services::with_defaults(db.clone())
        };

        let payload = CollectionCreate {
            name: CollectionName::try_from("nameless").unwrap(),
            description: Description::try_from("d").unwrap(),
            group: GroupName::source(),
            base_path: None,
            connection: Some(serde_json::json!({"type": "mysqlSrcConn"})),
            secrets: Some(vec![]),
        };

        create
            .create_collection(request(p.id, payload))
            .await
            .unwrap();

        let connection = linked_connection(&db, p.id, "nameless").await.unwrap();
        let stored = bridge
            .connection_store()
            .fetch(&p.id, &connection.base.id)
            .await
            .unwrap();
        assert_eq!(stored.connection["name"], "nameless");
    }

    #[td_test::test(db)]
    async fn test_create_source_requires_connection(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        let create = CollectionCreate {
            name: CollectionName::try_from("bare_src").unwrap(),
            description: Description::try_from("d").unwrap(),
            group: GroupName::source(),
            base_path: None,
            connection: None,
            secrets: None,
        };
        let err = Services::with_defaults(db.clone())
            .create_collection(request(p.id, create))
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
        let create = CollectionCreate {
            name: CollectionName::try_from("fac_conn").unwrap(),
            description: Description::try_from("d").unwrap(),
            group: GroupName::default_group(),
            base_path: None,
            connection: Some(document("fac_conn", "mysqlSrcConn")),
            secrets: Some(secrets()),
        };
        let err = Services::with_defaults(db.clone())
            .create_collection(request(p.id, create))
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
        assert_eq!(row.base.kind.to_string(), "mysqlSrcConn");
    }

    #[td_test::test(db)]
    async fn test_create_with_failing_store_persists_nothing(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        let create = CollectionCreate {
            name: CollectionName::try_from("src_fail").unwrap(),
            description: Description::try_from("d").unwrap(),
            group: GroupName::source(),
            base_path: None,
            connection: Some(document("src_fail", "mysqlSrcConn")),
            secrets: Some(secrets()),
        };
        let bridge = Ctx::new(BridgeProviderHandle::new(Arc::new(
            td_bridge::test::TestBridgeProvider::default().with_connection_store(FailingStore),
        )));
        let err = Services {
            bridge,
            ..Services::with_defaults(db.clone())
        }
        .create_collection(request(p.id, create))
        .await
        .expect_err("store failure must surface");
        assert!(err.to_string().contains("store unreachable"), "got: {err}");
        let found: Vec<CollectionDB> = db
            .fetch_all::<CollectionDB>(&CollectionName::try_from("src_fail").unwrap())
            .await
            .unwrap();
        assert!(found.is_empty(), "store-first: nothing persisted");
    }

    /// End-to-end: a request scoped to a project (as `project_resolver_layer`
    /// scopes it from the `{project}` URL segment) is isolated to that project
    /// through the real service stack — the connection switches to the app role
    /// and `SET LOCAL app.project_id`, so RLS tags writes and filters reads.
    #[td_test::test(db)]
    async fn test_collection_rls_scoped_by_project(db: TdPool) {
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

        for (project, name) in [(&project_a, "ca"), (&project_b, "cb")] {
            let create = CollectionCreate {
                name: CollectionName::try_from(name).unwrap(),
                description: Description::try_from("d").unwrap(),
                group: GroupName::default_group(),
                base_path: None,
                connection: None,
                secrets: None,
            };
            let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
                .create((), create)
                .in_project(*project);
            Services::with_defaults(db.clone())
                .create_collection(request)
                .await
                .unwrap();
        }

        // The flat list narrowed to each project sees only that project's
        // collection.
        for (project, name) in [("test", "ca"), ("project_b", "cb")] {
            let params = ListParams {
                filter: vec![format!("project:eq:{project}")],
                ..ListParams::default()
            };
            let request =
                RequestContext::with(AccessTokenId::default(), UserId::admin()).list((), params);
            let list = Services::with_defaults(db.clone())
                .list_collections(request)
                .await
                .unwrap()
                .data;
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

        let create = CollectionCreate {
            name: CollectionName::try_from("dev_df").unwrap(),
            description: Description::try_from("d").unwrap(),
            group: GroupName::default_group(),
            base_path: None,
            connection: None,
            secrets: None,
        };
        let request = RequestContext::with(AccessTokenId::default(), dev)
            .create((), create)
            .in_project(p.id);
        Services::with_defaults(db.clone())
            .create_collection(request)
            .await
            .expect("develop_project suffices for a DataFactory collection");

        let create = CollectionCreate {
            name: CollectionName::try_from("dev_src").unwrap(),
            description: Description::try_from("d").unwrap(),
            group: GroupName::source(),
            base_path: None,
            connection: Some(document("dev_src", "mysqlSrcConn")),
            secrets: Some(secrets()),
        };
        let request = RequestContext::with(AccessTokenId::default(), dev)
            .create((), create)
            .in_project(p.id);
        let err = Services::with_defaults(db.clone())
            .create_collection(request)
            .await
            .expect_err("develop_project must not suffice for a source collection");
        assert_eq!(err.api_error(), ApiError::Forbidden);
    }
}
