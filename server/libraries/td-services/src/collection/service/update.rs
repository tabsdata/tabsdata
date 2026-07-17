//
// Copyright 2024 Tabs Data Inc.
//

//! Update a collection. Renaming a source/destination collection renames its
//! connection too (1:1 — the collection's name IS the connection's name), so
//! the orchestration mirrors create, store-first:
//!
//! 1. resolve (read-only): authz, validate, build the update row and detect
//!    the rename — nothing persisted.
//! 2. store rename (rename only): rewrite the stored document's `name`, which
//!    also moves the worker artifact. No DB connection held across it.
//! 3. update (short TX): apply the row update. If this fails, the store is
//!    put back under the old name best-effort and the error surfaces.
//!
//! `collection_type` is immutable: it is inferred from the group at creation
//! and never accepted on update. `CollectionUpdate` carries no group/type
//! field and `CollectionUpdateDB` writes only name/description/modified_*, so
//! the type is preserved for free. Any future "move collection to group"
//! feature must reject the change when the collection_type is
//! Source/Destination.

use crate::collection::service::connection::{escalate_for_connected, rename_stored_connection};
use crate::collection::service::layer::update::{
    CollectionUpdatePlan, UpdateCollectionDBBuilderUpdate, apply_collection_update,
    plan_collection_update, update_collection_validate,
};
use std::sync::Arc;
use ta_services::factory::{FieldAccessor, ServiceFactory, service_factory};
use ta_services::service::TdService;
use td_authz::AuthzContext;
use td_authz::rbac::{DevelopProject, Project, Rbac, Scope};
use td_bridge::BridgeProviderHandle;
use td_database::pool::TdPool;
use td_database::pool::scoped::{InProject, Scoped};
use td_error::TdError;
use td_objects::dxo::collection::{
    CollectionDB, CollectionRead, CollectionUpdate, CollectionUpdateDB, CollectionUpdateDBBuilder,
};
use td_objects::dxo::crudl::{RequestContext, UpdateRequest};
use td_objects::rest_urls::CollectionParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{CollectionIdName, CollectionType, ProjectId};
use td_queries::By;
use td_tower::ctx_service::{CtxResponse, RawOneshot, ReqCtx};
use td_tower::default_services::{ConnectionProvider, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::layers;
use td_tower::service_provider::TdBoxService;
use tower::service_fn;
use tower::util::BoxCloneSyncService;
use tracing::warn;

#[service_factory(
    name = CollectionUpdateResolveService,
    request = UpdateRequest<CollectionParam, CollectionUpdate>,
    response = CollectionUpdatePlan,
    connection = ConnectionProvider,
    scope = "Project",
    context = AuthzContext,
)]
fn resolve() {
    layers!(
        from_fn(
            With::<UpdateRequest<CollectionParam, CollectionUpdate>>::extract::<RequestContext>
        ),
        from_fn(Scope::<Project>::set),
        from_fn(Rbac::<Project, DevelopProject>::check),
        from_fn(
            With::<UpdateRequest<CollectionParam, CollectionUpdate>>::extract_name::<CollectionParam>
        ),
        from_fn(With::<CollectionParam>::extract::<CollectionIdName>),
        from_fn(By::<CollectionIdName>::fetch_one::<CollectionDB>),
        // source/destination collections carry connection config: manage_project
        from_fn(With::<CollectionDB>::extract::<CollectionType>),
        from_fn(escalate_for_connected),
        from_fn(
            With::<UpdateRequest<CollectionParam, CollectionUpdate>>::extract_data::<
                CollectionUpdate,
            >
        ),
        from_fn(update_collection_validate),
        from_fn(With::<CollectionDB>::try_convert_to::<CollectionUpdateDBBuilder, _>),
        from_fn(With::<RequestContext>::update::<CollectionUpdateDBBuilder, _>),
        from_fn(With::<CollectionUpdate>::update_collection_update_db_builder),
        from_fn(With::<CollectionUpdateDBBuilder>::build::<CollectionUpdateDB>),
        from_fn(plan_collection_update),
    )
}

// Internal only (never routed; the orchestrator below is the routed surface),
// so no authz layers — the resolve already gated the request.
#[service_factory(
    name = CollectionUpdateTxService,
    request = UpdateRequest<(), CollectionUpdatePlan>,
    response = CollectionRead,
    connection = TransactionProvider,
    scope = "Project",
)]
fn update() {
    layers!(
        from_fn(
            With::<UpdateRequest<(), CollectionUpdatePlan>>::extract_data::<CollectionUpdatePlan>
        ),
        from_fn(apply_collection_update),
    )
}

/// The routed update service: resolve → store rename (when renaming a
/// connected collection) → update, each DB phase a factory-built sub-service
/// with its own short, project-scoped connection.
pub struct UpdateCollectionService {
    resolve: CollectionUpdateResolveService,
    update: CollectionUpdateTxService,
    bridge: Arc<BridgeProviderHandle>,
}

impl UpdateCollectionService {
    pub fn new(
        db: TdPool,
        authz_context: Arc<AuthzContext>,
        bridge: Arc<BridgeProviderHandle>,
    ) -> Self {
        Self {
            resolve: CollectionUpdateResolveService::new(db.clone(), authz_context),
            update: CollectionUpdateTxService::new(db),
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

impl<C> ServiceFactory<C> for UpdateCollectionService
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

impl TdService for UpdateCollectionService {
    type Request = Scoped<UpdateRequest<CollectionParam, CollectionUpdate>, ProjectId>;
    type Response = CollectionRead;
    type Error = TdError;

    fn service(&self) -> TdBoxService<Self::Request, Self::Response, Self::Error> {
        let resolve = self.resolve.service();
        let update = self.update.service();
        let bridge = self.bridge.clone();
        BoxCloneSyncService::new(service_fn(move |request: Self::Request| {
            let resolve = resolve.clone();
            let update = update.clone();
            let bridge = bridge.clone();
            async move {
                let project = request.project;
                let context = request.inner.context.clone();
                let plan = resolve.raw_oneshot(request).await?;
                let store = bridge.connection_store();
                let rename = plan.rename.clone();
                if let Some(rename) = &rename {
                    rename_stored_connection(
                        store,
                        &project,
                        &rename.connection_id,
                        &rename.collection_id,
                        &rename.new_name,
                    )
                    .await?;
                }
                let updated = update
                    .raw_oneshot(context.update((), plan).in_project(project))
                    .await;
                match updated {
                    Ok(read) => Ok(CtxResponse::new(read, ReqCtx::default())),
                    Err(e) => {
                        // The DB still references the old name — put the
                        // store back best-effort (idempotent; a retry of
                        // either direction overwrites).
                        if let Some(rename) = &rename
                            && let Err(revert) = rename_stored_connection(
                                store,
                                &project,
                                &rename.connection_id,
                                &rename.collection_id,
                                &rename.old_name,
                            )
                            .await
                        {
                            warn!(
                                error = %revert,
                                "could not revert the connection store rename; \
                                 upsert the connection to repair"
                            );
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Context;
    use crate::collection::CollectionError;
    use crate::collection::service::connection::test_support::{
        bridge, create_collection, create_request, linked_connection, seed_dev_user,
    };
    use crate::collection::service::create::CreateCollectionService;
    use crate::function::services::register::RegisterFunctionService;
    use crate::function::services::upload::UploadFunctionService;
    use axum::body::Body;
    use axum::extract::Request;
    use std::str::FromStr;
    use td_bridge::connection::memory::InMemoryConnectionStore;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::InProject;
    use td_error::ApiError;
    use td_error::{TdError, assert_service_error};
    use td_objects::Buildable;
    use td_objects::dxo::bundle::BundleDB;
    use td_objects::dxo::collection::CollectionDB;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function::{FunctionDB, FunctionRegister};
    use td_objects::dxo::function_upload::FunctionUpload;
    use td_objects::rest_urls::params::FunctionMaybeAtIdName;
    use td_objects::rest_urls::{CollectionParam, FunctionParam, OptionalAtTimeParam};
    use td_objects::types::basic::{
        AccessTokenId, AtTime, BasePath, CollectionName, Decorator, Description, FunctionIdName,
        FunctionName, FunctionRuntimeValues, FunctionStatus, GroupName, ProjectId, UserId,
        UserName,
    };
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    // The orchestrator's metadata is its resolve pipeline (the store/tx
    // phases are hand-rolled around it).
    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_update_collection(db: TdPool) {
        use td_database::pool::scoped::Scoped;
        use td_tower::metadata::type_of_val;

        UpdateCollectionService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<Scoped<UpdateRequest<CollectionParam, CollectionUpdate>, ProjectId>, CollectionUpdatePlan>(&[
                type_of_val(
                    &With::<UpdateRequest<CollectionParam, CollectionUpdate>>::extract::<
                        RequestContext,
                    >,
                ),
                type_of_val(&Scope::<Project>::set),
                type_of_val(&Rbac::<Project, DevelopProject>::check),
                type_of_val(
                    &With::<UpdateRequest<CollectionParam, CollectionUpdate>>::extract_name::<
                        CollectionParam,
                    >,
                ),
                type_of_val(&With::<CollectionParam>::extract::<CollectionIdName>),
                type_of_val(&By::<CollectionIdName>::fetch_one::<CollectionDB>),
                type_of_val(&With::<CollectionDB>::extract::<CollectionType>),
                type_of_val(&escalate_for_connected),
                type_of_val(
                    &With::<UpdateRequest<CollectionParam, CollectionUpdate>>::extract_data::<
                        CollectionUpdate,
                    >,
                ),
                type_of_val(&update_collection_validate),
                type_of_val(&With::<CollectionDB>::try_convert_to::<CollectionUpdateDBBuilder, _>),
                type_of_val(&With::<RequestContext>::update::<CollectionUpdateDBBuilder, _>),
                type_of_val(&With::<CollectionUpdate>::update_collection_update_db_builder),
                type_of_val(&With::<CollectionUpdateDBBuilder>::build::<CollectionUpdateDB>),
                type_of_val(&plan_collection_update),
            ]);
    }

    fn update_request(
        project: ProjectId,
        collection: &str,
        update: CollectionUpdate,
    ) -> Scoped<UpdateRequest<CollectionParam, CollectionUpdate>, ProjectId> {
        RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection).unwrap())
                    .build()
                    .unwrap(),
                update,
            )
            .in_project(project)
    }

    fn rename_to(name: &str) -> CollectionUpdate {
        CollectionUpdate::builder()
            .name(Some(CollectionName::try_from(name).unwrap()))
            .description(None)
            .build()
            .unwrap()
    }

    #[td_test::test(db)]
    async fn test_update_collection(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        let create_name = CollectionName::try_from("ds0").unwrap();
        let _ = db
            .scoped(p.id)
            .seed()
            .collection(&create_name, &UserId::admin())
            .await;

        let before_update = AtTime::now();

        let name = CollectionName::try_from("ds1").unwrap();
        let description = Description::try_from("DS1").unwrap();

        let update = CollectionUpdate::builder()
            .name(Some(name.clone()))
            .description(Some(description.clone()))
            .build()
            .unwrap();

        let service = UpdateCollectionService::with_defaults(db).service();
        let response = service
            .raw_oneshot(update_request(p.id, &create_name.to_string(), update))
            .await;
        assert!(response.is_ok());
        let updated = response.unwrap();

        assert_eq!(updated.name, name);
        assert_eq!(updated.description, description);
        assert!(updated.created_on < before_update);
        assert_eq!(updated.created_by_id, UserId::admin());
        assert_eq!(updated.created_by, UserName::admin());
        assert!(updated.modified_on > before_update);
        assert_eq!(updated.modified_by_id, UserId::admin());
        assert_eq!(updated.modified_by, UserName::admin());
    }

    /// Drives UpdateCollectionService against `collection_name`, optionally
    /// changing name/description/base_path. Returns the service result.
    async fn update_collection(
        db: &TdPool,
        project: ProjectId,
        collection_name: &CollectionName,
        name: Option<CollectionName>,
        description: Option<Description>,
        base_path: Option<Option<BasePath>>,
    ) -> Result<CollectionRead, TdError> {
        let update = CollectionUpdate::builder()
            .name(name)
            .description(description)
            .base_path(base_path)
            .build()?;
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name.as_str())?)
                    .build()?,
                update,
            )
            .in_project(project);
        UpdateCollectionService::with_defaults(db.clone())
            .service()
            .raw_oneshot(request)
            .await
    }

    /// The current Active version of the function named `name`.
    async fn active_function(db: &TdPool, name: &FunctionName) -> Result<FunctionDB, TdError> {
        let active: Vec<FunctionDB> = db
            .live()
            .fetch_all::<FunctionDB>(&FunctionStatus::Active)
            .await?;
        let function = active
            .into_iter()
            .find(|f| f.name == *name)
            .expect("matching active function exists");
        Ok(function)
    }

    #[td_test::test(db)]
    async fn test_update_collection_base_path(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("bp_coll")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        // Seeded collections start with NULL base_path (they inherit the project).
        assert_eq!(collection.base_path, None);

        let data = BasePath::try_from("/data")?;
        update_collection(
            &db,
            p.id,
            &collection_name,
            None,
            None,
            Some(Some(data.clone())),
        )
        .await?;

        // Read back the persisted CollectionDB and confirm the new base_path.
        let reloaded: CollectionDB = db.live().fetch_one::<CollectionDB>(&collection.id).await?;
        assert_eq!(reloaded.base_path, Some(data.clone()));

        // Clearing it (base_path = null) reverts to inheriting the project.
        update_collection(&db, p.id, &collection_name, None, None, Some(None)).await?;
        let reloaded: CollectionDB = db.live().fetch_one::<CollectionDB>(&collection.id).await?;
        assert_eq!(reloaded.base_path, None, "cleared back to inherit");
        Ok(())
    }

    /// The update DTO's `base_path` is a JSON tri-state: absent leaves it
    /// unchanged, an explicit `null` clears it (inherit the project), a value
    /// sets a concrete prefix.
    #[test]
    fn test_collection_update_base_path_tristate_deser() {
        let unchanged: CollectionUpdate = serde_json::from_str(r#"{}"#).unwrap();
        assert_eq!(unchanged.base_path, None, "absent -> unchanged");

        let cleared: CollectionUpdate = serde_json::from_str(r#"{"base_path":null}"#).unwrap();
        assert_eq!(cleared.base_path, Some(None), "null -> clear (inherit)");

        let set: CollectionUpdate = serde_json::from_str(r#"{"base_path":"/x"}"#).unwrap();
        assert_eq!(
            set.base_path,
            Some(Some(BasePath::try_from("/x").unwrap())),
            "value -> set concrete prefix"
        );
    }

    #[td_test::test(db)]
    async fn test_update_collection_nothing_to_update_errors(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        let collection_name = CollectionName::try_from("noop_coll").unwrap();
        let _ = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await;

        // name=None, description=None, base_path omitted (None) -> nothing to update.
        let update = CollectionUpdate::builder()
            .name(None)
            .description(None)
            .build()
            .unwrap();
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name.as_str()).unwrap())
                    .build()
                    .unwrap(),
                update,
            )
            .in_project(p.id);

        let service = UpdateCollectionService::with_defaults(db).service();
        assert_service_error(service, request, |err| {
            #[allow(unreachable_patterns)]
            match err {
                CollectionError::UpdateRequestHasNothingToUpdate => {}
                other => panic!("Expected 'UpdateRequestHasNothingToUpdate', got {other:?}"),
            }
        })
        .await;
    }

    /// A collection base_path change must not relocate artifacts that were
    /// generated under the old base_path: those rows carry their own stamped
    /// prefix so they stay findable. The collection update bumps no function
    /// version, so the function row is untouched and the bundle stays put.
    #[td_test::test(db)]
    async fn test_collection_base_path_update_does_not_relocate_existing_function_artifacts(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("move_coll")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;
        let context = Context::with_defaults(db.clone());

        // Put the collection on base_path "/a" before any artifacts exist.
        let a = BasePath::try_from("/a")?;
        update_collection(
            &db,
            p.id,
            &collection_name,
            None,
            None,
            Some(Some(a.clone())),
        )
        .await?;

        // Upload a real bundle so a Pending BundleDB row + storage object exist.
        let payload = b"BUNDLE-PAYLOAD".to_vec();
        let upload_request = Request::builder()
            .body(Body::from(payload.clone()))
            .unwrap();
        let upload = FunctionUpload::new(upload_request);
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name.as_str())?)
                    .build()?,
                upload,
            )
            .in_project(p.id);
        let bundle = UploadFunctionService::build(&context)
            .service()
            .raw_oneshot(request)
            .await?;

        // Register the function bound to that bundle.
        let create = FunctionRegister::builder()
            .try_name("artifact_fn")?
            .try_description("description")?
            .bundle_id(bundle.id)
            .try_snippet("snippet")?
            .decorator(Decorator::Transformer)
            .dependencies(None)
            .triggers(None)
            .tables(None)
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name.as_str())?)
                    .build()?,
                create,
            )
            .in_project(p.id);
        RegisterFunctionService::with_defaults(db.clone())
            .service()
            .raw_oneshot(request)
            .await?;

        // Both artifacts are stamped with the old "/a" prefix at create time.
        let function_name = FunctionName::try_from("artifact_fn")?;
        let function = active_function(&db, &function_name).await?;
        assert_eq!(function.stage_base_path, a);
        let bundle_before: Vec<BundleDB> =
            db.live().fetch_all::<BundleDB>(&function.bundle_id).await?;
        assert_eq!(bundle_before.len(), 1);
        assert_eq!(bundle_before[0].base_path, a);

        // Move the collection to "/b".
        let b = BasePath::try_from("/b")?;
        update_collection(
            &db,
            p.id,
            &collection_name,
            None,
            None,
            Some(Some(b.clone())),
        )
        .await?;
        let reloaded: CollectionDB = db.live().fetch_one::<CollectionDB>(&collection.id).await?;
        assert_eq!(reloaded.base_path, Some(b));

        // The previously-generated artifacts still carry "/a": the move did not
        // relocate them, and no new function version was created.
        let function_after = active_function(&db, &function_name).await?;
        assert_eq!(
            function_after.id, function.id,
            "collection update must not bump the function version"
        );
        assert_eq!(
            function_after.stage_base_path, a,
            "function stage_base_path stays on the prefix it was stamped with"
        );
        let bundle_after: Vec<BundleDB> = db
            .live()
            .fetch_all::<BundleDB>(&function_after.bundle_id)
            .await?;
        assert_eq!(bundle_after.len(), 1);
        assert_eq!(
            bundle_after[0].base_path, a,
            "bundle base_path stays on the prefix it was stamped with"
        );

        // And the bundle is still downloadable: the download layer reads the
        // stamped bundle.base_path ("/a"), unaffected by the collection move.
        use crate::function::services::download_bundle::DownloadBundleService;
        use bytes::Bytes;
        use futures::StreamExt;
        let function_param = FunctionParam::builder()
            .collection(CollectionIdName::from_str(collection_name.as_str())?)
            .function(FunctionIdName::from_str("artifact_fn")?)
            .build()?;
        let download_request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .read(FunctionMaybeAtIdName::new(
                function_param,
                OptionalAtTimeParam { at: None },
            ))
            .in_project(p.id);
        let response = DownloadBundleService::build(&context)
            .service()
            .raw_oneshot(download_request)
            .await?;
        assert_eq!(response.bundle_id, function_after.bundle_id);
        let mut stream = response.stream.into_inner();
        let mut buf = Vec::new();
        while let Some(chunk) = stream.next().await {
            buf.extend_from_slice(&chunk?);
        }
        assert_eq!(
            Bytes::from(buf).as_ref(),
            payload.as_slice(),
            "bundle still streams after the collection moved"
        );

        Ok(())
    }

    #[td_test::test(db)]
    async fn test_rename_connected_collection_renames_the_connection(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        let bridge = bridge(InMemoryConnectionStore::default());
        let authz = Arc::new(AuthzContext::default());

        CreateCollectionService::new(db.clone(), authz.clone(), bridge.clone())
            .service()
            .raw_oneshot(create_request(p.id, "src0", Some(GroupName::source())))
            .await
            .unwrap();

        let renamed = UpdateCollectionService::new(db.clone(), authz, bridge.clone())
            .service()
            .raw_oneshot(update_request(p.id, "src0", rename_to("src1")))
            .await
            .unwrap();
        assert_eq!(renamed.name.to_string(), "src1");

        // The stored document followed the rename, values intact.
        let connection = linked_connection(&db, p.id, "src1").await.unwrap();
        let stored = bridge
            .connection_store()
            .fetch(&p.id, &connection.id)
            .await
            .unwrap();
        assert_eq!(stored.connection["name"], "src1");
        assert_eq!(stored.secret_names, vec!["CREDENTIALS_PASSWORD"]);
    }

    #[td_test::test(db)]
    async fn test_failed_rename_puts_the_store_back(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        let bridge = bridge(InMemoryConnectionStore::default());
        let authz = Arc::new(AuthzContext::default());

        CreateCollectionService::new(db.clone(), authz.clone(), bridge.clone())
            .service()
            .raw_oneshot(create_request(p.id, "src0", Some(GroupName::source())))
            .await
            .unwrap();
        let taken = CollectionName::try_from("taken").unwrap();
        let _ = db
            .scoped(p.id)
            .seed()
            .collection(&taken, &UserId::admin())
            .await;

        // The resolve pre-check rejects the taken name before the store is
        // ever touched (the orchestrator's revert remains as the fallback
        // for the create/rename race the pre-check cannot see).
        UpdateCollectionService::new(db.clone(), authz, bridge.clone())
            .service()
            .raw_oneshot(update_request(p.id, "src0", rename_to("taken")))
            .await
            .expect_err("renaming onto an existing collection must fail");

        let connection = linked_connection(&db, p.id, "src0").await.unwrap();
        let stored = bridge
            .connection_store()
            .fetch(&p.id, &connection.id)
            .await
            .unwrap();
        assert_eq!(stored.connection["name"], "src0");
    }
    /// Renaming re-keys a source collection's connection document, so it
    /// requires ManageProject; plain collections need only DevelopProject.
    #[td_test::test(db)]
    async fn test_source_update_requires_manage_project(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let dev = seed_dev_user(&db, p.id).await;

        // DataFactory rename with develop_project: fine.
        CreateCollectionService::with_defaults(db.clone())
            .service()
            .raw_oneshot(create_request(p.id, "dev_df_u", None))
            .await?;
        let request = RequestContext::with(AccessTokenId::default(), dev)
            .update(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str("dev_df_u")?)
                    .build()?,
                rename_to("dev_df_u2"),
            )
            .in_project(p.id);
        UpdateCollectionService::with_defaults(db.clone())
            .service()
            .raw_oneshot(request)
            .await?;

        // Source rename with develop_project: forbidden.
        create_collection(&db, p.id, "dev_src_u", Some(GroupName::source())).await;
        let request = RequestContext::with(AccessTokenId::default(), dev)
            .update(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str("dev_src_u")?)
                    .build()?,
                rename_to("dev_src_u2"),
            )
            .in_project(p.id);
        let err = UpdateCollectionService::with_defaults(db.clone())
            .service()
            .raw_oneshot(request)
            .await
            .expect_err("develop_project must not suffice to rename a source collection");
        assert_eq!(err.api_error(), ApiError::Forbidden);
        Ok(())
    }
}
