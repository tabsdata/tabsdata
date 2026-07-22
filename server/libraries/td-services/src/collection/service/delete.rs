//
// Copyright 2024 Tabsdata Inc.
//

//! Delete a collection. The connection's store content goes with it (1:1),
//! AFTER the transaction commits — never inside it (an outbound store call
//! must not hold the transaction open) and never before it (a failed delete
//! must not destroy a live collection's secrets):
//!
//! 1. resolve (read-only): authz and fetch the collection.
//! 2. delete (short TX): soft-delete the collection and cascade over its
//!    tables/functions/triggers/dependencies/workers.
//! 3. store delete (source/destination only): best-effort once the DB row is
//!    committed deleted — a failure leaves invisible content an idempotent
//!    re-delete repairs; the connection row itself stays (the soft-deleted
//!    collection still references it).

use crate::collection::service::connection::{drop_connection_best_effort, escalate_for_connected};
use crate::collection::service::layer::delete::{
    build_deleted_functions, build_deleted_tables, update_deleted_workers,
};
use crate::table::layers::delete::{
    build_deleted_dependencies, build_deleted_triggers, build_frozen_functions,
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
    ActiveCollectionDB, CollectionDeleteDB, CollectionDeleteDBBuilder,
};
use td_objects::dxo::crudl::{DeleteRequest, RequestContext, UpdateRequest};
use td_objects::dxo::dependency::DependencyDB;
use td_objects::dxo::function::FunctionDB;
use td_objects::dxo::table::TableDB;
use td_objects::dxo::trigger::TriggerDB;
use td_objects::rest_urls::CollectionParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{
    CollectionId, CollectionIdName, CollectionType, FunctionId, ProjectId, TableId,
};
use td_queries::By;
use td_queries::insert_vec;
use td_tower::ctx_service::{CtxResponse, RawOneshot, ReqCtx};
use td_tower::default_services::{ConnectionProvider, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::layers;
use td_tower::service_provider::TdBoxService;
use tower::service_fn;
use tower::util::BoxCloneSyncService;

#[service_factory(
    name = CollectionDeleteResolveService,
    request = DeleteRequest<CollectionParam>,
    response = ActiveCollectionDB,
    connection = ConnectionProvider,
    scope = "Project",
    context = AuthzContext,
)]
fn resolve() {
    layers!(
        from_fn(With::<DeleteRequest<CollectionParam>>::extract::<RequestContext>),
        from_fn(Scope::<Project>::set),
        from_fn(Rbac::<Project, DevelopProject>::check),
        from_fn(With::<DeleteRequest<CollectionParam>>::extract_name::<CollectionParam>),
        from_fn(With::<CollectionParam>::extract::<CollectionIdName>),
        from_fn(By::<CollectionIdName>::fetch_one::<ActiveCollectionDB>),
        // source/destination collections carry connection config: manage_project
        from_fn(With::<ActiveCollectionDB>::extract::<CollectionType>),
        from_fn(escalate_for_connected),
    )
}

// Internal only (never routed; the orchestrator below is the routed surface),
// so no authz layers — the resolve already gated the request.
#[service_factory(
    name = CollectionDeleteTxService,
    request = UpdateRequest<(), ActiveCollectionDB>,
    response = (),
    connection = TransactionProvider,
    scope = "Project",
)]
fn delete() {
    layers!(
        from_fn(With::<UpdateRequest<(), ActiveCollectionDB>>::extract::<RequestContext>),
        from_fn(With::<UpdateRequest<(), ActiveCollectionDB>>::extract_data::<ActiveCollectionDB>),
        // Build deleted collection
        from_fn(With::<ActiveCollectionDB>::try_convert_to::<CollectionDeleteDBBuilder, _>),
        from_fn(With::<RequestContext>::update::<CollectionDeleteDBBuilder, _>),
        from_fn(With::<CollectionDeleteDBBuilder>::build::<CollectionDeleteDB>),
        // Update collection
        from_fn(With::<ActiveCollectionDB>::extract::<CollectionId>),
        from_fn(By::<CollectionId>::update::<CollectionDeleteDB, ActiveCollectionDB>),
        // Find tables in the collection
        from_fn(By::<CollectionId>::fetch_all_current::<{ TableDB::Available }, TableDB>),
        from_fn(With::<Vec<TableDB>>::extract::<Vec<TableId>>),
        // Find functions that use the tables and freeze them
        from_fn(
            By::<Vec<TableId>>::fetch_all_current_chunked::<{ DependencyDB::Active }, DependencyDB>
        ),
        from_fn(With::<Vec<DependencyDB>>::extract::<Vec<FunctionId>>),
        from_fn(
            By::<Vec<FunctionId>>::fetch_all_current_chunked::<{ FunctionDB::Active }, FunctionDB>
        ),
        from_fn(build_frozen_functions),
        from_fn(insert_vec::<FunctionDB>),
        // Delete tables in the collection
        from_fn(build_deleted_tables),
        from_fn(insert_vec::<TableDB>),
        // Find triggers for the tables in the collection and delete them
        from_fn(
            By::<Vec<TableId>>::fetch_all_current_chunked::<{ TriggerDB::Available }, TriggerDB>
        ),
        from_fn(build_deleted_triggers),
        from_fn(insert_vec::<TriggerDB>),
        // Find dependencies for the tables in the collection and delete them
        from_fn(
            By::<Vec<TableId>>::fetch_all_current_chunked::<{ DependencyDB::Active }, DependencyDB>
        ),
        from_fn(build_deleted_dependencies),
        from_fn(insert_vec::<DependencyDB>),
        // Find functions in the collection
        from_fn(By::<CollectionId>::fetch_all_current::<{ FunctionDB::Available }, FunctionDB>),
        // Delete functions in the collection (note this will delete possible frozen
        // functions created in the previous step, this is, functions using tables
        // in the same collection)
        from_fn(build_deleted_functions),
        from_fn(insert_vec::<FunctionDB>),
        // And last, set to stop whatever workers are running in the collection.
        from_fn(update_deleted_workers),
    )
}

/// The routed delete service: resolve → delete TX → store delete (after
/// commit, source/destination only), each DB phase a factory-built
/// sub-service with its own short, project-scoped connection.
pub struct DeleteCollectionService {
    resolve: CollectionDeleteResolveService,
    delete: CollectionDeleteTxService,
    bridge: Arc<BridgeProviderHandle>,
}

impl DeleteCollectionService {
    pub fn new(
        db: TdPool,
        authz_context: Arc<AuthzContext>,
        bridge: Arc<BridgeProviderHandle>,
    ) -> Self {
        Self {
            resolve: CollectionDeleteResolveService::new(db.clone(), authz_context),
            delete: CollectionDeleteTxService::new(db),
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

impl<C> ServiceFactory<C> for DeleteCollectionService
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

impl TdService for DeleteCollectionService {
    type Request = Scoped<DeleteRequest<CollectionParam>, ProjectId>;
    type Response = ();
    type Error = TdError;

    fn service(&self) -> TdBoxService<Self::Request, Self::Response, Self::Error> {
        let resolve = self.resolve.service();
        let delete = self.delete.service();
        let bridge = self.bridge.clone();
        BoxCloneSyncService::new(service_fn(move |request: Self::Request| {
            let resolve = resolve.clone();
            let delete = delete.clone();
            let bridge = bridge.clone();
            async move {
                let project = request.project;
                let context = request.inner.context.clone();
                let collection = resolve.raw_oneshot(request).await?;
                let connection_id = collection.connection_id;
                delete
                    .raw_oneshot(context.update((), collection).in_project(project))
                    .await?;
                // The row is committed deleted; now drop the store content so
                // its secrets aren't orphaned.
                if let Some(connection_id) = &connection_id {
                    drop_connection_best_effort(bridge.connection_store(), &project, connection_id)
                        .await;
                }
                Ok(CtxResponse::new((), ReqCtx::default()))
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
    use crate::collection::service::connection::test_support::{
        bridge, create_collection, create_request, linked_connection, seed_dev_user,
    };
    use crate::collection::service::create::CreateCollectionService;
    use std::str::FromStr;
    use td_bridge::connection::memory::InMemoryConnectionStore;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::InProject;
    use td_error::ApiError;
    use td_error::TdError;
    use td_objects::Buildable;
    use td_objects::dxo::collection::{CollectionCreateDB, CollectionDB};
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::dependency::DependencyDBWithNames;
    use td_objects::dxo::function::{FunctionDBWithNames, FunctionRegister};
    use td_objects::dxo::table::TableDBWithNames;
    use td_objects::dxo::trigger::TriggerDBWithNames;
    use td_objects::dxo::worker::WorkerDB;
    use td_objects::rest_urls::CollectionParam;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, DependencyStatus, FunctionName,
        FunctionRuntimeValues, FunctionStatus, GroupName, LogicalFunctionRunStatus, ProjectId,
        TableName, TableNameDto, TableStatus, TransactionKey, TriggerStatus, UserId,
        WorkerMessageStatus, WorkerStatus, WorkerType,
    };
    use td_objects::types::composed::{TableDependencyDto, TableTriggerDto};
    use td_queries::test_utils::SeedEntry;
    use td_queries::{CurrentEntry, LiveEntry};
    use td_tower::ctx_service::RawOneshot;

    // The orchestrator's metadata is its resolve pipeline; the TX layers are
    // asserted below against the sub-service directly.
    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_delete_collection(db: TdPool) {
        use td_database::pool::scoped::Scoped;
        use td_tower::metadata::type_of_val;

        DeleteCollectionService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<Scoped<DeleteRequest<CollectionParam>, ProjectId>, ActiveCollectionDB>(&[
                type_of_val(&With::<DeleteRequest<CollectionParam>>::extract::<RequestContext>),
                type_of_val(&Scope::<Project>::set),
                type_of_val(&Rbac::<Project, DevelopProject>::check),
                type_of_val(
                    &With::<DeleteRequest<CollectionParam>>::extract_name::<CollectionParam>,
                ),
                type_of_val(&With::<CollectionParam>::extract::<CollectionIdName>),
                type_of_val(&By::<CollectionIdName>::fetch_one::<ActiveCollectionDB>),
                type_of_val(&With::<ActiveCollectionDB>::extract::<CollectionType>),
                type_of_val(&escalate_for_connected),
            ]);
    }

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_delete_collection_tx(db: TdPool) {
        use td_database::pool::scoped::Scoped;
        use td_tower::metadata::type_of_val;

        CollectionDeleteTxService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<Scoped<UpdateRequest<(), ActiveCollectionDB>, ProjectId>, ()>(&[
                type_of_val(
                    &With::<UpdateRequest<(), ActiveCollectionDB>>::extract::<RequestContext>,
                ),
                type_of_val(
                    &With::<UpdateRequest<(), ActiveCollectionDB>>::extract_data::<
                        ActiveCollectionDB,
                    >,
                ),
                // Build deleted collection
                type_of_val(
                    &With::<ActiveCollectionDB>::try_convert_to::<CollectionDeleteDBBuilder, _>,
                ),
                type_of_val(&With::<RequestContext>::update::<CollectionDeleteDBBuilder, _>),
                type_of_val(&With::<CollectionDeleteDBBuilder>::build::<CollectionDeleteDB>),
                // Update collection
                type_of_val(&With::<ActiveCollectionDB>::extract::<CollectionId>),
                type_of_val(&By::<CollectionId>::update::<CollectionDeleteDB, ActiveCollectionDB>),
                // Find tables in the collection
                type_of_val(
                    &By::<CollectionId>::fetch_all_current::<{ TableDB::Available }, TableDB>,
                ),
                type_of_val(&With::<Vec<TableDB>>::extract::<Vec<TableId>>),
                // Find functions that use the tables and freeze them
                type_of_val(
                    &By::<Vec<TableId>>::fetch_all_current_chunked::<
                        { DependencyDB::Active },
                        DependencyDB,
                    >,
                ),
                type_of_val(&With::<Vec<DependencyDB>>::extract::<Vec<FunctionId>>),
                type_of_val(
                    &By::<Vec<FunctionId>>::fetch_all_current_chunked::<
                        { FunctionDB::Active },
                        FunctionDB,
                    >,
                ),
                type_of_val(&build_frozen_functions),
                type_of_val(&insert_vec::<FunctionDB>),
                // Delete tables in the collection
                type_of_val(&build_deleted_tables),
                type_of_val(&insert_vec::<TableDB>),
                // Find triggers for the tables in the collection and delete them
                type_of_val(
                    &By::<Vec<TableId>>::fetch_all_current_chunked::<
                        { TriggerDB::Available },
                        TriggerDB,
                    >,
                ),
                type_of_val(&build_deleted_triggers),
                type_of_val(&insert_vec::<TriggerDB>),
                // Find dependencies for the tables in the collection and delete them
                type_of_val(
                    &By::<Vec<TableId>>::fetch_all_current_chunked::<
                        { DependencyDB::Active },
                        DependencyDB,
                    >,
                ),
                type_of_val(&build_deleted_dependencies),
                type_of_val(&insert_vec::<DependencyDB>),
                // Find functions in the collection
                type_of_val(
                    &By::<CollectionId>::fetch_all_current::<{ FunctionDB::Available }, FunctionDB>,
                ),
                // Delete functions in the collection (note this will delete possible frozen
                // functions created in the previous step, this is, functions using tables
                // in the same collection)
                type_of_val(&build_deleted_functions),
                type_of_val(&insert_vec::<FunctionDB>),
                // And last, set to stop whatever workers are running in the collection.
                type_of_val(&update_deleted_workers),
            ]);
    }

    fn delete_request(
        project: ProjectId,
        collection: &str,
    ) -> Scoped<DeleteRequest<CollectionParam>, ProjectId> {
        RequestContext::with(AccessTokenId::default(), UserId::admin())
            .delete(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection).unwrap())
                    .build()
                    .unwrap(),
            )
            .in_project(project)
    }

    #[td_test::test(db)]
    async fn test_delete_drops_the_connection_store_content(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        let bridge = bridge(InMemoryConnectionStore::default());
        let authz = Arc::new(AuthzContext::default());

        CreateCollectionService::new(db.clone(), authz.clone(), bridge.clone())
            .service()
            .raw_oneshot(create_request(p.id, "src0", Some(GroupName::source())))
            .await
            .unwrap();
        let connection = linked_connection(&db, p.id, "src0").await.unwrap();

        DeleteCollectionService::new(db.clone(), authz, bridge.clone())
            .service()
            .raw_oneshot(delete_request(p.id, "src0"))
            .await
            .unwrap();

        // Deterministically gone once the delete returns — no polling.
        let err = bridge
            .connection_store()
            .fetch(&p.id, &connection.id)
            .await
            .expect_err("store content must be dropped with the collection");
        assert!(err.to_string().contains("no connection configured"));
    }

    #[td_test::test(db)]
    async fn test_delete_collection(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        // Create 3 functions and 3 tables in the same collection
        let name = CollectionName::try_from("c")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&name, &UserId::admin())
            .await?;

        for i in 0..3 {
            let create = FunctionRegister::builder()
                .try_name(format!("function_{i}"))?
                .try_description("description")?
                .bundle_id(BundleId::default())
                .try_snippet("snippet")?
                .decorator(Decorator::Publisher)
                .dependencies(None)
                .triggers(None)
                .tables(Some(vec![TableNameDto::try_from(format!("table_{i}"))?]))
                .runtime_values(FunctionRuntimeValues::default())
                .reuse_frozen_tables(false)
                .build()?;
            let _ = db
                .scoped(p.id)
                .seed()
                .function(&collection, &create)
                .await?;
        }

        // Create another one with triggers and dependencies
        let create = FunctionRegister::builder()
            .try_name("function_3")?
            .try_description("description")?
            .bundle_id(BundleId::default())
            .try_snippet("snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(Some(vec![TableDependencyDto::try_from("table_0")?]))
            .triggers(Some(vec![
                TableTriggerDto::try_from("table_0")?,
                TableTriggerDto::try_from("table_1")?,
            ]))
            .tables(Some(vec![TableNameDto::try_from("table_3")?]))
            .runtime_values(FunctionRuntimeValues::default())
            .reuse_frozen_tables(false)
            .build()?;
        let function = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;

        // Create workers for function_3
        let _cron_worker = db
            .scoped(p.id)
            .seed()
            .worker(
                &collection,
                &function,
                &WorkerType::CronTrigger,
                &WorkerStatus::Running,
                &WorkerMessageStatus::Unlocked,
            )
            .await?;

        // Create function runner worker
        let execution = db.scoped(p.id).seed().execution(&function).await?;
        let transaction_key = TransactionKey::try_from("ANY").unwrap();
        let transaction = db
            .scoped(p.id)
            .seed()
            .transaction(&execution, &transaction_key)
            .await?;
        let physical_function_run = db
            .scoped(p.id)
            .seed()
            .physical_function_run(
                &collection,
                &function,
                &execution,
                &transaction,
                &LogicalFunctionRunStatus::Scheduled,
            )
            .await?;
        let _function_worker = db
            .scoped(p.id)
            .seed()
            .function_runner_worker(
                &execution,
                &transaction,
                &physical_function_run,
                &WorkerMessageStatus::Unlocked,
            )
            .await?;

        // Test delete
        DeleteCollectionService::with_defaults(db.clone())
            .service()
            .raw_oneshot(delete_request(p.id, &name.to_string()))
            .await?;

        // Assert collection is deleted
        let found = db.live().fetch_all::<ActiveCollectionDB>(()).await?;
        assert_eq!(found.len(), 0);

        let res = db
            .live()
            .fetch_one::<CollectionCreateDB>(&collection.id)
            .await?;
        assert_eq!(res.name_when_deleted, Some(collection.name.clone()));

        // The resolved surface still returns the soft-deleted collection (its
        // scheduled run outlives it), carrying the real pre-deletion name and the
        // `deleted` flag — this is what dispatch/enrichment resolves against.
        let resolved = db.live().fetch_all::<CollectionDB>(()).await?;
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].name, collection.name);
        assert!(*resolved[0].deleted);

        // Assert functions are deleted
        let found = db.current().fetch_all::<FunctionDBWithNames>(()).await?;
        assert_eq!(found.len(), 4);
        for function in found {
            assert!(matches!(function.status, FunctionStatus::Deleted));
        }

        // Assert tables are deleted
        let found = db.current().fetch_all::<TableDBWithNames>(&()).await?;
        assert_eq!(found.len(), 4);
        for table in found {
            assert!(matches!(table.status, TableStatus::Deleted));
        }

        // Assert triggers are deleted
        let found = db.current().fetch_all::<TriggerDBWithNames>(()).await?;
        assert_eq!(found.len(), 2);
        for trigger in found {
            assert!(matches!(trigger.status, TriggerStatus::Deleted));
        }

        // Assert dependencies are deleted
        let found = db.current().fetch_all::<DependencyDBWithNames>(()).await?;
        assert_eq!(found.len(), 1);
        for dep in found {
            assert!(matches!(dep.status, DependencyStatus::Deleted));
        }

        // Assert non function runner workers are stopped
        let found = db.live().fetch_all::<WorkerDB>(()).await?;
        assert_eq!(found.len(), 2);
        for worker in found {
            if matches!(worker.worker_type, WorkerType::FunctionRunner) {
                // unchanged status from seed method
                assert!(matches!(worker.status, WorkerStatus::Requested));
            } else {
                assert!(matches!(worker.status, WorkerStatus::StopScheduled));
            }
        }
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_delete_collection_downstream_frozen(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let name_c_0 = CollectionName::try_from("c_0")?;
        let collection_0 = db
            .scoped(p.id)
            .seed()
            .collection(&name_c_0, &UserId::admin())
            .await?;
        let create = FunctionRegister::builder()
            .try_name("function_0")?
            .try_description("description")?
            .bundle_id(BundleId::default())
            .try_snippet("snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![TableNameDto::try_from("table_0")?]))
            .runtime_values(FunctionRuntimeValues::default())
            .reuse_frozen_tables(false)
            .build()?;
        let _ = db
            .scoped(p.id)
            .seed()
            .function(&collection_0, &create)
            .await?;

        let name_c_1 = CollectionName::try_from("c_1")?;
        let collection_1 = db
            .scoped(p.id)
            .seed()
            .collection(&name_c_1, &UserId::admin())
            .await?;
        let create = FunctionRegister::builder()
            .try_name("function_1")?
            .try_description("description")?
            .bundle_id(BundleId::default())
            .try_snippet("snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(vec![TableDependencyDto::try_from("c_0/table_0")?])
            .triggers(vec![
                TableTriggerDto::try_from("c_0/table_0")?,
                TableTriggerDto::try_from("table_1")?,
            ])
            .tables(Some(vec![TableNameDto::try_from("table_1")?]))
            .runtime_values(FunctionRuntimeValues::default())
            .reuse_frozen_tables(false)
            .build()?;
        let _ = db
            .scoped(p.id)
            .seed()
            .function(&collection_1, &create)
            .await?;

        DeleteCollectionService::with_defaults(db.clone())
            .service()
            .raw_oneshot(delete_request(p.id, &name_c_0.to_string()))
            .await?;

        // Assert only c_1 is active
        let found = db.live().fetch_all::<ActiveCollectionDB>(()).await?;
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].name, name_c_1);

        // Assert function_1 is frozen
        let found = db
            .current()
            .state(FunctionDBWithNames::available())
            .fetch_all(FunctionName::try_from("function_1")?)
            .await?;
        assert_eq!(found.len(), 1);

        // Assert table_1 is active still
        let found = db
            .current()
            .state(TableDBWithNames::available())
            .fetch_all(TableName::try_from("table_1")?)
            .await?;
        assert_eq!(found.len(), 1);

        Ok(())
    }
    /// Deleting a source collection destroys its connection, so it requires
    /// ManageProject.
    #[td_test::test(db)]
    async fn test_source_delete_requires_manage_project(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        create_collection(&db, p.id, "dev_src_d", Some(GroupName::source())).await;
        let dev = seed_dev_user(&db, p.id).await;
        let request = RequestContext::with(AccessTokenId::default(), dev)
            .delete(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str("dev_src_d").unwrap())
                    .build()
                    .unwrap(),
            )
            .in_project(p.id);
        let err = DeleteCollectionService::with_defaults(db.clone())
            .service()
            .raw_oneshot(request)
            .await
            .expect_err("develop_project must not suffice to delete a source collection");
        assert_eq!(err.api_error(), ApiError::Forbidden);
    }
}
