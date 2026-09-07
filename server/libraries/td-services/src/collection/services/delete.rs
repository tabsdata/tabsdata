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

use crate::collection::delete::update_deleted_workers;
use crate::collection::services::connection::{
    drop_connection_best_effort, escalate_for_connected,
};
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{DevelopProject, Project};
use td_bridge::BridgeProviderHandle;
use td_database::access::{DbPool, InProject};
use td_error::TdError;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::collection::{ActiveCollectionDB, CollectionCreateDB, CollectionDeleteDB};
use td_objects::dxo::crudl::{DeleteRequest, WithinProject};
use td_objects::dxo::dependency::DependencyDB;
use td_objects::dxo::function::FunctionDB;
use td_objects::dxo::table::TableDB;
use td_objects::dxo::trigger::TriggerDB;
use td_objects::types::basic::{FunctionId, TableId};
use td_queries::{ChunkedQueries, CurrentEntry, LiveEntry};

#[service_factory]
async fn delete_collection(
    authz: Ctx<AuthzContext>,
    bridge: Ctx<BridgeProviderHandle>,
    db: DbPool<InProject>,
    request: WithinProject<DeleteRequest<CollectionParam>>,
) -> Result<(), TdError> {
    let (project, request) = request.into_parts();
    let (ctx, param) = request.into_parts();

    // 1. resolve (read-only): authz and fetch the collection.
    let (collection, grant) = {
        let mut conn = db.read().await?;
        let grant = authz
            .require::<Project, DevelopProject>(&mut conn, &ctx, &project)
            .await?;
        let conn = conn.get(grant);
        let collection: ActiveCollectionDB = conn.live().fetch_one(&param.collection).await?;
        // source/destination collections carry connection config: manage_project
        escalate_for_connected(&authz, conn, &ctx, &project, &collection.collection_type).await?;
        (collection, grant)
    };

    // 2. delete (short TX): soft-delete the collection and cascade.
    let mut trx = db.begin().await?;
    let conn = trx.get(grant);
    // Bare-table row lock (views with outer joins aren't lockable): a
    // registration landing mid-cascade would otherwise escape it.
    let _: CollectionCreateDB = conn.live().fetch_one_for_update(&collection.id).await?;
    // Liveness re-read under the lock: a raced delete loses with not-found
    // instead of silently re-renaming the already-deleted row.
    let collection: ActiveCollectionDB = conn.live().fetch_one(&collection.id).await?;
    let delete_db = collection.to_delete_dao(&ctx);
    conn.live()
        .update::<CollectionDeleteDB, ActiveCollectionDB>(&delete_db, collection.id)
        .await?;

    let tables: Vec<TableDB> = conn
        .current()
        .state(TableDB::available())
        .fetch_all(&collection.id)
        .await?;
    let table_ids: Vec<TableId> = tables.iter().map(|x| x.table_id).collect();
    // Find functions that use the tables and freeze them.
    let dependencies: Vec<DependencyDB> = conn
        .current()
        .state(DependencyDB::active())
        .fetch_all_chunked::<TableId>(&table_ids)
        .await?;
    let function_ids: Vec<FunctionId> = dependencies.iter().map(|x| x.function_id).collect();
    let dependant_functions: Vec<FunctionDB> = conn
        .current()
        .state(FunctionDB::active())
        .fetch_all_chunked::<FunctionId>(&function_ids)
        .await?;
    let frozen: Vec<FunctionDB> = dependant_functions.iter().map(|f| f.frozen(&ctx)).collect();
    conn.insert_vec::<FunctionDB>(&frozen).await?;
    let deleted_tables: Vec<TableDB> = tables.iter().map(|t| t.deleted(&ctx)).collect();
    conn.insert_vec::<TableDB>(&deleted_tables).await?;
    let triggers: Vec<TriggerDB> = conn
        .current()
        .state(TriggerDB::available())
        .fetch_all_chunked::<TableId>(&table_ids)
        .await?;
    let deleted: Vec<TriggerDB> = triggers.iter().map(|t| t.deleted(&ctx)).collect();
    conn.insert_vec::<TriggerDB>(&deleted).await?;
    let dependencies: Vec<DependencyDB> = conn
        .current()
        .state(DependencyDB::active())
        .fetch_all_chunked::<TableId>(&table_ids)
        .await?;
    let deleted: Vec<DependencyDB> = dependencies.iter().map(|d| d.deleted(&ctx)).collect();
    conn.insert_vec::<DependencyDB>(&deleted).await?;
    // Find functions in the collection and delete them (note this will delete
    // possible frozen functions created in the previous step, this is,
    // functions using tables in the same collection).
    let functions: Vec<FunctionDB> = conn
        .current()
        .state(FunctionDB::available())
        .fetch_all(&collection.id)
        .await?;
    let deleted: Vec<FunctionDB> = functions.iter().map(|f| f.deleted(&ctx)).collect();
    conn.insert_vec::<FunctionDB>(&deleted).await?;
    update_deleted_workers(conn, &collection.id).await?;
    trx.commit().await?;

    // 3. The row is committed deleted; now drop the store content so its
    // secrets aren't orphaned.
    if let Some(connection_id) = &collection.connection_id {
        drop_connection_best_effort(bridge.connection_store(), &project, connection_id).await;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use crate::collection::services::connection::test_support::{
        bridge, create_collection, create_request, linked_connection, seed_dev_user,
    };
    use std::str::FromStr;
    use td_bridge::connection::memory::InMemoryConnectionStore;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt as _;
    use td_error::{ApiError, TdError};
    use td_objects::dxo::collection::CollectionParam;
    use td_objects::dxo::collection::{CollectionCreateDB, CollectionDB};
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::dependency::DependencyDBWithNames;
    use td_objects::dxo::function::{FunctionDBWithNames, FunctionRegister};
    use td_objects::dxo::table::TableDBWithNames;
    use td_objects::dxo::trigger::TriggerDBWithNames;
    use td_objects::dxo::worker::WorkerDB;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, DependencyStatus, FunctionName,
        FunctionRuntimeValues, FunctionStatus, GroupName, LogicalFunctionRunStatus, ProjectId,
        TableName, TableNameDto, TableStatus, TransactionKey, TriggerStatus, UserId,
        WorkerMessageStatus, WorkerStatus, WorkerType,
    };
    use td_objects::types::basic::{CollectionIdName, ReuseFrozen};
    use td_objects::types::composed::{TableDependencyDto, TableTriggerDto};
    use td_queries::test_utils::SeedEntry;
    use td_queries::{CurrentEntry, LiveEntry};

    // The orchestrator's metadata is its resolve pipeline; the TX layers are
    // asserted below against the sub-service directly.

    fn delete_request(
        project: ProjectId,
        collection: &str,
    ) -> WithinProject<DeleteRequest<CollectionParam>> {
        RequestContext::with(AccessTokenId::default(), UserId::admin())
            .delete(CollectionParam {
                collection: CollectionIdName::from_str(collection).unwrap(),
            })
            .in_project(project)
    }

    #[td_test::test(db)]
    async fn test_delete_drops_the_connection_store_content(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        let bridge = bridge(InMemoryConnectionStore::default());

        Services {
            bridge: bridge.clone(),
            ..Services::with_defaults(db.clone())
        }
        .create_collection(create_request(p.id, "src0", Some(GroupName::source())))
        .await
        .unwrap();
        let connection = linked_connection(&db, p.id, "src0").await.unwrap();

        Services {
            bridge: bridge.clone(),
            ..Services::with_defaults(db.clone())
        }
        .delete_collection(delete_request(p.id, "src0"))
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
            let create = FunctionRegister {
                name: format!("function_{i}").try_into()?,
                description: "description".try_into()?,
                bundle_id: BundleId::default(),
                snippet: "snippet".try_into()?,
                decorator: Decorator::Publisher,
                connector: None,
                connection: None,
                base_path: None,
                dependencies: None,
                triggers: None,
                tables: Some(vec![TableNameDto::try_from(format!("table_{i}"))?]),
                table_types: None,
                runtime_values: FunctionRuntimeValues::default(),
                reuse_frozen_tables: ReuseFrozen::FALSE,
                enabled: None,
                workers: None,
            };
            let _ = db
                .scoped(p.id)
                .seed()
                .function(&collection, &create)
                .await?;
        }

        let create = FunctionRegister {
            name: "function_3".try_into()?,
            description: "description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "snippet".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: Some(vec![TableDependencyDto::try_from("table_0")?]),
            triggers: Some(vec![
                TableTriggerDto::try_from("table_0")?,
                TableTriggerDto::try_from("table_1")?,
            ]),
            tables: Some(vec![TableNameDto::try_from("table_3")?]),
            table_types: None,
            runtime_values: FunctionRuntimeValues::default(),
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let function = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;

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

        Services::with_defaults(db.clone())
            .delete_collection(delete_request(p.id, &name.to_string()))
            .await?
            .data;

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
        assert_eq!(resolved[0].base.name, collection.name);
        assert!(*resolved[0].deleted);

        let found = db.current().fetch_all::<FunctionDBWithNames>(()).await?;
        assert_eq!(found.len(), 4);
        for function in found {
            assert!(matches!(function.base.status, FunctionStatus::Deleted));
        }

        let found = db.current().fetch_all::<TableDBWithNames>(&()).await?;
        assert_eq!(found.len(), 4);
        for table in found {
            assert!(matches!(table.base.status, TableStatus::Deleted));
        }

        let found = db.current().fetch_all::<TriggerDBWithNames>(()).await?;
        assert_eq!(found.len(), 2);
        for trigger in found {
            assert!(matches!(trigger.base.status, TriggerStatus::Deleted));
        }

        let found = db.current().fetch_all::<DependencyDBWithNames>(()).await?;
        assert_eq!(found.len(), 1);
        for dep in found {
            assert!(matches!(dep.base.status, DependencyStatus::Deleted));
        }

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
        let create = FunctionRegister {
            name: "function_0".try_into()?,
            description: "description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "snippet".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(vec![TableNameDto::try_from("table_0")?]),
            table_types: None,
            runtime_values: FunctionRuntimeValues::default(),
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
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
        let create = FunctionRegister {
            name: "function_1".try_into()?,
            description: "description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "snippet".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: vec![TableDependencyDto::try_from("c_0/table_0")?].into(),
            triggers: vec![
                TableTriggerDto::try_from("c_0/table_0")?,
                TableTriggerDto::try_from("table_1")?,
            ]
            .into(),
            tables: Some(vec![TableNameDto::try_from("table_1")?]),
            table_types: None,
            runtime_values: FunctionRuntimeValues::default(),
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let _ = db
            .scoped(p.id)
            .seed()
            .function(&collection_1, &create)
            .await?;

        Services::with_defaults(db.clone())
            .delete_collection(delete_request(p.id, &name_c_0.to_string()))
            .await?
            .data;

        let found = db.live().fetch_all::<ActiveCollectionDB>(()).await?;
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].name, name_c_1);

        let found = db
            .current()
            .state(FunctionDBWithNames::available())
            .fetch_all(FunctionName::try_from("function_1")?)
            .await?;
        assert_eq!(found.len(), 1);

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
            .delete(CollectionParam {
                collection: CollectionIdName::from_str("dev_src_d").unwrap(),
            })
            .in_project(p.id);
        let err = Services::with_defaults(db.clone())
            .delete_collection(request)
            .await
            .expect_err("develop_project must not suffice to delete a source collection");
        assert_eq!(err.api_error(), ApiError::Forbidden);
    }
}
