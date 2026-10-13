//
// Copyright 2024 Tabsdata Inc.
//

//! Delete a collection. The connection's store content goes with it (1:1),
//! AFTER the transaction commits — never inside it (an outbound store call
//! must not hold the transaction open) and never before it (a failed delete
//! must not destroy a live collection's secrets). The store delete is
//! best-effort: a failure leaves invisible content an idempotent re-delete
//! repairs; the connection row itself stays (the soft-deleted collection
//! still references it).

use crate::collection::services::connection::{
    drop_connection_best_effort, escalate_for_connected,
};
use crate::function::engine::dependants_of_tables;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{DevelopProject, Project};
use td_bridge::BridgeProviderHandle;
use td_database::access::SeesProjectRows;
use td_database::access::{DbPool, InProject, WriteMode, retrying};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::collection::{ActiveCollectionDB, CollectionDeleteDB};
use td_objects::dxo::crudl::{DeleteRequest, WithinProject};
use td_objects::dxo::dependency::DependencyDB;
use td_objects::dxo::function::FunctionDB;
use td_objects::dxo::function::FunctionInsertDB;
use td_objects::dxo::table::TableDB;
use td_objects::dxo::trigger::TriggerDB;
use td_objects::dxo::worker::WorkerUpdateDB;
use td_objects::types::basic::TableId;
use td_objects::types::basic::{CollectionId, WorkerType};
use td_objects::{and, or};
use td_queries::Queries;
use tracing::debug;

#[service_factory]
async fn delete_collection(
    authz: Ctx<AuthzContext>,
    bridge: Ctx<BridgeProviderHandle>,
    db: DbPool<InProject>,
    request: WithinProject<DeleteRequest<CollectionParam>>,
) -> Result<(), TdError> {
    let (project, request) = request.into_parts();
    let (ctx, param) = request.into_parts();

    let (collection, grant) = {
        let mut conn = db.read().await?;
        let grant = authz
            .require::<Project, DevelopProject>(&mut conn, &ctx, &project)
            .await?;
        let conn = conn.get(grant);
        let collection: ActiveCollectionDB = conn.fetch_one(&param.collection).await?;
        // Source/destination collections carry connection config: manage_project.
        escalate_for_connected(&authz, conn, &ctx, &project, &collection.collection_type).await?;
        (collection, grant)
    };

    let connection_id = retrying(|attempt| async {
        let mut trx = db.begin(attempt).await?;
        let at = trx.locked_at();
        let conn = trx.get(grant);
        // A raced delete loses with not-found instead of silently re-renaming
        // the already-deleted row.
        let collection: ActiveCollectionDB = conn.fetch_one(&collection.id).await?;
        let delete_db = collection.to_delete_dao(&ctx);
        conn.update::<CollectionDeleteDB>(&delete_db, collection.id)
            .await?;

        let tables: Vec<TableDB> = conn.scd2().fetch_all(&collection.id).await?;
        let table_ids: Vec<TableId> = tables.iter().map(|x| x.base.table_id).collect();
        // Same-collection dependants are closed with the collection below; a
        // frozen version for them would be closed in the same instant.
        let dependants: Vec<FunctionDB> = dependants_of_tables(conn, &table_ids)
            .await?
            .into_iter()
            .filter(|f| f.base.collection_id != collection.id)
            .collect();
        ensure_freezes_on_top(conn, &dependants, at).await?;
        freeze_functions(conn, &dependants, &ctx, at).await?;
        conn.scd2()
            .chunked()
            .close::<TriggerDB>(&table_ids, (), at)
            .await?;
        conn.scd2().close::<TriggerDB>(&collection.id, at).await?;
        conn.scd2()
            .chunked()
            .close::<DependencyDB>(&table_ids, (), at)
            .await?;
        conn.scd2()
            .close::<DependencyDB>(&collection.id, at)
            .await?;
        conn.scd2().close::<TableDB>(&collection.id, at).await?;
        conn.scd2().close::<FunctionDB>(&collection.id, at).await?;
        update_deleted_workers(conn, &collection.id).await?;
        trx.commit().await?;
        Ok(collection.connection_id)
    })
    .await?;

    // Only after commit: drop the store content so its secrets aren't orphaned.
    if let Some(connection_id) = &connection_id {
        drop_connection_best_effort(bridge.connection_store(), &project, connection_id).await;
    }
    Ok(())
}

pub async fn update_deleted_workers<S: SeesProjectRows, M: WriteMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    collection_id: &CollectionId,
) -> Result<(), TdError> {
    let update = WorkerUpdateDB::stop_scheduled();

    let res = conn
        .update(
            &update,
            and!(
                *collection_id,
                or!(WorkerType::CronTrigger, WorkerType::StageTrigger)
            ),
        )
        .await?;
    debug!(
        "Set stopped status in {} workers for collection {}.",
        res.rows_affected(),
        collection_id
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use crate::collection::services::connection::test_support::{
        bridge, create_collection, create_request, linked_connection, seed_dev_user,
    };
    use crate::collection::services::connection::{Reconciled, reconcile_stored_connection};
    use std::str::FromStr;
    use td_bridge::connection::memory::InMemoryConnectionStore;
    use td_database::access::{AcrossProjects, Grant};
    use td_database::domain::DomainPools;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt as _;
    use td_error::{ApiError, TdError};
    use td_objects::dxo::collection::CollectionParam;
    use td_objects::dxo::collection::{CollectionDB, ResolvedCollectionDB};
    use td_objects::dxo::connection::ConnectionUpsert;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::dependency::DependencyDBWithNames;
    use td_objects::dxo::function::{FunctionDBWithNames, FunctionRegister};
    use td_objects::dxo::table::TableDBWithNames;
    use td_objects::dxo::trigger::TriggerDBWithNames;
    use td_objects::dxo::worker::WorkerDB;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, FunctionName, FunctionRuntimeValues,
        GroupName, LogicalFunctionRunStatus, ProjectId, TableName, TableNameDto, TransactionKey,
        UserId, WorkerMessageStatus, WorkerStatus, WorkerType,
    };
    use td_objects::types::basic::{CollectionIdName, ReuseFrozen, WhenDeleted};
    use td_objects::types::composed::{TableDependencyDto, TableTriggerDto};
    use td_queries::Queries;
    use td_queries::test_utils::SeedEntry;

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

        let err = bridge
            .connection_store()
            .fetch(&p.id, &connection.base.id)
            .await
            .expect_err("store content must be dropped with the collection");
        assert!(err.to_string().contains("no connection configured"));
    }

    /// A store write landing after the collection's delete committed, as by a
    /// rename or an upsert racing it, is dropped by its writer's reconcile.
    #[td_test::test(db)]
    async fn test_a_store_write_after_the_delete_is_dropped(db: TdPool) {
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
        let name = CollectionName::try_from("src0").unwrap();
        let collection: ActiveCollectionDB = db.scoped(p.id).fetch_one(&name).await.unwrap();
        let store = bridge.connection_store();
        let late = store.fetch(&p.id, &connection.base.id).await.unwrap();

        Services {
            bridge: bridge.clone(),
            ..Services::with_defaults(db.clone())
        }
        .delete_collection(delete_request(p.id, "src0"))
        .await
        .unwrap();
        store
            .upsert(
                &p.id,
                &connection.base.id,
                &name,
                &collection.id,
                &ConnectionUpsert {
                    connection: Some(late.connection),
                    secrets: None,
                },
            )
            .await
            .unwrap();

        let pools =
            DbPool::<AcrossProjects>::new(DomainPools::of_apiserver(db.clone()), AcrossProjects);
        let reconciled = reconcile_stored_connection(
            &DbPool::in_project(&pools, &p.id),
            Grant::asserted("reconcile"),
            store,
            &p.id,
            &connection.base.id,
            &collection.id,
            &name,
        )
        .await
        .unwrap();

        assert!(matches!(reconciled, Reconciled::Deleted));
        store
            .fetch(&p.id, &connection.base.id)
            .await
            .expect_err("a write after the delete must be dropped");
    }

    #[td_test::test(db)]
    async fn test_delete_collection(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let name = CollectionName::try_from("c")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&name, &UserId::admin())
            .await?;

        for i in 0..3 {
            let create = FunctionRegister {
                child_functions: None,
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
                parent_tables: None,
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
            child_functions: None,
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
            parent_tables: None,
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
            .await?;

        let found = db.fetch_all::<ActiveCollectionDB>(()).await?;
        assert_eq!(found.len(), 0);

        let res = db.fetch_one::<CollectionDB>(&collection.id).await?;
        assert_eq!(
            res.base.name_when_deleted,
            Some(WhenDeleted::from(collection.name.clone()))
        );

        // The resolved surface still returns the soft-deleted collection (its
        // scheduled run outlives it), carrying the real pre-deletion name and the
        // `deleted` flag — this is what dispatch/enrichment resolves against.
        let resolved = db.fetch_all::<ResolvedCollectionDB>(()).await?;
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].base.name, collection.name);
        assert!(*resolved[0].deleted);

        let found = db.scd2().fetch_all::<FunctionDBWithNames>(()).await?;
        assert!(found.is_empty());
        let versions = db
            .timeline()
            .fetch_all::<FunctionDB>(&collection.id)
            .await?;
        assert_eq!(versions.len(), 4);
        assert!(versions.iter().all(|f| !f.interval.valid_to.is_infinity()));

        let found = db.scd2().fetch_all::<TableDBWithNames>(()).await?;
        assert!(found.is_empty());
        let versions = db.timeline().fetch_all::<TableDB>(&collection.id).await?;
        assert_eq!(versions.len(), 4);
        assert!(versions.iter().all(|t| !t.interval.valid_to.is_infinity()));

        let found = db.scd2().fetch_all::<TriggerDBWithNames>(()).await?;
        assert!(found.is_empty());
        let versions = db.timeline().fetch_all::<TriggerDB>(&collection.id).await?;
        assert_eq!(versions.len(), 2);
        assert!(versions.iter().all(|t| !t.interval.valid_to.is_infinity()));

        let found = db.scd2().fetch_all::<DependencyDBWithNames>(()).await?;
        assert!(found.is_empty());
        let versions = db
            .timeline()
            .fetch_all::<DependencyDB>(&collection.id)
            .await?;
        assert_eq!(versions.len(), 1);
        assert!(versions.iter().all(|d| !d.interval.valid_to.is_infinity()));

        let found = db.fetch_all::<WorkerDB>(()).await?;
        assert_eq!(found.len(), 2);
        for worker in found {
            if matches!(worker.base.worker_type, WorkerType::FunctionRunner) {
                // unchanged status from seed method
                assert!(matches!(worker.base.status, WorkerStatus::Requested));
            } else {
                assert!(matches!(worker.base.status, WorkerStatus::StopScheduled));
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
            child_functions: None,
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
            parent_tables: None,
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
            child_functions: None,
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
            parent_tables: None,
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
            .await?;

        let found = db.fetch_all::<ActiveCollectionDB>(()).await?;
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].name, name_c_1);

        let found = db
            .scd2()
            .fetch_all::<FunctionDBWithNames>(&FunctionName::try_from("function_1")?)
            .await?;
        assert_eq!(found.len(), 1);

        let found = db
            .scd2()
            .fetch_all::<TableDBWithNames>(&TableName::try_from("table_1")?)
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
