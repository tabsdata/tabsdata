//
// Copyright 2024 Tabsdata Inc.
//

//! Delete a collection. The connection's store content goes with it (1:1),
//! AFTER the transaction commits — never inside it (an outbound store call
//! must not hold the transaction open) and never before it (a failed delete
//! must not destroy a live collection's secrets). The store delete is
//! best-effort: a failure leaves invisible content an idempotent re-delete
//! repairs; the connection row itself stays (the deleted collection
//! still references it).

use crate::collection::services::connection::{
    drop_connection_best_effort, escalate_for_connected,
};
use crate::function::engine::dependants_of_tables;
use crate::function::freeze::freeze_functions;
use crate::function::on_top::ensure_freezes_on_top;
use crate::function::workers::stop_triggers;
use crate::tag::close_tags_of;
use crate::tag::user::close_user_tags_of;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{DevelopProject, Project};
use td_bridge::BridgeProviderHandle;
use td_database::access::SeesProjectRows;
use td_database::access::{DbPool, InProject, LockInstant, WriteMode, retrying};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::crudl::{DeleteRequest, WithinProject};
use td_objects::dxo::dependency::DependencyDB;
use td_objects::dxo::function::FunctionDB;
use td_objects::dxo::table::TableDB;
use td_objects::dxo::trigger::TriggerDB;
use td_objects::dxo::worker::WorkerDB;
use td_objects::types::basic::{CollectionId, WorkerType};
use td_objects::types::basic::{TableId, TagEntityId, TagEntityType};
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
        let collection: CollectionDB = conn.scd2().fetch_one(&param.collection).await?;
        // Source/destination collections carry connection config: manage_project.
        escalate_for_connected(&authz, conn, &ctx, &project, &collection.collection_type).await?;
        (collection, grant)
    };

    let connection_id = retrying(|attempt| async {
        let mut trx = db.begin(attempt).await?;
        let at = trx.locked_at();
        let conn = trx.get(grant);
        // A raced delete loses with not-found: the timeline is already closed.
        let collection: CollectionDB = conn.scd2().fetch_one(&collection.collection_id).await?;
        let tables: Vec<TableDB> = conn.scd2().fetch_all(&collection.collection_id).await?;
        let functions: Vec<FunctionDB> = conn.scd2().fetch_all(&collection.collection_id).await?;
        // Closing the collection's functions and tables changes the graph;
        // closing an empty collection does not.
        if !tables.is_empty() || !functions.is_empty() {
            let topology_at = conn.topology().bump(&project, at).await?;
            let table_ids: Vec<TableId> = tables.iter().map(|x| x.table_id).collect();
            // Same-collection dependants are closed with the collection below; a
            // frozen version for them would be closed in the same instant.
            let dependants: Vec<FunctionDB> = dependants_of_tables(conn, &table_ids)
                .await?
                .into_iter()
                .filter(|f| f.collection_id != collection.collection_id)
                .collect();
            ensure_freezes_on_top(conn, &dependants, at).await?;
            freeze_functions(conn, &dependants, &ctx, topology_at).await?;
            conn.scd2()
                .chunked()
                .close::<TriggerDB>(&table_ids, (), topology_at)
                .await?;
            conn.scd2()
                .close::<TriggerDB>(&collection.collection_id, topology_at)
                .await?;
            conn.scd2()
                .chunked()
                .close::<DependencyDB>(&table_ids, (), topology_at)
                .await?;
            conn.scd2()
                .close::<DependencyDB>(&collection.collection_id, topology_at)
                .await?;
            conn.scd2()
                .close::<TableDB>(&collection.collection_id, topology_at)
                .await?;
            conn.scd2()
                .close::<FunctionDB>(&collection.collection_id, topology_at)
                .await?;
            let table_tags = table_ids
                .iter()
                .map(|id| TagEntityId::try_from(*id))
                .collect::<Result<Vec<_>, _>>()?;
            close_tags_of(conn, TagEntityType::Table, &table_tags, topology_at).await?;
            let function_tags = functions
                .iter()
                .map(|f| TagEntityId::try_from(f.base.function_id))
                .collect::<Result<Vec<_>, _>>()?;
            close_tags_of(conn, TagEntityType::Function, &function_tags, topology_at).await?;
        }
        conn.scd2()
            .close::<CollectionDB>(&collection.collection_id, at)
            .await?;
        close_user_tags_of(
            conn,
            TagEntityType::Collection,
            &[TagEntityId::try_from(collection.collection_id)?],
            at,
        )
        .await?;
        update_deleted_workers(conn, &collection.collection_id, at).await?;
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
    at: LockInstant,
) -> Result<(), TdError> {
    let workers: Vec<WorkerDB> = conn
        .fetch_all(and!(
            *collection_id,
            or!(WorkerType::CronTrigger, WorkerType::StageTrigger)
        ))
        .await?;
    let stopped = stop_triggers(conn, &workers, at).await?;
    debug!("Set stopped status in {stopped} workers for collection {collection_id}.");

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
    use crate::tag::parent::test_support::function_parent;
    use std::str::FromStr;
    use td_bridge::connection::memory::InMemoryConnectionStore;
    use td_database::access::{AcrossProjects, Grant};
    use td_database::domain::DomainPools;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt as _;
    use td_error::{ApiError, TdError};
    use td_objects::dxo::collection::CollectionDB;
    use td_objects::dxo::collection::CollectionParam;
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
        UserId, WorkerStatus, WorkerType,
    };
    use td_objects::types::basic::{CollectionIdName, ReuseFrozen};
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
        .create_collection(create_request(
            p.project_id,
            "src0",
            Some(GroupName::source()),
        ))
        .await
        .unwrap();
        let connection = linked_connection(&db, p.project_id, "src0").await.unwrap();

        Services {
            bridge: bridge.clone(),
            ..Services::with_defaults(db.clone())
        }
        .delete_collection(delete_request(p.project_id, "src0"))
        .await
        .unwrap();

        let err = bridge
            .connection_store()
            .fetch(&p.project_id, &connection.id)
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
        .create_collection(create_request(
            p.project_id,
            "src0",
            Some(GroupName::source()),
        ))
        .await
        .unwrap();
        let connection = linked_connection(&db, p.project_id, "src0").await.unwrap();
        let name = CollectionName::try_from("src0").unwrap();
        let collection: CollectionDB = db
            .scoped(p.project_id)
            .scd2()
            .fetch_one(&name)
            .await
            .unwrap();
        let store = bridge.connection_store();
        let late = store.fetch(&p.project_id, &connection.id).await.unwrap();

        Services {
            bridge: bridge.clone(),
            ..Services::with_defaults(db.clone())
        }
        .delete_collection(delete_request(p.project_id, "src0"))
        .await
        .unwrap();
        store
            .upsert(
                &p.project_id,
                &collection.collection_id,
                &name,
                &connection.id,
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
            &DbPool::in_project(&pools, &p.project_id),
            Grant::asserted("reconcile"),
            store,
            &p.project_id,
            &collection.collection_id,
            &name,
            &connection.id,
        )
        .await
        .unwrap();

        assert!(matches!(reconciled, Reconciled::Deleted));
        store
            .fetch(&p.project_id, &connection.id)
            .await
            .expect_err("a write after the delete must be dropped");
    }

    #[td_test::test(db)]
    async fn test_delete_closes_the_parent_tags_of_its_functions(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection = db
            .scoped(p.base.project_id)
            .seed()
            .collection(&CollectionName::try_from("c")?, &UserId::admin())
            .await?;
        let register = |name: &str| -> Result<FunctionRegister, TdError> {
            Ok(FunctionRegister {
                child_functions: None,
                name: name.try_into()?,
                description: "description".try_into()?,
                bundle_id: BundleId::default(),
                snippet: "snippet".try_into()?,
                decorator: Decorator::Publisher,
                connector: None,
                connection: None,
                base_path: None,
                dependencies: None,
                triggers: None,
                tables: None,
                table_types: None,
                parent_tables: None,
                runtime_values: FunctionRuntimeValues::default(),
                reuse_frozen_tables: ReuseFrozen::FALSE,
                enabled: None,
                workers: None,
            })
        };
        let owner = db
            .scoped(p.base.project_id)
            .seed()
            .function(&collection, &register("owner")?)
            .await?;
        let child = db
            .scoped(p.base.project_id)
            .seed()
            .child_function(&collection, &register("child")?, &owner.base.function_id)
            .await?;

        Services::with_defaults(db.clone())
            .delete_collection(delete_request(p.base.project_id, "c"))
            .await?;

        assert_eq!(function_parent(&db, &child.base.function_id).await?, None);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_delete_collection(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let name = CollectionName::try_from("c")?;
        let collection = db
            .scoped(p.project_id)
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
                .scoped(p.project_id)
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
            .scoped(p.project_id)
            .seed()
            .function(&collection, &create)
            .await?;

        let _cron_worker = db
            .scoped(p.project_id)
            .seed()
            .worker(
                &collection,
                &function,
                &WorkerType::CronTrigger,
                &WorkerStatus::Running,
            )
            .await?;

        let execution = db.scoped(p.project_id).seed().execution(&function).await?;
        let transaction_key = TransactionKey::try_from("ANY").unwrap();
        let transaction = db
            .scoped(p.project_id)
            .seed()
            .transaction(&execution, &transaction_key)
            .await?;
        let physical_function_run = db
            .scoped(p.project_id)
            .seed()
            .physical_function_run(
                &collection,
                &function,
                &execution,
                &transaction,
                &LogicalFunctionRunStatus::Requested,
            )
            .await?;
        let _function_worker = db
            .scoped(p.project_id)
            .seed()
            .function_runner_worker(&execution, &transaction, &physical_function_run)
            .await?;

        Services::with_defaults(db.clone())
            .delete_collection(delete_request(p.project_id, &name.to_string()))
            .await?;

        let found = db.scd2().fetch_all::<CollectionDB>(()).await?;
        assert_eq!(found.len(), 0);

        let timeline = db
            .scoped(p.project_id)
            .timeline()
            .fetch_all::<CollectionDB>(&collection.collection_id)
            .await?;
        assert_eq!(timeline.len(), 1);
        assert_eq!(timeline[0].name, collection.name);
        assert!(!timeline[0].interval.valid_to.is_infinity());

        let found = db.scd2().fetch_all::<FunctionDBWithNames>(()).await?;
        assert!(found.is_empty());
        let versions = db
            .timeline()
            .fetch_all::<FunctionDB>(&collection.collection_id)
            .await?;
        assert_eq!(versions.len(), 4);
        assert!(versions.iter().all(|f| !f.interval.valid_to.is_infinity()));

        let found = db.scd2().fetch_all::<TableDBWithNames>(()).await?;
        assert!(found.is_empty());
        let versions = db
            .timeline()
            .fetch_all::<TableDB>(&collection.collection_id)
            .await?;
        assert_eq!(versions.len(), 4);
        assert!(versions.iter().all(|t| !t.interval.valid_to.is_infinity()));

        let found = db.scd2().fetch_all::<TriggerDBWithNames>(()).await?;
        assert!(found.is_empty());
        let versions = db
            .timeline()
            .fetch_all::<TriggerDB>(&collection.collection_id)
            .await?;
        assert_eq!(versions.len(), 2);
        assert!(versions.iter().all(|t| !t.interval.valid_to.is_infinity()));

        let found = db.scd2().fetch_all::<DependencyDBWithNames>(()).await?;
        assert!(found.is_empty());
        let versions = db
            .timeline()
            .fetch_all::<DependencyDB>(&collection.collection_id)
            .await?;
        assert_eq!(versions.len(), 1);
        assert!(versions.iter().all(|d| !d.interval.valid_to.is_infinity()));

        let found = db.fetch_all::<WorkerDB>(()).await?;
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
            .scoped(p.project_id)
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
            .scoped(p.project_id)
            .seed()
            .function(&collection_0, &create)
            .await?;

        let name_c_1 = CollectionName::try_from("c_1")?;
        let collection_1 = db
            .scoped(p.project_id)
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
            .scoped(p.project_id)
            .seed()
            .function(&collection_1, &create)
            .await?;

        Services::with_defaults(db.clone())
            .delete_collection(delete_request(p.project_id, &name_c_0.to_string()))
            .await?;

        let found = db.scd2().fetch_all::<CollectionDB>(()).await?;
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
        create_collection(&db, p.project_id, "dev_src_d", Some(GroupName::source())).await;
        let dev = seed_dev_user(&db, p.project_id).await;
        let request = RequestContext::with(AccessTokenId::default(), dev)
            .delete(CollectionParam {
                collection: CollectionIdName::from_str("dev_src_d").unwrap(),
            })
            .in_project(p.project_id);
        let err = Services::with_defaults(db.clone())
            .delete_collection(request)
            .await
            .expect_err("develop_project must not suffice to delete a source collection");
        assert_eq!(err.api_error(), ApiError::Forbidden);
    }
}
