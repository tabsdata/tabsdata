//
// Copyright 2024 Tabs Data Inc.
//

use crate::collection::service::layer::delete::{
    build_deleted_functions, build_deleted_tables, update_deleted_workers,
};
use crate::table::layers::delete::{
    build_deleted_dependencies, build_deleted_triggers, build_frozen_functions,
};
use ta_services::factory::service_factory;
use td_authz::tower::{AuthzOn, SysAdmin, System};
use td_authz::{Authz, AuthzContext};
use td_objects::dxo::collection::{CollectionDB, CollectionDeleteDB, CollectionDeleteDBBuilder};
use td_objects::dxo::crudl::{DeleteRequest, RequestContext};
use td_objects::dxo::dependency::DependencyDB;
use td_objects::dxo::function::FunctionDB;
use td_objects::dxo::table::TableDB;
use td_objects::dxo::trigger::TriggerDB;
use td_objects::rest_urls::CollectionParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{CollectionId, CollectionIdName, FunctionId, TableId};
use td_queries::By;
use td_queries::insert_vec;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = DeleteCollectionService,
    request = DeleteRequest<CollectionParam>,
    response = (),
    connection = TransactionProvider,
    scope = "Project",
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<DeleteRequest<CollectionParam>>::extract::<RequestContext>),
        from_fn(AuthzOn::<System>::set),
        from_fn(Authz::<SysAdmin>::check),
        from_fn(With::<DeleteRequest<CollectionParam>>::extract_name::<CollectionParam>),
        // Get collection
        from_fn(With::<CollectionParam>::extract::<CollectionIdName>),
        from_fn(By::<CollectionIdName>::fetch_one::<CollectionDB>),
        // Build deleted collection
        from_fn(With::<CollectionDB>::try_convert_to::<CollectionDeleteDBBuilder, _>),
        from_fn(With::<RequestContext>::update::<CollectionDeleteDBBuilder, _>),
        from_fn(With::<CollectionDeleteDBBuilder>::build::<CollectionDeleteDB>),
        // Update collection
        from_fn(With::<CollectionDB>::extract::<CollectionId>),
        from_fn(By::<CollectionId>::update::<CollectionDeleteDB, CollectionDB>),
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_database::scoped::InProject;
    use td_error::TdError;
    use td_objects::Buildable;
    use td_objects::dxo::collection::CollectionCreateDB;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::dependency::DependencyDBWithNames;
    use td_objects::dxo::function::{FunctionDBWithNames, FunctionRegister};
    use td_objects::dxo::table::TableDBWithNames;
    use td_objects::dxo::trigger::TriggerDBWithNames;
    use td_objects::dxo::worker::WorkerDB;
    use td_objects::rest_urls::CollectionParam;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, DependencyStatus, FunctionName,
        FunctionRuntimeValues, FunctionStatus, LogicalFunctionRunStatus, ProjectId, RoleId,
        TableName, TableNameDto, TableStatus, TransactionKey, TriggerStatus, UserId,
        WorkerMessageStatus, WorkerStatus, WorkerType,
    };
    use td_objects::types::composed::{TableDependencyDto, TableTriggerDto};
    use td_queries::test_utils::SeedEntry;
    use td_queries::{CurrentEntry, LiveEntry};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_delete_collection(db: TdPool) {
        use td_database::scoped::Scoped;
        use td_tower::metadata::type_of_val;

        DeleteCollectionService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<Scoped<DeleteRequest<CollectionParam>, ProjectId>, ()>(&[
                type_of_val(&With::<DeleteRequest<CollectionParam>>::extract::<RequestContext>),
                type_of_val(&AuthzOn::<System>::set),
                type_of_val(&Authz::<SysAdmin>::check),
                type_of_val(
                    &With::<DeleteRequest<CollectionParam>>::extract_name::<CollectionParam>,
                ),
                // Get collection
                type_of_val(&With::<CollectionParam>::extract::<CollectionIdName>),
                type_of_val(&By::<CollectionIdName>::fetch_one::<CollectionDB>),
                // Build deleted collection
                type_of_val(&With::<CollectionDB>::try_convert_to::<CollectionDeleteDBBuilder, _>),
                type_of_val(&With::<RequestContext>::update::<CollectionDeleteDBBuilder, _>),
                type_of_val(&With::<CollectionDeleteDBBuilder>::build::<CollectionDeleteDB>),
                // Update collection
                type_of_val(&With::<CollectionDB>::extract::<CollectionId>),
                type_of_val(&By::<CollectionId>::update::<CollectionDeleteDB, CollectionDB>),
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

    #[td_test::test(db)]
    async fn test_delete_collection(db: TdPool) -> Result<(), TdError> {
        // Create 3 functions and 3 tables in the same collection
        let name = CollectionName::try_from("c")?;
        let collection = db
            .scoped(ProjectId::sandbox())
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
                .scoped(ProjectId::sandbox())
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
            .scoped(ProjectId::sandbox())
            .seed()
            .function(&collection, &create)
            .await?;

        // Create workers for function_3
        let _cron_worker = db
            .scoped(ProjectId::sandbox())
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
        let execution = db
            .scoped(ProjectId::sandbox())
            .seed()
            .execution(&function)
            .await?;
        let transaction_key = TransactionKey::try_from("ANY").unwrap();
        let transaction = db
            .scoped(ProjectId::sandbox())
            .seed()
            .transaction(&execution, &transaction_key)
            .await?;
        let physical_function_run = db
            .scoped(ProjectId::sandbox())
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
            .scoped(ProjectId::sandbox())
            .seed()
            .function_runner_worker(
                &execution,
                &transaction,
                &physical_function_run,
                &WorkerMessageStatus::Unlocked,
            )
            .await?;

        // Test delete
        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sys_admin(),
        )
        .delete(
            CollectionParam::builder()
                .collection(CollectionIdName::from_str(&name.to_string())?)
                .build()?,
        )
        .in_project(ProjectId::sandbox());

        DeleteCollectionService::with_defaults(db.clone())
            .service()
            .raw_oneshot(request)
            .await?;

        // Assert collection is deleted
        let found = db.live().fetch_all::<CollectionDB>(()).await?;
        assert_eq!(found.len(), 0);

        let res = db
            .live()
            .fetch_one::<CollectionCreateDB>(&collection.id)
            .await?;
        assert_eq!(res.name_when_deleted, Some(collection.name));

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
        let name_c_0 = CollectionName::try_from("c_0")?;
        let collection_0 = db
            .scoped(ProjectId::sandbox())
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
            .scoped(ProjectId::sandbox())
            .seed()
            .function(&collection_0, &create)
            .await?;

        let name_c_1 = CollectionName::try_from("c_1")?;
        let collection_1 = db
            .scoped(ProjectId::sandbox())
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
            .scoped(ProjectId::sandbox())
            .seed()
            .function(&collection_1, &create)
            .await?;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sys_admin(),
        )
        .delete(
            CollectionParam::builder()
                .collection(CollectionIdName::from_str(&name_c_0.to_string())?)
                .build()?,
        )
        .in_project(ProjectId::sandbox());

        DeleteCollectionService::with_defaults(db.clone())
            .service()
            .raw_oneshot(request)
            .await?;

        // Assert only c_1 is active
        let found = db.live().fetch_all::<CollectionDB>(()).await?;
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
}
