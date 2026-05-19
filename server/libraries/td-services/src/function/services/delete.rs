//
// Copyright 2025 Tabs Data Inc.
//

use crate::do_authz::SKIP_AUTHZ;
use crate::function::layers::delete::build_deleted_function_version;
use crate::function::layers::{
    register_dependencies, register_tables, register_triggers, register_workers,
};
use crate::scheduler::layers::request::notify_scheduler;
use std::collections::HashMap;
use ta_services::factory::service_factory;
use td_authz::tower::{AuthzOn, CollAdmin, CollDev};
use td_authz::{Authz, AuthzContext};
use td_execution::SchedulerNotify;
use td_objects::And;
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::crudl::{DeleteRequest, RequestContext};
use td_objects::dxo::dependency::DependencyDB;
use td_objects::dxo::function::{FunctionDB, FunctionDBBuilder, FunctionDBWithNames};
use td_objects::dxo::table::TableDB;
use td_objects::dxo::trigger::TriggerDBWithNames;
use td_objects::rest_urls::FunctionParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{
    AtTime, CollectionId, CollectionIdName, CollectionName, FunctionId, FunctionIdName,
    FunctionVersionId, ReuseFrozen, TableNameDto, TableType, WorkerConfig, WorkerTypeDto,
};
use td_objects::types::composed::{TableDependencyDto, TableTriggerDto};
use td_queries::By;
use td_queries::insert;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = DeleteFunctionService,
    request = DeleteRequest<FunctionParam>,
    response = (),
    connection = TransactionProvider,
    context = AuthzContext,
    context = SchedulerNotify,
)]
fn service() {
    layers!(
        from_fn(With::<DeleteRequest<FunctionParam>>::extract::<RequestContext>),
        from_fn(With::<DeleteRequest<FunctionParam>>::extract_name::<FunctionParam>),
        // Extract collection and function from request.
        from_fn(With::<FunctionParam>::extract::<CollectionIdName>),
        from_fn(With::<FunctionParam>::extract::<FunctionIdName>),
        // Get collection. Extract collection id and name.
        from_fn(By::<CollectionIdName>::fetch_one::<CollectionDB>),
        from_fn(With::<CollectionDB>::extract::<CollectionId>),
        // check requester is coll_admin or coll_dev for the function's collection
        from_fn(AuthzOn::<CollectionId>::set),
        from_fn(Authz::<CollAdmin, CollDev>::check),
        from_fn(With::<CollectionDB>::extract::<CollectionName>),
        // Get function. Extract function version id.
        from_fn(With::<RequestContext>::extract::<AtTime>),
        from_fn(
            By::<And![CollectionIdName, FunctionIdName]>::historic_fetch_one::<
                { FunctionDBWithNames::Available },
                FunctionDBWithNames,
            >
        ),
        from_fn(With::<FunctionDBWithNames>::extract::<FunctionId>),
        from_fn(With::<FunctionDBWithNames>::extract::<FunctionVersionId>),
        // Insert into function_versions(sql) status=Deleted.
        from_fn(By::<FunctionVersionId>::fetch_one::<FunctionDB>),
        from_fn(With::<FunctionDB>::try_convert_to::<FunctionDBBuilder, _>),
        from_fn(With::<RequestContext>::update::<FunctionDBBuilder, _>),
        from_fn(build_deleted_function_version),
        from_fn(insert::<FunctionDB>),
        // Register associations
        // Find previous versions.
        from_fn(With::<FunctionVersionId>::some),
        from_fn(By::<FunctionId>::historic_fetch_all::<{ TableDB::Active }, TableDB>),
        from_fn(By::<FunctionId>::historic_fetch_all::<{ DependencyDB::Active }, DependencyDB>),
        from_fn(
            By::<FunctionId>::historic_fetch_all::<
                { TriggerDBWithNames::Available },
                TriggerDBWithNames,
            >
        ),
        // Extract new associations (empty because it is a delete operation).
        from_fn(With::<Option<Vec<TableNameDto>>>::default),
        from_fn(With::<Option<HashMap<TableNameDto, TableType>>>::default),
        from_fn(With::<Option<Vec<TableDependencyDto>>>::default),
        from_fn(With::<Option<Vec<TableTriggerDto>>>::default),
        // Create an empty hashmap as workers so all the previous workers get deleted
        from_fn(With::<HashMap<WorkerTypeDto, WorkerConfig>>::default),
        from_fn(With::<HashMap<WorkerTypeDto, WorkerConfig>>::some),
        // Extract reuse frozen (default as deletes are not reusing anything)
        from_fn(With::<ReuseFrozen>::default),
        // And register new ones
        register_tables(),
        register_dependencies::<_, SKIP_AUTHZ>(),
        register_triggers::<_, SKIP_AUTHZ>(),
        register_workers(),
        // Wake the scheduler so cron/stage workers stop immediately
        // instead of at the next polling tick.
        from_fn(notify_scheduler),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::function::services::register::RegisterFunctionService;
    use crate::function::services::tests::{assert_delete, assert_register};
    use std::str::FromStr;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::Buildable;
    use td_objects::dxo::function::{FunctionBuilder, FunctionRegister};
    use td_objects::dxo::worker::WorkerDB;
    use td_objects::rest_urls::CollectionParam;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, Decorator, FunctionRuntimeValues, RoleId, UserId,
        WorkerMessageStatus, WorkerStatus, WorkerType,
    };
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_delete_function(db: TdPool) {
        use td_tower::metadata::type_of_val;

        use crate::function::layers::register::{
            build_dependency_versions, build_table_versions, build_tables_trigger_versions,
            build_trigger_versions, build_workers, stop_current_workers,
        };
        use td_objects::dxo::dependency::DependencyDBBuilder;
        use td_objects::dxo::table::TableDBBuilder;
        use td_objects::dxo::trigger::{TriggerDB, TriggerDBBuilder, TriggerDBWithNames};
        use td_objects::dxo::worker::{WorkerDB, WorkerDBBuilder};
        use td_objects::types::basic::ReuseFrozen;
        use td_queries::insert_vec;

        DeleteFunctionService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<DeleteRequest<FunctionParam>, ()>(&[
                type_of_val(&With::<DeleteRequest<FunctionParam>>::extract::<RequestContext>),
                type_of_val(&With::<DeleteRequest<FunctionParam>>::extract_name::<FunctionParam>),
                // Extract collection and function from request.
                type_of_val(&With::<FunctionParam>::extract::<CollectionIdName>),
                type_of_val(&With::<FunctionParam>::extract::<FunctionIdName>),
                // Get collection. Extract collection id and name.
                type_of_val(&By::<CollectionIdName>::fetch_one::<CollectionDB>),
                type_of_val(&With::<CollectionDB>::extract::<CollectionId>),
                // check requester is coll_admin or coll_dev for the function's collection
                type_of_val(&AuthzOn::<CollectionId>::set),
                type_of_val(&Authz::<CollAdmin, CollDev>::check),
                type_of_val(&With::<CollectionDB>::extract::<CollectionName>),
                // Get function. Extract function version id.
                type_of_val(&With::<RequestContext>::extract::<AtTime>),
                type_of_val(
                    &By::<And![CollectionIdName, FunctionIdName]>::historic_fetch_one::<
                        { FunctionDBWithNames::Available },
                        FunctionDBWithNames,
                    >,
                ),
                type_of_val(&With::<FunctionDBWithNames>::extract::<FunctionId>),
                type_of_val(&With::<FunctionDBWithNames>::extract::<FunctionVersionId>),
                // Insert into function_versions(sql) status=Deleted.
                type_of_val(&By::<FunctionVersionId>::fetch_one::<FunctionDB>),
                type_of_val(&With::<FunctionDB>::try_convert_to::<FunctionDBBuilder, _>),
                type_of_val(&With::<RequestContext>::update::<FunctionDBBuilder, _>),
                type_of_val(&build_deleted_function_version),
                type_of_val(&insert::<FunctionDB>),
                // Register associations
                // Find previous versions.
                type_of_val(&With::<FunctionVersionId>::some),
                type_of_val(&By::<FunctionId>::historic_fetch_all::<{ TableDB::Active }, TableDB>),
                type_of_val(
                    &By::<FunctionId>::historic_fetch_all::<{ DependencyDB::Active }, DependencyDB>,
                ),
                type_of_val(
                    &By::<FunctionId>::historic_fetch_all::<
                        { TriggerDBWithNames::Available },
                        TriggerDBWithNames,
                    >,
                ),
                // Extract new associations (empty because it is a delete operation).
                type_of_val(&With::<Option<Vec<TableNameDto>>>::default),
                type_of_val(&With::<Option<HashMap<TableNameDto, TableType>>>::default),
                type_of_val(&With::<Option<Vec<TableDependencyDto>>>::default),
                type_of_val(&With::<Option<Vec<TableTriggerDto>>>::default),
                // Create an empty hashmap as workers so all the previous workers get deleted
                type_of_val(&With::<HashMap<WorkerTypeDto, WorkerConfig>>::default),
                type_of_val(&With::<HashMap<WorkerTypeDto, WorkerConfig>>::some),
                // Extract reuse frozen (default as deletes are not reusing anything)
                type_of_val(&With::<ReuseFrozen>::default),
                // Insert into table_versions(sql) current function tables status=Active.
                // Reuse table_id for tables that existed (had status=Frozen)
                type_of_val(&With::<FunctionDB>::try_convert_to::<TableDBBuilder, _>),
                type_of_val(&With::<RequestContext>::update::<TableDBBuilder, _>),
                type_of_val(&build_table_versions),
                type_of_val(&insert_vec::<TableDB>),
                // Insert into trigger_versions(sql) downstream table triggers updates.
                type_of_val(&build_tables_trigger_versions),
                type_of_val(&insert_vec::<TriggerDB>),
                // Build dependency_versions(sql) current function table dependencies status=Active.
                type_of_val(&With::<FunctionDB>::try_convert_to::<DependencyDBBuilder, _>),
                type_of_val(&With::<RequestContext>::update::<DependencyDBBuilder, _>),
                type_of_val(&build_dependency_versions),
                type_of_val(&insert_vec::<DependencyDB>),
                // Insert into trigger_versions(sql) current function trigger status=Active.
                type_of_val(&With::<FunctionDB>::try_convert_to::<TriggerDBBuilder, _>),
                type_of_val(&With::<RequestContext>::update::<TriggerDBBuilder, _>),
                type_of_val(&build_trigger_versions),
                type_of_val(&insert_vec::<TriggerDB>),
                // Set stopped for current workers and insert into workers(sql) new workers.
                type_of_val(&stop_current_workers),
                type_of_val(&With::<FunctionDB>::try_convert_to::<WorkerDBBuilder, _>),
                type_of_val(&build_workers),
                type_of_val(&insert_vec::<WorkerDB>),
                type_of_val(&notify_scheduler),
            ]);
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_delete_function(db: TdPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let create = FunctionRegister::builder()
            .try_name("joaquin_workout")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(None)
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let created_function = db.seed().function(&collection, &create).await?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).delete(
                FunctionParam::builder()
                    .collection(CollectionIdName::from_str(&format!("~{}", collection.id))?)
                    .function(FunctionIdName::from_str("joaquin_workout")?)
                    .build()?,
            );

        let service = DeleteFunctionService::with_defaults(db.clone()).service();
        service.raw_oneshot(request).await?;

        assert_delete(
            &db,
            &UserId::admin(),
            &collection,
            &create,
            &created_function,
        )
        .await
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_delete_function_with_tables(db: TdPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let create = FunctionRegister::builder()
            .try_name("joaquin_workout")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![
                TableNameDto::try_from("table1")?,
                TableNameDto::try_from("table2")?,
            ]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let created_function = db.seed().function(&collection, &create).await?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).delete(
                FunctionParam::builder()
                    .collection(CollectionIdName::from_str(&format!("{}", collection.name))?)
                    .function(FunctionIdName::from_str("joaquin_workout")?)
                    .build()?,
            );

        let service = DeleteFunctionService::with_defaults(db.clone()).service();
        service.raw_oneshot(request).await?;

        assert_delete(
            &db,
            &UserId::admin(),
            &collection,
            &create,
            &created_function,
        )
        .await
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_delete_function_with_dependencies(db: TdPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let trigger_create = FunctionRegister::builder()
            .try_name("the_trigger")?
            .try_description("wanted")?
            .bundle_id(BundleId::default())
            .try_snippet("the_trigger snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![TableNameDto::try_from("trigger_table")?]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let _ = db.seed().function(&collection, &trigger_create).await?;

        let create = FunctionRegister::builder()
            .try_name("joaquin_workout")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(Some(vec![TableDependencyDto::try_from("trigger_table")?]))
            .triggers(None)
            .tables(None)
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let created_function = db.seed().function(&collection, &create).await?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).delete(
                FunctionParam::builder()
                    .collection(CollectionIdName::from_str(&format!("{}", collection.name))?)
                    .function(FunctionIdName::from_str("joaquin_workout")?)
                    .build()?,
            );

        let service = DeleteFunctionService::with_defaults(db.clone()).service();
        service.raw_oneshot(request).await?;

        assert_delete(
            &db,
            &UserId::admin(),
            &collection,
            &create,
            &created_function,
        )
        .await
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_delete_function_with_triggers(db: TdPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let trigger_create = FunctionRegister::builder()
            .try_name("the_trigger")?
            .try_description("wanted")?
            .bundle_id(BundleId::default())
            .try_snippet("the_trigger snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![TableNameDto::try_from("trigger_table")?]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let _ = db.seed().function(&collection, &trigger_create).await?;

        let create = FunctionRegister::builder()
            .try_name("joaquin_workout")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(Some(vec![TableTriggerDto::try_from("trigger_table")?]))
            .tables(None)
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let created_function = db.seed().function(&collection, &create).await?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).delete(
                FunctionParam::builder()
                    .collection(CollectionIdName::from_str(&format!("{}", collection.name))?)
                    .function(FunctionIdName::from_str("joaquin_workout")?)
                    .build()?,
            );

        let service = DeleteFunctionService::with_defaults(db.clone()).service();
        service.raw_oneshot(request).await?;

        assert_delete(
            &db,
            &UserId::admin(),
            &collection,
            &create,
            &created_function,
        )
        .await
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_delete_function_unique(db: TdPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let trigger_create = FunctionRegister::builder()
            .try_name("the_trigger")?
            .try_description("wanted")?
            .bundle_id(BundleId::default())
            .try_snippet("the_trigger snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![TableNameDto::try_from("trigger_table")?]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let _ = db.seed().function(&collection, &trigger_create).await?;

        let create_1 = FunctionRegister::builder()
            .try_name("joaquin_workout_1")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(Some(vec![TableDependencyDto::try_from("trigger_table")?]))
            .triggers(Some(vec![TableTriggerDto::try_from("trigger_table")?]))
            .tables(Some(vec![TableNameDto::try_from("workout_1")?]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name.as_str())?)
                    .build()?,
                create_1.clone(),
            );

        let service = RegisterFunctionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let created_function_version_1 = response?;

        let create_2 = FunctionRegister::builder()
            .try_name("joaquin_workout_2")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(Some(vec![TableDependencyDto::try_from("trigger_table")?]))
            .triggers(Some(vec![TableTriggerDto::try_from("trigger_table")?]))
            .tables(Some(vec![TableNameDto::try_from("workout_2")?]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let created_function_2 = db.seed().function(&collection, &create_2).await?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).delete(
                FunctionParam::builder()
                    .collection(CollectionIdName::from_str(&format!("~{}", collection.id))?)
                    .function(FunctionIdName::from_str("joaquin_workout_2")?)
                    .build()?,
            );

        let service = DeleteFunctionService::with_defaults(db.clone()).service();
        service.raw_oneshot(request).await?;

        // Assert that the first function is as if it just got registered
        let function_version: FunctionDBWithNames = db
            .live()
            .fetch_one::<FunctionDBWithNames>(&(&created_function_version_1.id))
            .await?;
        let function_version = FunctionBuilder::try_from(&function_version)?.build()?;
        assert_register(
            &db,
            &UserId::admin(),
            &collection,
            &create_1,
            &function_version,
        )
        .await?;

        // But the second function is deleted
        assert_delete(
            &db,
            &UserId::admin(),
            &collection,
            &create_2,
            &created_function_2,
        )
        .await
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_delete_workers(db: TdPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        // register a function
        let create = FunctionRegister::builder()
            .try_name("function_1")?
            .try_description("wanted")?
            .bundle_id(BundleId::default())
            .try_snippet("the_trigger snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![TableNameDto::try_from("trigger_table")?]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .workers(Some(HashMap::from([(
                WorkerTypeDto::CronTrigger,
                WorkerConfig::default(),
            )])))
            .build()?;
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name.as_str())?)
                    .build()?,
                create,
            );
        let service = RegisterFunctionService::with_defaults(db.clone()).service();
        let function = service.raw_oneshot(request).await?;

        // and then just deleted
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).delete(
                FunctionParam::builder()
                    .collection(CollectionIdName::from_str(&format!("~{}", collection.id))?)
                    .function(FunctionIdName::from_str("function_1")?)
                    .build()?,
            );
        let service = DeleteFunctionService::with_defaults(db.clone()).service();
        service.raw_oneshot(request).await?;

        // check its workers also got stopped
        let workers: Vec<WorkerDB> = db.live().fetch_all::<WorkerDB>(()).await.unwrap();
        assert_eq!(workers.len(), 1);
        let worker = &workers[0];
        assert_eq!(worker.collection_id, collection.id);
        assert_eq!(worker.execution_id, None);
        assert_eq!(worker.transaction_id, None);
        assert_eq!(worker.physical_function_run_id, None);
        assert_eq!(worker.function_version_id, function.id);
        assert_eq!(worker.message_status, WorkerMessageStatus::None);
        assert_eq!(worker.started_on, None);
        assert_eq!(worker.ended_on, None);
        assert_eq!(worker.status, WorkerStatus::StopScheduled);
        assert_eq!(worker.worker_type, WorkerType::CronTrigger);
        assert_eq!(worker.worker_config, Some(WorkerConfig::default()));
        Ok(())
    }
}
