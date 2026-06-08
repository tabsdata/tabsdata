//
// Copyright 2025 Tabs Data Inc.
//

use crate::do_authz::DO_AUTHZ;
use crate::function::layers::register::{data_location, validate_tables_do_not_exist};
use crate::function::layers::update::{
    assert_function_name_not_exists, update_function_db_builder,
};
use crate::function::layers::{
    check_private_tables, register_dependencies, register_tables, register_triggers,
    register_workers,
};
use crate::scheduler::layers::request::notify_scheduler;
use std::collections::HashMap;
use ta_services::factory::service_factory;
use td_authz::tower::{AuthzOn, CollAdmin, CollDev};
use td_authz::{Authz, AuthzContext};
use td_execution::SchedulerNotify;
use td_objects::And;
use td_objects::dxo::bundle::BundleDB;
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::crudl::{RequestContext, UpdateRequest};
use td_objects::dxo::dependency::DependencyDB;
use td_objects::dxo::function::{
    Function, FunctionBuilder, FunctionDB, FunctionDBBuilder, FunctionDBWithNames, FunctionUpdate,
};
use td_objects::dxo::table::TableDB;
use td_objects::dxo::trigger::TriggerDBWithNames;
use td_objects::rest_urls::FunctionParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{
    AtTime, BundleId, CollectionId, CollectionIdName, CollectionName, DataLocation, FunctionId,
    FunctionIdName, FunctionVersionId, ResetState, ReuseFrozen, StorageVersion, TableNameDto,
    TableType, WorkerConfig, WorkerTypeDto,
};
use td_objects::types::composed::{TableDependencyDto, TableTriggerDto};
use td_queries::By;
use td_queries::insert;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = UpdateFunctionService,
    request = UpdateRequest<FunctionParam, FunctionUpdate>,
    response = Function,
    connection = TransactionProvider,
    scope = "Project",
    context = AuthzContext,
    context = SchedulerNotify,
)]
fn service() {
    layers!(
        from_fn(With::<UpdateRequest<FunctionParam, FunctionUpdate>>::extract::<RequestContext>),
        from_fn(
            With::<UpdateRequest<FunctionParam, FunctionUpdate>>::extract_name::<FunctionParam>
        ),
        from_fn(
            With::<UpdateRequest<FunctionParam, FunctionUpdate>>::extract_data::<FunctionUpdate>
        ),
        // Extract collection and current function from request.
        from_fn(With::<FunctionParam>::extract::<CollectionIdName>),
        from_fn(With::<FunctionParam>::extract::<FunctionIdName>),
        // Get collection. Extract collection id and name.
        from_fn(By::<CollectionIdName>::fetch_one::<CollectionDB>),
        from_fn(With::<CollectionDB>::extract::<CollectionId>),
        // check requester is coll_admin or coll_dev for the function's collection
        from_fn(AuthzOn::<CollectionId>::set),
        from_fn(Authz::<CollAdmin, CollDev>::check),
        from_fn(With::<CollectionDB>::extract::<CollectionName>),
        // Get function. Extract function id and name.
        from_fn(With::<RequestContext>::extract::<AtTime>),
        from_fn(
            By::<And![CollectionIdName, FunctionIdName]>::historic_fetch_one::<
                { FunctionDBWithNames::Available },
                FunctionDBWithNames,
            >
        ),
        // This is, before update function id and function version id. Function id does
        // not change, but function version id does.
        from_fn(With::<FunctionDBWithNames>::extract::<FunctionId>),
        from_fn(With::<FunctionDBWithNames>::extract::<FunctionVersionId>),
        // If function has a new name, check new name does not exist in collection.
        from_fn(assert_function_name_not_exists),
        // Get location and storage version.
        from_fn(With::<StorageVersion>::default),
        from_fn(data_location),
        // Insert into function_versions(sql) status=Active.
        from_fn(By::<FunctionVersionId>::fetch_one::<FunctionDB>),
        from_fn(With::<FunctionDB>::try_convert_to::<FunctionDBBuilder, _>),
        from_fn(update_function_db_builder),
        from_fn(With::<RequestContext>::update::<FunctionDBBuilder, _>),
        from_fn(With::<StorageVersion>::set::<FunctionDBBuilder>),
        from_fn(With::<DataLocation>::set::<FunctionDBBuilder>),
        from_fn(With::<FunctionDBBuilder>::build::<FunctionDB>),
        from_fn(insert::<FunctionDB>),
        // Remove from bundles
        from_fn(With::<FunctionDB>::extract::<BundleId>),
        from_fn(By::<BundleId>::delete::<BundleDB>),
        // Register associations
        // Find previous versions
        from_fn(With::<FunctionVersionId>::some),
        from_fn(By::<FunctionId>::historic_fetch_all::<{ TableDB::Available }, TableDB>),
        from_fn(By::<FunctionId>::historic_fetch_all::<{ DependencyDB::Active }, DependencyDB>),
        from_fn(
            By::<FunctionId>::historic_fetch_all::<
                { TriggerDBWithNames::Available },
                TriggerDBWithNames,
            >
        ),
        // Extract new associations
        from_fn(With::<FunctionUpdate>::extract::<Option<Vec<TableNameDto>>>),
        from_fn(With::<FunctionUpdate>::extract::<Option<HashMap<TableNameDto, TableType>>>),
        from_fn(With::<FunctionUpdate>::extract::<Option<Vec<TableDependencyDto>>>),
        from_fn(With::<FunctionUpdate>::extract::<Option<Vec<TableTriggerDto>>>),
        from_fn(With::<FunctionUpdate>::extract::<Option<HashMap<WorkerTypeDto, WorkerConfig>>>),
        // Validate tables do not exist
        from_fn(validate_tables_do_not_exist),
        // check private tables
        from_fn(check_private_tables::<TableDependencyDto>),
        from_fn(check_private_tables::<TableTriggerDto>),
        // Extract reuse frozen
        from_fn(With::<FunctionUpdate>::extract::<ReuseFrozen>),
        // Extract reset state (regenerate the function state table id)
        from_fn(With::<FunctionUpdate>::extract::<ResetState>),
        // And register new ones
        register_tables(),
        register_dependencies::<_, DO_AUTHZ>(),
        register_triggers::<_, DO_AUTHZ>(),
        register_workers(),
        // Response
        // Extract new function version id
        from_fn(With::<FunctionDB>::extract::<FunctionVersionId>),
        from_fn(By::<FunctionVersionId>::fetch_one::<FunctionDBWithNames>),
        from_fn(With::<FunctionDBWithNames>::try_convert_to::<FunctionBuilder, _>),
        from_fn(With::<FunctionBuilder>::build::<Function>),
        // Wake the scheduler so worker rotations (stop old / start new
        // cron/stage workers) are picked up immediately.
        from_fn(notify_scheduler),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::function::layers::register::RegisterFunctionError;
    use crate::function::services::register::RegisterFunctionService;
    use crate::function::services::tests::{assert_register, assert_update};
    use std::collections::HashMap;
    use std::ops::Deref;
    use std::str::FromStr;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::dxo::trigger::TriggerDB;
    use td_objects::dxo::worker::WorkerDB;
    use td_objects::rest_urls::CollectionParam;
    use td_objects::system::TD_FN_STATE_PREFIX;
    use td_objects::types::basic::{
        AccessTokenId, Decorator, Depth, FunctionRuntimeValues, ProjectId, RoleId, TableId,
        TableName, TableStatus, ToCollectionId, UserId, WorkerMessageStatus, WorkerStatus,
        WorkerType,
    };
    use td_objects::types::recursion::RecursionOptions;
    use td_objects::{Buildable, and};
    use td_queries::test_utils::SeedEntry;
    use td_queries::{CurrentEntry, HistoricEntry, LiveEntry};
    use td_tower::ctx_service::RawOneshot;
    use td_tower::project_scope::InProject;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_update_function(db: TdPool) {
        use crate::function::layers::register::{
            build_dependency_versions, build_table_versions, build_tables_trigger_versions,
            build_trigger_versions, build_workers, stop_current_workers,
        };
        use td_authz::tower::InterColl;
        use td_objects::dxo::dependency::DependencyDBBuilder;
        use td_objects::dxo::inter_collection_access::{
            InterCollectionAccess, InterCollectionAccessBuilder,
        };
        use td_objects::dxo::table::TableDBBuilder;
        use td_objects::dxo::trigger::{TriggerDB, TriggerDBBuilder, TriggerDBWithNames};
        use td_objects::dxo::worker::{WorkerDB, WorkerDBBuilder};
        use td_objects::types::basic::{ResetState, ReuseFrozen};
        use td_queries::insert_vec;

        use td_tower::metadata::type_of_val;
        use td_tower::project_scope::Scoped;

        UpdateFunctionService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<Scoped<UpdateRequest<FunctionParam, FunctionUpdate>, ProjectId>, Function>(
            &[
                type_of_val(&With::<UpdateRequest<FunctionParam, FunctionUpdate>>::extract::<RequestContext>),
                type_of_val(&With::<UpdateRequest<FunctionParam, FunctionUpdate>>::extract_name::<FunctionParam>),
                type_of_val(&With::<UpdateRequest<FunctionParam, FunctionUpdate>>::extract_data::<FunctionUpdate>),
                // Extract collection and current function from request.
                type_of_val(&With::<FunctionParam>::extract::<CollectionIdName>),
                type_of_val(&With::<FunctionParam>::extract::<FunctionIdName>),
                // Get collection. Extract collection id and name.
                type_of_val(&By::<CollectionIdName>::fetch_one::<CollectionDB>),
                type_of_val(&With::<CollectionDB>::extract::<CollectionId>),
                // check requester is coll_admin or coll_dev for the function's collection
                type_of_val(&AuthzOn::<CollectionId>::set),
                type_of_val(&Authz::<CollAdmin, CollDev>::check),

                type_of_val(&With::<CollectionDB>::extract::<CollectionName>),
                // Get function. Extract function id and name.
                type_of_val(&With::<RequestContext>::extract::<AtTime>),
                type_of_val(&By::<And![CollectionIdName, FunctionIdName]>::historic_fetch_one::<
                    { FunctionDBWithNames::Available },
                    FunctionDBWithNames,
                >),
                // This is, before update function id and function version id. Function id does
                // not change, but function version id does.
                type_of_val(&With::<FunctionDBWithNames>::extract::<FunctionId>),
                type_of_val(&With::<FunctionDBWithNames>::extract::<FunctionVersionId>),
                // If function has a new name, check new name does not exist in collection.
                type_of_val(&assert_function_name_not_exists),
                // Get location and storage version.
                type_of_val(&With::<StorageVersion>::default),
                type_of_val(&data_location),
                // Insert into function_versions(sql) status=Active.
                type_of_val(&By::<FunctionVersionId>::fetch_one::<FunctionDB>),
                type_of_val(&With::<FunctionDB>::try_convert_to::<FunctionDBBuilder, _>),
                type_of_val(&update_function_db_builder),
                type_of_val(&With::<RequestContext>::update::<FunctionDBBuilder, _>),
                type_of_val(&With::<StorageVersion>::set::<FunctionDBBuilder>),
                type_of_val(&With::<DataLocation>::set::<FunctionDBBuilder>),
                type_of_val(&With::<FunctionDBBuilder>::build::<FunctionDB>),
                type_of_val(&insert::<FunctionDB>),
                // Remove from bundles
                type_of_val(&With::<FunctionDB>::extract::<BundleId>),
                type_of_val(&By::<BundleId>::delete::<BundleDB>),
                // Register associations
                // Find previous versions
                type_of_val(&With::<FunctionVersionId>::some),
                type_of_val(&By::<FunctionId>::historic_fetch_all::<{ TableDB::Available }, TableDB>),
                type_of_val(&By::<FunctionId>::historic_fetch_all::<{ DependencyDB::Active }, DependencyDB>),
                type_of_val(&By::<FunctionId>::historic_fetch_all::<
                    { TriggerDBWithNames::Available },
                    TriggerDBWithNames,
                >),
                // Extract new associations
                type_of_val(&With::<FunctionUpdate>::extract::<Option<Vec<TableNameDto>>>),
                type_of_val(&With::<FunctionUpdate>::extract::<Option<HashMap<TableNameDto, TableType>>>),
                type_of_val(&With::<FunctionUpdate>::extract::<Option<Vec<TableDependencyDto>>>),
                type_of_val(&With::<FunctionUpdate>::extract::<Option<Vec<TableTriggerDto>>>),
                type_of_val(&With::<FunctionUpdate>::extract::<Option<HashMap<WorkerTypeDto, WorkerConfig>>>),
                // Validate tables do not exist
                type_of_val(&validate_tables_do_not_exist),
                // check private tables
                type_of_val(&check_private_tables::<TableDependencyDto>),
                type_of_val(&check_private_tables::<TableTriggerDto>),
                // Extract reuse frozen
                type_of_val(&With::<FunctionUpdate>::extract::<ReuseFrozen>),
                // Extract reset state
                type_of_val(&With::<FunctionUpdate>::extract::<ResetState>),
                // And register new ones
                // Insert into table_versions(sql) current function tables status=Active.
                // Reuse table_id for tables that existed (had status=Frozen)
                type_of_val(&With::<FunctionDB>::try_convert_to::<TableDBBuilder, _>),
                type_of_val(&With::<RequestContext>::update::<TableDBBuilder, _>),
                type_of_val(&build_table_versions),
                type_of_val(&insert_vec::<TableDB>),
                type_of_val(&build_tables_trigger_versions),
                type_of_val(&insert_vec::<TriggerDB>),
                // Insert into dependency_versions(sql) current function table dependencies status=Active.
                type_of_val(&With::<FunctionDB>::try_convert_to::<DependencyDBBuilder, _>),
                type_of_val(&With::<RequestContext>::update::<DependencyDBBuilder, _>),
                type_of_val(&build_dependency_versions),
                // inter collections check for dependencies
                type_of_val(&With::<DependencyDB>::vec_convert_to::<InterCollectionAccessBuilder, _>),
                type_of_val(&With::<Vec<InterCollectionAccessBuilder>>::build::<Vec<InterCollectionAccess>>),
                type_of_val(&Authz::<InterColl>::check_inter_collection),
                type_of_val(&insert_vec::<DependencyDB>),
                // Insert into trigger_versions(sql) current function trigger status=Active.
                type_of_val(&With::<FunctionDB>::try_convert_to::<TriggerDBBuilder, _>),
                type_of_val(&With::<RequestContext>::update::<TriggerDBBuilder, _>),
                type_of_val(&build_trigger_versions),
                // inter collections check for trigger
                type_of_val(&With::<TriggerDB>::vec_convert_to::<InterCollectionAccessBuilder, _>),
                type_of_val(&With::<Vec<InterCollectionAccessBuilder>>::build::<Vec<InterCollectionAccess>>),
                type_of_val(&Authz::<InterColl>::check_inter_collection),
                type_of_val(&insert_vec::<TriggerDB>),
                // Set stopped for current workers and insert into workers(sql) new workers.
                type_of_val(&stop_current_workers),
                type_of_val(&With::<FunctionDB>::try_convert_to::<WorkerDBBuilder, _>),
                type_of_val(&With::<ProjectId>::set::<WorkerDBBuilder>),
                type_of_val(&build_workers),
                type_of_val(&insert_vec::<WorkerDB>),
                // Response
                // Extract new function version id
                type_of_val(&With::<FunctionDB>::extract::<FunctionVersionId>),
                type_of_val(&By::<FunctionVersionId>::fetch_one::<FunctionDBWithNames>),
                type_of_val(&With::<FunctionDBWithNames>::try_convert_to::<FunctionBuilder, _>),
                type_of_val(&With::<FunctionBuilder>::build::<Function>),
                type_of_val(&notify_scheduler),
            ],
        );
    }

    #[td_test::test(db)]
    async fn test_update_fields(db: TdPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let create = FunctionRegister::builder()
            .try_name("foo")?
            .try_description("foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(None)
            .runtime_values(FunctionRuntimeValues::try_from("foo runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let created_function = db.seed().function(&collection, &create).await?;

        let update = FunctionUpdate::builder()
            .name(Some("foo_updated".try_into()?))
            .description(Some("foo_updated description updated".try_into()?))
            .bundle_id(BundleId::default())
            .snippet(Some("foo_updated snippet".try_into()?))
            .decorator(Decorator::Publisher)
            .build()?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                .update(
                    FunctionParam::builder()
                        .collection(CollectionIdName::from_str(&format!("{}", collection.name))?)
                        .function(FunctionIdName::from_str("foo")?)
                        .build()?,
                    update.clone(),
                )
                .in_project(ProjectId::sandbox());

        let service = UpdateFunctionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;

        assert_update(
            &db,
            &UserId::admin(),
            &collection,
            &create,
            &created_function,
            &update,
            &response,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_update_add_new_table(db: TdPool) -> Result<(), TdError> {
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

        let update = FunctionUpdate::builder()
            .name(Some("joaquin_workout_updated".try_into()?))
            .description(Some("foo_updated description updated".try_into()?))
            .bundle_id(BundleId::default())
            .snippet(Some("function_foo snippet updated".try_into()?))
            .decorator(Decorator::Publisher)
            .tables(Some(vec![TableNameDto::try_from("new_table")?]))
            .runtime_values(FunctionRuntimeValues::try_from("new mock runtime values")?)
            .build()?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                .update(
                    FunctionParam::builder()
                        .collection(CollectionIdName::from_str(&format!("~{}", collection.id))?)
                        .function(FunctionIdName::from_str("joaquin_workout")?)
                        .build()?,
                    update.clone(),
                )
                .in_project(ProjectId::sandbox());

        let service = UpdateFunctionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;

        assert_update(
            &db,
            &UserId::admin(),
            &collection,
            &create,
            &created_function,
            &update,
            &response,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_update_remove_table(db: TdPool) -> Result<(), TdError> {
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
            .tables(Some(vec![TableNameDto::try_from("new_table")?]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let created_function = db.seed().function(&collection, &create).await?;

        let update = FunctionUpdate::builder()
            .name(Some("joaquin_workout_updated".try_into()?))
            .description(Some("foo_updated description updated".try_into()?))
            .bundle_id(BundleId::default())
            .snippet(Some("function_foo snippet updated".try_into()?))
            .decorator(Decorator::Publisher)
            .runtime_values(FunctionRuntimeValues::try_from("new mock runtime values")?)
            .build()?;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sys_admin(),
        )
        .update(
            FunctionParam::builder()
                .collection(CollectionIdName::from_str(&format!("~{}", collection.id))?)
                .function(FunctionIdName::from_str("joaquin_workout")?)
                .build()?,
            update.clone(),
        )
        .in_project(ProjectId::sandbox());

        let service = UpdateFunctionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;

        assert_update(
            &db,
            &UserId::admin(),
            &collection,
            &create,
            &created_function,
            &update,
            &response,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_update_maintain_table(db: TdPool) -> Result<(), TdError> {
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
            .tables(Some(vec![TableNameDto::try_from("new_table")?]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let created_function = db.seed().function(&collection, &create).await?;

        let update = FunctionUpdate::builder()
            .name(Some("joaquin_workout_updated".try_into()?))
            .description(Some("foo_updated description updated".try_into()?))
            .bundle_id(BundleId::default())
            .snippet(Some("function_foo snippet updated".try_into()?))
            .decorator(Decorator::Publisher)
            .tables(Some(vec![TableNameDto::try_from("new_table")?]))
            .runtime_values(FunctionRuntimeValues::try_from("new mock runtime values")?)
            .build()?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                .update(
                    FunctionParam::builder()
                        .collection(CollectionIdName::from_str(&format!("~{}", collection.id))?)
                        .function(FunctionIdName::from_str("joaquin_workout")?)
                        .build()?,
                    update.clone(),
                )
                .in_project(ProjectId::sandbox());

        let service = UpdateFunctionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;

        assert_update(
            &db,
            &UserId::admin(),
            &collection,
            &create,
            &created_function,
            &update,
            &response,
        )
        .await
    }

    // Helper: the table_id of the current Active version of a table whose name
    // matches `pred`, among all active tables.
    async fn active_table_id(
        db: &TdPool,
        pred: impl Fn(&TableName) -> bool,
    ) -> Result<TableId, TdError> {
        let active: Vec<TableDB> = db.live().fetch_all::<TableDB>(&TableStatus::Active).await?;
        let table = active
            .into_iter()
            .find(|t| pred(&t.name))
            .expect("matching active table exists");
        Ok(table.table_id)
    }

    #[td_test::test(db)]
    async fn test_update_reset_state(db: TdPool) -> Result<(), TdError> {
        let collection = db
            .seed()
            .collection(&CollectionName::try_from("reset_coll")?, &UserId::admin())
            .await?;

        let create = FunctionRegister::builder()
            .try_name("reset_fn")?
            .try_description("desc")?
            .bundle_id(BundleId::default())
            .try_snippet("snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![TableNameDto::try_from("user_table")?]))
            .runtime_values(FunctionRuntimeValues::try_from("rv")?)
            .reuse_frozen_tables(false)
            .build()?;
        // Register via the service so the system state table is created.
        let register =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                .create(
                    CollectionParam::builder()
                        .collection(CollectionIdName::from_str(&format!("~{}", collection.id))?)
                        .build()?,
                    create,
                )
                .in_project(ProjectId::sandbox());
        RegisterFunctionService::with_defaults(db.clone())
            .service()
            .raw_oneshot(register)
            .await?;

        let user_table = TableName::try_from("user_table")?;
        let state_id_before = active_table_id(&db, |n| n.starts_with(TD_FN_STATE_PREFIX)).await?;
        let user_id_before = active_table_id(&db, |n| *n == user_table).await?;

        let update = FunctionUpdate::builder()
            .name(Some("reset_fn".try_into()?))
            .tables(Some(vec![TableNameDto::try_from("user_table")?]))
            .reset_state(ResetState::TRUE)
            .build()?;
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                .update(
                    FunctionParam::builder()
                        .collection(CollectionIdName::from_str(&format!("~{}", collection.id))?)
                        .function(FunctionIdName::from_str("reset_fn")?)
                        .build()?,
                    update,
                )
                .in_project(ProjectId::sandbox());
        let service = UpdateFunctionService::with_defaults(db.clone()).service();
        let _ = service.raw_oneshot(request).await?;

        let state_id_after = active_table_id(&db, |n| n.starts_with(TD_FN_STATE_PREFIX)).await?;
        let user_id_after = active_table_id(&db, |n| *n == user_table).await?;

        assert_ne!(
            state_id_before, state_id_after,
            "reset_state should regenerate the state table id"
        );
        assert_eq!(
            user_id_before, user_id_after,
            "user table id should be preserved"
        );
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_update_no_reset_state_reuses_state_id(db: TdPool) -> Result<(), TdError> {
        let collection = db
            .seed()
            .collection(&CollectionName::try_from("noreset_coll")?, &UserId::admin())
            .await?;

        let create = FunctionRegister::builder()
            .try_name("noreset_fn")?
            .try_description("desc")?
            .bundle_id(BundleId::default())
            .try_snippet("snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![TableNameDto::try_from("user_table")?]))
            .runtime_values(FunctionRuntimeValues::try_from("rv")?)
            .reuse_frozen_tables(false)
            .build()?;
        let register =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                .create(
                    CollectionParam::builder()
                        .collection(CollectionIdName::from_str(&format!("~{}", collection.id))?)
                        .build()?,
                    create,
                )
                .in_project(ProjectId::sandbox());
        RegisterFunctionService::with_defaults(db.clone())
            .service()
            .raw_oneshot(register)
            .await?;

        let state_id_before = active_table_id(&db, |n| n.starts_with(TD_FN_STATE_PREFIX)).await?;

        // Default reset_state = false.
        let update = FunctionUpdate::builder()
            .name(Some("noreset_fn".try_into()?))
            .tables(Some(vec![TableNameDto::try_from("user_table")?]))
            .build()?;
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                .update(
                    FunctionParam::builder()
                        .collection(CollectionIdName::from_str(&format!("~{}", collection.id))?)
                        .function(FunctionIdName::from_str("noreset_fn")?)
                        .build()?,
                    update,
                )
                .in_project(ProjectId::sandbox());
        let service = UpdateFunctionService::with_defaults(db.clone()).service();
        let _ = service.raw_oneshot(request).await?;

        let state_id_after = active_table_id(&db, |n| n.starts_with(TD_FN_STATE_PREFIX)).await?;
        assert_eq!(
            state_id_before, state_id_after,
            "without reset_state the state table id is reused"
        );
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_update_add_dependencies(db: TdPool) -> Result<(), TdError> {
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
            .tables(Some(vec![TableNameDto::try_from("new_table")?]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let created_function = db.seed().function(&collection, &create).await?;

        let update = FunctionUpdate::builder()
            .name(Some("joaquin_workout_updated".try_into()?))
            .description(Some("foo_updated description updated".try_into()?))
            .bundle_id(BundleId::default())
            .snippet(Some("function_foo snippet updated".try_into()?))
            .decorator(Decorator::Publisher)
            .dependencies(Some(vec![TableDependencyDto::try_from("new_table")?]))
            .tables(Some(vec![TableNameDto::try_from("new_table")?]))
            .runtime_values(FunctionRuntimeValues::try_from("new mock runtime values")?)
            .build()?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                .update(
                    FunctionParam::builder()
                        .collection(CollectionIdName::from_str(&format!("~{}", collection.id))?)
                        .function(FunctionIdName::from_str("joaquin_workout")?)
                        .build()?,
                    update.clone(),
                )
                .in_project(ProjectId::sandbox());

        let service = UpdateFunctionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;

        assert_update(
            &db,
            &UserId::admin(),
            &collection,
            &create,
            &created_function,
            &update,
            &response,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_update_remove_dependencies(db: TdPool) -> Result<(), TdError> {
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
            .dependencies(Some(vec![TableDependencyDto::try_from("new_table")?]))
            .triggers(None)
            .tables(Some(vec![TableNameDto::try_from("new_table")?]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let created_function = db.seed().function(&collection, &create).await?;

        let update = FunctionUpdate::builder()
            .name(Some("joaquin_workout_updated".try_into()?))
            .description(Some("foo_updated description updated".try_into()?))
            .bundle_id(BundleId::default())
            .snippet(Some("function_foo snippet updated".try_into()?))
            .decorator(Decorator::Publisher)
            .tables(Some(vec![TableNameDto::try_from("new_table")?]))
            .runtime_values(FunctionRuntimeValues::try_from("new mock runtime values")?)
            .build()?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                .update(
                    FunctionParam::builder()
                        .collection(CollectionIdName::from_str(&format!("~{}", collection.id))?)
                        .function(FunctionIdName::from_str("joaquin_workout")?)
                        .build()?,
                    update.clone(),
                )
                .in_project(ProjectId::sandbox());

        let service = UpdateFunctionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;

        assert_update(
            &db,
            &UserId::admin(),
            &collection,
            &create,
            &created_function,
            &update,
            &response,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_update_maintain_dependencies(db: TdPool) -> Result<(), TdError> {
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
            .dependencies(Some(vec![TableDependencyDto::try_from("new_table")?]))
            .triggers(None)
            .tables(Some(vec![TableNameDto::try_from("new_table")?]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let created_function = db.seed().function(&collection, &create).await?;

        let update = FunctionUpdate::builder()
            .name(Some("joaquin_workout_updated".try_into()?))
            .description(Some("foo_updated description updated".try_into()?))
            .bundle_id(BundleId::default())
            .snippet(Some("function_foo snippet updated".try_into()?))
            .decorator(Decorator::Publisher)
            .dependencies(Some(vec![TableDependencyDto::try_from("new_table")?]))
            .tables(Some(vec![TableNameDto::try_from("new_table")?]))
            .runtime_values(FunctionRuntimeValues::try_from("new mock runtime values")?)
            .build()?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                .update(
                    FunctionParam::builder()
                        .collection(CollectionIdName::from_str(&format!("~{}", collection.id))?)
                        .function(FunctionIdName::from_str("joaquin_workout")?)
                        .build()?,
                    update.clone(),
                )
                .in_project(ProjectId::sandbox());

        let service = UpdateFunctionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;

        assert_update(
            &db,
            &UserId::admin(),
            &collection,
            &create,
            &created_function,
            &update,
            &response,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_update_add_trigger(db: TdPool) -> Result<(), TdError> {
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

        // Actual test
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

        let update = FunctionUpdate::builder()
            .name(Some("joaquin_workout_updated".try_into()?))
            .description(Some("function_foo description updated".try_into()?))
            .bundle_id(BundleId::default())
            .snippet(Some("function_foo snippet updated".try_into()?))
            .decorator(Decorator::Publisher)
            .triggers(Some(vec![TableTriggerDto::try_from("trigger_table")?]))
            .runtime_values(FunctionRuntimeValues::try_from("new mock runtime values")?)
            .build()?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                .update(
                    FunctionParam::builder()
                        .collection(CollectionIdName::from_str(&format!("{}", collection.name))?)
                        .function(FunctionIdName::from_str("joaquin_workout")?)
                        .build()?,
                    update.clone(),
                )
                .in_project(ProjectId::sandbox());

        let service = UpdateFunctionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;

        assert_update(
            &db,
            &UserId::admin(),
            &collection,
            &create,
            &created_function,
            &update,
            &response,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_update_remove_trigger(db: TdPool) -> Result<(), TdError> {
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

        // Actual test
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

        let update = FunctionUpdate::builder()
            .name(Some("joaquin_workout_updated".try_into()?))
            .description(Some("function_foo description updated".try_into()?))
            .bundle_id(BundleId::default())
            .snippet(Some("function_foo snippet updated".try_into()?))
            .decorator(Decorator::Publisher)
            .runtime_values(FunctionRuntimeValues::try_from("new mock runtime values")?)
            .build()?;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sys_admin(),
        )
        .update(
            FunctionParam::builder()
                .collection(CollectionIdName::from_str(&format!("{}", collection.name))?)
                .function(FunctionIdName::from_str("joaquin_workout")?)
                .build()?,
            update.clone(),
        )
        .in_project(ProjectId::sandbox());

        let service = UpdateFunctionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;

        assert_update(
            &db,
            &UserId::admin(),
            &collection,
            &create,
            &created_function,
            &update,
            &response,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_update_maintain_trigger(db: TdPool) -> Result<(), TdError> {
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

        // Actual test
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

        let update = FunctionUpdate::builder()
            .name(Some("joaquin_workout_updated".try_into()?))
            .description(Some("function_foo description updated".try_into()?))
            .bundle_id(BundleId::default())
            .snippet(Some("function_foo snippet updated".try_into()?))
            .decorator(Decorator::Publisher)
            .triggers(Some(vec![TableTriggerDto::try_from("trigger_table")?]))
            .runtime_values(FunctionRuntimeValues::try_from("new mock runtime values")?)
            .build()?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                .update(
                    FunctionParam::builder()
                        .collection(CollectionIdName::from_str(&format!("{}", collection.name))?)
                        .function(FunctionIdName::from_str("joaquin_workout")?)
                        .build()?,
                    update.clone(),
                )
                .in_project(ProjectId::sandbox());

        let service = UpdateFunctionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;

        assert_update(
            &db,
            &UserId::admin(),
            &collection,
            &create,
            &created_function,
            &update,
            &response,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_update_change_everything(db: TdPool) -> Result<(), TdError> {
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
            .tables(Some(vec![
                TableNameDto::try_from("trigger_table")?,
                TableNameDto::try_from("trigger_table_2")?,
            ]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let _ = db.seed().function(&collection, &trigger_create).await?;

        // Actual test
        let create = FunctionRegister::builder()
            .try_name("joaquin_workout")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(Some(vec![TableDependencyDto::try_from("trigger_table")?]))
            .triggers(Some(vec![TableTriggerDto::try_from("trigger_table")?]))
            .tables(Some(vec![
                TableNameDto::try_from("joaquin_table")?,
                TableNameDto::try_from("joaquin_table_2")?,
            ]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let created_function = db.seed().function(&collection, &create).await?;

        let update = FunctionUpdate::builder()
            .name(Some("joaquin_workout_updated".try_into()?))
            .description(Some("function_foo description updated".try_into()?))
            .bundle_id(BundleId::default())
            .snippet(Some("function_foo snippet updated".try_into()?))
            .decorator(Decorator::Publisher)
            .dependencies(Some(vec![
                TableDependencyDto::try_from("trigger_table")?,
                TableDependencyDto::try_from("trigger_table_2")?,
            ]))
            .triggers(Some(vec![TableTriggerDto::try_from("trigger_table_2")?]))
            .tables(Some(vec![
                TableNameDto::try_from("joaquin_table")?,
                TableNameDto::try_from("joaquin_table_3")?,
            ]))
            .runtime_values(FunctionRuntimeValues::try_from("new mock runtime values")?)
            .build()?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                .update(
                    FunctionParam::builder()
                        .collection(CollectionIdName::from_str(&format!("{}", collection.name))?)
                        .function(FunctionIdName::from_str("joaquin_workout")?)
                        .build()?,
                    update.clone(),
                )
                .in_project(ProjectId::sandbox());

        let service = UpdateFunctionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;

        assert_update(
            &db,
            &UserId::admin(),
            &collection,
            &create,
            &created_function,
            &update,
            &response,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_update_freeze_unfreeze(db: TdPool) -> Result<(), TdError> {
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
            .tables(Some(vec![TableNameDto::try_from("joaquin_table")?]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let _created_function = db.seed().function(&collection, &create).await?;

        let update = FunctionUpdate::builder()
            .name(Some("joaquin_workout".try_into()?))
            .description(Some("function_foo description".try_into()?))
            .bundle_id(BundleId::default())
            .snippet(Some("function_foo snippet".try_into()?))
            .decorator(Decorator::Publisher)
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .build()?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                .update(
                    FunctionParam::builder()
                        .collection(CollectionIdName::from_str(&format!("{}", collection.name))?)
                        .function(FunctionIdName::from_str("joaquin_workout")?)
                        .build()?,
                    update.clone(),
                )
                .in_project(ProjectId::sandbox());

        let service = UpdateFunctionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let _response = response?;

        let tables = db
            .live()
            .fetch_all::<TableDB>(and!(
                TableName::try_from("joaquin_table")?,
                TableStatus::Frozen
            ))
            .await?;
        assert_eq!(tables.len(), 1);

        let update = FunctionUpdate::builder()
            .name(Some("joaquin_workout".try_into()?))
            .tables(Some(vec![TableNameDto::try_from("joaquin_table")?]))
            .build()?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                .update(
                    FunctionParam::builder()
                        .collection(CollectionIdName::from_str(&format!("{}", collection.name))?)
                        .function(FunctionIdName::from_str("joaquin_workout")?)
                        .build()?,
                    update.clone(),
                )
                .in_project(ProjectId::sandbox());

        let service = UpdateFunctionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        // Without reuse_frozen, we just get an error
        let response = response;
        assert!(response.is_err());
        let error = response.unwrap_err();
        let error = error.domain_err::<RegisterFunctionError>();
        assert!(matches!(
            error,
            RegisterFunctionError::FrozenTableAlreadyExists(..)
        ));

        let update = FunctionUpdate::builder()
            .name(Some("joaquin_workout".try_into()?))
            .tables(Some(vec![TableNameDto::try_from("joaquin_table")?]))
            .reuse_frozen_tables(true)
            .build()?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                .update(
                    FunctionParam::builder()
                        .collection(CollectionIdName::from_str(&format!("{}", collection.name))?)
                        .function(FunctionIdName::from_str("joaquin_workout")?)
                        .build()?,
                    update.clone(),
                )
                .in_project(ProjectId::sandbox());

        let service = UpdateFunctionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let _response = response?;
        // But with reuse_frozen, we get the expected response

        let tables: Vec<TableDB> = db
            .live()
            .fetch_all::<TableDB>(&(&TableName::try_from("joaquin_table")?))
            .await?;
        assert_eq!(tables.len(), 3);
        assert_eq!(tables[0].status, TableStatus::Active);
        assert_eq!(tables[1].status, TableStatus::Frozen);
        assert_eq!(tables[2].status, TableStatus::Active);
        assert_eq!(tables[0].table_id, tables[1].table_id);
        assert_eq!(tables[1].table_id, tables[2].table_id);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_update_same_name(db: TdPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let dependencies = None;
        let triggers = None;
        let tables = None;

        let bundle_id = BundleId::default();
        let create = FunctionRegister::builder()
            .try_name("function_foo")?
            .try_description("function_foo description")?
            .bundle_id(bundle_id)
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(dependencies.clone())
            .triggers(triggers.clone())
            .tables(tables.clone())
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;
        let created_function = db.seed().function(&collection, &create).await?;

        let update = FunctionUpdate::builder()
            .name(Some("function_foo_2".try_into()?))
            .tables(Some(vec![TableNameDto::try_from("joaquin_table")?]))
            .build()?;

        let update_request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                .update(
                    FunctionParam::builder()
                        .collection(CollectionIdName::from_str(&format!("{}", collection.name))?)
                        .function(FunctionIdName::from_str("function_foo")?)
                        .build()?,
                    update.clone(),
                )
                .in_project(ProjectId::sandbox());

        let service = UpdateFunctionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(update_request).await;
        let update_response = response?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                .create(
                    CollectionParam::builder()
                        .collection(CollectionIdName::from_str(collection_name.as_str())?)
                        .build()?,
                    create.clone(),
                )
                .in_project(ProjectId::sandbox());
        let service = RegisterFunctionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let create_response = response?;

        assert_update(
            &db,
            &UserId::admin(),
            &collection,
            &create,
            &created_function,
            &update,
            &update_response,
        )
        .await?;
        assert_register(
            &db,
            &UserId::admin(),
            &collection,
            &create,
            &create_response,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_update_private_tables_all_same_collection(db: TdPool) -> Result<(), TdError> {
        test_private_tables(db, false, false).await
    }

    #[td_test::test(db)]
    async fn test_update_private_tables_deps_diff_collection(db: TdPool) -> Result<(), TdError> {
        test_private_tables(db, true, false).await
    }

    #[td_test::test(db)]
    async fn test_update_private_tables_triggers_diff_collection(
        db: TdPool,
    ) -> Result<(), TdError> {
        test_private_tables(db, false, true).await
    }

    async fn test_private_tables(
        db: TdPool,
        deps_diff_collection: bool,
        triggers_diff_collection: bool,
    ) -> Result<(), TdError> {
        let collection_name_1 = CollectionName::try_from("collection_1")?;
        let collection_1 = db
            .seed()
            .collection(&collection_name_1, &UserId::admin())
            .await?;
        let collection_name_2 = CollectionName::try_from("collection_2")?;
        let collection_2 = db
            .seed()
            .collection(&collection_name_2, &UserId::admin())
            .await?;

        db.seed()
            .inter_collection_permission(
                &collection_1.id,
                &ToCollectionId::try_from(collection_2.id)?,
            )
            .await?;

        for collection in [&collection_1, &collection_2] {
            let dependencies = None;
            let triggers = None;
            let tables = Some(vec![
                TableNameDto::try_from("_table_1")?,
                TableNameDto::try_from("_table_2")?,
            ]);

            let bundle_id = BundleId::default();
            let create = FunctionRegister::builder()
                .try_name("function_1")?
                .try_description("function_1 description")?
                .bundle_id(bundle_id)
                .try_snippet("function_1 snippet")?
                .decorator(Decorator::Subscriber)
                .dependencies(dependencies.clone())
                .triggers(triggers.clone())
                .tables(tables.clone())
                .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
                .reuse_frozen_tables(false)
                .build()?;
            db.seed().function(collection, &create).await?;
        }

        let dependencies = Some(vec![
            TableDependencyDto::try_from("_table_1")?,
            TableDependencyDto::try_from("_table_2")?,
        ]);
        let triggers = Some(vec![
            TableTriggerDto::try_from("_table_1")?,
            TableTriggerDto::try_from("_table_2")?,
        ]);
        let tables = Some(vec![TableNameDto::try_from("output_1")?]);

        let bundle_id = BundleId::default();
        let create = FunctionRegister::builder()
            .try_name("function_2")?
            .try_description("function_2 description")?
            .bundle_id(bundle_id)
            .try_snippet("function_2 snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(dependencies)
            .triggers(triggers)
            .tables(tables)
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;
        db.seed().function(&collection_2, &create).await?;

        // Actual test

        let deps_collection = if deps_diff_collection {
            "collection_1"
        } else {
            "collection_2"
        };
        let triggers_collection = if triggers_diff_collection {
            "collection_1"
        } else {
            "collection_2"
        };

        let dependencies = Some(vec![
            TableDependencyDto::try_from(format!("{deps_collection}/_table_1"))?,
            TableDependencyDto::try_from("_table_2")?,
        ]);
        let triggers = Some(vec![
            TableTriggerDto::try_from(format!("{triggers_collection}/_table_1"))?,
            TableTriggerDto::try_from("_table_2")?,
        ]);
        let tables = Some(vec![TableNameDto::try_from("output_1")?]);

        let bundle_id = BundleId::default();
        let update = FunctionUpdate::builder()
            .name(Some("function_2".try_into()?))
            .description(Some("function_2 description".try_into()?))
            .bundle_id(bundle_id)
            .snippet(Some("function_2 snippet".try_into()?))
            .decorator(Decorator::Publisher)
            .dependencies(dependencies)
            .triggers(triggers)
            .tables(tables)
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                .update(
                    FunctionParam::builder()
                        .collection(CollectionIdName::from_str(&format!(
                            "{}",
                            collection_2.name
                        ))?)
                        .function(FunctionIdName::from_str("function_2")?)
                        .build()?,
                    update.clone(),
                )
                .in_project(ProjectId::sandbox());

        let service = UpdateFunctionService::with_defaults(db.clone()).service();
        let res = service.raw_oneshot(request).await;
        if deps_diff_collection || triggers_diff_collection {
            assert!(res.is_err());
            assert!(res.err().unwrap().code().contains("PrivateTableError"))
        } else {
            assert!(res.is_ok());
        }
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_private_table_flag(db: TdPool) -> Result<(), TdError> {
        let collection_name_1 = CollectionName::try_from("collection_1")?;
        let collection_1 = db
            .seed()
            .collection(&collection_name_1, &UserId::admin())
            .await?;

        let tables = Some(vec![TableNameDto::try_from("table0")?]);

        let bundle_id = BundleId::default();
        let create = FunctionRegister::builder()
            .try_name("function")?
            .try_description("description")?
            .bundle_id(bundle_id)
            .try_snippet("snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(Some(vec![]))
            .triggers(Some(vec![]))
            .tables(tables)
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;
        db.seed().function(&collection_1, &create).await?;

        let tables = Some(vec![
            TableNameDto::try_from("table0")?,
            TableNameDto::try_from("_table0")?,
        ]);

        let bundle_id = BundleId::default();
        let update = FunctionUpdate::builder()
            .bundle_id(bundle_id)
            .tables(tables)
            .build()?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                .update(
                    FunctionParam::builder()
                        .collection(CollectionIdName::from_str(&format!(
                            "{}",
                            collection_1.name
                        ))?)
                        .function(FunctionIdName::from_str("function")?)
                        .build()?,
                    update.clone(),
                )
                .in_project(ProjectId::sandbox());

        let service = UpdateFunctionService::with_defaults(db.clone()).service();
        let res = service.raw_oneshot(request).await;
        assert!(res.is_ok());

        let tables: Vec<TableDB> = db.live().fetch_all::<TableDB>(&(res.unwrap().id)).await?;
        let map: HashMap<String, TableDB> = tables
            .into_iter()
            .map(|table| (table.name.to_string(), table))
            .collect();
        assert!(!map.get("table0").unwrap().private.deref());
        assert!(map.get("_table0").unwrap().private.deref());
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_update_maintain_triggers_dependencies(db: TdPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let create = FunctionRegister::builder()
            .try_name("function_1")?
            .try_description("description")?
            .bundle_id(BundleId::default())
            .try_snippet("snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(vec![TableNameDto::try_from("table_1")?])
            .runtime_values(FunctionRuntimeValues::default())
            .reuse_frozen_tables(false)
            .build()?;

        let created_function = db.seed().function(&collection, &create).await?;

        let dependant = FunctionRegister::builder()
            .try_name("function_2")?
            .try_description("description")?
            .bundle_id(BundleId::default())
            .try_snippet("snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(vec![TableDependencyDto::try_from("table_1")?])
            .triggers(vec![TableTriggerDto::try_from("table_1")?])
            .tables(None)
            .runtime_values(FunctionRuntimeValues::default())
            .reuse_frozen_tables(false)
            .build()?;

        let _ = db.seed().function(&collection, &dependant).await?;

        let update = FunctionUpdate::builder()
            .name(Some("function_1".try_into()?))
            .description(Some("description".try_into()?))
            .bundle_id(BundleId::default())
            .snippet(Some("snippet".try_into()?))
            .decorator(Decorator::Publisher)
            .tables(Some(vec![TableNameDto::try_from("table_1")?]))
            .build()?;

        // With an update, a new function version and table version is created
        // But the trigger and dependency should still be valid (as the use id, not version id)
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                .update(
                    FunctionParam::builder()
                        .collection(CollectionIdName::from_str(&format!("~{}", collection.id))?)
                        .function(FunctionIdName::from_str("function_1")?)
                        .build()?,
                    update.clone(),
                )
                .in_project(ProjectId::sandbox());

        let service = UpdateFunctionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;

        assert_update(
            &db,
            &UserId::admin(),
            &collection,
            &create,
            &created_function,
            &update,
            &response,
        )
        .await?;

        let recursive_downstream_trigger: Vec<TriggerDB> = db
            .historic(&AtTime::now())
            .recursive::<TriggerDB, FunctionDB>(RecursionOptions::new(Some(Depth::ZERO), None))
            .direct_reference(&response.function_id)
            .d_state(TriggerDB::active())
            .r_state(FunctionDB::executable())
            .fetch_all()
            .await
            .unwrap();
        // 2 triggered functions, 1 downstream
        assert_eq!(recursive_downstream_trigger.len(), 1);
        let triggered_function_id = &recursive_downstream_trigger[0].function_id;
        let triggered_function: FunctionDBWithNames = db
            .current()
            .fetch_one::<FunctionDBWithNames>(&(triggered_function_id))
            .await
            .unwrap();
        assert_eq!(triggered_function.name, dependant.name);

        let downstream_dependencies: Vec<DependencyDB> = db
            .current()
            .state(DependencyDB::active())
            .fetch_all(&(triggered_function_id))
            .await
            .unwrap();
        // 1 downstream dependency on the triggered function
        assert_eq!(downstream_dependencies.len(), 1);
        let dependency_function_id = &recursive_downstream_trigger[0].function_id;
        let dependency_function: FunctionDBWithNames = db
            .current()
            .fetch_one::<FunctionDBWithNames>(&dependency_function_id)
            .await
            .unwrap();
        assert_eq!(dependency_function.name, dependant.name);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_update_remove_triggers_dependencies(db: TdPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let create = FunctionRegister::builder()
            .try_name("function_1")?
            .try_description("description")?
            .bundle_id(BundleId::default())
            .try_snippet("snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(vec![TableNameDto::try_from("table_1")?])
            .runtime_values(FunctionRuntimeValues::default())
            .reuse_frozen_tables(false)
            .build()?;

        let _ = db.seed().function(&collection, &create).await?;

        let dependant = FunctionRegister::builder()
            .try_name("function_2")?
            .try_description("description")?
            .bundle_id(BundleId::default())
            .try_snippet("snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(vec![TableDependencyDto::try_from("table_1")?])
            .triggers(vec![TableTriggerDto::try_from("table_1")?])
            .tables(None)
            .runtime_values(FunctionRuntimeValues::default())
            .reuse_frozen_tables(false)
            .build()?;

        let dependant_function = db.seed().function(&collection, &dependant).await?;

        let update = FunctionUpdate::builder()
            .name(Some("function_2".try_into()?))
            .build()?;

        // With an update, a new function version and table version is created
        // But the trigger and dependency should still be valid (as the use id, not version id)
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                .update(
                    FunctionParam::builder()
                        .collection(CollectionIdName::from_str(&collection.name.to_string())?)
                        .function(FunctionIdName::from_str("function_2")?)
                        .build()?,
                    update.clone(),
                )
                .in_project(ProjectId::sandbox());

        let service = UpdateFunctionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;

        assert_update(
            &db,
            &UserId::admin(),
            &collection,
            &dependant,
            &dependant_function,
            &update,
            &response,
        )
        .await?;

        let recursive_downstream_trigger: Vec<TriggerDB> = db
            .historic(&AtTime::now())
            .recursive::<TriggerDB, FunctionDB>(RecursionOptions::new(Some(Depth::ZERO), None))
            .direct_reference(&response.function_id)
            .d_state(TriggerDB::active())
            .r_state(FunctionDB::executable())
            .fetch_all()
            .await
            .unwrap();
        // Downstream trigger removed
        assert!(recursive_downstream_trigger.is_empty());

        // Downstream dependency removed
        let dependency_function: Vec<FunctionDBWithNames> = db
            .current()
            .fetch_all::<FunctionDBWithNames>(&(&response.function_id))
            .await
            .unwrap();
        assert_eq!(dependency_function.len(), 1); // only implicit self dependency
        assert_eq!(dependency_function[0].name, dependant.name);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_workers_created(db: TdPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        // Create it without workers
        let create = FunctionRegister::builder()
            .try_name("function_1")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(None)
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .workers(None)
            .build()?;
        let function_db = db.seed().function(&collection, &create).await?;
        let workers: Vec<WorkerDB> = db
            .live()
            .fetch_all::<WorkerDB>(&function_db.id)
            .await
            .unwrap();
        assert!(workers.is_empty());

        // Then update to add a worker
        let update = FunctionUpdate::builder()
            .workers(Some(HashMap::from([(
                WorkerTypeDto::StageTrigger,
                WorkerConfig::default(),
            )])))
            .build()?;
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                .update(
                    FunctionParam::builder()
                        .collection(CollectionIdName::from_str(&collection.name.to_string())?)
                        .function(FunctionIdName::from_str("function_1")?)
                        .build()?,
                    update.clone(),
                )
                .in_project(ProjectId::sandbox());
        let service = UpdateFunctionService::with_defaults(db.clone()).service();
        let updated_function_1 = service.raw_oneshot(request).await?;
        let workers: Vec<WorkerDB> = db
            .live()
            .fetch_all::<WorkerDB>(&updated_function_1.id)
            .await
            .unwrap();
        assert_eq!(workers.len(), 1);
        let worker = &workers[0];
        assert_eq!(worker.collection_id, collection.id);
        assert_eq!(worker.execution_id, None);
        assert_eq!(worker.transaction_id, None);
        assert_eq!(worker.physical_function_run_id, None);
        assert_eq!(worker.function_version_id, updated_function_1.id);
        assert_eq!(worker.message_status, WorkerMessageStatus::None);
        assert_eq!(worker.started_on, None);
        assert_eq!(worker.ended_on, None);
        assert_eq!(worker.status, WorkerStatus::Scheduled);
        assert_eq!(worker.worker_type, WorkerType::StageTrigger);
        assert_eq!(worker.worker_config, Some(WorkerConfig::default()));

        // Then update with None
        let update = FunctionUpdate::builder().workers(None).build()?;
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                .update(
                    FunctionParam::builder()
                        .collection(CollectionIdName::from_str(&collection.name.to_string())?)
                        .function(FunctionIdName::from_str("function_1")?)
                        .build()?,
                    update.clone(),
                )
                .in_project(ProjectId::sandbox());
        let service = UpdateFunctionService::with_defaults(db.clone()).service();
        let updated_function_2 = service.raw_oneshot(request).await?;

        // updated_function_1 workers should be dropped
        let workers: Vec<WorkerDB> = db
            .live()
            .fetch_all::<WorkerDB>(&updated_function_1.id)
            .await
            .unwrap();
        assert_eq!(workers.len(), 1);
        let worker = &workers[0];
        assert_eq!(worker.collection_id, collection.id);
        assert_eq!(worker.execution_id, None);
        assert_eq!(worker.transaction_id, None);
        assert_eq!(worker.physical_function_run_id, None);
        assert_eq!(worker.function_version_id, updated_function_1.id);
        assert_eq!(worker.message_status, WorkerMessageStatus::None);
        assert_eq!(worker.started_on, None);
        assert_eq!(worker.ended_on, None);
        assert_eq!(worker.status, WorkerStatus::StopScheduled);
        assert_eq!(worker.worker_type, WorkerType::StageTrigger);
        assert_eq!(worker.worker_config, Some(WorkerConfig::default()));

        // updated_function_2 should still have the same worker, recreated
        let workers: Vec<WorkerDB> = db
            .live()
            .fetch_all::<WorkerDB>(&updated_function_2.id)
            .await
            .unwrap();
        assert_eq!(workers.len(), 1);
        let worker = &workers[0];
        assert_eq!(worker.collection_id, collection.id);
        assert_eq!(worker.execution_id, None);
        assert_eq!(worker.transaction_id, None);
        assert_eq!(worker.physical_function_run_id, None);
        assert_eq!(worker.function_version_id, updated_function_2.id);
        assert_eq!(worker.message_status, WorkerMessageStatus::None);
        assert_eq!(worker.started_on, None);
        assert_eq!(worker.ended_on, None);
        assert_eq!(worker.status, WorkerStatus::Scheduled);
        assert_eq!(worker.worker_type, WorkerType::StageTrigger);
        assert_eq!(worker.worker_config, Some(WorkerConfig::default()));

        // Then change the workers
        let update = FunctionUpdate::builder()
            .workers(Some(HashMap::from([(
                WorkerTypeDto::CronTrigger,
                WorkerConfig::default(),
            )])))
            .build()?;
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                .update(
                    FunctionParam::builder()
                        .collection(CollectionIdName::from_str(&collection.name.to_string())?)
                        .function(FunctionIdName::from_str("function_1")?)
                        .build()?,
                    update.clone(),
                )
                .in_project(ProjectId::sandbox());
        let service = UpdateFunctionService::with_defaults(db.clone()).service();
        let updated_function_3 = service.raw_oneshot(request).await?;

        // updated_function_2 should still have the same worker, stopped
        let workers: Vec<WorkerDB> = db
            .live()
            .fetch_all::<WorkerDB>(&updated_function_2.id)
            .await
            .unwrap();
        assert_eq!(workers.len(), 1);
        let worker = &workers[0];
        assert_eq!(worker.collection_id, collection.id);
        assert_eq!(worker.execution_id, None);
        assert_eq!(worker.transaction_id, None);
        assert_eq!(worker.physical_function_run_id, None);
        assert_eq!(worker.function_version_id, updated_function_2.id);
        assert_eq!(worker.message_status, WorkerMessageStatus::None);
        assert_eq!(worker.started_on, None);
        assert_eq!(worker.ended_on, None);
        assert_eq!(worker.status, WorkerStatus::StopScheduled);
        assert_eq!(worker.worker_type, WorkerType::StageTrigger);
        assert_eq!(worker.worker_config, Some(WorkerConfig::default()));

        // updated_function_3 should have a same worker
        let workers: Vec<WorkerDB> = db
            .live()
            .fetch_all::<WorkerDB>(&updated_function_3.id)
            .await
            .unwrap();
        assert_eq!(workers.len(), 1);
        let worker = &workers[0];
        assert_eq!(worker.collection_id, collection.id);
        assert_eq!(worker.execution_id, None);
        assert_eq!(worker.transaction_id, None);
        assert_eq!(worker.physical_function_run_id, None);
        assert_eq!(worker.function_version_id, updated_function_3.id);
        assert_eq!(worker.message_status, WorkerMessageStatus::None);
        assert_eq!(worker.started_on, None);
        assert_eq!(worker.ended_on, None);
        assert_eq!(worker.status, WorkerStatus::Scheduled);
        assert_eq!(worker.worker_type, WorkerType::CronTrigger);
        assert_eq!(worker.worker_config, Some(WorkerConfig::default()));

        // And last, drop them all, by passing empty map
        let update = FunctionUpdate::builder()
            .workers(Some(HashMap::new()))
            .build()?;
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                .update(
                    FunctionParam::builder()
                        .collection(CollectionIdName::from_str(&collection.name.to_string())?)
                        .function(FunctionIdName::from_str("function_1")?)
                        .build()?,
                    update.clone(),
                )
                .in_project(ProjectId::sandbox());
        let service = UpdateFunctionService::with_defaults(db.clone()).service();
        let updated_function_4 = service.raw_oneshot(request).await?;

        // updated_function_3 should still have the same worker, stopped
        let workers: Vec<WorkerDB> = db
            .live()
            .fetch_all::<WorkerDB>(&updated_function_3.id)
            .await
            .unwrap();
        assert_eq!(workers.len(), 1);
        let worker = &workers[0];
        assert_eq!(worker.collection_id, collection.id);
        assert_eq!(worker.execution_id, None);
        assert_eq!(worker.transaction_id, None);
        assert_eq!(worker.physical_function_run_id, None);
        assert_eq!(worker.function_version_id, updated_function_3.id);
        assert_eq!(worker.message_status, WorkerMessageStatus::None);
        assert_eq!(worker.started_on, None);
        assert_eq!(worker.ended_on, None);
        assert_eq!(worker.status, WorkerStatus::StopScheduled);
        assert_eq!(worker.worker_type, WorkerType::CronTrigger);
        assert_eq!(worker.worker_config, Some(WorkerConfig::default()));

        // updated_function_4 should have a no worker
        let workers: Vec<WorkerDB> = db
            .live()
            .fetch_all::<WorkerDB>(&updated_function_4.id)
            .await
            .unwrap();
        assert_eq!(workers.len(), 0);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_multiple_workers_error(db: TdPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        // Create it without workers
        let create = FunctionRegister::builder()
            .try_name("function_1")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(None)
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .workers(None)
            .build()?;
        let function_db = db.seed().function(&collection, &create).await?;
        let workers: Vec<WorkerDB> = db
            .live()
            .fetch_all::<WorkerDB>(&function_db.id)
            .await
            .unwrap();
        assert!(workers.is_empty());

        // Then update to add more than 1 worker
        let update = FunctionUpdate::builder()
            .workers(Some(HashMap::from([
                (WorkerTypeDto::CronTrigger, WorkerConfig::default()),
                (WorkerTypeDto::StageTrigger, WorkerConfig::default()),
            ])))
            .build()?;
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                .update(
                    FunctionParam::builder()
                        .collection(CollectionIdName::from_str(&collection.name.to_string())?)
                        .function(FunctionIdName::from_str("function_1")?)
                        .build()?,
                    update.clone(),
                )
                .in_project(ProjectId::sandbox());
        let service = UpdateFunctionService::with_defaults(db.clone()).service();
        let res = service.raw_oneshot(request).await;
        assert!(res.is_err());
        let err = res.err().unwrap();
        assert!(format!("{err}").contains("Multiple workers are not supported."));
        Ok(())
    }
}
