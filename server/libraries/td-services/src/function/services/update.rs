//
// Copyright 2025 Tabs Data Inc.
//

use crate::function::layers::register::{data_location, validate_tables_do_not_exist};
use crate::function::layers::update::assert_function_name_not_exists;
use crate::function::layers::{
    DO_AUTHZ, check_private_tables, register_dependencies, register_tables, register_triggers,
};
use ta_services::factory::service_factory;
use td_authz::{Authz, AuthzContext};
use td_objects::crudl::{RequestContext, UpdateRequest};
use td_objects::rest_urls::FunctionParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{AuthzOn, CollAdmin, CollDev};
use td_objects::tower_service::from::{
    BuildService, DefaultService, ExtractDataService, ExtractNameService, ExtractService,
    SetService, TryIntoService, UpdateService, With, combine,
};
use td_objects::tower_service::sql::{
    By, SqlDeleteService, SqlSelectAllService, SqlSelectService, insert,
};
use td_objects::types::basic::{
    AtTime, BundleId, CollectionId, CollectionIdName, CollectionName, DataLocation,
    DependencyStatus, FunctionId, FunctionIdName, FunctionStatus, FunctionVersionId, ReuseFrozen,
    StorageVersion, TableDependencyDto, TableNameDto, TableStatus, TableTriggerDto, TriggerStatus,
};
use td_objects::types::collection::CollectionDB;
use td_objects::types::dependency::DependencyDB;
use td_objects::types::function::{
    BundleDB, Function, FunctionBuilder, FunctionDB, FunctionDBBuilder, FunctionDBWithNames,
    FunctionUpdate,
};
use td_objects::types::table::TableDB;
use td_objects::types::trigger::TriggerDBWithNames;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = UpdateFunctionService,
    request = UpdateRequest<FunctionParam, FunctionUpdate>,
    response = Function,
    connection = TransactionProvider,
    context = DaoQueries,
    context = AuthzContext,
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
        from_fn(By::<CollectionIdName>::select::<CollectionDB>),
        from_fn(With::<CollectionDB>::extract::<CollectionId>),
        // check requester is coll_admin or coll_dev for the function's collection
        from_fn(AuthzOn::<CollectionId>::set),
        from_fn(Authz::<CollAdmin, CollDev>::check),
        from_fn(With::<CollectionDB>::extract::<CollectionName>),
        // Get function. Extract function id and name.
        from_fn(combine::<CollectionIdName, FunctionIdName>),
        from_fn(With::<RequestContext>::extract::<AtTime>),
        from_fn(FunctionStatus::active_or_frozen),
        from_fn(By::<(CollectionIdName, FunctionIdName)>::select_version::<FunctionDBWithNames>),
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
        from_fn(With::<FunctionUpdate>::convert_to::<FunctionDBBuilder, _>),
        from_fn(With::<RequestContext>::update::<FunctionDBBuilder, _>),
        from_fn(With::<CollectionId>::set::<FunctionDBBuilder>),
        // We maintain the same function id
        from_fn(With::<FunctionId>::set::<FunctionDBBuilder>),
        from_fn(With::<StorageVersion>::set::<FunctionDBBuilder>),
        from_fn(With::<DataLocation>::set::<FunctionDBBuilder>),
        from_fn(With::<FunctionDBBuilder>::build::<FunctionDB, _>),
        from_fn(insert::<FunctionDB>),
        // Remove from bundles
        from_fn(With::<FunctionDB>::extract::<BundleId>),
        from_fn(By::<BundleId>::delete::<BundleDB>),
        // Register associations
        // Find previous versions
        from_fn(TableStatus::active_or_frozen),
        from_fn(By::<FunctionId>::select_all_versions::<TableDB>),
        from_fn(DependencyStatus::active),
        from_fn(By::<FunctionId>::select_all_versions::<DependencyDB>),
        from_fn(TriggerStatus::active_or_frozen),
        from_fn(By::<FunctionId>::select_all_versions::<TriggerDBWithNames>),
        // Extract new associations
        from_fn(With::<FunctionUpdate>::extract::<Option<Vec<TableNameDto>>>),
        from_fn(With::<FunctionUpdate>::extract::<Option<Vec<TableDependencyDto>>>),
        from_fn(With::<FunctionUpdate>::extract::<Option<Vec<TableTriggerDto>>>),
        // Validate tables do not exist
        from_fn(validate_tables_do_not_exist),
        // check private tables
        from_fn(check_private_tables::<TableDependencyDto>),
        from_fn(check_private_tables::<TableTriggerDto>),
        // Extract reuse frozen
        from_fn(With::<FunctionUpdate>::extract::<ReuseFrozen>),
        // And register new ones
        register_tables(),
        register_dependencies::<_, DO_AUTHZ>(),
        register_triggers::<_, DO_AUTHZ>(),
        // Response
        // Extract new function version id
        from_fn(With::<FunctionDB>::extract::<FunctionVersionId>),
        from_fn(By::<FunctionVersionId>::select::<FunctionDBWithNames>),
        from_fn(With::<FunctionDBWithNames>::convert_to::<FunctionBuilder, _>),
        from_fn(With::<FunctionBuilder>::build::<Function, _>),
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
    use ta_services::service::TdService;
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_objects::crudl::handle_sql_err;
    use td_objects::rest_urls::CollectionParam;
    use td_objects::sql::SelectBy;
    use td_objects::sql::cte::CteQueries;
    use td_objects::sql::recursive::RecursiveQueries;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_function::seed_function;
    use td_objects::test_utils::seed_inter_collection_permission::seed_inter_collection_permission;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, Decorator, FunctionRuntimeValues, RoleId, TableDependencyDto,
        TableName, TableNameDto, TableStatus, ToCollectionId, UserId,
    };
    use td_objects::types::function::FunctionRegister;
    use td_objects::types::table::TableDB;
    use td_objects::types::trigger::TriggerDB;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_update_function(db: DbPool) {
        use crate::function::layers::register::{
            build_dependency_versions, build_table_versions, build_tables_trigger_versions,
            build_trigger_versions,
        };
        use td_objects::tower_service::authz::InterColl;
        use td_objects::tower_service::from::{ConvertIntoMapService, VecBuildService};
        use td_objects::tower_service::sql::insert_vec;
        use td_objects::types::basic::ReuseFrozen;
        use td_objects::types::dependency::DependencyDBBuilder;
        use td_objects::types::permission::{InterCollectionAccess, InterCollectionAccessBuilder};
        use td_objects::types::table::TableDBBuilder;
        use td_objects::types::trigger::{TriggerDB, TriggerDBBuilder, TriggerDBWithNames};

        use td_tower::metadata::type_of_val;

        UpdateFunctionService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<UpdateRequest<FunctionParam, FunctionUpdate>, Function>(
            &[
                type_of_val(&With::<UpdateRequest<FunctionParam, FunctionUpdate>>::extract::<RequestContext>),
                type_of_val(&With::<UpdateRequest<FunctionParam, FunctionUpdate>>::extract_name::<FunctionParam>),
                type_of_val(&With::<UpdateRequest<FunctionParam, FunctionUpdate>>::extract_data::<FunctionUpdate>),
                // Extract collection and current function from request.
                type_of_val(&With::<FunctionParam>::extract::<CollectionIdName>),
                type_of_val(&With::<FunctionParam>::extract::<FunctionIdName>),
                // Get collection. Extract collection id and name.
                type_of_val(&By::<CollectionIdName>::select::<CollectionDB>),
                type_of_val(&With::<CollectionDB>::extract::<CollectionId>),
                // check requester is coll_admin or coll_dev for the function's collection
                type_of_val(&AuthzOn::<CollectionId>::set),
                type_of_val(&Authz::<CollAdmin, CollDev>::check),

                type_of_val(&With::<CollectionDB>::extract::<CollectionName>),
                // Get function. Extract function id and name.
                type_of_val(&combine::<CollectionIdName, FunctionIdName>),
                type_of_val(&With::<RequestContext>::extract::<AtTime>),
                type_of_val(&FunctionStatus::active_or_frozen),
                type_of_val(&By::<(CollectionIdName, FunctionIdName)>::select_version::<FunctionDBWithNames>),
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
                type_of_val(&With::<FunctionUpdate>::convert_to::<FunctionDBBuilder, _>),
                type_of_val(&With::<RequestContext>::update::<FunctionDBBuilder, _>),
                type_of_val(&With::<CollectionId>::set::<FunctionDBBuilder>),
                // We maintain the same function id
                type_of_val(&With::<FunctionId>::set::<FunctionDBBuilder>),
                type_of_val(&With::<StorageVersion>::set::<FunctionDBBuilder>),
                type_of_val(&With::<DataLocation>::set::<FunctionDBBuilder>),
                type_of_val(&With::<FunctionDBBuilder>::build::<FunctionDB, _>),
                type_of_val(&insert::<FunctionDB>),
                // Remove from bundles
                type_of_val(&With::<FunctionDB>::extract::<BundleId>),
                type_of_val(&By::<BundleId>::delete::<BundleDB>),
                // Register associations
                // Find previous versions
                type_of_val(&TableStatus::active_or_frozen),
                type_of_val(&By::<FunctionId>::select_all_versions::<TableDB>),
                type_of_val(&DependencyStatus::active),
                type_of_val(&By::<FunctionId>::select_all_versions::<DependencyDB>),
                type_of_val(&TriggerStatus::active_or_frozen),
                type_of_val(&By::<FunctionId>::select_all_versions::<TriggerDBWithNames>),
                // Extract new associations
                type_of_val(&With::<FunctionUpdate>::extract::<Option<Vec<TableNameDto>>>),
                type_of_val(&With::<FunctionUpdate>::extract::<Option<Vec<TableDependencyDto>>>),
                type_of_val(&With::<FunctionUpdate>::extract::<Option<Vec<TableTriggerDto>>>),
                // Validate tables do not exist
                type_of_val(&validate_tables_do_not_exist),
                // check private tables
                type_of_val(&check_private_tables::<TableDependencyDto>),
                type_of_val(&check_private_tables::<TableTriggerDto>),
                // Extract reuse frozen
                type_of_val(&With::<FunctionUpdate>::extract::<ReuseFrozen>),
                // And register new ones
                // Insert into table_versions(sql) current function tables status=Active.
                // Reuse table_id for tables that existed (had status=Frozen)
                type_of_val(&With::<FunctionDB>::convert_to::<TableDBBuilder, _>),
                type_of_val(&With::<RequestContext>::update::<TableDBBuilder, _>),
                type_of_val(&build_table_versions),
                type_of_val(&insert_vec::<TableDB>),
                type_of_val(&build_tables_trigger_versions),
                type_of_val(&insert_vec::<TriggerDB>),
                // Insert into dependency_versions(sql) current function table dependencies status=Active.
                type_of_val(&With::<FunctionDB>::convert_to::<DependencyDBBuilder, _>),
                type_of_val(&With::<RequestContext>::update::<DependencyDBBuilder, _>),
                type_of_val(&build_dependency_versions),

                // inter collections check for dependencies
                type_of_val(&With::<DependencyDB>::vec_convert_to::<InterCollectionAccessBuilder, _>),
                type_of_val(&With::<InterCollectionAccessBuilder>::vec_build::<InterCollectionAccess, _>),
                type_of_val(&Authz::<InterColl>::check_inter_collection),

                type_of_val(&insert_vec::<DependencyDB>),
                // Insert into trigger_versions(sql) current function trigger status=Active.
                type_of_val(&With::<FunctionDB>::convert_to::<TriggerDBBuilder, _>),
                type_of_val(&With::<RequestContext>::update::<TriggerDBBuilder, _>),
                type_of_val(&build_trigger_versions),

                // inter collections check for trigger
                type_of_val(&With::<TriggerDB>::vec_convert_to::<InterCollectionAccessBuilder, _>),
                type_of_val(&With::<InterCollectionAccessBuilder>::vec_build::<InterCollectionAccess, _>),
                type_of_val(&Authz::<InterColl>::check_inter_collection),

                type_of_val(&insert_vec::<TriggerDB>),
                // Response
                // Extract new function version id
                type_of_val(&With::<FunctionDB>::extract::<FunctionVersionId>),
                type_of_val(&By::<FunctionVersionId>::select::<FunctionDBWithNames>),
                type_of_val(&With::<FunctionDBWithNames>::convert_to::<FunctionBuilder, _>),
                type_of_val(&With::<FunctionBuilder>::build::<Function, _>),
            ],
        );
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_update_fields(db: DbPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &UserId::admin()).await;

        let create = FunctionUpdate::builder()
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

        let created_function = seed_function(&db, &collection, &create).await;

        let update = FunctionUpdate::builder()
            .try_name("foo_updated")?
            .try_description("foo_updated description updated")?
            .bundle_id(BundleId::default())
            .try_snippet("foo_updated snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(None)
            .runtime_values(FunctionRuntimeValues::try_from(
                "foo_updated runtime values",
            )?)
            .reuse_frozen_tables(false)
            .build()?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).update(
                FunctionParam::builder()
                    .try_collection(format!("{}", collection.name()))?
                    .try_function("foo")?
                    .build()?,
                update.clone(),
            );

        let service = UpdateFunctionService::with_defaults(db.clone())
            .service()
            .await;
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

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_update_add_new_table(db: DbPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &UserId::admin()).await;

        let create = FunctionUpdate::builder()
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

        let created_function = seed_function(&db, &collection, &create).await;

        let update = FunctionUpdate::builder()
            .try_name("joaquin_workout_updated")?
            .try_description("function_foo description updated")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet updated")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![TableNameDto::try_from("new_table")?]))
            .runtime_values(FunctionRuntimeValues::try_from("new mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).update(
                FunctionParam::builder()
                    .try_collection(format!("~{}", collection.id()))?
                    .try_function("joaquin_workout")?
                    .build()?,
                update.clone(),
            );

        let service = UpdateFunctionService::with_defaults(db.clone())
            .service()
            .await;
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

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_update_remove_table(db: DbPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &UserId::admin()).await;

        let create = FunctionUpdate::builder()
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

        let created_function = seed_function(&db, &collection, &create).await;

        let update = FunctionUpdate::builder()
            .try_name("joaquin_workout_updated")?
            .try_description("function_foo description updated")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet updated")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(None)
            .runtime_values(FunctionRuntimeValues::try_from("new mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sys_admin(),
        )
        .update(
            FunctionParam::builder()
                .try_collection(format!("~{}", collection.id()))?
                .try_function("joaquin_workout")?
                .build()?,
            update.clone(),
        );

        let service = UpdateFunctionService::with_defaults(db.clone())
            .service()
            .await;
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

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_update_maintain_table(db: DbPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &UserId::admin()).await;

        let create = FunctionUpdate::builder()
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

        let created_function = seed_function(&db, &collection, &create).await;

        let update = FunctionUpdate::builder()
            .try_name("joaquin_workout_updated")?
            .try_description("function_foo description updated")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet updated")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![TableNameDto::try_from("new_table")?]))
            .runtime_values(FunctionRuntimeValues::try_from("new mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).update(
                FunctionParam::builder()
                    .try_collection(format!("~{}", collection.id()))?
                    .try_function("joaquin_workout")?
                    .build()?,
                update.clone(),
            );

        let service = UpdateFunctionService::with_defaults(db.clone())
            .service()
            .await;
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

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_update_add_dependencies(db: DbPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &UserId::admin()).await;

        let create = FunctionUpdate::builder()
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

        let created_function = seed_function(&db, &collection, &create).await;

        let update = FunctionUpdate::builder()
            .try_name("joaquin_workout_updated")?
            .try_description("function_foo description updated")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet updated")?
            .decorator(Decorator::Publisher)
            .dependencies(Some(vec![TableDependencyDto::try_from("new_table")?]))
            .triggers(None)
            .tables(Some(vec![TableNameDto::try_from("new_table")?]))
            .runtime_values(FunctionRuntimeValues::try_from("new mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).update(
                FunctionParam::builder()
                    .try_collection(format!("~{}", collection.id()))?
                    .try_function("joaquin_workout")?
                    .build()?,
                update.clone(),
            );

        let service = UpdateFunctionService::with_defaults(db.clone())
            .service()
            .await;
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

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_update_remove_dependencies(db: DbPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &UserId::admin()).await;

        let create = FunctionUpdate::builder()
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

        let created_function = seed_function(&db, &collection, &create).await;

        let update = FunctionUpdate::builder()
            .try_name("joaquin_workout_updated")?
            .try_description("function_foo description updated")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet updated")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![TableNameDto::try_from("new_table")?]))
            .runtime_values(FunctionRuntimeValues::try_from("new mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).update(
                FunctionParam::builder()
                    .try_collection(format!("~{}", collection.id()))?
                    .try_function("joaquin_workout")?
                    .build()?,
                update.clone(),
            );

        let service = UpdateFunctionService::with_defaults(db.clone())
            .service()
            .await;
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

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_update_maintain_dependencies(db: DbPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &UserId::admin()).await;

        let create = FunctionUpdate::builder()
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

        let created_function = seed_function(&db, &collection, &create).await;

        let update = FunctionUpdate::builder()
            .try_name("joaquin_workout_updated")?
            .try_description("function_foo description updated")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet updated")?
            .decorator(Decorator::Publisher)
            .dependencies(Some(vec![TableDependencyDto::try_from("new_table")?]))
            .triggers(None)
            .tables(Some(vec![TableNameDto::try_from("new_table")?]))
            .runtime_values(FunctionRuntimeValues::try_from("new mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).update(
                FunctionParam::builder()
                    .try_collection(format!("~{}", collection.id()))?
                    .try_function("joaquin_workout")?
                    .build()?,
                update.clone(),
            );

        let service = UpdateFunctionService::with_defaults(db.clone())
            .service()
            .await;
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

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_update_add_trigger(db: DbPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &UserId::admin()).await;

        let trigger_create = FunctionUpdate::builder()
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

        let _ = seed_function(&db, &collection, &trigger_create).await;

        // Actual test
        let create = FunctionUpdate::builder()
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

        let created_function = seed_function(&db, &collection, &create).await;

        let update = FunctionUpdate::builder()
            .try_name("joaquin_workout_updated")?
            .try_description("function_foo description updated")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet updated")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(Some(vec![TableTriggerDto::try_from("trigger_table")?]))
            .tables(None)
            .runtime_values(FunctionRuntimeValues::try_from("new mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).update(
                FunctionParam::builder()
                    .try_collection(format!("{}", collection.name()))?
                    .try_function("joaquin_workout")?
                    .build()?,
                update.clone(),
            );

        let service = UpdateFunctionService::with_defaults(db.clone())
            .service()
            .await;
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

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_update_remove_trigger(db: DbPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &UserId::admin()).await;

        let trigger_create = FunctionUpdate::builder()
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

        let _ = seed_function(&db, &collection, &trigger_create).await;

        // Actual test
        let create = FunctionUpdate::builder()
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

        let created_function = seed_function(&db, &collection, &create).await;

        let update = FunctionUpdate::builder()
            .try_name("joaquin_workout_updated")?
            .try_description("function_foo description updated")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet updated")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(None)
            .runtime_values(FunctionRuntimeValues::try_from("new mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sys_admin(),
        )
        .update(
            FunctionParam::builder()
                .try_collection(format!("{}", collection.name()))?
                .try_function("joaquin_workout")?
                .build()?,
            update.clone(),
        );

        let service = UpdateFunctionService::with_defaults(db.clone())
            .service()
            .await;
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

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_update_maintain_trigger(db: DbPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &UserId::admin()).await;

        let trigger_create = FunctionUpdate::builder()
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

        let _ = seed_function(&db, &collection, &trigger_create).await;

        // Actual test
        let create = FunctionUpdate::builder()
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

        let created_function = seed_function(&db, &collection, &create).await;

        let update = FunctionUpdate::builder()
            .try_name("joaquin_workout_updated")?
            .try_description("function_foo description updated")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet updated")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(Some(vec![TableTriggerDto::try_from("trigger_table")?]))
            .tables(None)
            .runtime_values(FunctionRuntimeValues::try_from("new mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).update(
                FunctionParam::builder()
                    .try_collection(format!("{}", collection.name()))?
                    .try_function("joaquin_workout")?
                    .build()?,
                update.clone(),
            );

        let service = UpdateFunctionService::with_defaults(db.clone())
            .service()
            .await;
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

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_update_change_everything(db: DbPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &UserId::admin()).await;

        let trigger_create = FunctionUpdate::builder()
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

        let _ = seed_function(&db, &collection, &trigger_create).await;

        // Actual test
        let create = FunctionUpdate::builder()
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

        let created_function = seed_function(&db, &collection, &create).await;

        let update = FunctionUpdate::builder()
            .try_name("joaquin_workout_updated")?
            .try_description("function_foo description updated")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet updated")?
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
            .reuse_frozen_tables(false)
            .build()?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).update(
                FunctionParam::builder()
                    .try_collection(format!("{}", collection.name()))?
                    .try_function("joaquin_workout")?
                    .build()?,
                update.clone(),
            );

        let service = UpdateFunctionService::with_defaults(db.clone())
            .service()
            .await;
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

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_update_freeze_unfreeze(db: DbPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &UserId::admin()).await;

        let create = FunctionUpdate::builder()
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

        let _created_function = seed_function(&db, &collection, &create).await;

        let update = FunctionUpdate::builder()
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

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).update(
                FunctionParam::builder()
                    .try_collection(format!("{}", collection.name()))?
                    .try_function("joaquin_workout")?
                    .build()?,
                update.clone(),
            );

        let service = UpdateFunctionService::with_defaults(db.clone())
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        let _response = response?;

        let tables: Vec<TableDB> = DaoQueries::default()
            .select_by::<TableDB>(&(&TableName::try_from("joaquin_table")?, &TableStatus::Frozen))?
            .build_query_as()
            .fetch_all(&db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(tables.len(), 1);

        let update = FunctionUpdate::builder()
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

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).update(
                FunctionParam::builder()
                    .try_collection(format!("{}", collection.name()))?
                    .try_function("joaquin_workout")?
                    .build()?,
                update.clone(),
            );

        let service = UpdateFunctionService::with_defaults(db.clone())
            .service()
            .await;
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
            .try_name("joaquin_workout")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![TableNameDto::try_from("joaquin_table")?]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(true)
            .build()?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).update(
                FunctionParam::builder()
                    .try_collection(format!("{}", collection.name()))?
                    .try_function("joaquin_workout")?
                    .build()?,
                update.clone(),
            );

        let service = UpdateFunctionService::with_defaults(db.clone())
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        let _response = response?;
        // But with reuse_frozen, we get the expected response

        let tables: Vec<TableDB> = DaoQueries::default()
            .select_by::<TableDB>(&(&TableName::try_from("joaquin_table")?))?
            .build_query_as()
            .fetch_all(&db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(tables.len(), 3);
        assert_eq!(*tables[0].status(), TableStatus::Active);
        assert_eq!(*tables[1].status(), TableStatus::Frozen);
        assert_eq!(*tables[2].status(), TableStatus::Active);
        assert_eq!(tables[0].table_id(), tables[1].table_id());
        assert_eq!(tables[1].table_id(), tables[2].table_id());
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_update_same_name(db: DbPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &UserId::admin()).await;

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
        let created_function = seed_function(&db, &collection, &create).await;

        let update = FunctionUpdate::builder()
            .try_name("function_foo_2")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![TableNameDto::try_from("joaquin_table")?]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(true)
            .build()?;

        let update_request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).update(
                FunctionParam::builder()
                    .try_collection(format!("{}", collection.name()))?
                    .try_function("function_foo")?
                    .build()?,
                update.clone(),
            );

        let service = UpdateFunctionService::with_defaults(db.clone())
            .service()
            .await;
        let response = service.raw_oneshot(update_request).await;
        let update_response = response?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).create(
                CollectionParam::builder()
                    .try_collection(collection_name.as_str())?
                    .build()?,
                create.clone(),
            );
        let service = RegisterFunctionService::with_defaults(db.clone())
            .service()
            .await;
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

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_update_private_tables_all_same_collection(db: DbPool) -> Result<(), TdError> {
        test_private_tables(db, false, false).await
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_update_private_tables_deps_diff_collection(db: DbPool) -> Result<(), TdError> {
        test_private_tables(db, true, false).await
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_update_private_tables_triggers_diff_collection(
        db: DbPool,
    ) -> Result<(), TdError> {
        test_private_tables(db, false, true).await
    }

    async fn test_private_tables(
        db: DbPool,
        deps_diff_collection: bool,
        triggers_diff_collection: bool,
    ) -> Result<(), TdError> {
        let collection_name_1 = CollectionName::try_from("collection_1")?;
        let collection_1 = seed_collection(&db, &collection_name_1, &UserId::admin()).await;
        let collection_name_2 = CollectionName::try_from("collection_2")?;
        let collection_2 = seed_collection(&db, &collection_name_2, &UserId::admin()).await;

        seed_inter_collection_permission(
            &db,
            collection_1.id(),
            &ToCollectionId::try_from(collection_2.id())?,
        )
        .await;

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
            seed_function(&db, collection, &create).await;
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
        seed_function(&db, &collection_2, &create).await;

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
        let update = FunctionRegister::builder()
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

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).update(
                FunctionParam::builder()
                    .try_collection(format!("{}", collection_2.name()))?
                    .try_function("function_2")?
                    .build()?,
                update.clone(),
            );

        let service = UpdateFunctionService::with_defaults(db.clone())
            .service()
            .await;
        let res = service.raw_oneshot(request).await;
        if deps_diff_collection || triggers_diff_collection {
            assert!(res.is_err());
            assert!(res.err().unwrap().code().contains("PrivateTableError"))
        } else {
            assert!(res.is_ok());
        }
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_private_table_flag(db: DbPool) -> Result<(), TdError> {
        let collection_name_1 = CollectionName::try_from("collection_1")?;
        let collection_1 = seed_collection(&db, &collection_name_1, &UserId::admin()).await;

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
        seed_function(&db, &collection_1, &create).await;

        let tables = Some(vec![
            TableNameDto::try_from("table0")?,
            TableNameDto::try_from("_table0")?,
        ]);

        let bundle_id = BundleId::default();
        let update = FunctionRegister::builder()
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

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).update(
                FunctionParam::builder()
                    .try_collection(format!("{}", collection_1.name()))?
                    .try_function("function")?
                    .build()?,
                update.clone(),
            );

        let service = UpdateFunctionService::with_defaults(db.clone())
            .service()
            .await;
        let res = service.raw_oneshot(request).await;
        assert!(res.is_ok());

        let tables: Vec<TableDB> = DaoQueries::default()
            .select_by::<TableDB>(&(res.unwrap().id()))?
            .build_query_as()
            .fetch_all(&db)
            .await
            .map_err(handle_sql_err)?;
        let map: HashMap<String, TableDB> = tables
            .into_iter()
            .map(|table| (table.name().to_string(), table))
            .collect();
        assert!(!map.get("table0").unwrap().private().deref());
        assert!(map.get("_table0").unwrap().private().deref());
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_update_maintain_triggers_dependencies(db: DbPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &UserId::admin()).await;

        let create = FunctionUpdate::builder()
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

        let created_function = seed_function(&db, &collection, &create).await;

        let dependant = FunctionUpdate::builder()
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

        let _ = seed_function(&db, &collection, &dependant).await;

        let update = FunctionUpdate::builder()
            .try_name("function_1")?
            .try_description("description")?
            .bundle_id(BundleId::default())
            .try_snippet("snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![TableNameDto::try_from("table_1")?]))
            .runtime_values(FunctionRuntimeValues::default())
            .reuse_frozen_tables(false)
            .build()?;

        // With an update, a new function version and table version is created
        // But the trigger and dependency should still be valid (as the use id, not version id)
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).update(
                FunctionParam::builder()
                    .try_collection(format!("~{}", collection.id()))?
                    .try_function("function_1")?
                    .build()?,
                update.clone(),
            );

        let service = UpdateFunctionService::with_defaults(db.clone())
            .service()
            .await;
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

        let recursive_downstream_trigger: Vec<TriggerDB> = DaoQueries::default()
            .select_recursive_versions_at::<TriggerDB, FunctionDB, _>(
                None,
                Some(&[&TriggerStatus::Active]),
                None,
                Some(&[&FunctionStatus::Active]),
                response.function_id(),
            )?
            .build_query_as()
            .fetch_all(&db)
            .await
            .unwrap();
        // 2 triggered functions, 1 downstream
        assert_eq!(recursive_downstream_trigger.len(), 1);
        let triggered_function_id = recursive_downstream_trigger[0].function_id();
        let triggered_function: FunctionDBWithNames = DaoQueries::default()
            .select_versions_at::<FunctionDBWithNames>(None, None, &(triggered_function_id))?
            .build_query_as()
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(triggered_function.name(), dependant.name());

        let downstream_dependencies: Vec<DependencyDB> = DaoQueries::default()
            .select_versions_at::<DependencyDB>(
                None,
                Some(&[&DependencyStatus::Active]),
                &(triggered_function_id),
            )?
            .build_query_as()
            .fetch_all(&db)
            .await
            .unwrap();
        // 1 downstream dependency on the triggered function
        assert_eq!(downstream_dependencies.len(), 1);
        let dependency_function_id = recursive_downstream_trigger[0].function_id();
        let dependency_function: FunctionDBWithNames = DaoQueries::default()
            .select_versions_at::<FunctionDBWithNames>(None, None, &(dependency_function_id))?
            .build_query_as()
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(dependency_function.name(), dependant.name());
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_update_remove_triggers_dependencies(db: DbPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &UserId::admin()).await;

        let create = FunctionUpdate::builder()
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

        let _ = seed_function(&db, &collection, &create).await;

        let dependant = FunctionUpdate::builder()
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

        let dependant_function = seed_function(&db, &collection, &dependant).await;

        let update = FunctionUpdate::builder()
            .try_name("function_2")?
            .try_description("description")?
            .bundle_id(BundleId::default())
            .try_snippet("snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(None)
            .runtime_values(FunctionRuntimeValues::default())
            .reuse_frozen_tables(false)
            .build()?;

        // With an update, a new function version and table version is created
        // But the trigger and dependency should still be valid (as the use id, not version id)
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).update(
                FunctionParam::builder()
                    .try_collection(collection.name().to_string())?
                    .try_function("function_2")?
                    .build()?,
                update.clone(),
            );

        let service = UpdateFunctionService::with_defaults(db.clone())
            .service()
            .await;
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

        let recursive_downstream_trigger: Vec<TriggerDB> = DaoQueries::default()
            .select_recursive_versions_at::<TriggerDB, FunctionDB, _>(
                None,
                Some(&[&TriggerStatus::Active]),
                None,
                Some(&[&FunctionStatus::Active]),
                response.function_id(),
            )?
            .build_query_as()
            .fetch_all(&db)
            .await
            .unwrap();
        // Downstream trigger removed
        assert!(recursive_downstream_trigger.is_empty());

        // Downstream dependency removed
        let dependency_function: Vec<FunctionDBWithNames> = DaoQueries::default()
            .select_versions_at::<FunctionDBWithNames>(None, None, &(response.function_id()))?
            .build_query_as()
            .fetch_all(&db)
            .await
            .unwrap();
        assert_eq!(dependency_function.len(), 1); // only implicit self dependency
        assert_eq!(dependency_function[0].name(), dependant.name());
        Ok(())
    }
}
