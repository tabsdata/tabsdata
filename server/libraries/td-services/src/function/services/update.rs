//
// Copyright 2025 Tabs Data Inc.
//

use crate::function::layers::register::data_location;
use crate::function::layers::update::assert_function_name_not_exists;
use std::sync::Arc;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{RequestContext, UpdateRequest};
use td_objects::rest_urls::FunctionParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::from::{
    combine, BuildService, DefaultService, ExtractDataService, ExtractNameService, ExtractService,
    SetService, TryIntoService, UpdateService, With,
};
use td_objects::tower_service::sql::{
    insert, By, SqlDeleteService, SqlSelectAllService, SqlSelectService,
};
use td_objects::types::basic::{
    AtTime, BundleId, CollectionId, CollectionIdName, CollectionName, DataLocation, FunctionId,
    FunctionIdName, FunctionStatus, FunctionVersionId, ReuseFrozen, StorageVersion,
    TableDependencyDto, TableNameDto, TableTriggerDto,
};
use td_objects::types::collection::CollectionDB;
use td_objects::types::dependency::DependencyDB;
use td_objects::types::function::{
    BundleDB, Function, FunctionBuilder, FunctionDB, FunctionDBBuilder, FunctionDBWithNames,
    FunctionUpdate,
};
use td_objects::types::table::TableDB;
use td_objects::types::trigger::TriggerDBWithNames;
use td_tower::default_services::{SrvCtxProvider, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

use super::register::RegisterFunctionService;

pub struct UpdateFunctionService {
    provider: ServiceProvider<UpdateRequest<FunctionParam, FunctionUpdate>, Function, TdError>,
}

impl UpdateFunctionService {
    pub fn new(db: DbPool) -> Self {
        let queries = Arc::new(DaoQueries::default());
        Self {
            provider: Self::provider(db, queries),
        }
    }

    p! {
        provider(db: DbPool, queries: Arc<DaoQueries>) {
            service_provider!(layers!(
                SrvCtxProvider::new(queries),
                from_fn(With::<UpdateRequest<FunctionParam, FunctionUpdate>>::extract::<RequestContext>),
                from_fn(With::<UpdateRequest<FunctionParam, FunctionUpdate>>::extract_name::<FunctionParam>),
                from_fn(With::<UpdateRequest<FunctionParam, FunctionUpdate>>::extract_data::<FunctionUpdate>),

                TransactionProvider::new(db),

                // Extract collection and current function from request.
                from_fn(With::<FunctionParam>::extract::<CollectionIdName>),
                from_fn(With::<FunctionParam>::extract::<FunctionIdName>),

                // Get collection. Extract collection id and name.
                from_fn(By::<CollectionIdName>::select::<DaoQueries, CollectionDB>),
                from_fn(With::<CollectionDB>::extract::<CollectionId>),
                from_fn(With::<CollectionDB>::extract::<CollectionName>),

                // Get function. Extract function id and name.
                from_fn(combine::<CollectionIdName, FunctionIdName>),
                from_fn(With::<RequestContext>::extract::<AtTime>),
                from_fn(FunctionStatus::active),
                from_fn(By::<(CollectionIdName, FunctionIdName)>::select_version::<DaoQueries, FunctionDBWithNames>),
                // This is, before update function id and function version id. Function id does
                // not change, but function version id does.
                from_fn(With::<FunctionDBWithNames>::extract::<FunctionId>),
                from_fn(With::<FunctionDBWithNames>::extract::<FunctionVersionId>),

                // If function has a new name, check new name does not exist in collection.
                from_fn(assert_function_name_not_exists::<DaoQueries>),

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
                from_fn(insert::<DaoQueries, FunctionDB>),

                // Remove from bundles
                from_fn(With::<FunctionDB>::extract::<BundleId>),
                from_fn(By::<BundleId>::delete::<DaoQueries, BundleDB>),

                // Register associations
                // Find previous versions
                from_fn(By::<FunctionVersionId>::select_all::<DaoQueries, TableDB>),
                from_fn(By::<FunctionVersionId>::select_all::<DaoQueries, DependencyDB>),
                from_fn(By::<FunctionVersionId>::select_all::<DaoQueries, TriggerDBWithNames>),
                // Extract new associations
                from_fn(With::<FunctionUpdate>::extract::<Option<Vec<TableNameDto>>>),
                from_fn(With::<FunctionUpdate>::extract::<Option<Vec<TableDependencyDto>>>),
                from_fn(With::<FunctionUpdate>::extract::<Option<Vec<TableTriggerDto>>>),
                // Extract reuse frozen
                from_fn(With::<FunctionUpdate>::extract::<ReuseFrozen>),
                // And register new ones
                RegisterFunctionService::register_tables(),
                RegisterFunctionService::register_dependencies(),
                RegisterFunctionService::register_triggers(),

                // Response
                // Extract new function version id
                from_fn(With::<FunctionDB>::extract::<FunctionVersionId>),
                from_fn(By::<FunctionVersionId>::select::<DaoQueries, FunctionDBWithNames>),
                from_fn(With::<FunctionDBWithNames>::convert_to::<FunctionBuilder, _>),
                from_fn(With::<FunctionBuilder>::build::<Function, _>),
            ))
        }
    }

    pub async fn service(
        &self,
    ) -> TdBoxService<UpdateRequest<FunctionParam, FunctionUpdate>, Function, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::function::layers::register::RegisterFunctionError;
    use crate::function::services::tests::assert_update;
    use td_objects::crudl::handle_sql_err;
    use td_objects::sql::SelectBy;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_function::seed_function;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, Decorator, FunctionRuntimeValues, RoleId, TableDependencyDto,
        TableName, TableNameDto, TableStatus, UserId,
    };
    use td_objects::types::table::TableDB;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_update_function(db: DbPool) {
        use td_tower::metadata::{type_of_val, Metadata};

        use crate::function::layers::register::{
            build_dependency_versions, build_table_versions, build_trigger_versions,
        };
        use td_objects::tower_service::sql::insert_vec;
        use td_objects::types::basic::ReuseFrozen;
        use td_objects::types::dependency::DependencyDBBuilder;
        use td_objects::types::table::TableDBBuilder;
        use td_objects::types::trigger::{TriggerDB, TriggerDBBuilder, TriggerDBWithNames};

        let queries = Arc::new(DaoQueries::default());
        let provider = UpdateFunctionService::provider(db, queries);
        let service = provider.make().await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();

        metadata.assert_service::<UpdateRequest<FunctionParam, FunctionUpdate>, Function>(
            &[
                type_of_val(&With::<UpdateRequest<FunctionParam, FunctionUpdate>>::extract::<RequestContext>),
                type_of_val(&With::<UpdateRequest<FunctionParam, FunctionUpdate>>::extract_name::<FunctionParam>),
                type_of_val(&With::<UpdateRequest<FunctionParam, FunctionUpdate>>::extract_data::<FunctionUpdate>),
                // Extract collection and current function from request.
                type_of_val(&With::<FunctionParam>::extract::<CollectionIdName>),
                type_of_val(&With::<FunctionParam>::extract::<FunctionIdName>),
                // Get collection. Extract collection id and name.
                type_of_val(&By::<CollectionIdName>::select::<DaoQueries, CollectionDB>),
                type_of_val(&With::<CollectionDB>::extract::<CollectionId>),
                type_of_val(&With::<CollectionDB>::extract::<CollectionName>),
                // Get function. Extract function id and name.
                type_of_val(&combine::<CollectionIdName, FunctionIdName>),
                type_of_val(&With::<RequestContext>::extract::<AtTime>),
                type_of_val(&FunctionStatus::active),
                type_of_val(&By::<(CollectionIdName, FunctionIdName)>::select_version::<DaoQueries, FunctionDBWithNames>),
                // This is, before update function id and function version id. Function id does
                // not change, but function version id does.
                type_of_val(&With::<FunctionDBWithNames>::extract::<FunctionId>),
                type_of_val(&With::<FunctionDBWithNames>::extract::<FunctionVersionId>),
                // If function has a new name, check new name does not exist in collection.
                type_of_val(&assert_function_name_not_exists::<DaoQueries>),
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
                type_of_val(&insert::<DaoQueries, FunctionDB>),
                // Remove from bundles
                type_of_val(&With::<FunctionDB>::extract::<BundleId>),
                type_of_val(&By::<BundleId>::delete::<DaoQueries, BundleDB>),
                // Register associations
                // Find previous versions
                type_of_val(&By::<FunctionVersionId>::select_all::<DaoQueries, TableDB>),
                type_of_val(&By::<FunctionVersionId>::select_all::<DaoQueries, DependencyDB>),
                type_of_val(&By::<FunctionVersionId>::select_all::<DaoQueries, TriggerDBWithNames>),
                // Extract new associations
                type_of_val(&With::<FunctionUpdate>::extract::<Option<Vec<TableNameDto>>>),
                type_of_val(&With::<FunctionUpdate>::extract::<Option<Vec<TableDependencyDto>>>),
                type_of_val(&With::<FunctionUpdate>::extract::<Option<Vec<TableTriggerDto>>>),
                // Extract reuse frozen
                type_of_val(&With::<FunctionUpdate>::extract::<ReuseFrozen>),
                // And register new ones
                // Insert into table_versions(sql) current function tables status=Active.
                // Reuse table_id for tables that existed (had status=Frozen)
                type_of_val(&With::<FunctionDB>::convert_to::<TableDBBuilder, _>),
                type_of_val(&With::<RequestContext>::update::<TableDBBuilder, _>),
                type_of_val(&build_table_versions),
                type_of_val(&insert_vec::<DaoQueries, TableDB>),
                // Insert into dependency_versions(sql) current function table dependencies status=Active.
                type_of_val(&With::<FunctionDB>::convert_to::<DependencyDBBuilder, _>),
                type_of_val(&With::<RequestContext>::update::<DependencyDBBuilder, _>),
                type_of_val(&build_dependency_versions::<DaoQueries>),
                type_of_val(&insert_vec::<DaoQueries, DependencyDB>),
                // Insert into trigger_versions(sql) current function trigger status=Active.
                type_of_val(&With::<FunctionDB>::convert_to::<TriggerDBBuilder, _>),
                type_of_val(&With::<RequestContext>::update::<TriggerDBBuilder, _>),
                type_of_val(&build_trigger_versions::<DaoQueries>),
                type_of_val(&insert_vec::<DaoQueries, TriggerDB>),
                // Response
                // Extract new function version id
                type_of_val(&With::<FunctionDB>::extract::<FunctionVersionId>),
                type_of_val(&By::<FunctionVersionId>::select::<DaoQueries, FunctionDBWithNames>),
                type_of_val(&With::<FunctionDBWithNames>::convert_to::<FunctionBuilder, _>),
                type_of_val(&With::<FunctionBuilder>::build::<Function, _>),
            ],
        );
    }

    #[td_test::test(sqlx)]
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

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            true,
        )
        .update(
            FunctionParam::builder()
                .try_collection(format!("{}", collection.name()))?
                .try_function("foo")?
                .build()?,
            update.clone(),
        );

        let service = UpdateFunctionService::new(db.clone()).service().await;
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

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            true,
        )
        .update(
            FunctionParam::builder()
                .try_collection(format!("~{}", collection.id()))?
                .try_function("joaquin_workout")?
                .build()?,
            update.clone(),
        );

        let service = UpdateFunctionService::new(db.clone()).service().await;
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
            true,
        )
        .update(
            FunctionParam::builder()
                .try_collection(format!("~{}", collection.id()))?
                .try_function("joaquin_workout")?
                .build()?,
            update.clone(),
        );

        let service = UpdateFunctionService::new(db.clone()).service().await;
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

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            true,
        )
        .update(
            FunctionParam::builder()
                .try_collection(format!("~{}", collection.id()))?
                .try_function("joaquin_workout")?
                .build()?,
            update.clone(),
        );

        let service = UpdateFunctionService::new(db.clone()).service().await;
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

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            true,
        )
        .update(
            FunctionParam::builder()
                .try_collection(format!("~{}", collection.id()))?
                .try_function("joaquin_workout")?
                .build()?,
            update.clone(),
        );

        let service = UpdateFunctionService::new(db.clone()).service().await;
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

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            true,
        )
        .update(
            FunctionParam::builder()
                .try_collection(format!("~{}", collection.id()))?
                .try_function("joaquin_workout")?
                .build()?,
            update.clone(),
        );

        let service = UpdateFunctionService::new(db.clone()).service().await;
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

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            true,
        )
        .update(
            FunctionParam::builder()
                .try_collection(format!("~{}", collection.id()))?
                .try_function("joaquin_workout")?
                .build()?,
            update.clone(),
        );

        let service = UpdateFunctionService::new(db.clone()).service().await;
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

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            true,
        )
        .update(
            FunctionParam::builder()
                .try_collection(format!("{}", collection.name()))?
                .try_function("joaquin_workout")?
                .build()?,
            update.clone(),
        );

        let service = UpdateFunctionService::new(db.clone()).service().await;
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
            true,
        )
        .update(
            FunctionParam::builder()
                .try_collection(format!("{}", collection.name()))?
                .try_function("joaquin_workout")?
                .build()?,
            update.clone(),
        );

        let service = UpdateFunctionService::new(db.clone()).service().await;
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

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            true,
        )
        .update(
            FunctionParam::builder()
                .try_collection(format!("{}", collection.name()))?
                .try_function("joaquin_workout")?
                .build()?,
            update.clone(),
        );

        let service = UpdateFunctionService::new(db.clone()).service().await;
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

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            true,
        )
        .update(
            FunctionParam::builder()
                .try_collection(format!("{}", collection.name()))?
                .try_function("joaquin_workout")?
                .build()?,
            update.clone(),
        );

        let service = UpdateFunctionService::new(db.clone()).service().await;
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
    async fn test_update_freeze_unfreeze(db: DbPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &UserId::admin()).await;
        let queries = DaoQueries::default();

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

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            true,
        )
        .update(
            FunctionParam::builder()
                .try_collection(format!("{}", collection.name()))?
                .try_function("joaquin_workout")?
                .build()?,
            update.clone(),
        );

        let service = UpdateFunctionService::new(db.clone()).service().await;
        let response = service.raw_oneshot(request).await;
        let _response = response?;

        let tables: Vec<TableDB> = queries
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

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            true,
        )
        .update(
            FunctionParam::builder()
                .try_collection(format!("{}", collection.name()))?
                .try_function("joaquin_workout")?
                .build()?,
            update.clone(),
        );

        let service = UpdateFunctionService::new(db.clone()).service().await;
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

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            true,
        )
        .update(
            FunctionParam::builder()
                .try_collection(format!("{}", collection.name()))?
                .try_function("joaquin_workout")?
                .build()?,
            update.clone(),
        );

        let service = UpdateFunctionService::new(db.clone()).service().await;
        let response = service.raw_oneshot(request).await;
        let _response = response?;
        // But with reuse_frozen, we get the expected response

        let tables: Vec<TableDB> = queries
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
}
