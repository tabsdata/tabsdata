//
// Copyright 2025 Tabs Data Inc.
//

use crate::common::layers::extractor::extract_req_dto;
use crate::function::layers::update::assert_function_name_not_exists;
use std::sync::Arc;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{RequestContext, UpdateRequest};
use td_objects::rest_urls::FunctionParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::extractor::{extract_req_context, extract_req_name};
use td_objects::tower_service::from::{
    combine, BuildService, ExtractService, SetService, TryIntoService, UpdateService, With,
};
use td_objects::tower_service::sql::{
    insert, By, SqlSelectAllService, SqlSelectIdOrNameService, SqlSelectService, SqlUpdateService,
};
use td_objects::types::basic::{
    CollectionId, CollectionIdName, CollectionName, FunctionId, FunctionIdName, FunctionVersionId,
    ReuseFrozen, TableDependency, TableName, TableTrigger,
};
use td_objects::types::collection::CollectionDB;
use td_objects::types::dependency::DependencyVersionDB;
use td_objects::types::function::{
    FunctionDB, FunctionDBBuilder, FunctionDBWithNames, FunctionUpdate, FunctionVersion,
    FunctionVersionBuilder, FunctionVersionDB, FunctionVersionDBBuilder,
    FunctionVersionDBWithNames,
};
use td_objects::types::table::TableVersionDB;
use td_objects::types::trigger::TriggerVersionDBWithNames;
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{SrvCtxProvider, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

use super::register::RegisterFunctionService;

pub struct UpdateFunctionService {
    provider:
        ServiceProvider<UpdateRequest<FunctionParam, FunctionUpdate>, FunctionVersion, TdError>,
}

impl UpdateFunctionService {
    pub fn new(db: DbPool) -> Self {
        let queries = Arc::new(DaoQueries::default());
        Self {
            provider: Self::provider(db, queries),
        }
    }

    p! {
        provider(db: DbPool, queries: Arc<DaoQueries>) -> TdError {
            service_provider!(layers!(
                SrvCtxProvider::new(queries),
                from_fn(extract_req_context::<UpdateRequest<FunctionParam, FunctionUpdate>>),
                from_fn(extract_req_dto::<UpdateRequest<FunctionParam, FunctionUpdate>, _>),
                from_fn(extract_req_name::<UpdateRequest<FunctionParam, FunctionUpdate>, _>),

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
                from_fn(By::<(CollectionIdName, FunctionIdName)>::select::<DaoQueries, FunctionDBWithNames>),
                // This is, before update function id and function version id. Function id does
                // not change, but function version id does.
                from_fn(With::<FunctionDBWithNames>::extract::<FunctionId>),
                from_fn(With::<FunctionDBWithNames>::extract::<FunctionVersionId>),

                // If function has a new name, check new name does not exist in collection.
                from_fn(assert_function_name_not_exists::<DaoQueries>),

                // Insert into function_versions(sql) status=Active.
                from_fn(With::<FunctionUpdate>::convert_to::<FunctionVersionDBBuilder, _>),
                from_fn(With::<RequestContext>::update::<FunctionVersionDBBuilder, _>),
                from_fn(With::<CollectionId>::set::<FunctionVersionDBBuilder>),
                // We maintain the same function id
                from_fn(With::<FunctionId>::set::<FunctionVersionDBBuilder>),
                // TODO missing data_location and storage_version
                from_fn(With::<FunctionVersionDBBuilder>::build::<FunctionVersionDB, _>),
                from_fn(insert::<DaoQueries, FunctionVersionDB>),

                // Update functions(sql) table.
                from_fn(With::<FunctionVersionDB>::convert_to::<FunctionDBBuilder, _>),
                from_fn(With::<FunctionDBBuilder>::build::<FunctionDB, _>),
                from_fn(By::<FunctionId>::update::<DaoQueries, FunctionDB, FunctionDB>),

                // Register associations
                // Extract new function id
                from_fn(With::<FunctionDB>::extract::<FunctionId>),
                // Find previous versions
                from_fn(By::<FunctionVersionId>::select_all::<DaoQueries, TableVersionDB>),
                from_fn(By::<FunctionVersionId>::select_all::<DaoQueries, DependencyVersionDB>),
                from_fn(By::<FunctionVersionId>::select_all::<DaoQueries, TriggerVersionDBWithNames>),
                // Extract new associations
                from_fn(With::<FunctionUpdate>::extract::<Option<Vec<TableName>>>),
                from_fn(With::<FunctionUpdate>::extract::<Option<Vec<TableDependency>>>),
                from_fn(With::<FunctionUpdate>::extract::<Option<Vec<TableTrigger>>>),
                // Extract reuse frozen
                from_fn(With::<FunctionUpdate>::extract::<ReuseFrozen>),
                // And register new ones
                RegisterFunctionService::register_tables(),
                RegisterFunctionService::register_dependencies(),
                RegisterFunctionService::register_triggers(),

                // Response
                from_fn(By::<FunctionId>::select::<DaoQueries, FunctionVersionDBWithNames>),
                from_fn(With::<FunctionVersionDBWithNames>::convert_to::<FunctionVersionBuilder, _>),
                from_fn(With::<FunctionVersionBuilder>::build::<FunctionVersion, _>),
            ))
        }
    }

    pub async fn service(
        &self,
    ) -> TdBoxService<UpdateRequest<FunctionParam, FunctionUpdate>, FunctionVersion, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::function::layers::register::RegisterFunctionError;
    use crate::function::services::tests::assert_update;
    use td_common::id::Id;
    use td_objects::crudl::handle_sql_err;
    use td_objects::sql::SelectBy;
    use td_objects::test_utils::seed_collection2::seed_collection;
    use td_objects::test_utils::seed_function2::seed_function;
    use td_objects::test_utils::seed_user::admin_user;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, Frozen, FunctionRuntimeValues, RoleId, UserId,
    };
    use td_objects::types::table::TableDB;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_update_function(db: DbPool) {
        use td_tower::metadata::{type_of_val, Metadata};

        use crate::function::layers::register::{
            build_dependency_versions, build_table_versions, build_trigger_versions,
            insert_and_update_dependencies, insert_and_update_tables, insert_and_update_triggers,
        };
        use td_objects::tower_service::sql::insert_vec;
        use td_objects::types::basic::ReuseFrozen;
        use td_objects::types::dependency::{DependencyDBWithNames, DependencyVersionDBBuilder};
        use td_objects::types::table::{TableDBWithNames, TableVersionDBBuilder};
        use td_objects::types::trigger::{
            TriggerDBWithNames, TriggerVersionDB, TriggerVersionDBBuilder,
        };

        let queries = Arc::new(DaoQueries::default());
        let provider = UpdateFunctionService::provider(db, queries);
        let service = provider.make().await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();

        metadata.assert_service::<UpdateRequest<FunctionParam, FunctionUpdate>, FunctionVersion>(
            &[
                type_of_val(&extract_req_context::<UpdateRequest<FunctionParam, FunctionUpdate>>),
                type_of_val(&extract_req_dto::<UpdateRequest<FunctionParam, FunctionUpdate>, _>),
                type_of_val(&extract_req_name::<UpdateRequest<FunctionParam, FunctionUpdate>, _>),
                // Extract collection and current function from request.
                type_of_val(&With::<FunctionParam>::extract::<CollectionIdName>),
                type_of_val(&With::<FunctionParam>::extract::<FunctionIdName>),
                // Get collection. Extract collection id and name.
                type_of_val(&By::<CollectionIdName>::select::<DaoQueries, CollectionDB>),
                type_of_val(&With::<CollectionDB>::extract::<CollectionId>),
                type_of_val(&With::<CollectionDB>::extract::<CollectionName>),
                // Get function. Extract function id and name.
                type_of_val(&combine::<CollectionIdName, FunctionIdName>),
                type_of_val(
                    &By::<(CollectionIdName, FunctionIdName)>::select::<
                        DaoQueries,
                        FunctionDBWithNames,
                    >,
                ),
                // This is, before update function id and function version id. Function id does
                // not change, but function version id does.
                type_of_val(&With::<FunctionDBWithNames>::extract::<FunctionId>),
                type_of_val(&With::<FunctionDBWithNames>::extract::<FunctionVersionId>),
                // If function has a new name, check new name does not exist in collection.
                type_of_val(&assert_function_name_not_exists::<DaoQueries>),
                // Insert into function_versions(sql) status=Active.
                type_of_val(&With::<FunctionUpdate>::convert_to::<FunctionVersionDBBuilder, _>),
                type_of_val(&With::<RequestContext>::update::<FunctionVersionDBBuilder, _>),
                type_of_val(&With::<CollectionId>::set::<FunctionVersionDBBuilder>),
                // We maintain the same function id
                type_of_val(&With::<FunctionId>::set::<FunctionVersionDBBuilder>),
                // TODO missing data_location and storage_version
                type_of_val(&With::<FunctionVersionDBBuilder>::build::<FunctionVersionDB, _>),
                type_of_val(&insert::<DaoQueries, FunctionVersionDB>),
                // Update functions(sql) table.
                type_of_val(&With::<FunctionVersionDB>::convert_to::<FunctionDBBuilder, _>),
                type_of_val(&With::<FunctionDBBuilder>::build::<FunctionDB, _>),
                type_of_val(&By::<FunctionId>::update::<DaoQueries, FunctionDB, FunctionDB>),
                // Register associations
                // Extract new function id
                type_of_val(&With::<FunctionDB>::extract::<FunctionId>),
                // Find previous versions
                type_of_val(&By::<FunctionVersionId>::select_all::<DaoQueries, TableVersionDB>),
                type_of_val(
                    &By::<FunctionVersionId>::select_all::<DaoQueries, DependencyVersionDB>,
                ),
                type_of_val(
                    &By::<FunctionVersionId>::select_all::<DaoQueries, TriggerVersionDBWithNames>,
                ),
                // Extract new associations
                type_of_val(&With::<FunctionUpdate>::extract::<Option<Vec<TableName>>>),
                type_of_val(&With::<FunctionUpdate>::extract::<Option<Vec<TableDependency>>>),
                type_of_val(&With::<FunctionUpdate>::extract::<Option<Vec<TableTrigger>>>),
                // Extract reuse frozen
                type_of_val(&With::<FunctionUpdate>::extract::<ReuseFrozen>),
                // And register new ones
                // Insert into table_versions(sql) current function tables status=Active.
                // Reuse table_id for tables that existed (had status=Frozen)
                type_of_val(&With::<FunctionVersionDB>::convert_to::<TableVersionDBBuilder, _>),
                type_of_val(&With::<RequestContext>::update::<TableVersionDBBuilder, _>),
                type_of_val(&build_table_versions),
                type_of_val(&insert_vec::<DaoQueries, TableVersionDB>),
                // Insert into tables(sql) function tables info and update already existing tables (frozen tables).
                type_of_val(&By::<FunctionId>::select_all::<DaoQueries, TableDBWithNames>),
                type_of_val(&insert_and_update_tables::<DaoQueries>),
                // Insert into dependency_versions(sql) current function table dependencies status=Active.
                type_of_val(
                    &With::<FunctionVersionDB>::convert_to::<DependencyVersionDBBuilder, _>,
                ),
                type_of_val(&With::<RequestContext>::update::<DependencyVersionDBBuilder, _>),
                type_of_val(&build_dependency_versions::<DaoQueries>),
                type_of_val(&insert_vec::<DaoQueries, DependencyVersionDB>),
                // Insert into dependencies(sql) function dependencies info.
                type_of_val(&By::<FunctionId>::select_all::<DaoQueries, DependencyDBWithNames>),
                type_of_val(&insert_and_update_dependencies::<DaoQueries>),
                // Insert into trigger_versions(sql) current function trigger status=Active.
                type_of_val(&With::<FunctionVersionDB>::convert_to::<TriggerVersionDBBuilder, _>),
                type_of_val(&With::<RequestContext>::update::<TriggerVersionDBBuilder, _>),
                type_of_val(&build_trigger_versions::<DaoQueries>),
                type_of_val(&insert_vec::<DaoQueries, TriggerVersionDB>),
                // Insert into triggers(sql) function trigger info.
                type_of_val(&By::<FunctionId>::select_all::<DaoQueries, TriggerDBWithNames>),
                type_of_val(&insert_and_update_triggers::<DaoQueries>),
                // Response
                type_of_val(&By::<FunctionId>::select::<DaoQueries, FunctionVersionDBWithNames>),
                type_of_val(
                    &With::<FunctionVersionDBWithNames>::convert_to::<FunctionVersionBuilder, _>,
                ),
                type_of_val(&With::<FunctionVersionBuilder>::build::<FunctionVersion, _>),
            ],
        );
    }

    #[td_test::test(sqlx)]
    async fn test_update_fields(db: DbPool) -> Result<(), TdError> {
        let admin_id = UserId::from(Id::try_from(&admin_user(&db).await)?);
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &admin_id).await;

        let create = FunctionUpdate::builder()
            .try_name("foo")?
            .try_description("foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("foo snippet")?
            .dependencies(None)
            .triggers(None)
            .tables(None)
            .runtime_values(FunctionRuntimeValues::try_from("foo runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let (created_function, created_function_version) =
            seed_function(&db, &collection, &create).await;

        let update = FunctionUpdate::builder()
            .try_name("foo_updated")?
            .try_description("foo_updated description updated")?
            .bundle_id(BundleId::default())
            .try_snippet("foo_updated snippet")?
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
            &admin_id,
            &collection,
            &create,
            &created_function,
            &created_function_version,
            &update,
            &response,
        )
        .await
    }

    #[td_test::test(sqlx)]
    async fn test_update_add_new_table(db: DbPool) -> Result<(), TdError> {
        let admin_id = UserId::from(Id::try_from(&admin_user(&db).await)?);
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &admin_id).await;

        let create = FunctionUpdate::builder()
            .try_name("joaquin_workout")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .dependencies(None)
            .triggers(None)
            .tables(None)
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let (created_function, created_function_version) =
            seed_function(&db, &collection, &create).await;

        let update = FunctionUpdate::builder()
            .try_name("joaquin_workout_updated")?
            .try_description("function_foo description updated")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet updated")?
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![TableName::try_from("new_table")?]))
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
            &admin_id,
            &collection,
            &create,
            &created_function,
            &created_function_version,
            &update,
            &response,
        )
        .await
    }

    #[td_test::test(sqlx)]
    async fn test_update_remove_table(db: DbPool) -> Result<(), TdError> {
        let admin_id = UserId::from(Id::try_from(&admin_user(&db).await)?);
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &admin_id).await;

        let create = FunctionUpdate::builder()
            .try_name("joaquin_workout")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![TableName::try_from("new_table")?]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let (created_function, created_function_version) =
            seed_function(&db, &collection, &create).await;

        let update = FunctionUpdate::builder()
            .try_name("joaquin_workout_updated")?
            .try_description("function_foo description updated")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet updated")?
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
            &admin_id,
            &collection,
            &create,
            &created_function,
            &created_function_version,
            &update,
            &response,
        )
        .await
    }

    #[td_test::test(sqlx)]
    async fn test_update_maintain_table(db: DbPool) -> Result<(), TdError> {
        let admin_id = UserId::from(Id::try_from(&admin_user(&db).await)?);
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &admin_id).await;

        let create = FunctionUpdate::builder()
            .try_name("joaquin_workout")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![TableName::try_from("new_table")?]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let (created_function, created_function_version) =
            seed_function(&db, &collection, &create).await;

        let update = FunctionUpdate::builder()
            .try_name("joaquin_workout_updated")?
            .try_description("function_foo description updated")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet updated")?
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![TableName::try_from("new_table")?]))
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
            &admin_id,
            &collection,
            &create,
            &created_function,
            &created_function_version,
            &update,
            &response,
        )
        .await
    }

    #[td_test::test(sqlx)]
    async fn test_update_add_dependencies(db: DbPool) -> Result<(), TdError> {
        let admin_id = UserId::from(Id::try_from(&admin_user(&db).await)?);
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &admin_id).await;

        let create = FunctionUpdate::builder()
            .try_name("joaquin_workout")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![TableName::try_from("new_table")?]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let (created_function, created_function_version) =
            seed_function(&db, &collection, &create).await;

        let update = FunctionUpdate::builder()
            .try_name("joaquin_workout_updated")?
            .try_description("function_foo description updated")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet updated")?
            .dependencies(Some(vec![TableDependency::try_from("new_table")?]))
            .triggers(None)
            .tables(Some(vec![TableName::try_from("new_table")?]))
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
            &admin_id,
            &collection,
            &create,
            &created_function,
            &created_function_version,
            &update,
            &response,
        )
        .await
    }

    #[td_test::test(sqlx)]
    async fn test_update_remove_dependencies(db: DbPool) -> Result<(), TdError> {
        let admin_id = UserId::from(Id::try_from(&admin_user(&db).await)?);
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &admin_id).await;

        let create = FunctionUpdate::builder()
            .try_name("joaquin_workout")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .dependencies(Some(vec![TableDependency::try_from("new_table")?]))
            .triggers(None)
            .tables(Some(vec![TableName::try_from("new_table")?]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let (created_function, created_function_version) =
            seed_function(&db, &collection, &create).await;

        let update = FunctionUpdate::builder()
            .try_name("joaquin_workout_updated")?
            .try_description("function_foo description updated")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet updated")?
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![TableName::try_from("new_table")?]))
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
            &admin_id,
            &collection,
            &create,
            &created_function,
            &created_function_version,
            &update,
            &response,
        )
        .await
    }

    #[td_test::test(sqlx)]
    async fn test_update_maintain_dependencies(db: DbPool) -> Result<(), TdError> {
        let admin_id = UserId::from(Id::try_from(&admin_user(&db).await)?);
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &admin_id).await;

        let create = FunctionUpdate::builder()
            .try_name("joaquin_workout")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .dependencies(Some(vec![TableDependency::try_from("new_table")?]))
            .triggers(None)
            .tables(Some(vec![TableName::try_from("new_table")?]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let (created_function, created_function_version) =
            seed_function(&db, &collection, &create).await;

        let update = FunctionUpdate::builder()
            .try_name("joaquin_workout_updated")?
            .try_description("function_foo description updated")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet updated")?
            .dependencies(Some(vec![TableDependency::try_from("new_table")?]))
            .triggers(None)
            .tables(Some(vec![TableName::try_from("new_table")?]))
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
            &admin_id,
            &collection,
            &create,
            &created_function,
            &created_function_version,
            &update,
            &response,
        )
        .await
    }

    #[td_test::test(sqlx)]
    async fn test_update_add_trigger(db: DbPool) -> Result<(), TdError> {
        let admin_id = UserId::from(Id::try_from(&admin_user(&db).await)?);
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &admin_id).await;

        let trigger_create = FunctionUpdate::builder()
            .try_name("the_trigger")?
            .try_description("wanted")?
            .bundle_id(BundleId::default())
            .try_snippet("the_trigger snippet")?
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![TableName::try_from("trigger_table")?]))
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
            .dependencies(None)
            .triggers(None)
            .tables(None)
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let (created_function, created_function_version) =
            seed_function(&db, &collection, &create).await;

        let update = FunctionUpdate::builder()
            .try_name("joaquin_workout_updated")?
            .try_description("function_foo description updated")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet updated")?
            .dependencies(None)
            .triggers(Some(vec![TableTrigger::try_from("trigger_table")?]))
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
            &admin_id,
            &collection,
            &create,
            &created_function,
            &created_function_version,
            &update,
            &response,
        )
        .await
    }

    #[td_test::test(sqlx)]
    async fn test_update_remove_trigger(db: DbPool) -> Result<(), TdError> {
        let admin_id = UserId::from(Id::try_from(&admin_user(&db).await)?);
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &admin_id).await;

        let trigger_create = FunctionUpdate::builder()
            .try_name("the_trigger")?
            .try_description("wanted")?
            .bundle_id(BundleId::default())
            .try_snippet("the_trigger snippet")?
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![TableName::try_from("trigger_table")?]))
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
            .dependencies(None)
            .triggers(Some(vec![TableTrigger::try_from("trigger_table")?]))
            .tables(None)
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let (created_function, created_function_version) =
            seed_function(&db, &collection, &create).await;

        let update = FunctionUpdate::builder()
            .try_name("joaquin_workout_updated")?
            .try_description("function_foo description updated")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet updated")?
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
            &admin_id,
            &collection,
            &create,
            &created_function,
            &created_function_version,
            &update,
            &response,
        )
        .await
    }

    #[td_test::test(sqlx)]
    async fn test_update_maintain_trigger(db: DbPool) -> Result<(), TdError> {
        let admin_id = UserId::from(Id::try_from(&admin_user(&db).await)?);
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &admin_id).await;

        let trigger_create = FunctionUpdate::builder()
            .try_name("the_trigger")?
            .try_description("wanted")?
            .bundle_id(BundleId::default())
            .try_snippet("the_trigger snippet")?
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![TableName::try_from("trigger_table")?]))
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
            .dependencies(None)
            .triggers(Some(vec![TableTrigger::try_from("trigger_table")?]))
            .tables(None)
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let (created_function, created_function_version) =
            seed_function(&db, &collection, &create).await;

        let update = FunctionUpdate::builder()
            .try_name("joaquin_workout_updated")?
            .try_description("function_foo description updated")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet updated")?
            .dependencies(None)
            .triggers(Some(vec![TableTrigger::try_from("trigger_table")?]))
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
            &admin_id,
            &collection,
            &create,
            &created_function,
            &created_function_version,
            &update,
            &response,
        )
        .await
    }

    #[td_test::test(sqlx)]
    async fn test_update_change_everything(db: DbPool) -> Result<(), TdError> {
        let admin_id = UserId::from(Id::try_from(&admin_user(&db).await)?);
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &admin_id).await;

        let trigger_create = FunctionUpdate::builder()
            .try_name("the_trigger")?
            .try_description("wanted")?
            .bundle_id(BundleId::default())
            .try_snippet("the_trigger snippet")?
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![
                TableName::try_from("trigger_table")?,
                TableName::try_from("trigger_table_2")?,
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
            .dependencies(Some(vec![TableDependency::try_from("trigger_table")?]))
            .triggers(Some(vec![TableTrigger::try_from("trigger_table")?]))
            .tables(Some(vec![
                TableName::try_from("joaquin_table")?,
                TableName::try_from("joaquin_table_2")?,
            ]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let (created_function, created_function_version) =
            seed_function(&db, &collection, &create).await;

        let update = FunctionUpdate::builder()
            .try_name("joaquin_workout_updated")?
            .try_description("function_foo description updated")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet updated")?
            .dependencies(Some(vec![
                TableDependency::try_from("trigger_table")?,
                TableDependency::try_from("trigger_table_2")?,
            ]))
            .triggers(Some(vec![TableTrigger::try_from("trigger_table_2")?]))
            .tables(Some(vec![
                TableName::try_from("joaquin_table")?,
                TableName::try_from("joaquin_table_3")?,
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
            &admin_id,
            &collection,
            &create,
            &created_function,
            &created_function_version,
            &update,
            &response,
        )
        .await
    }

    #[td_test::test(sqlx)]
    async fn test_update_freeze_unfreeze(db: DbPool) -> Result<(), TdError> {
        let admin_id = UserId::from(Id::try_from(&admin_user(&db).await)?);
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &admin_id).await;
        let queries = DaoQueries::default();

        let create = FunctionUpdate::builder()
            .try_name("joaquin_workout")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![TableName::try_from("joaquin_table")?]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let (created_function, created_function_version) =
            seed_function(&db, &collection, &create).await;

        let update = FunctionUpdate::builder()
            .try_name("joaquin_workout")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
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
            .select_by::<TableDB>(&(&TableName::try_from("joaquin_table")?))?
            .build_query_as()
            .fetch_all(&db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(tables.len(), 1);
        assert_eq!(*tables[0].frozen(), Frozen::from(true));

        let update = FunctionUpdate::builder()
            .try_name("joaquin_workout")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![TableName::try_from("joaquin_table")?]))
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
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![TableName::try_from("joaquin_table")?]))
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
        let response = response?;
        // But with reuse_frozen, we get the expected response

        let tables: Vec<TableDB> = queries
            .select_by::<TableDB>(&(&TableName::try_from("joaquin_table")?))?
            .build_query_as()
            .fetch_all(&db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(tables.len(), 1);
        assert_eq!(*tables[0].frozen(), Frozen::from(false));

        assert_update(
            &db,
            &admin_id,
            &collection,
            &create,
            &created_function,
            &created_function_version,
            &update,
            &response,
        )
        .await
    }
}
