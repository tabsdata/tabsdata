//
// Copyright 2025 Tabs Data Inc.
//

use crate::function::layers::delete::build_deleted_function_version;
use std::sync::Arc;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{DeleteRequest, RequestContext};
use td_objects::rest_urls::FunctionParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::from::{
    combine, DefaultService, ExtractNameService, ExtractService, With,
};
use td_objects::tower_service::sql::{
    insert, By, SqlDeleteService, SqlSelectAllService, SqlSelectIdOrNameService, SqlSelectService,
};
use td_objects::types::basic::{
    CollectionId, CollectionIdName, CollectionName, FunctionId, FunctionIdName, FunctionVersionId,
    ReuseFrozen, TableDependency, TableName, TableTrigger,
};
use td_objects::types::collection::CollectionDB;
use td_objects::types::dependency::DependencyVersionDB;
use td_objects::types::function::{FunctionDB, FunctionDBWithNames, FunctionVersionDB};
use td_objects::types::table::TableVersionDB;
use td_objects::types::trigger::TriggerVersionDBWithNames;
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{SrvCtxProvider, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

use super::register::RegisterFunctionService;

pub struct DeleteFunctionService {
    provider: ServiceProvider<DeleteRequest<FunctionParam>, (), TdError>,
}

impl DeleteFunctionService {
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
                from_fn(With::<DeleteRequest<FunctionParam>>::extract::<RequestContext>),
                from_fn(With::<DeleteRequest<FunctionParam>>::extract_name::<FunctionParam>),

                TransactionProvider::new(db),

                // Extract collection and function from request.
                from_fn(With::<FunctionParam>::extract::<CollectionIdName>),
                from_fn(With::<FunctionParam>::extract::<FunctionIdName>),

                // Get collection. Extract collection id and name.
                from_fn(By::<CollectionIdName>::select::<DaoQueries, CollectionDB>),
                from_fn(With::<CollectionDB>::extract::<CollectionId>),
                from_fn(With::<CollectionDB>::extract::<CollectionName>),

                // Get function. Extract function id and function version id.
                from_fn(combine::<CollectionIdName, FunctionIdName>),
                from_fn(By::<(CollectionIdName, FunctionIdName)>::select::<DaoQueries, FunctionDBWithNames>),
                from_fn(With::<FunctionDBWithNames>::extract::<FunctionId>),
                from_fn(With::<FunctionDBWithNames>::extract::<FunctionVersionId>),

                // Insert into function_versions(sql) status=Deleted.
                from_fn(By::<FunctionVersionId>::select::<DaoQueries, FunctionVersionDB>),
                from_fn(build_deleted_function_version),
                from_fn(insert::<DaoQueries, FunctionVersionDB>),

                // Delete functions(sql) table.
                from_fn(By::<FunctionId>::delete::<DaoQueries, FunctionDB>),

                // Register associations
                // Find previous versions.
                from_fn(By::<FunctionVersionId>::select_all::<DaoQueries, TableVersionDB>),
                from_fn(By::<FunctionVersionId>::select_all::<DaoQueries, DependencyVersionDB>),
                from_fn(By::<FunctionVersionId>::select_all::<DaoQueries, TriggerVersionDBWithNames>),
                // Extract new associations (empty because it is a delete operation).
                from_fn(With::<Option<Vec<TableName>>>::default),
                from_fn(With::<Option<Vec<TableDependency>>>::default),
                from_fn(With::<Option<Vec<TableTrigger>>>::default),
                // Extract reuse frozen (default as deletes are not creating reusing anything)
                from_fn(With::<ReuseFrozen>::default),
                // And register new ones
                RegisterFunctionService::register_tables(),
                RegisterFunctionService::register_dependencies(),
                RegisterFunctionService::register_triggers(),
            ))
        }
    }

    pub async fn service(&self) -> TdBoxService<DeleteRequest<FunctionParam>, (), TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::function::services::tests::{assert_delete, assert_register};
    use td_objects::crudl::{handle_sql_err, RequestContext};
    use td_objects::rest_urls::CollectionParam;
    use td_objects::sql::SelectBy;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_function::seed_function;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, Decorator, FunctionRuntimeValues, RoleId, UserId,
    };
    use td_objects::types::function::{
        FunctionRegister, FunctionVersionBuilder, FunctionVersionDBWithNames,
    };
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_delete_function(db: DbPool) {
        use td_tower::metadata::{type_of_val, Metadata};

        use crate::function::layers::register::{
            build_dependency_versions, build_table_versions, build_trigger_versions,
            insert_and_update_dependencies, insert_and_update_tables, insert_and_update_triggers,
        };
        use td_objects::tower_service::from::{TryIntoService, UpdateService};
        use td_objects::tower_service::sql::insert_vec;
        use td_objects::types::basic::ReuseFrozen;
        use td_objects::types::dependency::{DependencyDBWithNames, DependencyVersionDBBuilder};
        use td_objects::types::table::{TableDBWithNames, TableVersionDBBuilder};
        use td_objects::types::trigger::{
            TriggerDBWithNames, TriggerVersionDB, TriggerVersionDBBuilder,
        };

        let queries = Arc::new(DaoQueries::default());
        let provider = DeleteFunctionService::provider(db, queries);
        let service = provider.make().await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();

        metadata.assert_service::<DeleteRequest<FunctionParam>, ()>(&[
                type_of_val(&With::<DeleteRequest<FunctionParam>>::extract::<RequestContext>),
                type_of_val(&With::<DeleteRequest<FunctionParam>>::extract_name::<FunctionParam>),
                // Extract collection and function from request.
                type_of_val(&With::<FunctionParam>::extract::<CollectionIdName>),
                type_of_val(&With::<FunctionParam>::extract::<FunctionIdName>),
                // Get collection. Extract collection id and name.
                type_of_val(&By::<CollectionIdName>::select::<DaoQueries, CollectionDB>),
                type_of_val(&With::<CollectionDB>::extract::<CollectionId>),
                type_of_val(&With::<CollectionDB>::extract::<CollectionName>),
                // Get function. Extract function id and function version id.
                type_of_val(&combine::<CollectionIdName, FunctionIdName>),
                type_of_val(&By::<(CollectionIdName, FunctionIdName)>::select::<DaoQueries, FunctionDBWithNames>),
                type_of_val(&With::<FunctionDBWithNames>::extract::<FunctionId>),
                type_of_val(&With::<FunctionDBWithNames>::extract::<FunctionVersionId>),
                // Insert into function_versions(sql) status=Deleted.
                type_of_val(&By::<FunctionVersionId>::select::<DaoQueries, FunctionVersionDB>),
                type_of_val(&build_deleted_function_version),
                type_of_val(&insert::<DaoQueries, FunctionVersionDB>),
                // Delete functions(sql) table.
                type_of_val(&By::<FunctionId>::delete::<DaoQueries, FunctionDB>),
                // Register associations
                // Find previous versions.
                type_of_val(&By::<FunctionVersionId>::select_all::<DaoQueries, TableVersionDB>),
                type_of_val(&By::<FunctionVersionId>::select_all::<DaoQueries, DependencyVersionDB>),
                type_of_val(&By::<FunctionVersionId>::select_all::<DaoQueries, TriggerVersionDBWithNames>),
                // Extract new associations (empty because it is a delete operation).
                type_of_val(&With::<Option<Vec<TableName>>>::default),
                type_of_val(&With::<Option<Vec<TableDependency>>>::default),
                type_of_val(&With::<Option<Vec<TableTrigger>>>::default),
                // Extract reuse frozen (default as deletes are not creating reusing anything)
                type_of_val(&With::<ReuseFrozen>::default),
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
                type_of_val(&With::<FunctionVersionDB>::convert_to::<DependencyVersionDBBuilder, _>),
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
            ]);
    }

    #[td_test::test(sqlx)]
    async fn test_delete_function(db: DbPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &UserId::admin()).await;

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

        let (created_function, created_function_version) =
            seed_function(&db, &collection, &create).await;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            true,
        )
        .delete(
            FunctionParam::builder()
                .try_collection(format!("~{}", collection.id()))?
                .try_function("joaquin_workout")?
                .build()?,
        );

        let service = DeleteFunctionService::new(db.clone()).service().await;
        service.raw_oneshot(request).await?;

        assert_delete(
            &db,
            &UserId::admin(),
            &collection,
            &create,
            &created_function,
            &created_function_version,
        )
        .await
    }

    #[td_test::test(sqlx)]
    async fn test_delete_function_with_tables(db: DbPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &UserId::admin()).await;

        let create = FunctionRegister::builder()
            .try_name("joaquin_workout")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![
                TableName::try_from("table1")?,
                TableName::try_from("table2")?,
            ]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let (created_function, created_function_version) =
            seed_function(&db, &collection, &create).await;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            true,
        )
        .delete(
            FunctionParam::builder()
                .try_collection(format!("{}", collection.name()))?
                .try_function("joaquin_workout")?
                .build()?,
        );

        let service = DeleteFunctionService::new(db.clone()).service().await;
        service.raw_oneshot(request).await?;

        assert_delete(
            &db,
            &UserId::admin(),
            &collection,
            &create,
            &created_function,
            &created_function_version,
        )
        .await
    }

    #[td_test::test(sqlx)]
    async fn test_delete_function_with_dependencies(db: DbPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &UserId::admin()).await;

        let trigger_create = FunctionRegister::builder()
            .try_name("the_trigger")?
            .try_description("wanted")?
            .bundle_id(BundleId::default())
            .try_snippet("the_trigger snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![TableName::try_from("trigger_table")?]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let _ = seed_function(&db, &collection, &trigger_create).await;

        let create = FunctionRegister::builder()
            .try_name("joaquin_workout")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(Some(vec![TableDependency::try_from("trigger_table")?]))
            .triggers(None)
            .tables(None)
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let (created_function, created_function_version) =
            seed_function(&db, &collection, &create).await;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            true,
        )
        .delete(
            FunctionParam::builder()
                .try_collection(format!("{}", collection.name()))?
                .try_function("joaquin_workout")?
                .build()?,
        );

        let service = DeleteFunctionService::new(db.clone()).service().await;
        service.raw_oneshot(request).await?;

        assert_delete(
            &db,
            &UserId::admin(),
            &collection,
            &create,
            &created_function,
            &created_function_version,
        )
        .await
    }

    #[td_test::test(sqlx)]
    async fn test_delete_function_with_triggers(db: DbPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &UserId::admin()).await;

        let trigger_create = FunctionRegister::builder()
            .try_name("the_trigger")?
            .try_description("wanted")?
            .bundle_id(BundleId::default())
            .try_snippet("the_trigger snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![TableName::try_from("trigger_table")?]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let _ = seed_function(&db, &collection, &trigger_create).await;

        let create = FunctionRegister::builder()
            .try_name("joaquin_workout")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(Some(vec![TableTrigger::try_from("trigger_table")?]))
            .tables(None)
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let (created_function, created_function_version) =
            seed_function(&db, &collection, &create).await;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            true,
        )
        .delete(
            FunctionParam::builder()
                .try_collection(format!("{}", collection.name()))?
                .try_function("joaquin_workout")?
                .build()?,
        );

        let service = DeleteFunctionService::new(db.clone()).service().await;
        service.raw_oneshot(request).await?;

        assert_delete(
            &db,
            &UserId::admin(),
            &collection,
            &create,
            &created_function,
            &created_function_version,
        )
        .await
    }

    #[td_test::test(sqlx)]
    async fn test_delete_function_unique(db: DbPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &UserId::admin()).await;

        let trigger_create = FunctionRegister::builder()
            .try_name("the_trigger")?
            .try_description("wanted")?
            .bundle_id(BundleId::default())
            .try_snippet("the_trigger snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![TableName::try_from("trigger_table")?]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let _ = seed_function(&db, &collection, &trigger_create).await;

        let create_1 = FunctionRegister::builder()
            .try_name("joaquin_workout_1")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(Some(vec![TableDependency::try_from("trigger_table")?]))
            .triggers(Some(vec![TableTrigger::try_from("trigger_table")?]))
            .tables(Some(vec![TableName::try_from("workout_1")?]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            true,
        )
        .create(
            CollectionParam::builder()
                .try_collection(collection_name.as_str())?
                .build()?,
            create_1.clone(),
        );

        let service = RegisterFunctionService::new(db.clone()).service().await;
        let response = service.raw_oneshot(request).await;
        let created_function_version_1 = response?;

        let create_2 = FunctionRegister::builder()
            .try_name("joaquin_workout_2")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(Some(vec![TableDependency::try_from("trigger_table")?]))
            .triggers(Some(vec![TableTrigger::try_from("trigger_table")?]))
            .tables(Some(vec![TableName::try_from("workout_2")?]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let (created_function_2, created_function_version_2) =
            seed_function(&db, &collection, &create_2).await;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            true,
        )
        .delete(
            FunctionParam::builder()
                .try_collection(format!("~{}", collection.id()))?
                .try_function("joaquin_workout_2")?
                .build()?,
        );

        let service = DeleteFunctionService::new(db.clone()).service().await;
        service.raw_oneshot(request).await?;

        // Assert that the first function is as if it just got registered
        let queries = DaoQueries::default();
        let function_version: FunctionVersionDBWithNames = queries
            .select_by::<FunctionVersionDBWithNames>(&(created_function_version_1.id()))?
            .build_query_as()
            .fetch_one(&db)
            .await
            .map_err(handle_sql_err)?;
        let function_version = FunctionVersionBuilder::try_from(&function_version)?.build()?;
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
            &created_function_version_2,
        )
        .await
    }
}
