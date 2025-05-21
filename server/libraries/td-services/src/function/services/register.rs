//
// Copyright 2025 Tabs Data Inc.
//

use crate::function::layers::register::{
    build_dependency_versions, build_table_versions, insert_and_update_tables,
};
use crate::function::layers::register::{
    build_trigger_versions, data_location, insert_and_update_dependencies,
    insert_and_update_triggers,
};
use std::sync::Arc;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{CreateRequest, RequestContext};
use td_objects::rest_urls::CollectionParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::from::{
    combine, BuildService, DefaultService, EmptyVecService, ExtractDataService, ExtractNameService,
    ExtractService, SetService, TryIntoService, UpdateService, With,
};
use td_objects::tower_service::sql::{
    insert, insert_vec, By, SqlDeleteService, SqlSelectAllService, SqlSelectService,
};
use td_objects::tower_service::sql::{SqlAssertNotExistsService, SqlSelectIdOrNameService};
use td_objects::types::basic::{
    BundleId, CollectionId, CollectionIdName, CollectionName, DataLocation, FunctionId,
    FunctionName, ReuseFrozen, StorageVersion, TableDependency, TableName, TableTrigger,
};
use td_objects::types::collection::CollectionDB;
use td_objects::types::dependency::{
    DependencyDBWithNames, DependencyVersionDB, DependencyVersionDBBuilder,
};
use td_objects::types::function::{
    BundleDB, FunctionDB, FunctionDBBuilder, FunctionDBWithNames, FunctionRegister,
    FunctionVersion, FunctionVersionBuilder, FunctionVersionDB, FunctionVersionDBBuilder,
    FunctionVersionDBWithNames,
};
use td_objects::types::table::{TableDBWithNames, TableVersionDB, TableVersionDBBuilder};
use td_objects::types::trigger::{
    TriggerDBWithNames, TriggerVersionDB, TriggerVersionDBBuilder, TriggerVersionDBWithNames,
};
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{SrvCtxProvider, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{l, layers, p, service_provider};

pub struct RegisterFunctionService {
    provider:
        ServiceProvider<CreateRequest<CollectionParam, FunctionRegister>, FunctionVersion, TdError>,
}

impl RegisterFunctionService {
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
                from_fn(With::<CreateRequest<CollectionParam, FunctionRegister>>::extract::<RequestContext>),
                from_fn(With::<CreateRequest<CollectionParam, FunctionRegister>>::extract_name::<CollectionParam>),
                from_fn(With::<CreateRequest<CollectionParam, FunctionRegister>>::extract_data::<FunctionRegister>),

                TransactionProvider::new(db),

                // Extract collection from request.
                from_fn(With::<CollectionParam>::extract::<CollectionIdName>),

                // Get collection. Extract collection id and name.
                from_fn(By::<CollectionIdName>::select::<DaoQueries, CollectionDB>),
                from_fn(With::<CollectionDB>::extract::<CollectionId>),
                from_fn(With::<CollectionDB>::extract::<CollectionName>),

                // Get function.
                from_fn(With::<FunctionRegister>::extract::<FunctionName>),

                // Check function name does not exist in collection.
                from_fn(combine::<CollectionId, FunctionName>),
                from_fn(By::<(CollectionId, FunctionName)>::assert_not_exists::<DaoQueries, FunctionDBWithNames>),

                // Get location and storage version.
                from_fn(With::<StorageVersion>::default),
                from_fn(data_location),

                // Insert into function_versions(sql) status=Active.
                from_fn(With::<FunctionRegister>::convert_to::<FunctionVersionDBBuilder, _>),
                from_fn(With::<RequestContext>::update::<FunctionVersionDBBuilder, _>),
                from_fn(With::<CollectionId>::set::<FunctionVersionDBBuilder>),
                from_fn(With::<StorageVersion>::set::<FunctionVersionDBBuilder>),
                from_fn(With::<DataLocation>::set::<FunctionVersionDBBuilder>),
                from_fn(With::<FunctionVersionDBBuilder>::build::<FunctionVersionDB, _>),
                from_fn(insert::<DaoQueries, FunctionVersionDB>),

                // Remove from bundles
                from_fn(With::<FunctionVersionDB>::extract::<BundleId>),
                from_fn(By::<BundleId>::delete::<DaoQueries, BundleDB>),

                // Insert into functions(sql) function info.
                from_fn(With::<FunctionVersionDB>::convert_to::<FunctionDBBuilder, _>),
                from_fn(With::<FunctionDBBuilder>::build::<FunctionDB, _>),
                from_fn(insert::<DaoQueries, FunctionDB>),

                // Register associations
                // Extract new function id
                from_fn(With::<FunctionDB>::extract::<FunctionId>),
                // Find previous versions (empty because it is a new function)
                from_fn(With::<TableVersionDB>::empty_vec),
                from_fn(With::<DependencyVersionDB>::empty_vec),
                from_fn(With::<TriggerVersionDBWithNames>::empty_vec),
                // Extract new associations
                from_fn(With::<FunctionRegister>::extract::<Option<Vec<TableName>>>),
                from_fn(With::<FunctionRegister>::extract::<Option<Vec<TableDependency>>>),
                from_fn(With::<FunctionRegister>::extract::<Option<Vec<TableTrigger>>>),
                // Extract reuse frozen
                from_fn(With::<FunctionRegister>::extract::<ReuseFrozen>),
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

    l! {
        register_tables() {
            layers!(
                // Insert into table_versions(sql) current function tables status=Active.
                // Reuse table_id for tables that existed (had status=Frozen)
                from_fn(With::<FunctionVersionDB>::convert_to::<TableVersionDBBuilder, _>),
                from_fn(With::<RequestContext>::update::<TableVersionDBBuilder, _>),
                from_fn(build_table_versions),
                from_fn(insert_vec::<DaoQueries, TableVersionDB>),

                // Insert into tables(sql) function tables info and update already existing tables (frozen tables).
                from_fn(By::<FunctionId>::select_all::<DaoQueries, TableDBWithNames>),
                from_fn(insert_and_update_tables::<DaoQueries>),
            )
        }
    }

    l! {
        register_dependencies() {
            layers!(
                // Insert into dependency_versions(sql) current function table dependencies status=Active.
                from_fn(With::<FunctionVersionDB>::convert_to::<DependencyVersionDBBuilder, _>),
                from_fn(With::<RequestContext>::update::<DependencyVersionDBBuilder, _>),
                from_fn(build_dependency_versions::<DaoQueries>),
                from_fn(insert_vec::<DaoQueries, DependencyVersionDB>),

                // Insert into dependencies(sql) function dependencies info.
                from_fn(By::<FunctionId>::select_all::<DaoQueries, DependencyDBWithNames>),
                from_fn(insert_and_update_dependencies::<DaoQueries>),
            )
        }
    }

    l! {
        register_triggers() {
            layers!(
                // Insert into trigger_versions(sql) current function trigger status=Active.
                from_fn(With::<FunctionVersionDB>::convert_to::<TriggerVersionDBBuilder, _>),
                from_fn(With::<RequestContext>::update::<TriggerVersionDBBuilder, _>),
                from_fn(build_trigger_versions::<DaoQueries>),
                from_fn(insert_vec::<DaoQueries, TriggerVersionDB>),

                // Insert into triggers(sql) function trigger info.
                from_fn(By::<FunctionId>::select_all::<DaoQueries, TriggerDBWithNames>),
                from_fn(insert_and_update_triggers::<DaoQueries>),
            )
        }
    }

    pub async fn service(
        &self,
    ) -> TdBoxService<CreateRequest<CollectionParam, FunctionRegister>, FunctionVersion, TdError>
    {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::function::services::tests::assert_register;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, Decorator, FunctionRuntimeValues, RoleId, UserId,
    };
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_register_function(db: DbPool) {
        use td_tower::metadata::{type_of_val, Metadata};

        let queries = Arc::new(DaoQueries::default());
        let provider = RegisterFunctionService::provider(db, queries);
        let service = provider.make().await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();

        metadata.assert_service::<CreateRequest<CollectionParam, FunctionRegister>, FunctionVersion>(
            &[
                type_of_val(&With::<CreateRequest<CollectionParam, FunctionRegister>>::extract::<RequestContext>),
                type_of_val(&With::<CreateRequest<CollectionParam, FunctionRegister>>::extract_name::<CollectionParam>),
                type_of_val(&With::<CreateRequest<CollectionParam, FunctionRegister>>::extract_data::<FunctionRegister>),
                // Extract collection from request.
                type_of_val(&With::<CollectionParam>::extract::<CollectionIdName>),
                // Get collection. Extract collection id and name.
                type_of_val(&By::<CollectionIdName>::select::<DaoQueries, CollectionDB>),
                type_of_val(&With::<CollectionDB>::extract::<CollectionId>),
                type_of_val(&With::<CollectionDB>::extract::<CollectionName>),
                // Get function.
                type_of_val(&With::<FunctionRegister>::extract::<FunctionName>),
                // Check function name does not exist in collection.
                type_of_val(&combine::<CollectionId, FunctionName>),
                type_of_val(&By::<(CollectionId, FunctionName)>::assert_not_exists::<DaoQueries, FunctionDBWithNames>),
                // Get location and storage version.
                type_of_val(&With::<StorageVersion>::default),
                type_of_val(&data_location),
                // Insert into function_versions(sql) status=Active.
                type_of_val(&With::<FunctionRegister>::convert_to::<FunctionVersionDBBuilder, _>),
                type_of_val(&With::<RequestContext>::update::<FunctionVersionDBBuilder, _>),
                type_of_val(&With::<CollectionId>::set::<FunctionVersionDBBuilder>),
                type_of_val(&With::<StorageVersion>::set::<FunctionVersionDBBuilder>),
                type_of_val(&With::<DataLocation>::set::<FunctionVersionDBBuilder>),
                type_of_val(&With::<FunctionVersionDBBuilder>::build::<FunctionVersionDB, _>),
                type_of_val(&insert::<DaoQueries, FunctionVersionDB>),
                // Remove from bundles
                type_of_val(&With::<FunctionVersionDB>::extract::<BundleId>),
                type_of_val(&By::<BundleId>::delete::<DaoQueries, BundleDB>),
                // Insert into functions(sql) function info.
                type_of_val(&With::<FunctionVersionDB>::convert_to::<FunctionDBBuilder, _>),
                type_of_val(&With::<FunctionDBBuilder>::build::<FunctionDB, _>),
                type_of_val(&insert::<DaoQueries, FunctionDB>),
                // Register associations
                // Extract new function id
                type_of_val(&With::<FunctionDB>::extract::<FunctionId>),
                // Find previous versions (empty because it is a new function)
                type_of_val(&With::<TableVersionDB>::empty_vec),
                type_of_val(&With::<DependencyVersionDB>::empty_vec),
                type_of_val(&With::<TriggerVersionDBWithNames>::empty_vec),
                // Extract new associations
                type_of_val(&With::<FunctionRegister>::extract::<Option<Vec<TableName>>>),
                type_of_val(&With::<FunctionRegister>::extract::<Option<Vec<TableDependency>>>),
                type_of_val(&With::<FunctionRegister>::extract::<Option<Vec<TableTrigger>>>),
                // Extract reuse frozen
                type_of_val(&With::<FunctionRegister>::extract::<ReuseFrozen>),
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
                // Response
                type_of_val(&By::<FunctionId>::select::<DaoQueries, FunctionVersionDBWithNames>),
                type_of_val(&
                                With::<FunctionVersionDBWithNames>::convert_to::<FunctionVersionBuilder, _>,
                ),
                type_of_val(&With::<FunctionVersionBuilder>::build::<FunctionVersion, _>),
            ],
        );
    }

    #[td_test::test(sqlx)]
    async fn test_register_empty(db: DbPool) -> Result<(), TdError> {
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
            create.clone(),
        );

        let service = RegisterFunctionService::new(db.clone()).service().await;
        let response = service.raw_oneshot(request).await;
        let response = response?;

        assert_register(&db, &UserId::admin(), &collection, &create, &response).await
    }

    #[td_test::test(sqlx)]
    async fn test_register_empty_vec(db: DbPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &UserId::admin()).await;

        let dependencies = Some(vec![]);
        let triggers = Some(vec![]);
        let tables = Some(vec![]);

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
            create.clone(),
        );

        let service = RegisterFunctionService::new(db.clone()).service().await;
        let response = service.raw_oneshot(request).await;
        let response = response?;

        assert_register(&db, &UserId::admin(), &collection, &create, &response).await
    }

    #[td_test::test(sqlx)]
    async fn test_register_table_output(db: DbPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &UserId::admin()).await;

        let dependencies = None;
        let triggers = None;
        let tables = Some(vec![TableName::try_from("table_foo")?]);

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
            create.clone(),
        );

        let service = RegisterFunctionService::new(db.clone()).service().await;
        let response = service.raw_oneshot(request).await;
        let response = response?;

        assert_register(&db, &UserId::admin(), &collection, &create, &response).await
    }

    #[td_test::test(sqlx)]
    async fn test_register_table_dependency(db: DbPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &UserId::admin()).await;

        let dependencies = Some(vec![TableDependency::try_from("table_foo")?]);
        let triggers = None;
        let tables = Some(vec![TableName::try_from("table_foo")?]);

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
            create.clone(),
        );

        let service = RegisterFunctionService::new(db.clone()).service().await;
        let response = service.raw_oneshot(request).await;
        let response = response?;

        assert_register(&db, &UserId::admin(), &collection, &create, &response).await
    }

    #[td_test::test(sqlx)]
    async fn test_register_trigger(db: DbPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &UserId::admin()).await;

        let dependencies = None;
        let triggers = None;
        let tables = Some(vec![TableName::try_from("foo")?]);

        let bundle_id = BundleId::default();
        let create = FunctionRegister::builder()
            .try_name("function_1")?
            .try_description("function_1 description")?
            .bundle_id(bundle_id)
            .try_snippet("function_1 snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(dependencies.clone())
            .triggers(triggers.clone())
            .tables(tables.clone())
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
            create.clone(),
        );

        let service = RegisterFunctionService::new(db.clone()).service().await;
        let _response = service.raw_oneshot(request).await?;

        // Actual test
        let dependencies = None;
        let triggers = Some(vec![TableTrigger::try_from("foo")?]);
        let tables = None;

        let bundle_id = BundleId::default();
        let create = FunctionRegister::builder()
            .try_name("function_2")?
            .try_description("function_2 description")?
            .bundle_id(bundle_id)
            .try_snippet("function_2 snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(dependencies.clone())
            .triggers(triggers.clone())
            .tables(tables.clone())
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
            create.clone(),
        );

        let service = RegisterFunctionService::new(db.clone()).service().await;
        let response = service.raw_oneshot(request).await;
        let response = response?;

        assert_register(&db, &UserId::admin(), &collection, &create, &response).await
    }

    #[td_test::test(sqlx)]
    async fn test_register_tables_dependencies_triggers(db: DbPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &UserId::admin()).await;

        let dependencies = None;
        let triggers = None;
        let tables = Some(vec![
            TableName::try_from("table_1")?,
            TableName::try_from("table_2")?,
        ]);

        let bundle_id = BundleId::default();
        let create = FunctionRegister::builder()
            .try_name("function_1")?
            .try_description("function_1 description")?
            .bundle_id(bundle_id)
            .try_snippet("function_1 snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(dependencies.clone())
            .triggers(triggers.clone())
            .tables(tables.clone())
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
            create.clone(),
        );

        let service = RegisterFunctionService::new(db.clone()).service().await;
        let _response = service.raw_oneshot(request).await?;

        // Actual test
        let dependencies = Some(vec![
            TableDependency::try_from("table_1")?,
            TableDependency::try_from("table_2")?,
        ]);
        let triggers = Some(vec![
            TableTrigger::try_from("table_1")?,
            TableTrigger::try_from("table_2")?,
        ]);
        let tables = Some(vec![
            TableName::try_from("output_1")?,
            TableName::try_from("output_2")?,
        ]);

        let bundle_id = BundleId::default();
        let create = FunctionRegister::builder()
            .try_name("function_2")?
            .try_description("function_2 description")?
            .bundle_id(bundle_id)
            .try_snippet("function_2 snippet")?
            .decorator(Decorator::Transformer)
            .dependencies(dependencies.clone())
            .triggers(triggers.clone())
            .tables(tables.clone())
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
            create.clone(),
        );

        let service = RegisterFunctionService::new(db.clone()).service().await;
        let response = service.raw_oneshot(request).await;
        let response = response?;

        assert_register(&db, &UserId::admin(), &collection, &create, &response).await
    }

    #[td_test::test(sqlx)]
    async fn test_register_tables_dependencies_triggers_different_collections(
        db: DbPool,
    ) -> Result<(), TdError> {
        let collection_name_1 = CollectionName::try_from("collection_1")?;
        let _collection_id = seed_collection(&db, &collection_name_1, &UserId::admin()).await;

        let dependencies = None;
        let triggers = None;
        let tables = Some(vec![
            TableName::try_from("table_1")?,
            TableName::try_from("table_2")?,
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

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            true,
        )
        .create(
            CollectionParam::builder()
                .try_collection(collection_name_1.as_str())?
                .build()?,
            create.clone(),
        );

        let service = RegisterFunctionService::new(db.clone()).service().await;
        let _response = service.raw_oneshot(request).await?;

        // Actual test
        let collection_name_2 = CollectionName::try_from("collection_2")?;
        let collection_2 = seed_collection(&db, &collection_name_2, &UserId::admin()).await;

        let dependencies = Some(vec![
            TableDependency::try_from("collection_1/table_1")?,
            TableDependency::try_from("collection_1/table_2")?,
            TableDependency::try_from("collection_2/output_1")?,
            TableDependency::try_from("output_2")?,
        ]);
        let triggers = Some(vec![
            TableTrigger::try_from("collection_1/table_1")?,
            TableTrigger::try_from("collection_1/table_2")?,
        ]);
        let tables = Some(vec![
            TableName::try_from("output_1")?,
            TableName::try_from("output_2")?,
        ]);

        let bundle_id = BundleId::default();
        let create = FunctionRegister::builder()
            .try_name("function_2")?
            .try_description("function_2 description")?
            .bundle_id(bundle_id)
            .try_snippet("function_2 snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(dependencies.clone())
            .triggers(triggers.clone())
            .tables(tables.clone())
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
                .try_collection(collection_name_2.as_str())?
                .build()?,
            create.clone(),
        );

        let service = RegisterFunctionService::new(db.clone()).service().await;
        let response = service.raw_oneshot(request).await;
        let response = response?;

        assert_register(&db, &UserId::admin(), &collection_2, &create, &response).await
    }
}
