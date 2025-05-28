//
// Copyright 2025 Tabs Data Inc.
//

use crate::function::layers::register::{build_dependency_versions, build_table_versions};
use crate::function::layers::register::{build_trigger_versions, data_location};
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
use td_objects::tower_service::sql::SqlAssertNotExistsService;
use td_objects::tower_service::sql::{insert, insert_vec, By, SqlDeleteService, SqlSelectService};
use td_objects::types::basic::{
    BundleId, CollectionId, CollectionIdName, CollectionName, DataLocation, FunctionId,
    FunctionName, ReuseFrozen, StorageVersion, TableDependencyDto, TableNameDto, TableTriggerDto,
};
use td_objects::types::collection::CollectionDB;
use td_objects::types::dependency::{DependencyDB, DependencyDBBuilder};
use td_objects::types::function::{
    BundleDB, Function, FunctionBuilder, FunctionDB, FunctionDBBuilder, FunctionDBWithNames,
    FunctionRegister,
};
use td_objects::types::table::{TableDB, TableDBBuilder};
use td_objects::types::trigger::{TriggerDB, TriggerDBBuilder, TriggerDBWithNames};
use td_tower::default_services::{SrvCtxProvider, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{l, layers, p, service_provider};

pub struct RegisterFunctionService {
    provider: ServiceProvider<CreateRequest<CollectionParam, FunctionRegister>, Function, TdError>,
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
                from_fn(With::<FunctionRegister>::convert_to::<FunctionDBBuilder, _>),
                from_fn(With::<RequestContext>::update::<FunctionDBBuilder, _>),
                from_fn(With::<CollectionId>::set::<FunctionDBBuilder>),
                from_fn(With::<StorageVersion>::set::<FunctionDBBuilder>),
                from_fn(With::<DataLocation>::set::<FunctionDBBuilder>),
                from_fn(With::<FunctionDBBuilder>::build::<FunctionDB, _>),
                from_fn(insert::<DaoQueries, FunctionDB>),

                // Remove from bundles
                from_fn(With::<FunctionDB>::extract::<BundleId>),
                from_fn(By::<BundleId>::delete::<DaoQueries, BundleDB>),

                // Register associations
                // Extract new function id
                from_fn(With::<FunctionDB>::extract::<FunctionId>),
                // Find previous versions (empty because it is a new function)
                from_fn(With::<TableDB>::empty_vec),
                from_fn(With::<DependencyDB>::empty_vec),
                from_fn(With::<TriggerDBWithNames>::empty_vec),
                // Extract new associations
                from_fn(With::<FunctionRegister>::extract::<Option<Vec<TableNameDto>>>),
                from_fn(With::<FunctionRegister>::extract::<Option<Vec<TableDependencyDto>>>),
                from_fn(With::<FunctionRegister>::extract::<Option<Vec<TableTriggerDto>>>),
                // Extract reuse frozen
                from_fn(With::<FunctionRegister>::extract::<ReuseFrozen>),
                // And register new ones
                RegisterFunctionService::register_tables(),
                RegisterFunctionService::register_dependencies(),
                RegisterFunctionService::register_triggers(),

                // Response
                from_fn(By::<FunctionId>::select::<DaoQueries, FunctionDBWithNames>),
                from_fn(With::<FunctionDBWithNames>::convert_to::<FunctionBuilder, _>),
                from_fn(With::<FunctionBuilder>::build::<Function, _>),
            ))
        }
    }

    l! {
        register_tables() {
            layers!(
                // Insert into table_versions(sql) current function tables status=Active.
                // Reuse table_id for tables that existed (had status=Frozen)
                from_fn(With::<FunctionDB>::convert_to::<TableDBBuilder, _>),
                from_fn(With::<RequestContext>::update::<TableDBBuilder, _>),
                from_fn(build_table_versions),
                from_fn(insert_vec::<DaoQueries, TableDB>),
            )
        }
    }

    l! {
        register_dependencies() {
            layers!(
                // Insert into dependency_versions(sql) current function table dependencies status=Active.
                from_fn(With::<FunctionDB>::convert_to::<DependencyDBBuilder, _>),
                from_fn(With::<RequestContext>::update::<DependencyDBBuilder, _>),
                from_fn(build_dependency_versions::<DaoQueries>),
                from_fn(insert_vec::<DaoQueries, DependencyDB>),
            )
        }
    }

    l! {
        register_triggers() {
            layers!(
                // Insert into trigger_versions(sql) current function trigger status=Active.
                from_fn(With::<FunctionDB>::convert_to::<TriggerDBBuilder, _>),
                from_fn(With::<RequestContext>::update::<TriggerDBBuilder, _>),
                from_fn(build_trigger_versions::<DaoQueries>),
                from_fn(insert_vec::<DaoQueries, TriggerDB>),
            )
        }
    }

    pub async fn service(
        &self,
    ) -> TdBoxService<CreateRequest<CollectionParam, FunctionRegister>, Function, TdError> {
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

        metadata
            .assert_service::<CreateRequest<CollectionParam, FunctionRegister>, Function>(
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
                    type_of_val(&With::<FunctionRegister>::convert_to::<FunctionDBBuilder, _>),
                    type_of_val(&With::<RequestContext>::update::<FunctionDBBuilder, _>),
                    type_of_val(&With::<CollectionId>::set::<FunctionDBBuilder>),
                    type_of_val(&With::<StorageVersion>::set::<FunctionDBBuilder>),
                    type_of_val(&With::<DataLocation>::set::<FunctionDBBuilder>),
                    type_of_val(&With::<FunctionDBBuilder>::build::<FunctionDB, _>),
                    type_of_val(&insert::<DaoQueries, FunctionDB>),

                    // Remove from bundles
                    type_of_val(&With::<FunctionDB>::extract::<BundleId>),
                    type_of_val(&By::<BundleId>::delete::<DaoQueries, BundleDB>),

                    // Register associations
                    // Extract new function id
                    type_of_val(&With::<FunctionDB>::extract::<FunctionId>),
                    // Find previous versions (empty because it is a new function)
                    type_of_val(&With::<TableDB>::empty_vec),
                    type_of_val(&With::<DependencyDB>::empty_vec),
                    type_of_val(&With::<TriggerDBWithNames>::empty_vec),
                    // Extract new associations
                    type_of_val(&With::<FunctionRegister>::extract::<Option<Vec<TableNameDto>>>),
                    type_of_val(&With::<FunctionRegister>::extract::<Option<Vec<TableDependencyDto>>>),
                    type_of_val(&With::<FunctionRegister>::extract::<Option<Vec<TableTriggerDto>>>),
                    // Extract reuse frozen
                    type_of_val(&With::<FunctionRegister>::extract::<ReuseFrozen>),
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
                    type_of_val(&By::<FunctionId>::select::<DaoQueries, FunctionDBWithNames>),
                    type_of_val(&With::<FunctionDBWithNames>::convert_to::<FunctionBuilder, _>),
                    type_of_val(&With::<FunctionBuilder>::build::<Function, _>),
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
        let tables = Some(vec![TableNameDto::try_from("table_foo")?]);

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

        let dependencies = Some(vec![TableDependencyDto::try_from("table_foo")?]);
        let triggers = None;
        let tables = Some(vec![TableNameDto::try_from("table_foo")?]);

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
        let tables = Some(vec![TableNameDto::try_from("foo")?]);

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
        let triggers = Some(vec![TableTriggerDto::try_from("foo")?]);
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
            TableNameDto::try_from("table_1")?,
            TableNameDto::try_from("table_2")?,
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
            TableDependencyDto::try_from("table_1")?,
            TableDependencyDto::try_from("table_2")?,
        ]);
        let triggers = Some(vec![
            TableTriggerDto::try_from("table_1")?,
            TableTriggerDto::try_from("table_2")?,
        ]);
        let tables = Some(vec![
            TableNameDto::try_from("output_1")?,
            TableNameDto::try_from("output_2")?,
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
            TableNameDto::try_from("table_1")?,
            TableNameDto::try_from("table_2")?,
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
            TableDependencyDto::try_from("collection_1/table_1")?,
            TableDependencyDto::try_from("collection_1/table_2")?,
            TableDependencyDto::try_from("collection_2/output_1")?,
            TableDependencyDto::try_from("output_2")?,
        ]);
        let triggers = Some(vec![
            TableTriggerDto::try_from("collection_1/table_1")?,
            TableTriggerDto::try_from("collection_1/table_2")?,
        ]);
        let tables = Some(vec![
            TableNameDto::try_from("output_1")?,
            TableNameDto::try_from("output_2")?,
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
