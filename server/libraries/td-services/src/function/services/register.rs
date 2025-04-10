//
// Copyright 2025 Tabs Data Inc.
//

use crate::common::layers::extractor::extract_req_dto;
use crate::function::layers::register::build_trigger_versions;
use crate::function::layers::register::{
    build_dependency_versions, build_table_versions, insert_and_update_output_tables,
};
use std::sync::Arc;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{CreateRequest, RequestContext};
use td_objects::rest_urls::CollectionParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::extractor::{extract_req_context, extract_req_name};
use td_objects::tower_service::from::{
    combine, BuildService, ConvertIntoMapService, ExtractService, SetService, TryIntoService,
    UpdateService, VecBuildService, With,
};
use td_objects::tower_service::sql::{insert, insert_vec, By, SqlFindService, SqlSelectService};
use td_objects::tower_service::sql::{SqlAssertNotExistsService, SqlSelectIdOrNameService};
use td_objects::types::basic::{
    CollectionId, CollectionIdName, CollectionName, FunctionId, FunctionName, ReuseFrozen,
    TableDependency, TableName, TableTrigger,
};
use td_objects::types::collection::CollectionDB;
use td_objects::types::dependency::{
    DependencyDB, DependencyDBBuilder, DependencyVersionDB, DependencyVersionDBBuilder,
};
use td_objects::types::function::{
    FunctionCreate, FunctionDB, FunctionDBBuilder, FunctionVersion, FunctionVersionBuilder,
    FunctionVersionDB, FunctionVersionDBBuilder, FunctionVersionDBWithNames,
};
use td_objects::types::table::{TableDB, TableVersionDB, TableVersionDBBuilder};
use td_objects::types::trigger::{
    TriggerDB, TriggerDBBuilder, TriggerVersionDB, TriggerVersionDBBuilder,
};
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{SrvCtxProvider, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct RegisterFunctionService {
    provider:
        ServiceProvider<CreateRequest<CollectionParam, FunctionCreate>, FunctionVersion, TdError>,
}

impl RegisterFunctionService {
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
                from_fn(extract_req_context::<CreateRequest<CollectionParam, FunctionCreate>>),
                from_fn(extract_req_dto::<CreateRequest<CollectionParam, FunctionCreate>, _>),
                from_fn(extract_req_name::<CreateRequest<CollectionParam, FunctionCreate>, _>),

                TransactionProvider::new(db),

                // Extract collection from request.
                from_fn(With::<CollectionParam>::extract::<CollectionIdName>),
                from_fn(By::<CollectionIdName>::select::<DaoQueries, CollectionDB>),
                from_fn(With::<CollectionDB>::extract::<CollectionId>),
                from_fn(With::<CollectionDB>::extract::<CollectionName>),

                // Check function name does not exist in collection.
                from_fn(With::<FunctionCreate>::extract::<FunctionName>),
                from_fn(combine::<CollectionId, FunctionName>),
                from_fn(By::<(CollectionId, FunctionName)>::assert_not_exists::<DaoQueries, FunctionDB>),

                // Extract output tables, table dependencies and triggers.
                from_fn(With::<FunctionCreate>::extract::<Option<Vec<TableName>>>),
                from_fn(With::<FunctionCreate>::extract::<Option<Vec<TableDependency>>>),
                from_fn(With::<FunctionCreate>::extract::<Option<Vec<TableTrigger>>>),

                // Insert into function_versions(sql) status=Active.
                from_fn(With::<FunctionCreate>::convert_to::<FunctionVersionDBBuilder, _>),
                from_fn(With::<RequestContext>::update::<FunctionVersionDBBuilder, _>),
                from_fn(With::<CollectionId>::set::<FunctionVersionDBBuilder>),
                // TODO missing data_location and storage_version
                from_fn(With::<FunctionVersionDBBuilder>::build::<FunctionVersionDB, _>),
                from_fn(insert::<DaoQueries, FunctionVersionDB>),

                // Insert into functions(sql) function info.
                from_fn(With::<FunctionVersionDB>::convert_to::<FunctionDBBuilder, _>),
                from_fn(With::<FunctionDBBuilder>::build::<FunctionDB, _>),
                from_fn(insert::<DaoQueries, FunctionDB>),

                // Insert into table_versions(sql) current function tables status=Active.
                // Reuse table_id for tables that existed (had status=Frozen)
                from_fn(With::<FunctionVersionDB>::convert_to::<TableVersionDBBuilder, _>),
                from_fn(With::<RequestContext>::update::<TableVersionDBBuilder, _>),
                from_fn(build_table_versions::<DaoQueries>),
                from_fn(insert_vec::<DaoQueries, TableVersionDB>),

                // Insert into tables(sql) function tables info and update already existing
                // tables (frozen tables).
                from_fn(With::<FunctionCreate>::extract::<ReuseFrozen>),
                from_fn(With::<FunctionDB>::extract::<FunctionId>),
                from_fn(By::<(TableVersionDB, (CollectionId, TableName))>::find::<DaoQueries, TableDB>),
                from_fn(insert_and_update_output_tables::<DaoQueries, false>),

                // Insert into dependency_versions(sql) current function table dependencies status=Active.
                from_fn(With::<FunctionVersionDB>::convert_to::<DependencyVersionDBBuilder, _>),
                from_fn(With::<RequestContext>::update::<DependencyVersionDBBuilder, _>),
                from_fn(build_dependency_versions::<DaoQueries>),
                from_fn(insert_vec::<DaoQueries, DependencyVersionDB>),

                // Insert into trigger_versions(sql) current function trigger status=Active.
                from_fn(With::<FunctionVersionDB>::convert_to::<TriggerVersionDBBuilder, _>),
                from_fn(With::<RequestContext>::update::<TriggerVersionDBBuilder, _>),
                from_fn(build_trigger_versions::<DaoQueries>),
                from_fn(insert_vec::<DaoQueries, TriggerVersionDB>),

                // Insert into dependencies(sql) function dependencies info.
                from_fn(With::<DependencyVersionDB>::vec_convert_to::<DependencyDBBuilder, _>),
                from_fn(With::<DependencyDBBuilder>::vec_build::<DependencyDB, _>),
                from_fn(insert_vec::<DaoQueries, DependencyDB>),

                // Insert into triggers(sql) function trigger info.
                from_fn(With::<TriggerVersionDB>::vec_convert_to::<TriggerDBBuilder, _>),
                from_fn(With::<TriggerDBBuilder>::vec_build::<TriggerDB, _>),
                from_fn(insert_vec::<DaoQueries, TriggerDB>),

                // Response
                from_fn(With::<FunctionDB>::extract::<FunctionId>),
                from_fn(By::<FunctionId>::select::<DaoQueries, FunctionVersionDBWithNames>),
                from_fn(With::<FunctionVersionDBWithNames>::convert_to::<FunctionVersionBuilder, _>),
                from_fn(With::<FunctionVersionBuilder>::build::<FunctionVersion, _>),
            ))
        }
    }

    pub async fn service(
        &self,
    ) -> TdBoxService<CreateRequest<CollectionParam, FunctionCreate>, FunctionVersion, TdError>
    {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::function::services::tests::assert_register;
    use td_common::id::Id;
    use td_objects::test_utils::seed_collection2::seed_collection;
    use td_objects::test_utils::seed_user::admin_user;
    use td_objects::types::basic::{BundleId, FunctionRuntimeValues, UserId};
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

        metadata.assert_service::<CreateRequest<CollectionParam, FunctionCreate>, FunctionVersion>(&[
            type_of_val(&extract_req_context::<CreateRequest<CollectionParam, FunctionCreate>>),
            type_of_val(&extract_req_dto::<CreateRequest<CollectionParam, FunctionCreate>, _>),
            type_of_val(&extract_req_name::<CreateRequest<CollectionParam, FunctionCreate>, _>),
            // Extract collection from request.
            type_of_val(&With::<CollectionParam>::extract::<CollectionIdName>),
            type_of_val(&By::<CollectionIdName>::select::<DaoQueries, CollectionDB>),
            type_of_val(&With::<CollectionDB>::extract::<CollectionId>),
            type_of_val(&With::<CollectionDB>::extract::<CollectionName>),
            // Check function name does not exist in collection.
            type_of_val(&With::<FunctionCreate>::extract::<FunctionName>),
            type_of_val(&combine::<CollectionId, FunctionName>),
            type_of_val(
                &By::<(CollectionId, FunctionName)>::assert_not_exists::<DaoQueries, FunctionDB>,
            ),
            // Extract output tables, table dependencies and triggers.
            type_of_val(&With::<FunctionCreate>::extract::<Option<Vec<TableName>>>),
            type_of_val(&With::<FunctionCreate>::extract::<Option<Vec<TableDependency>>>),
            type_of_val(&With::<FunctionCreate>::extract::<Option<Vec<TableTrigger>>>),
            // Insert into function_versions(sql) status=Active.
            type_of_val(&With::<FunctionCreate>::convert_to::<FunctionVersionDBBuilder, _>),
            type_of_val(&With::<RequestContext>::update::<FunctionVersionDBBuilder, _>),
            type_of_val(&With::<CollectionId>::set::<FunctionVersionDBBuilder>),
            // TODO missing data_location and storage_version
            type_of_val(&With::<FunctionVersionDBBuilder>::build::<FunctionVersionDB, _>),
            type_of_val(&insert::<DaoQueries, FunctionVersionDB>),
            // Insert into functions(sql) function info.
            type_of_val(&With::<FunctionVersionDB>::convert_to::<FunctionDBBuilder, _>),
            type_of_val(&With::<FunctionDBBuilder>::build::<FunctionDB, _>),
            type_of_val(&insert::<DaoQueries, FunctionDB>),
            // Insert into table_versions(sql) current function tables status=Active.
            // Reuse table_id for tables that existed (had status=Frozen)
            type_of_val(&With::<FunctionVersionDB>::convert_to::<TableVersionDBBuilder, _>),
            type_of_val(&With::<RequestContext>::update::<TableVersionDBBuilder, _>),
            type_of_val(&build_table_versions::<DaoQueries>),
            type_of_val(&insert_vec::<DaoQueries, TableVersionDB>),
            // Insert into tables(sql) function tables info and update already existing tables (frozen tables).
            type_of_val(&With::<FunctionCreate>::extract::<ReuseFrozen>),
            type_of_val(&With::<FunctionDB>::extract::<FunctionId>),
            type_of_val(&By::<(TableVersionDB, (CollectionId, TableName))>::find::<DaoQueries, TableDB>),
            type_of_val(&insert_and_update_output_tables::<DaoQueries, false>),
            // Insert into dependency_versions(sql) current function table dependencies status=Active.
            type_of_val(&With::<FunctionVersionDB>::convert_to::<DependencyVersionDBBuilder, _>),
            type_of_val(&With::<RequestContext>::update::<DependencyVersionDBBuilder, _>),
            type_of_val(&build_dependency_versions::<DaoQueries>),
            type_of_val(&insert_vec::<DaoQueries, DependencyVersionDB>),
            // Insert into trigger_versions(sql) current function trigger status=Active.
            type_of_val(&With::<FunctionVersionDB>::convert_to::<TriggerVersionDBBuilder, _>),
            type_of_val(&With::<RequestContext>::update::<TriggerVersionDBBuilder, _>),
            type_of_val(&build_trigger_versions::<DaoQueries>),
            type_of_val(&insert_vec::<DaoQueries, TriggerVersionDB>),
            // Insert into dependencies(sql) function dependencies info.
            type_of_val(&With::<DependencyVersionDB>::vec_convert_to::<DependencyDBBuilder, _>),
            type_of_val(&With::<DependencyDBBuilder>::vec_build::<DependencyDB, _>),
            type_of_val(&insert_vec::<DaoQueries, DependencyDB>),
            // Insert into triggers(sql) function trigger info.
            type_of_val(&With::<TriggerVersionDB>::vec_convert_to::<TriggerDBBuilder, _>),
            type_of_val(&With::<TriggerDBBuilder>::vec_build::<TriggerDB, _>),
            type_of_val(&insert_vec::<DaoQueries, TriggerDB>),
            // Response
            type_of_val(&With::<FunctionDB>::extract::<FunctionId>),
            type_of_val(&By::<FunctionId>::select::<DaoQueries, FunctionVersionDBWithNames>),
            type_of_val(&With::<FunctionVersionDBWithNames>::convert_to::<FunctionVersionBuilder, _>),
            type_of_val(&With::<FunctionVersionBuilder>::build::<FunctionVersion, _>),
        ]);
    }

    #[td_test::test(sqlx)]
    async fn test_register_empty(db: DbPool) -> Result<(), TdError> {
        let admin_id = UserId::from(Id::try_from(&admin_user(&db).await)?);
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &admin_id).await;

        let bundle_id = BundleId::default();
        let create = FunctionCreate::builder()
            .try_name("function_foo")?
            .try_description("function_foo description")?
            .bundle_id(&bundle_id)
            .try_snippet("function_foo snippet")?
            .dependencies(None)
            .triggers(None)
            .tables(None)
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let request = RequestContext::with(admin_id.to_string(), "r", true)
            .await
            .create(
                CollectionParam::builder()
                    .try_collection(collection_name.as_str())?
                    .build()?,
                create.clone(),
            );

        let service = RegisterFunctionService::new(db.clone()).service().await;
        let response = service.raw_oneshot(request).await;
        let response = response?;

        assert_register(&db, &admin_id, &collection, &create, &response).await
    }

    #[td_test::test(sqlx)]
    async fn test_register_empty_vec(db: DbPool) -> Result<(), TdError> {
        let admin_id = UserId::from(Id::try_from(&admin_user(&db).await)?);
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &admin_id).await;

        let bundle_id = BundleId::default();
        let create = FunctionCreate::builder()
            .try_name("function_foo")?
            .try_description("function_foo description")?
            .bundle_id(&bundle_id)
            .try_snippet("function_foo snippet")?
            .dependencies(Some(vec![]))
            .triggers(Some(vec![]))
            .tables(Some(vec![]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let request = RequestContext::with(&admin_id.to_string(), "r", true)
            .await
            .create(
                CollectionParam::builder()
                    .try_collection(collection_name.as_str())?
                    .build()?,
                create.clone(),
            );

        let service = RegisterFunctionService::new(db.clone()).service().await;
        let response = service.raw_oneshot(request).await;
        let response = response?;

        assert_register(&db, &admin_id, &collection, &create, &response).await
    }

    #[td_test::test(sqlx)]
    async fn test_register_table_output(db: DbPool) -> Result<(), TdError> {
        let admin_id = UserId::from(Id::try_from(&admin_user(&db).await)?);
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &admin_id).await;

        let bundle_id = BundleId::default();
        let create = FunctionCreate::builder()
            .try_name("function_foo")?
            .try_description("function_foo description")?
            .bundle_id(&bundle_id)
            .try_snippet("function_foo snippet")?
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![TableName::try_from("table_foo")?]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let request = RequestContext::with(&admin_id.to_string(), "r", true)
            .await
            .create(
                CollectionParam::builder()
                    .try_collection(collection_name.as_str())?
                    .build()?,
                create.clone(),
            );

        let service = RegisterFunctionService::new(db.clone()).service().await;
        let response = service.raw_oneshot(request).await;
        let response = response?;

        assert_register(&db, &admin_id, &collection, &create, &response).await
    }

    #[td_test::test(sqlx)]
    async fn test_register_table_dependency(db: DbPool) -> Result<(), TdError> {
        let admin_id = UserId::from(Id::try_from(&admin_user(&db).await)?);
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &admin_id).await;

        let bundle_id = BundleId::default();
        let create = FunctionCreate::builder()
            .try_name("function_foo")?
            .try_description("function_foo description")?
            .bundle_id(&bundle_id)
            .try_snippet("function_foo snippet")?
            .dependencies(Some(vec![TableDependency::try_from("table_foo")?]))
            .triggers(None)
            .tables(Some(vec![TableName::try_from("table_foo")?]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let request = RequestContext::with(&admin_id.to_string(), "r", true)
            .await
            .create(
                CollectionParam::builder()
                    .try_collection(collection_name.as_str())?
                    .build()?,
                create.clone(),
            );

        let service = RegisterFunctionService::new(db.clone()).service().await;
        let response = service.raw_oneshot(request).await;
        let response = response?;

        assert_register(&db, &admin_id, &collection, &create, &response).await
    }

    #[td_test::test(sqlx)]
    async fn test_register_trigger(db: DbPool) -> Result<(), TdError> {
        let admin_id = UserId::from(Id::try_from(&admin_user(&db).await)?);
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &admin_id).await;

        let bundle_id = BundleId::default();
        let create = FunctionCreate::builder()
            .try_name("function_1")?
            .try_description("function_1 description")?
            .bundle_id(&bundle_id)
            .try_snippet("function_1 snippet")?
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![TableName::try_from("foo")?]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let request = RequestContext::with(&admin_id.to_string(), "r", true)
            .await
            .create(
                CollectionParam::builder()
                    .try_collection(collection_name.as_str())?
                    .build()?,
                create.clone(),
            );

        let service = RegisterFunctionService::new(db.clone()).service().await;
        let _response = service.raw_oneshot(request).await?;

        // Actual test
        let bundle_id = BundleId::default();
        let create = FunctionCreate::builder()
            .try_name("function_2")?
            .try_description("function_2 description")?
            .bundle_id(&bundle_id)
            .try_snippet("function_2 snippet")?
            .dependencies(None)
            .triggers(Some(vec![TableTrigger::try_from("foo")?]))
            .tables(None)
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let request = RequestContext::with(&admin_id.to_string(), "r", true)
            .await
            .create(
                CollectionParam::builder()
                    .try_collection(collection_name.as_str())?
                    .build()?,
                create.clone(),
            );

        let service = RegisterFunctionService::new(db.clone()).service().await;
        let response = service.raw_oneshot(request).await;
        let response = response?;

        assert_register(&db, &admin_id, &collection, &create, &response).await
    }

    #[td_test::test(sqlx)]
    async fn test_register_tables_dependencies_triggers(db: DbPool) -> Result<(), TdError> {
        let admin_id = UserId::from(Id::try_from(&admin_user(&db).await)?);
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &admin_id).await;

        let bundle_id = BundleId::default();
        let create = FunctionCreate::builder()
            .try_name("function_1")?
            .try_description("function_1 description")?
            .bundle_id(&bundle_id)
            .try_snippet("function_1 snippet")?
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![
                TableName::try_from("table_1")?,
                TableName::try_from("table_2")?,
            ]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let request = RequestContext::with(&admin_id.to_string(), "r", true)
            .await
            .create(
                CollectionParam::builder()
                    .try_collection(collection_name.as_str())?
                    .build()?,
                create.clone(),
            );

        let service = RegisterFunctionService::new(db.clone()).service().await;
        let _response = service.raw_oneshot(request).await?;

        // Actual test
        let bundle_id = BundleId::default();
        let create = FunctionCreate::builder()
            .try_name("function_2")?
            .try_description("function_2 description")?
            .bundle_id(&bundle_id)
            .try_snippet("function_2 snippet")?
            .dependencies(Some(vec![
                TableDependency::try_from("table_1")?,
                TableDependency::try_from("table_2")?,
            ]))
            .triggers(Some(vec![
                TableTrigger::try_from("table_1")?,
                TableTrigger::try_from("table_2")?,
            ]))
            .tables(Some(vec![
                TableName::try_from("output_1")?,
                TableName::try_from("output_2")?,
            ]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let request = RequestContext::with(&admin_id.to_string(), "r", true)
            .await
            .create(
                CollectionParam::builder()
                    .try_collection(collection_name.as_str())?
                    .build()?,
                create.clone(),
            );

        let service = RegisterFunctionService::new(db.clone()).service().await;
        let response = service.raw_oneshot(request).await;
        let response = response?;

        assert_register(&db, &admin_id, &collection, &create, &response).await
    }

    #[td_test::test(sqlx)]
    async fn test_register_tables_dependencies_triggers_different_collections(
        db: DbPool,
    ) -> Result<(), TdError> {
        let admin_id = UserId::from(Id::try_from(&admin_user(&db).await)?);
        let collection_name_1 = CollectionName::try_from("collection_1")?;
        let _collection = seed_collection(&db, &collection_name_1, &admin_id).await;

        let bundle_id = BundleId::default();
        let create = FunctionCreate::builder()
            .try_name("function_1")?
            .try_description("function_1 description")?
            .bundle_id(&bundle_id)
            .try_snippet("function_1 snippet")?
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![
                TableName::try_from("table_1")?,
                TableName::try_from("table_2")?,
            ]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let request = RequestContext::with(&admin_id.to_string(), "r", true)
            .await
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
        let collection_2 = seed_collection(&db, &collection_name_2, &admin_id).await;

        let bundle_id = BundleId::default();
        let create = FunctionCreate::builder()
            .try_name("function_2")?
            .try_description("function_2 description")?
            .bundle_id(&bundle_id)
            .try_snippet("function_2 snippet")?
            .dependencies(Some(vec![
                TableDependency::try_from("collection_1/table_1")?,
                TableDependency::try_from("collection_1/table_2")?,
                TableDependency::try_from("collection_2/output_1")?,
                TableDependency::try_from("output_2")?,
            ]))
            .triggers(Some(vec![
                TableTrigger::try_from("collection_1/table_1")?,
                TableTrigger::try_from("collection_1/table_2")?,
            ]))
            .tables(Some(vec![
                TableName::try_from("output_1")?,
                TableName::try_from("output_2")?,
            ]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let request = RequestContext::with(&admin_id.to_string(), "r", true)
            .await
            .create(
                CollectionParam::builder()
                    .try_collection(collection_name_2.as_str())?
                    .build()?,
                create.clone(),
            );

        let service = RegisterFunctionService::new(db.clone()).service().await;
        let response = service.raw_oneshot(request).await;
        let response = response?;

        assert_register(&db, &admin_id, &collection_2, &create, &response).await
    }
}
