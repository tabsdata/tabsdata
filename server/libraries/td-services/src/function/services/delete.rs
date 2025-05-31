//
// Copyright 2025 Tabs Data Inc.
//

use crate::function::layers::delete::build_deleted_function_version;
use crate::function::layers::{
    register_dependencies, register_tables, register_triggers, SKIP_AUTHZ,
};
use td_authz::{Authz, AuthzContext};
use td_error::TdError;
use td_objects::crudl::{DeleteRequest, RequestContext};
use td_objects::rest_urls::FunctionParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{AuthzOn, CollAdmin, CollDev};
use td_objects::tower_service::from::{
    combine, DefaultService, ExtractNameService, ExtractService, TryIntoService, UpdateService,
    With,
};
use td_objects::tower_service::sql::{insert, By, SqlSelectAllService, SqlSelectService};
use td_objects::types::basic::{
    AtTime, CollectionId, CollectionIdName, CollectionName, FunctionId, FunctionIdName,
    FunctionStatus, FunctionVersionId, ReuseFrozen, TableDependencyDto, TableNameDto,
    TableTriggerDto,
};
use td_objects::types::collection::CollectionDB;
use td_objects::types::dependency::DependencyDB;
use td_objects::types::function::{FunctionDB, FunctionDBBuilder, FunctionDBWithNames};
use td_objects::types::table::TableDB;
use td_objects::types::trigger::TriggerDBWithNames;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::service_provider::IntoServiceProvider;
use td_tower::{layers, provider};

#[provider(
    name = DeleteFunctionService,
    request = DeleteRequest<FunctionParam>,
    response = (),
    connection = TransactionProvider,
    context = DaoQueries,
    context = AuthzContext,
)]
fn provider() {
    layers!(
        from_fn(With::<DeleteRequest<FunctionParam>>::extract::<RequestContext>),
        from_fn(With::<DeleteRequest<FunctionParam>>::extract_name::<FunctionParam>),
        // Extract collection and function from request.
        from_fn(With::<FunctionParam>::extract::<CollectionIdName>),
        from_fn(With::<FunctionParam>::extract::<FunctionIdName>),
        // Get collection. Extract collection id and name.
        from_fn(By::<CollectionIdName>::select::<DaoQueries, CollectionDB>),
        from_fn(With::<CollectionDB>::extract::<CollectionId>),
        // check requester is coll_admin or coll_dev for the function's collection
        from_fn(AuthzOn::<CollectionId>::set),
        from_fn(Authz::<CollAdmin, CollDev>::check),
        from_fn(With::<CollectionDB>::extract::<CollectionName>),
        // Get function. Extract function version id.
        from_fn(combine::<CollectionIdName, FunctionIdName>),
        from_fn(With::<RequestContext>::extract::<AtTime>),
        from_fn(FunctionStatus::active),
        from_fn(
            By::<(CollectionIdName, FunctionIdName)>::select_version::<
                DaoQueries,
                FunctionDBWithNames,
            >
        ),
        from_fn(With::<FunctionDBWithNames>::extract::<FunctionId>),
        from_fn(With::<FunctionDBWithNames>::extract::<FunctionVersionId>),
        // Insert into function_versions(sql) status=Deleted.
        from_fn(By::<FunctionVersionId>::select::<DaoQueries, FunctionDB>),
        from_fn(With::<FunctionDB>::convert_to::<FunctionDBBuilder, _>),
        from_fn(With::<RequestContext>::update::<FunctionDBBuilder, _>),
        from_fn(build_deleted_function_version),
        from_fn(insert::<DaoQueries, FunctionDB>),
        // Register associations
        // Find previous versions.
        from_fn(By::<FunctionVersionId>::select_all::<DaoQueries, TableDB>),
        from_fn(By::<FunctionVersionId>::select_all::<DaoQueries, DependencyDB>),
        from_fn(By::<FunctionVersionId>::select_all::<DaoQueries, TriggerDBWithNames>),
        // Extract new associations (empty because it is a delete operation).
        from_fn(With::<Option<Vec<TableNameDto>>>::default),
        from_fn(With::<Option<Vec<TableDependencyDto>>>::default),
        from_fn(With::<Option<Vec<TableTriggerDto>>>::default),
        // Extract reuse frozen (default as deletes are not reusing anything)
        from_fn(With::<ReuseFrozen>::default),
        // And register new ones
        register_tables(),
        register_dependencies::<_, SKIP_AUTHZ>(),
        register_triggers::<_, SKIP_AUTHZ>(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::function::services::register::RegisterFunctionService;
    use crate::function::services::tests::{assert_delete, assert_register};
    use std::sync::Arc;
    use td_database::sql::DbPool;
    use td_objects::crudl::{handle_sql_err, RequestContext};
    use td_objects::rest_urls::CollectionParam;
    use td_objects::sql::SelectBy;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_function::seed_function;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, Decorator, FunctionRuntimeValues, RoleId, TableDependencyDto,
        TableNameDto, TableTriggerDto, UserId,
    };
    use td_objects::types::function::{FunctionBuilder, FunctionDBWithNames, FunctionRegister};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_delete_function(db: DbPool) {
        use td_tower::metadata::{type_of_val, Metadata};

        use crate::function::layers::register::{
            build_dependency_versions, build_table_versions, build_trigger_versions,
        };
        use td_objects::tower_service::from::{TryIntoService, UpdateService};
        use td_objects::tower_service::sql::insert_vec;
        use td_objects::types::basic::ReuseFrozen;
        use td_objects::types::dependency::DependencyDBBuilder;
        use td_objects::types::table::TableDBBuilder;
        use td_objects::types::trigger::{TriggerDB, TriggerDBBuilder, TriggerDBWithNames};

        let queries = Arc::new(DaoQueries::default());
        let provider =
            DeleteFunctionService::provider(db, queries, Arc::new(AuthzContext::default()));
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
            // check requester is coll_admin or coll_exec for the function's collection
            type_of_val(&AuthzOn::<CollectionId>::set),
            type_of_val(&Authz::<CollAdmin, CollDev>::check),
            type_of_val(&With::<CollectionDB>::extract::<CollectionName>),
            // Get function. Extract function version id.
            type_of_val(&combine::<CollectionIdName, FunctionIdName>),
            type_of_val(&With::<RequestContext>::extract::<AtTime>),
            type_of_val(&FunctionStatus::active),
            type_of_val(
                &By::<(CollectionIdName, FunctionIdName)>::select_version::<
                    DaoQueries,
                    FunctionDBWithNames,
                >,
            ),
            type_of_val(&With::<FunctionDBWithNames>::extract::<FunctionId>),
            type_of_val(&With::<FunctionDBWithNames>::extract::<FunctionVersionId>),
            // Insert into function_versions(sql) status=Deleted.
            type_of_val(&By::<FunctionVersionId>::select::<DaoQueries, FunctionDB>),
            type_of_val(&With::<FunctionDB>::convert_to::<FunctionDBBuilder, _>),
            type_of_val(&With::<RequestContext>::update::<FunctionDBBuilder, _>),
            type_of_val(&build_deleted_function_version),
            type_of_val(&insert::<DaoQueries, FunctionDB>),
            // Register associations
            // Find previous versions.
            type_of_val(&By::<FunctionVersionId>::select_all::<DaoQueries, TableDB>),
            type_of_val(&By::<FunctionVersionId>::select_all::<DaoQueries, DependencyDB>),
            type_of_val(&By::<FunctionVersionId>::select_all::<DaoQueries, TriggerDBWithNames>),
            // Extract new associations (empty because it is a delete operation).
            type_of_val(&With::<Option<Vec<TableNameDto>>>::default),
            type_of_val(&With::<Option<Vec<TableDependencyDto>>>::default),
            type_of_val(&With::<Option<Vec<TableTriggerDto>>>::default),
            // Extract reuse frozen (default as deletes are not reusing anything)
            type_of_val(&With::<ReuseFrozen>::default),
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

        let created_function = seed_function(&db, &collection, &create).await;

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

        let queries = Arc::new(DaoQueries::default());
        let authz_context = Arc::new(AuthzContext::default());
        let service =
            DeleteFunctionService::new(db.clone(), queries.clone(), authz_context.clone())
                .service()
                .await;
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
                TableNameDto::try_from("table1")?,
                TableNameDto::try_from("table2")?,
            ]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let created_function = seed_function(&db, &collection, &create).await;

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

        let queries = Arc::new(DaoQueries::default());
        let authz_context = Arc::new(AuthzContext::default());
        let service =
            DeleteFunctionService::new(db.clone(), queries.clone(), authz_context.clone())
                .service()
                .await;
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
            .tables(Some(vec![TableNameDto::try_from("trigger_table")?]))
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
            .dependencies(Some(vec![TableDependencyDto::try_from("trigger_table")?]))
            .triggers(None)
            .tables(None)
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let created_function = seed_function(&db, &collection, &create).await;

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

        let queries = Arc::new(DaoQueries::default());
        let authz_context = Arc::new(AuthzContext::default());
        let service =
            DeleteFunctionService::new(db.clone(), queries.clone(), authz_context.clone())
                .service()
                .await;
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
            .tables(Some(vec![TableNameDto::try_from("trigger_table")?]))
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
            .triggers(Some(vec![TableTriggerDto::try_from("trigger_table")?]))
            .tables(None)
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let created_function = seed_function(&db, &collection, &create).await;

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

        let queries = Arc::new(DaoQueries::default());
        let authz_context = Arc::new(AuthzContext::default());
        let service =
            DeleteFunctionService::new(db.clone(), queries.clone(), authz_context.clone())
                .service()
                .await;
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
            .tables(Some(vec![TableNameDto::try_from("trigger_table")?]))
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
            .dependencies(Some(vec![TableDependencyDto::try_from("trigger_table")?]))
            .triggers(Some(vec![TableTriggerDto::try_from("trigger_table")?]))
            .tables(Some(vec![TableNameDto::try_from("workout_1")?]))
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

        let queries = Arc::new(DaoQueries::default());
        let authz_context = Arc::new(AuthzContext::default());
        let service =
            RegisterFunctionService::new(db.clone(), queries.clone(), authz_context.clone())
                .service()
                .await;
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

        let created_function_2 = seed_function(&db, &collection, &create_2).await;

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

        let queries = Arc::new(DaoQueries::default());
        let authz_context = Arc::new(AuthzContext::default());
        let service =
            DeleteFunctionService::new(db.clone(), queries.clone(), authz_context.clone())
                .service()
                .await;
        service.raw_oneshot(request).await?;

        // Assert that the first function is as if it just got registered
        let queries = DaoQueries::default();
        let function_version: FunctionDBWithNames = queries
            .select_by::<FunctionDBWithNames>(&(created_function_version_1.id()))?
            .build_query_as()
            .fetch_one(&db)
            .await
            .map_err(handle_sql_err)?;
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
}
