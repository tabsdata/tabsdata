//
// Copyright 2025 Tabs Data Inc.
//

use crate::function::layers::register::data_location;
use crate::function::layers::{
    check_private_tables, register_dependencies, register_tables, register_triggers, DO_AUTHZ,
};
use td_authz::{Authz, AuthzContext};
use td_error::TdError;
use td_objects::crudl::{CreateRequest, RequestContext};
use td_objects::rest_urls::CollectionParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{AuthzOn, CollAdmin, CollDev};
use td_objects::tower_service::from::{
    combine, BuildService, DefaultService, EmptyVecService, ExtractDataService, ExtractNameService,
    ExtractService, SetService, TryIntoService, UpdateService, With,
};
use td_objects::tower_service::sql::SqlAssertNotExistsService;
use td_objects::tower_service::sql::{insert, By, SqlDeleteService, SqlSelectService};
use td_objects::types::basic::{
    AtTime, BundleId, CollectionId, CollectionIdName, CollectionName, DataLocation, FunctionId,
    FunctionName, FunctionStatus, ReuseFrozen, StorageVersion, TableDependencyDto, TableNameDto,
    TableTriggerDto,
};
use td_objects::types::collection::CollectionDB;
use td_objects::types::dependency::DependencyDB;
use td_objects::types::function::{
    BundleDB, Function, FunctionBuilder, FunctionDB, FunctionDBBuilder, FunctionDBWithNames,
    FunctionRegister,
};
use td_objects::types::table::TableDB;
use td_objects::types::trigger::TriggerDBWithNames;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::service_provider::IntoServiceProvider;
use td_tower::{layers, provider};

#[provider(
    name = RegisterFunctionService,
    request = CreateRequest<CollectionParam, FunctionRegister>,
    response = Function,
    connection = TransactionProvider,
    context = DaoQueries,
    context = AuthzContext,
)]
fn provider() {
    layers!(
        from_fn(
            With::<CreateRequest<CollectionParam, FunctionRegister>>::extract::<RequestContext>
        ),
        from_fn(
            With::<CreateRequest<CollectionParam, FunctionRegister>>::extract_name::<CollectionParam>
        ),
        from_fn(
            With::<CreateRequest<CollectionParam, FunctionRegister>>::extract_data::<
                FunctionRegister,
            >
        ),
        // Extract collection from request.
        from_fn(With::<CollectionParam>::extract::<CollectionIdName>),
        // Get collection. Extract collection id and name.
        from_fn(By::<CollectionIdName>::select::<DaoQueries, CollectionDB>),
        from_fn(With::<CollectionDB>::extract::<CollectionId>),
        // check requester is coll_admin or coll_dev for the function's collection
        from_fn(AuthzOn::<CollectionId>::set),
        from_fn(Authz::<CollAdmin, CollDev>::check),
        from_fn(With::<CollectionDB>::extract::<CollectionName>),
        // Get function.
        from_fn(With::<FunctionRegister>::extract::<FunctionName>),
        // Check function name does not exist in collection at the time of register.
        from_fn(combine::<CollectionId, FunctionName>),
        from_fn(FunctionStatus::active),
        from_fn(With::<RequestContext>::extract::<AtTime>),
        from_fn(
            By::<(CollectionId, FunctionName)>::assert_version_not_exists::<
                DaoQueries,
                FunctionDBWithNames,
            >
        ),
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
        // check private tables
        from_fn(check_private_tables::<TableDependencyDto>),
        from_fn(check_private_tables::<TableTriggerDto>),
        // Extract reuse frozen
        from_fn(With::<FunctionRegister>::extract::<ReuseFrozen>),
        // And register new ones
        register_tables(),
        register_dependencies::<_, DO_AUTHZ>(),
        register_triggers::<_, DO_AUTHZ>(),
        // Response
        from_fn(By::<FunctionId>::select::<DaoQueries, FunctionDBWithNames>),
        from_fn(With::<FunctionDBWithNames>::convert_to::<FunctionBuilder, _>),
        from_fn(With::<FunctionBuilder>::build::<Function, _>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::function::services::tests::assert_register;
    use std::collections::HashMap;
    use std::ops::Deref;
    use std::sync::Arc;
    use td_database::sql::DbPool;
    use td_objects::crudl::handle_sql_err;
    use td_objects::sql::SelectBy;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_function::seed_function;
    use td_objects::test_utils::seed_inter_collection_permission::seed_inter_collection_permission;
    use td_objects::tower_service::authz::AuthzError;
    use td_objects::tower_service::sql::SqlError;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, Decorator, FunctionRuntimeValues, RoleId, ToCollectionId, UserId,
    };
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_register_function(db: DbPool) {
        use crate::function::layers::register::{
            build_dependency_versions, build_table_versions, build_trigger_versions,
        };
        use td_objects::tower_service::authz::InterColl;
        use td_objects::tower_service::from::{
            ConvertIntoMapService, TryIntoService, UpdateService, VecBuildService, With,
        };
        use td_objects::tower_service::sql::insert_vec;
        use td_objects::types::dependency::{DependencyDB, DependencyDBBuilder};
        use td_objects::types::permission::{InterCollectionAccess, InterCollectionAccessBuilder};
        use td_objects::types::table::{TableDB, TableDBBuilder};
        use td_objects::types::trigger::{TriggerDB, TriggerDBBuilder};

        use td_tower::metadata::{type_of_val, Metadata};

        let queries = Arc::new(DaoQueries::default());
        let provider =
            RegisterFunctionService::provider(db, queries, Arc::new(AuthzContext::default()));
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
                    // check requester is coll_admin or coll_dev for the function's collection
                    type_of_val(&AuthzOn::<CollectionId>::set),
                    type_of_val(&Authz::<CollAdmin, CollDev>::check),

                    type_of_val(&With::<CollectionDB>::extract::<CollectionName>),

                    // Get function.
                    type_of_val(&With::<FunctionRegister>::extract::<FunctionName>),

                    // Check function name does not exist in collection.
                    type_of_val(&combine::<CollectionId, FunctionName>),
                    type_of_val(&FunctionStatus::active),
                    type_of_val(&With::<RequestContext>::extract::<AtTime>),
                    type_of_val(&By::<(CollectionId, FunctionName)>::assert_version_not_exists::<DaoQueries, FunctionDBWithNames>),

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
                    // check private tables
                    type_of_val(&check_private_tables::<TableDependencyDto>),
                    type_of_val(&check_private_tables::<TableTriggerDto>),
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

                    // inter collections check for dependencies
                    type_of_val(&With::<DependencyDB>::vec_convert_to::<InterCollectionAccessBuilder, _>),
                    type_of_val(&With::<InterCollectionAccessBuilder>::vec_build::<InterCollectionAccess, _>),
                    type_of_val(&Authz::<InterColl>::check_inter_collection),

                    type_of_val(&insert_vec::<DaoQueries, DependencyDB>),
                    // Insert into trigger_versions(sql) current function trigger status=Active.
                    type_of_val(&With::<FunctionDB>::convert_to::<TriggerDBBuilder, _>),
                    type_of_val(&With::<RequestContext>::update::<TriggerDBBuilder, _>),
                    type_of_val(&build_trigger_versions::<DaoQueries>),

                    // inter collections check for trigger
                    type_of_val(&With::<TriggerDB>::vec_convert_to::<InterCollectionAccessBuilder, _>),
                    type_of_val(&With::<InterCollectionAccessBuilder>::vec_build::<InterCollectionAccess, _>),
                    type_of_val(&Authz::<InterColl>::check_inter_collection),

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

        let queries = Arc::new(DaoQueries::default());
        let authz = Arc::new(AuthzContext::default());
        let service = RegisterFunctionService::new(db.clone(), queries.clone(), authz.clone())
            .service()
            .await;
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

        let queries = Arc::new(DaoQueries::default());
        let authz = Arc::new(AuthzContext::default());
        let service = RegisterFunctionService::new(db.clone(), queries.clone(), authz.clone())
            .service()
            .await;
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

        let queries = Arc::new(DaoQueries::default());
        let authz = Arc::new(AuthzContext::default());
        let service = RegisterFunctionService::new(db.clone(), queries.clone(), authz.clone())
            .service()
            .await;
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

        let queries = Arc::new(DaoQueries::default());
        let authz = Arc::new(AuthzContext::default());
        let service = RegisterFunctionService::new(db.clone(), queries.clone(), authz.clone())
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        let response = response?;

        assert_register(&db, &UserId::admin(), &collection, &create, &response).await
    }

    #[td_test::test(sqlx)]
    async fn test_register_implicit_trigger_none(db: DbPool) -> Result<(), TdError> {
        test_register_trigger(db, None).await
    }

    #[td_test::test(sqlx)]
    async fn test_register_implicit_trigger_some_empty(db: DbPool) -> Result<(), TdError> {
        let test_triggers = Some(vec![]);
        test_register_trigger(db, test_triggers).await
    }

    #[td_test::test(sqlx)]
    async fn test_register_implicit_trigger_some(db: DbPool) -> Result<(), TdError> {
        let test_triggers = Some(vec![
            TableTriggerDto::try_from("table_1")?,
            TableTriggerDto::try_from("table_2")?,
        ]);
        test_register_trigger(db, test_triggers).await
    }

    async fn test_register_trigger(
        db: DbPool,
        test_triggers: Option<Vec<TableTriggerDto>>,
    ) -> Result<(), TdError> {
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

        let queries = Arc::new(DaoQueries::default());
        let authz = Arc::new(AuthzContext::default());
        let service = RegisterFunctionService::new(db.clone(), queries.clone(), authz.clone())
            .service()
            .await;
        let _response = service.raw_oneshot(request).await?;

        // Actual test
        let dependencies = Some(vec![
            TableDependencyDto::try_from("table_1")?,
            TableDependencyDto::try_from("table_2")?,
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
            .triggers(test_triggers.clone())
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

        let queries = Arc::new(DaoQueries::default());
        let authz = Arc::new(AuthzContext::default());
        let service = RegisterFunctionService::new(db.clone(), queries.clone(), authz.clone())
            .service()
            .await;
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

        let queries = Arc::new(DaoQueries::default());
        let authz = Arc::new(AuthzContext::default());
        let service = RegisterFunctionService::new(db.clone(), queries.clone(), authz.clone())
            .service()
            .await;
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

        let queries = Arc::new(DaoQueries::default());
        let authz = Arc::new(AuthzContext::default());
        let service = RegisterFunctionService::new(db.clone(), queries.clone(), authz.clone())
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        let response = response?;

        assert_register(&db, &UserId::admin(), &collection, &create, &response).await
    }

    #[td_test::test(sqlx)]
    async fn test_register_tables_dependencies_different_collections_permissions_ok(
        db: DbPool,
    ) -> Result<(), TdError> {
        test_register_tables_dependencies_triggers_different_collections(db, true, false, true)
            .await
    }

    #[td_test::test(sqlx)]
    async fn test_register_tables_dependencies_different_collections_permissions_forbidden(
        db: DbPool,
    ) -> Result<(), TdError> {
        test_register_tables_dependencies_triggers_different_collections(db, true, false, false)
            .await
    }

    #[td_test::test(sqlx)]
    async fn test_register_tables_triggers_different_collections_with_tr_permissions_ok(
        db: DbPool,
    ) -> Result<(), TdError> {
        test_register_tables_dependencies_triggers_different_collections(db, false, true, true)
            .await
    }

    #[td_test::test(sqlx)]
    async fn test_register_tables_triggers_different_collections_with_tr_permissions_forbidden(
        db: DbPool,
    ) -> Result<(), TdError> {
        test_register_tables_dependencies_triggers_different_collections(db, false, true, false)
            .await
    }

    async fn test_register_tables_dependencies_triggers_different_collections(
        db: DbPool,
        deps_diff_collection: bool,
        triggers_diff_collection: bool,
        with_permission: bool,
    ) -> Result<(), TdError> {
        let collection_name_1 = CollectionName::try_from("collection_1")?;
        let collection_1 = seed_collection(&db, &collection_name_1, &UserId::admin()).await;
        let collection_name_2 = CollectionName::try_from("collection_2")?;
        let collection_2 = seed_collection(&db, &collection_name_2, &UserId::admin()).await;

        if with_permission {
            seed_inter_collection_permission(
                &db,
                collection_1.id(),
                &ToCollectionId::try_from(collection_2.id())?,
            )
            .await;
        }

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

        let queries = Arc::new(DaoQueries::default());
        let authz = Arc::new(AuthzContext::default());
        let service = RegisterFunctionService::new(db.clone(), queries.clone(), authz.clone())
            .service()
            .await;
        let _response = service.raw_oneshot(request).await?;

        let dependencies = Some(vec![]);
        let triggers = Some(vec![]);
        let tables = Some(vec![
            TableNameDto::try_from("table_1")?,
            TableNameDto::try_from("table_2")?,
        ]);

        let bundle_id = BundleId::default();
        let create = FunctionRegister::builder()
            .try_name("function_3")?
            .try_description("function_3 description")?
            .bundle_id(bundle_id)
            .try_snippet("function_3 snippet")?
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

        let queries = Arc::new(DaoQueries::default());
        let authz = Arc::new(AuthzContext::default());
        let service = RegisterFunctionService::new(db.clone(), queries.clone(), authz.clone())
            .service()
            .await;
        let _ = service.raw_oneshot(request).await?;

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
            TableDependencyDto::try_from(format!("{}/table_1", deps_collection))?,
            TableDependencyDto::try_from(format!("{}/table_2", deps_collection))?,
            TableDependencyDto::try_from("collection_2/output_1")?,
            TableDependencyDto::try_from("output_2")?,
        ]);
        let triggers = Some(vec![
            TableTriggerDto::try_from("collection_1/table_1")?,
            TableTriggerDto::try_from(format!("{}/table_2", triggers_collection))?,
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

        let queries = Arc::new(DaoQueries::default());
        let authz = Arc::new(AuthzContext::default());
        let service = RegisterFunctionService::new(db.clone(), queries.clone(), authz.clone())
            .service()
            .await;
        let res = service.raw_oneshot(request).await;

        if with_permission {
            let _ = assert_register(&db, &UserId::admin(), &collection_2, &create, &res?).await;
        } else {
            let err = res.err().unwrap();
            assert_eq!(
                std::mem::discriminant(&AuthzError::ForbiddenInterCollectionAccess("".to_string())),
                std::mem::discriminant(err.domain_err())
            );
        }
        Ok(())
    }

    #[td_test::test(sqlx)]
    async fn test_register_same_name(db: DbPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let _ = seed_collection(&db, &collection_name, &UserId::admin()).await;

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

        let queries = Arc::new(DaoQueries::default());
        let authz = Arc::new(AuthzContext::default());
        let service = RegisterFunctionService::new(db.clone(), queries.clone(), authz.clone())
            .service()
            .await;
        let response = service.raw_oneshot(request.clone()).await;
        let _ = response?;

        let service = RegisterFunctionService::new(db.clone(), queries.clone(), authz.clone())
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        let response = response;
        let err = response.err().unwrap();
        assert!(matches!(
            err.domain_err(),
            SqlError::EntityAlreadyExists(_, _, _)
        ));
        Ok(())
    }

    #[td_test::test(sqlx)]
    async fn test_register_private_tables_all_same_collection(db: DbPool) -> Result<(), TdError> {
        test_private_tables(db, false, false).await
    }

    #[td_test::test(sqlx)]
    async fn test_register_private_tables_deps_diff_collection(db: DbPool) -> Result<(), TdError> {
        test_private_tables(db, true, false).await
    }

    #[td_test::test(sqlx)]
    async fn test_register_private_tables_triggers_diff_collection(
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
            TableDependencyDto::try_from(format!("{}/_table_1", deps_collection))?,
            TableDependencyDto::try_from("_table_2")?,
        ]);
        let triggers = Some(vec![
            TableTriggerDto::try_from(format!("{}/_table_1", triggers_collection))?,
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

        let queries = Arc::new(DaoQueries::default());
        let authz = Arc::new(AuthzContext::default());
        let service = RegisterFunctionService::new(db.clone(), queries.clone(), authz.clone())
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
    async fn test_private_table_flag(db: DbPool) -> Result<(), TdError> {
        let collection_name_1 = CollectionName::try_from("collection_1")?;
        seed_collection(&db, &collection_name_1, &UserId::admin()).await;

        let tables = Some(vec![
            TableNameDto::try_from("table0")?,
            TableNameDto::try_from("_table0")?,
        ]);

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
            create,
        );

        let queries = Arc::new(DaoQueries::default());
        let authz = Arc::new(AuthzContext::default());
        let service = RegisterFunctionService::new(db.clone(), queries.clone(), authz.clone())
            .service()
            .await;
        let res = service.raw_oneshot(request).await;
        assert!(res.is_ok());

        let tables: Vec<TableDB> = queries
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
}
