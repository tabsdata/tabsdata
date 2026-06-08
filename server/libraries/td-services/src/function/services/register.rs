//
// Copyright 2025 Tabs Data Inc.
//

use crate::do_authz::DO_AUTHZ;
use crate::function::layers::register::{data_location, validate_tables_do_not_exist};
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
use td_objects::dxo::crudl::{CreateRequest, RequestContext};
use td_objects::dxo::dependency::DependencyDB;
use td_objects::dxo::function::{
    Function, FunctionBuilder, FunctionDB, FunctionDBBuilder, FunctionDBWithNames, FunctionRegister,
};
use td_objects::dxo::table::TableDB;
use td_objects::dxo::trigger::TriggerDBWithNames;
use td_objects::rest_urls::CollectionParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{
    AtTime, BundleId, CollectionId, CollectionIdName, CollectionName, DataLocation, Enabled,
    FunctionId, FunctionName, FunctionVersionId, ProjectId, ReuseFrozen, StorageVersion,
    TableNameDto, TableType, WorkerConfig, WorkerTypeDto,
};
use td_objects::types::composed::{TableDependencyDto, TableTriggerDto};
use td_queries::By;
use td_queries::insert;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = RegisterFunctionService,
    request = CreateRequest<CollectionParam, FunctionRegister>,
    response = Function,
    connection = TransactionProvider,
    scope = "Project",
    context = AuthzContext,
    context = SchedulerNotify,
)]
fn service() {
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
        from_fn(By::<CollectionIdName>::fetch_one::<CollectionDB>),
        from_fn(With::<CollectionDB>::extract::<CollectionId>),
        // check requester is coll_admin or coll_dev for the function's collection
        from_fn(AuthzOn::<CollectionId>::set),
        from_fn(Authz::<CollAdmin, CollDev>::check),
        from_fn(With::<CollectionDB>::extract::<CollectionName>),
        // Get function.
        from_fn(With::<FunctionRegister>::extract::<FunctionName>),
        // Check function name does not exist in collection at the time of register.
        from_fn(With::<RequestContext>::extract::<AtTime>),
        // Validate function does not exist
        from_fn(
            By::<And![CollectionId, FunctionName]>::assert_version_not_exists::<
                { FunctionDBWithNames::Available },
                FunctionDBWithNames,
            >
        ),
        // Get location and storage version.
        from_fn(With::<StorageVersion>::default),
        from_fn(data_location),
        // Get enabled flag
        from_fn(With::<FunctionRegister>::extract::<Option<Enabled>>),
        from_fn(With::<Option<Enabled>>::unwrap_or_default),
        // Insert into function_versions(sql) status=Active. The function
        // inherits the collection's project — there's no path where a
        // function's project differs from its collection's.
        from_fn(With::<CollectionDB>::extract::<ProjectId>),
        from_fn(With::<FunctionRegister>::try_convert_to::<FunctionDBBuilder, _>),
        from_fn(With::<RequestContext>::update::<FunctionDBBuilder, _>),
        from_fn(With::<CollectionId>::set::<FunctionDBBuilder>),
        from_fn(With::<ProjectId>::set::<FunctionDBBuilder>),
        from_fn(With::<StorageVersion>::set::<FunctionDBBuilder>),
        from_fn(With::<DataLocation>::set::<FunctionDBBuilder>),
        from_fn(With::<Enabled>::set::<FunctionDBBuilder>),
        from_fn(With::<FunctionDBBuilder>::build::<FunctionDB>),
        from_fn(insert::<FunctionDB>),
        // Remove from bundles
        from_fn(With::<FunctionDB>::extract::<BundleId>),
        from_fn(By::<BundleId>::delete::<BundleDB>),
        // Register associations
        // Find previous versions (empty because it is a new function)
        from_fn(With::<Option<FunctionVersionId>>::default),
        from_fn(With::<TableDB>::empty_vec),
        from_fn(With::<DependencyDB>::empty_vec),
        from_fn(With::<TriggerDBWithNames>::empty_vec),
        // Extract new associations
        from_fn(With::<FunctionDB>::extract::<FunctionId>),
        from_fn(With::<FunctionRegister>::extract::<Option<Vec<TableNameDto>>>),
        from_fn(With::<FunctionRegister>::extract::<Option<HashMap<TableNameDto, TableType>>>),
        from_fn(With::<FunctionRegister>::extract::<Option<Vec<TableDependencyDto>>>),
        from_fn(With::<FunctionRegister>::extract::<Option<Vec<TableTriggerDto>>>),
        from_fn(With::<FunctionRegister>::extract::<Option<HashMap<WorkerTypeDto, WorkerConfig>>>),
        // Validate tables do not exist
        from_fn(validate_tables_do_not_exist),
        // check private tables
        from_fn(check_private_tables::<TableDependencyDto>),
        from_fn(check_private_tables::<TableTriggerDto>),
        // Extract reuse frozen
        from_fn(With::<FunctionRegister>::extract::<ReuseFrozen>),
        // And register new ones
        register_tables(),
        register_dependencies::<_, DO_AUTHZ>(),
        register_triggers::<_, DO_AUTHZ>(),
        register_workers(),
        // Response
        from_fn(With::<FunctionDB>::extract::<FunctionVersionId>),
        from_fn(By::<FunctionVersionId>::fetch_one::<FunctionDBWithNames>),
        from_fn(With::<FunctionDBWithNames>::try_convert_to::<FunctionBuilder, _>),
        from_fn(With::<FunctionBuilder>::build::<Function>),
        // Wake the scheduler so newly registered cron/stage workers
        // start immediately instead of at the next polling tick.
        from_fn(notify_scheduler),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::function::layers::register::RegisterFunctionError;
    use crate::function::services::tests::assert_register;
    use std::collections::HashMap;
    use std::str::FromStr;
    use ta_services::service::TdService;
    use td_authz::tower::AuthzError;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_execution::VersionResolverError;
    use td_objects::Buildable;
    use td_objects::dxo::worker::WorkerDB;
    use td_objects::errors::SqlError;
    use td_objects::types::basic::{
        ProjectId,
        AccessTokenId, Decorator, FunctionRuntimeValues, RoleId, ToCollectionId, UserId,
        WorkerMessageStatus, WorkerStatus, WorkerType,
    };
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_register_function(db: TdPool) {
        use crate::function::layers::register::{
            build_dependency_versions, build_table_versions, build_tables_trigger_versions,
            build_trigger_versions, build_workers, stop_current_workers,
        };
        use td_authz::tower::InterColl;
        use td_objects::dxo::dependency::{DependencyDB, DependencyDBBuilder};
        use td_objects::dxo::inter_collection_access::{
            InterCollectionAccess, InterCollectionAccessBuilder,
        };
        use td_objects::dxo::table::{TableDB, TableDBBuilder};
        use td_objects::dxo::trigger::{TriggerDB, TriggerDBBuilder};
        use td_objects::dxo::worker::{WorkerDB, WorkerDBBuilder};
        use td_objects::tower_service::with::With;
        use td_queries::insert_vec;

        use td_tower::metadata::type_of_val;
        use td_tower::project_scope::Scoped;

        RegisterFunctionService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<Scoped<CreateRequest<CollectionParam, FunctionRegister>, ProjectId>, Function>(&[
                type_of_val(
                    &With::<CreateRequest<CollectionParam, FunctionRegister>>::extract::<
                        RequestContext,
                    >,
                ),
                type_of_val(
                    &With::<CreateRequest<CollectionParam, FunctionRegister>>::extract_name::<
                        CollectionParam,
                    >,
                ),
                type_of_val(
                    &With::<CreateRequest<CollectionParam, FunctionRegister>>::extract_data::<
                        FunctionRegister,
                    >,
                ),
                // Extract collection from request.
                type_of_val(&With::<CollectionParam>::extract::<CollectionIdName>),
                // Get collection. Extract collection id and name.
                type_of_val(&By::<CollectionIdName>::fetch_one::<CollectionDB>),
                type_of_val(&With::<CollectionDB>::extract::<CollectionId>),
                // check requester is coll_admin or coll_dev for the function's collection
                type_of_val(&AuthzOn::<CollectionId>::set),
                type_of_val(&Authz::<CollAdmin, CollDev>::check),
                type_of_val(&With::<CollectionDB>::extract::<CollectionName>),
                // Get function.
                type_of_val(&With::<FunctionRegister>::extract::<FunctionName>),
                // Check function name does not exist in collection.
                type_of_val(&With::<RequestContext>::extract::<AtTime>),
                type_of_val(
                    &By::<And![CollectionId, FunctionName]>::assert_version_not_exists::<
                        { FunctionDBWithNames::Available },
                        FunctionDBWithNames,
                    >,
                ),
                // Get location and storage version.
                type_of_val(&With::<StorageVersion>::default),
                type_of_val(&data_location),
                // Get enabled flag
                type_of_val(&With::<FunctionRegister>::extract::<Option<Enabled>>),
                type_of_val(&With::<Option<Enabled>>::unwrap_or_default),
                // Insert into function_versions(sql) status=Active.
                type_of_val(&With::<FunctionRegister>::try_convert_to::<FunctionDBBuilder, _>),
                type_of_val(&With::<RequestContext>::update::<FunctionDBBuilder, _>),
                type_of_val(&With::<CollectionId>::set::<FunctionDBBuilder>),
                type_of_val(&With::<StorageVersion>::set::<FunctionDBBuilder>),
                type_of_val(&With::<DataLocation>::set::<FunctionDBBuilder>),
                type_of_val(&With::<Enabled>::set::<FunctionDBBuilder>),
                type_of_val(&With::<FunctionDBBuilder>::build::<FunctionDB>),
                type_of_val(&insert::<FunctionDB>),
                // Remove from bundles
                type_of_val(&With::<FunctionDB>::extract::<BundleId>),
                type_of_val(&By::<BundleId>::delete::<BundleDB>),
                // Register associations
                // Find previous versions (empty because it is a new function)
                type_of_val(&With::<Option<FunctionVersionId>>::default),
                type_of_val(&With::<TableDB>::empty_vec),
                type_of_val(&With::<DependencyDB>::empty_vec),
                type_of_val(&With::<TriggerDBWithNames>::empty_vec),
                // Extract new associations
                type_of_val(&With::<FunctionDB>::extract::<FunctionId>),
                type_of_val(&With::<FunctionRegister>::extract::<Option<Vec<TableNameDto>>>),
                type_of_val(
                    &With::<FunctionRegister>::extract::<Option<HashMap<TableNameDto, TableType>>>,
                ),
                type_of_val(&With::<FunctionRegister>::extract::<Option<Vec<TableDependencyDto>>>),
                type_of_val(&With::<FunctionRegister>::extract::<Option<Vec<TableTriggerDto>>>),
                type_of_val(
                    &With::<FunctionRegister>::extract::<
                        Option<HashMap<WorkerTypeDto, WorkerConfig>>,
                    >,
                ),
                // Validate tables do not exist
                type_of_val(&validate_tables_do_not_exist),
                // check private tables
                type_of_val(&check_private_tables::<TableDependencyDto>),
                type_of_val(&check_private_tables::<TableTriggerDto>),
                // Extract reuse frozen
                type_of_val(&With::<FunctionRegister>::extract::<ReuseFrozen>),
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
                type_of_val(
                    &With::<DependencyDB>::vec_convert_to::<InterCollectionAccessBuilder, _>,
                ),
                type_of_val(
                    &With::<Vec<InterCollectionAccessBuilder>>::build::<Vec<InterCollectionAccess>>,
                ),
                type_of_val(&Authz::<InterColl>::check_inter_collection),
                type_of_val(&insert_vec::<DependencyDB>),
                // Insert into trigger_versions(sql) current function trigger status=Active.
                type_of_val(&With::<FunctionDB>::try_convert_to::<TriggerDBBuilder, _>),
                type_of_val(&With::<RequestContext>::update::<TriggerDBBuilder, _>),
                type_of_val(&build_trigger_versions),
                // inter collections check for trigger
                type_of_val(&With::<TriggerDB>::vec_convert_to::<InterCollectionAccessBuilder, _>),
                type_of_val(
                    &With::<Vec<InterCollectionAccessBuilder>>::build::<Vec<InterCollectionAccess>>,
                ),
                type_of_val(&Authz::<InterColl>::check_inter_collection),
                type_of_val(&insert_vec::<TriggerDB>),
                // Set stopped for current workers and insert into workers(sql) new workers.
                type_of_val(&stop_current_workers),
                type_of_val(&With::<FunctionDB>::try_convert_to::<WorkerDBBuilder, _>),
                type_of_val(&build_workers),
                type_of_val(&insert_vec::<WorkerDB>),
                // Response
                type_of_val(&With::<FunctionDB>::extract::<FunctionVersionId>),
                type_of_val(&By::<FunctionVersionId>::fetch_one::<FunctionDBWithNames>),
                type_of_val(&With::<FunctionDBWithNames>::try_convert_to::<FunctionBuilder, _>),
                type_of_val(&With::<FunctionBuilder>::build::<Function>),
                type_of_val(&notify_scheduler),
            ]);
    }

    #[td_test::test(db)]
    async fn test_register_empty(db: TdPool) -> Result<(), TdError> {
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

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name.as_str())?)
                    .build()?,
                create.clone(),
            )
            .in_project(ProjectId::sandbox());

        let service = RegisterFunctionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;

        assert_register(&db, &UserId::admin(), &collection, &create, &response).await
    }

    #[td_test::test(db)]
    async fn test_register_empty_vec(db: TdPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

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

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name.as_str())?)
                    .build()?,
                create.clone(),
            )
            .in_project(ProjectId::sandbox());

        let service = RegisterFunctionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;

        assert_register(&db, &UserId::admin(), &collection, &create, &response).await
    }

    #[td_test::test(db)]
    async fn test_register_table_output(db: TdPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

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

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name.as_str())?)
                    .build()?,
                create.clone(),
            )
            .in_project(ProjectId::sandbox());

        let service = RegisterFunctionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;

        assert_register(&db, &UserId::admin(), &collection, &create, &response).await
    }

    #[td_test::test(db)]
    async fn test_register_table_output_duplicate_output_tables_err(
        db: TdPool,
    ) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let _collection = db
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let dependencies = None;
        let triggers = None;
        let tables = Some(vec![
            TableNameDto::try_from("table_foo")?,
            TableNameDto::try_from("table_foo")?,
        ]);

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

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name.as_str())?)
                    .build()?,
                create.clone(),
            )
            .in_project(ProjectId::sandbox());

        let service = RegisterFunctionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let err = response.unwrap_err();
        let err = err.domain_err::<RegisterFunctionError>();
        if let RegisterFunctionError::DuplicateTableDefinition(duplicate) = err {
            assert_eq!(duplicate.len(), 1);
            assert_eq!(
                duplicate[0],
                TableNameDto::try_from("table_foo")?.try_into()?
            );
        } else {
            panic!("Unexpected error: {:?}", err);
        }
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_register_table_dependency(db: TdPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

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

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name.as_str())?)
                    .build()?,
                create.clone(),
            )
            .in_project(ProjectId::sandbox());

        let service = RegisterFunctionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;

        assert_register(&db, &UserId::admin(), &collection, &create, &response).await
    }

    #[td_test::test(db)]
    async fn test_register_implicit_trigger_none(db: TdPool) -> Result<(), TdError> {
        test_register_trigger(db, None).await
    }

    #[td_test::test(db)]
    async fn test_register_implicit_trigger_some_empty(db: TdPool) -> Result<(), TdError> {
        let test_triggers = Some(vec![]);
        test_register_trigger(db, test_triggers).await
    }

    #[td_test::test(db)]
    async fn test_register_implicit_trigger_some(db: TdPool) -> Result<(), TdError> {
        let test_triggers = Some(vec![
            TableTriggerDto::try_from("table_1")?,
            TableTriggerDto::try_from("table_2")?,
        ]);
        test_register_trigger(db, test_triggers).await
    }

    async fn test_register_trigger(
        db: TdPool,
        test_triggers: Option<Vec<TableTriggerDto>>,
    ) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

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

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name.as_str())?)
                    .build()?,
                create.clone(),
            )
            .in_project(ProjectId::sandbox());

        let service = RegisterFunctionService::with_defaults(db.clone()).service();
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

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name.as_str())?)
                    .build()?,
                create.clone(),
            )
            .in_project(ProjectId::sandbox());

        let service = RegisterFunctionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;

        assert_register(&db, &UserId::admin(), &collection, &create, &response).await
    }

    #[td_test::test(db)]
    async fn test_register_tables_dependencies_triggers(db: TdPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

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

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name.as_str())?)
                    .build()?,
                create.clone(),
            )
            .in_project(ProjectId::sandbox());

        let service = RegisterFunctionService::with_defaults(db.clone()).service();
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

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name.as_str())?)
                    .build()?,
                create.clone(),
            )
            .in_project(ProjectId::sandbox());

        let service = RegisterFunctionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;

        assert_register(&db, &UserId::admin(), &collection, &create, &response).await
    }

    #[td_test::test(db)]
    async fn test_register_tables_dependencies_different_collections_permissions_ok(
        db: TdPool,
    ) -> Result<(), TdError> {
        test_register_tables_dependencies_triggers_different_collections(db, true, false, true)
            .await
    }

    #[td_test::test(db)]
    async fn test_register_tables_dependencies_different_collections_permissions_forbidden(
        db: TdPool,
    ) -> Result<(), TdError> {
        test_register_tables_dependencies_triggers_different_collections(db, true, false, false)
            .await
    }

    #[td_test::test(db)]
    async fn test_register_tables_triggers_different_collections_with_tr_permissions_ok(
        db: TdPool,
    ) -> Result<(), TdError> {
        test_register_tables_dependencies_triggers_different_collections(db, false, true, true)
            .await
    }

    #[td_test::test(db)]
    async fn test_register_tables_triggers_different_collections_with_tr_permissions_forbidden(
        db: TdPool,
    ) -> Result<(), TdError> {
        test_register_tables_dependencies_triggers_different_collections(db, false, true, false)
            .await
    }

    async fn test_register_tables_dependencies_triggers_different_collections(
        db: TdPool,
        deps_diff_collection: bool,
        triggers_diff_collection: bool,
        with_permission: bool,
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

        if with_permission {
            db.seed()
                .inter_collection_permission(
                    &collection_1.id,
                    &ToCollectionId::try_from(collection_2.id)?,
                )
                .await?;
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

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name_1.as_str())?)
                    .build()?,
                create.clone(),
            )
            .in_project(ProjectId::sandbox());

        let service = RegisterFunctionService::with_defaults(db.clone()).service();
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

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name_2.as_str())?)
                    .build()?,
                create.clone(),
            )
            .in_project(ProjectId::sandbox());

        let service = RegisterFunctionService::with_defaults(db.clone()).service();
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
            TableDependencyDto::try_from(format!("{deps_collection}/table_1"))?,
            TableDependencyDto::try_from(format!("{deps_collection}/table_2"))?,
            TableDependencyDto::try_from("collection_2/output_1")?,
            TableDependencyDto::try_from("output_2")?,
        ]);
        let triggers = Some(vec![
            TableTriggerDto::try_from("collection_1/table_1")?,
            TableTriggerDto::try_from(format!("{triggers_collection}/table_2"))?,
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

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name_2.as_str())?)
                    .build()?,
                create.clone(),
            )
            .in_project(ProjectId::sandbox());

        let service = RegisterFunctionService::with_defaults(db.clone()).service();
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

    #[td_test::test(db)]
    async fn test_register_same_name(db: TdPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let _ = db
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

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name.as_str())?)
                    .build()?,
                create.clone(),
            )
            .in_project(ProjectId::sandbox());

        let service = RegisterFunctionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request.clone()).await;
        let _ = response?;

        let service = RegisterFunctionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response;
        let err = response.err().unwrap();
        assert!(matches!(
            err.domain_err(),
            SqlError::EntityAlreadyExists(_, _)
        ));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_register_private_tables_all_same_collection(db: TdPool) -> Result<(), TdError> {
        test_private_tables(db, false, false).await
    }

    #[td_test::test(db)]
    async fn test_register_private_tables_deps_diff_collection(db: TdPool) -> Result<(), TdError> {
        test_private_tables(db, true, false).await
    }

    #[td_test::test(db)]
    async fn test_register_private_tables_triggers_diff_collection(
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

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name_2.as_str())?)
                    .build()?,
                create.clone(),
            )
            .in_project(ProjectId::sandbox());

        let service = RegisterFunctionService::with_defaults(db.clone()).service();
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
        db.seed()
            .collection(&collection_name_1, &UserId::admin())
            .await?;

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

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name_1.as_str())?)
                    .build()?,
                create,
            )
            .in_project(ProjectId::sandbox());

        let service = RegisterFunctionService::with_defaults(db.clone()).service();
        let res = service.raw_oneshot(request).await;
        assert!(res.is_ok());

        let tables: Vec<TableDB> = db.live().fetch_all::<TableDB>(&(res.unwrap().id)).await?;
        let map: HashMap<String, TableDB> = tables
            .into_iter()
            .map(|table| (table.name.to_string(), table))
            .collect();
        assert!(!*map.get("table0").unwrap().private);
        assert!(*map.get("_table0").unwrap().private);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_dependency_fixed_validation(db: TdPool) -> Result<(), TdError> {
        let collection_name_1 = CollectionName::try_from("collection_1")?;
        db.seed()
            .collection(&collection_name_1, &UserId::admin())
            .await?;

        let bundle_id = BundleId::default();
        let create = FunctionRegister::builder()
            .try_name("function")?
            .try_description("description")?
            .bundle_id(bundle_id)
            .try_snippet("snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(Some(vec![TableDependencyDto::try_from(
                "table_0@00000000000000000000000004",
            )?]))
            .triggers(Some(vec![]))
            .tables(Some(vec![TableNameDto::try_from("table_0")?]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name_1.as_str())?)
                    .build()?,
                create,
            )
            .in_project(ProjectId::sandbox());

        let service = RegisterFunctionService::with_defaults(db.clone()).service();
        let res = service.raw_oneshot(request).await;
        assert!(res.is_err());
        let err = res.err().unwrap();
        let err = err.domain_err::<VersionResolverError>();
        assert!(matches!(
            err,
            VersionResolverError::FixedTableDataVersionsNotFound(_)
        ));

        let bundle_id = BundleId::default();
        let create = FunctionRegister::builder()
            .try_name("function")?
            .try_description("description")?
            .bundle_id(bundle_id)
            .try_snippet("snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(Some(vec![TableDependencyDto::try_from(
                "table_0@00000000000000000000000004..00000000000000000000000004",
            )?]))
            .triggers(Some(vec![]))
            .tables(Some(vec![TableNameDto::try_from("table_0")?]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name_1.as_str())?)
                    .build()?,
                create,
            )
            .in_project(ProjectId::sandbox());

        let service = RegisterFunctionService::with_defaults(db.clone()).service();
        let res = service.raw_oneshot(request).await;
        assert!(res.is_err());
        let err = res.err().unwrap();
        let err = err.domain_err::<VersionResolverError>();
        assert!(matches!(
            err,
            VersionResolverError::FixedTableDataVersionsNotFound(_)
        ));

        Ok(())
    }

    #[td_test::test(db)]
    async fn test_dependency_range_validation(db: TdPool) -> Result<(), TdError> {
        let collection_name_1 = CollectionName::try_from("collection_1")?;
        db.seed()
            .collection(&collection_name_1, &UserId::admin())
            .await?;

        let bundle_id = BundleId::default();
        let create = FunctionRegister::builder()
            .try_name("function")?
            .try_description("description")?
            .bundle_id(bundle_id)
            .try_snippet("snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(Some(vec![TableDependencyDto::try_from(
                "table_0@HEAD..HEAD~2",
            )?]))
            .triggers(Some(vec![]))
            .tables(Some(vec![TableNameDto::try_from("table_0")?]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name_1.as_str())?)
                    .build()?,
                create,
            )
            .in_project(ProjectId::sandbox());

        let service = RegisterFunctionService::with_defaults(db.clone()).service();
        let res = service.raw_oneshot(request).await;
        assert!(res.is_err());
        let err = res.err().unwrap();
        let err = err.domain_err::<VersionResolverError>();
        assert!(matches!(err, VersionResolverError::InvalidRange(_)));

        Ok(())
    }

    #[td_test::test(db)]
    async fn test_workers_created(db: TdPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("collection")?;
        let collection = db
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let create = FunctionRegister::builder()
            .try_name("function")?
            .try_description("description")?
            .bundle_id(BundleId::default())
            .try_snippet("snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(None)
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .workers(Some(HashMap::from([(
                WorkerTypeDto::CronTrigger,
                WorkerConfig::default(),
            )])))
            .build()?;
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name.as_str())?)
                    .build()?,
                create,
            )
            .in_project(ProjectId::sandbox());
        let service = RegisterFunctionService::with_defaults(db.clone()).service();
        let res = service.raw_oneshot(request).await?;

        let workers: Vec<WorkerDB> = db.live().fetch_all::<WorkerDB>(&res.id).await.unwrap();
        assert_eq!(workers.len(), 1);
        let worker = &workers[0];
        assert_eq!(worker.collection_id, collection.id);
        assert_eq!(worker.execution_id, None);
        assert_eq!(worker.transaction_id, None);
        assert_eq!(worker.physical_function_run_id, None);
        assert_eq!(worker.function_version_id, res.id);
        assert_eq!(worker.message_status, WorkerMessageStatus::None);
        assert_eq!(worker.started_on, None);
        assert_eq!(worker.ended_on, None);
        assert_eq!(worker.status, WorkerStatus::Scheduled);
        assert_eq!(worker.worker_type, WorkerType::CronTrigger);
        assert_eq!(worker.worker_config, Some(WorkerConfig::default()));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_multiple_workers_error(db: TdPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("collection")?;
        db.seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let create = FunctionRegister::builder()
            .try_name("function")?
            .try_description("description")?
            .bundle_id(BundleId::default())
            .try_snippet("snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(None)
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .workers(Some(HashMap::from([
                (WorkerTypeDto::CronTrigger, WorkerConfig::default()),
                (WorkerTypeDto::StageTrigger, WorkerConfig::default()),
            ])))
            .build()?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name.as_str())?)
                    .build()?,
                create,
            )
            .in_project(ProjectId::sandbox());

        let service = RegisterFunctionService::with_defaults(db.clone()).service();
        let res = service.raw_oneshot(request).await;
        assert!(res.is_err());
        let err = res.err().unwrap();
        assert!(format!("{err}").contains("Multiple workers are not supported."));
        Ok(())
    }
}
