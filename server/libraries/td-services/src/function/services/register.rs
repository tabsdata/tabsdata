//
// Copyright 2025 Tabsdata Inc.
//

use crate::function::layers::register::{
    bundle_registered, ensure_connection_kind_matches, validate_tables_do_not_exist,
};
use crate::function::layers::{
    check_private_tables, register_dependencies, register_tables, register_triggers,
    register_workers,
};
use crate::scheduler::layers::request::notify_scheduler;
use std::collections::HashMap;
use ta_services::factory::service_factory;
use td_authz::AuthzContext;
use td_authz::rbac::{DevelopProject, Project, Rbac, Scope};
use td_bridge::BridgeProviderHandle;
use td_execution::SchedulerNotify;
use td_objects::And;
use td_objects::dxo::bundle::{BundleDB, BundleRegisteredDB};
use td_objects::dxo::collection::ActiveCollectionDB;
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
    AtTime, BasePath, BundleId, CollectionId, CollectionIdName, CollectionName, Enabled,
    FunctionId, FunctionName, FunctionVersionId, ResetState, ReuseFrozen, StorageVersion,
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
    context = BridgeProviderHandle,
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
        from_fn(By::<CollectionIdName>::fetch_one::<ActiveCollectionDB>),
        from_fn(With::<ActiveCollectionDB>::extract::<CollectionId>),
        // check requester can manage or develop the current project
        from_fn(Scope::<Project>::set),
        from_fn(Rbac::<Project, DevelopProject>::check),
        from_fn(With::<ActiveCollectionDB>::extract::<CollectionName>),
        // Stage/checkpoint storage prefix: the collection's effective base_path (its
        // own, else the project's — already resolved by the collections_active view).
        from_fn(With::<ActiveCollectionDB>::extract::<BasePath>),
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
        // Get storage version.
        from_fn(With::<StorageVersion>::default),
        // Get enabled flag
        from_fn(With::<FunctionRegister>::extract::<Option<Enabled>>),
        from_fn(With::<Option<Enabled>>::unwrap_or_default),
        // Insert into function_versions(sql) status=Active.
        from_fn(With::<FunctionRegister>::try_convert_to::<FunctionDBBuilder, _>),
        from_fn(With::<RequestContext>::update::<FunctionDBBuilder, _>),
        from_fn(With::<CollectionId>::set::<FunctionDBBuilder>),
        from_fn(With::<StorageVersion>::set::<FunctionDBBuilder>),
        // Stamp stage_base_path from the collection base_path (BasePath in context).
        from_fn(With::<BasePath>::set::<FunctionDBBuilder>),
        from_fn(With::<Enabled>::set::<FunctionDBBuilder>),
        from_fn(With::<FunctionDBBuilder>::build::<FunctionDB>),
        // A publisher/subscriber's connector must match the collection's connection kind.
        from_fn(ensure_connection_kind_matches),
        from_fn(insert::<FunctionDB>),
        // Mark the uploaded bundle as Registered (kept as a durable record, not deleted)
        from_fn(With::<FunctionDB>::extract::<BundleId>),
        from_fn(bundle_registered),
        from_fn(By::<BundleId>::update::<BundleRegisteredDB, BundleDB>),
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
        // Register never resets state (fresh tables get new ids anyway).
        from_fn(With::<ResetState>::default),
        // And register new ones
        register_tables(),
        register_dependencies(),
        register_triggers(),
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
    use crate::collection::service::connection::test_support::create_collection;
    use crate::collection::service::update::UpdateCollectionService;
    use crate::function::layers::register::RegisterFunctionError;
    use crate::function::services::tests::assert_register;
    use std::collections::HashMap;
    use std::str::FromStr;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::InProject;
    use td_error::TdError;
    use td_execution::VersionResolverError;
    use td_objects::Buildable;
    use td_objects::dxo::bundle::BundleDB;
    use td_objects::dxo::collection::CollectionUpdate;
    use td_objects::dxo::function::FunctionDB;
    use td_objects::dxo::worker::WorkerDB;
    use td_objects::errors::SqlError;
    use td_objects::types::basic::{
        AccessTokenId, BasePath, BundleStatus, ConnectionKind, Decorator, FunctionName,
        FunctionRuntimeValues, FunctionStatus, GroupName, ProjectId, UserId, WorkerMessageStatus,
        WorkerStatus, WorkerType,
    };
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    // Helper: the current Active version of the function named `name`.
    async fn active_function(db: &TdPool, name: &FunctionName) -> Result<FunctionDB, TdError> {
        let active: Vec<FunctionDB> = db
            .live()
            .fetch_all::<FunctionDB>(&FunctionStatus::Active)
            .await?;
        let function = active
            .into_iter()
            .find(|f| f.name == *name)
            .expect("matching active function exists");
        Ok(function)
    }

    // Helper: move `collection_name` onto a non-default base_path via the
    // collection update service, returning the applied BasePath.
    async fn set_collection_base_path(
        db: &TdPool,
        project_id: ProjectId,
        collection_name: &CollectionName,
        base_path: &str,
    ) -> Result<BasePath, TdError> {
        let base_path = BasePath::try_from(base_path)?;
        let update = CollectionUpdate::builder()
            .name(None)
            .description(None)
            .base_path(Some(base_path.clone()))
            .build()?;
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name.as_str())?)
                    .build()?,
                update,
            )
            .in_project(project_id);
        UpdateCollectionService::with_defaults(db.clone())
            .service()
            .raw_oneshot(request)
            .await?;
        Ok(base_path)
    }

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_register_function(db: TdPool) {
        use crate::function::layers::register::{
            build_dependency_versions, build_table_versions, build_tables_trigger_versions,
            build_trigger_versions, build_workers, stop_current_workers,
        };
        use td_objects::dxo::dependency::{DependencyDB, DependencyDBBuilder};
        use td_objects::dxo::table::{TableDB, TableDBBuilder};
        use td_objects::dxo::trigger::{TriggerDB, TriggerDBBuilder};
        use td_objects::dxo::worker::{WorkerDB, WorkerDBBuilder};
        use td_objects::tower_service::with::With;
        use td_queries::insert_vec;

        use td_database::pool::scoped::Scoped;
        use td_tower::metadata::type_of_val;

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
                type_of_val(&By::<CollectionIdName>::fetch_one::<ActiveCollectionDB>),
                type_of_val(&With::<ActiveCollectionDB>::extract::<CollectionId>),
                // check requester can manage or develop the current project
                type_of_val(&Scope::<Project>::set),
                type_of_val(&Rbac::<Project, DevelopProject>::check),
                type_of_val(&With::<ActiveCollectionDB>::extract::<CollectionName>),
                type_of_val(&With::<ActiveCollectionDB>::extract::<BasePath>),
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
                // Get storage version.
                type_of_val(&With::<StorageVersion>::default),
                // Get enabled flag
                type_of_val(&With::<FunctionRegister>::extract::<Option<Enabled>>),
                type_of_val(&With::<Option<Enabled>>::unwrap_or_default),
                // Insert into function_versions(sql) status=Active.
                type_of_val(&With::<FunctionRegister>::try_convert_to::<FunctionDBBuilder, _>),
                type_of_val(&With::<RequestContext>::update::<FunctionDBBuilder, _>),
                type_of_val(&With::<CollectionId>::set::<FunctionDBBuilder>),
                type_of_val(&With::<StorageVersion>::set::<FunctionDBBuilder>),
                type_of_val(&With::<BasePath>::set::<FunctionDBBuilder>),
                type_of_val(&With::<Enabled>::set::<FunctionDBBuilder>),
                type_of_val(&With::<FunctionDBBuilder>::build::<FunctionDB>),
                type_of_val(&ensure_connection_kind_matches),
                type_of_val(&insert::<FunctionDB>),
                // Mark the uploaded bundle as Registered (kept, not deleted)
                type_of_val(&With::<FunctionDB>::extract::<BundleId>),
                type_of_val(&bundle_registered),
                type_of_val(&By::<BundleId>::update::<BundleRegisteredDB, BundleDB>),
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
                type_of_val(&With::<ResetState>::default),
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
                type_of_val(&insert_vec::<DependencyDB>),
                // Insert into trigger_versions(sql) current function trigger status=Active.
                type_of_val(&With::<FunctionDB>::try_convert_to::<TriggerDBBuilder, _>),
                type_of_val(&With::<RequestContext>::update::<TriggerDBBuilder, _>),
                type_of_val(&build_trigger_versions),
                type_of_val(&insert_vec::<TriggerDB>),
                // Set stopped for current workers and insert into workers(sql) new workers.
                type_of_val(&stop_current_workers),
                type_of_val(&With::<FunctionDB>::try_convert_to::<WorkerDBBuilder, _>),
                type_of_val(&With::<ProjectId>::set::<WorkerDBBuilder>),
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
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
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
            .decorator(Decorator::Transformer)
            .dependencies(dependencies.clone())
            .triggers(triggers.clone())
            .tables(tables.clone())
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name.as_str())?)
                    .build()?,
                create.clone(),
            )
            .in_project(p.id);

        let service = RegisterFunctionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;

        assert_register(&db, &UserId::admin(), &collection, &create, &response).await
    }

    #[td_test::test(db)]
    async fn test_register_empty_vec(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
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
            .decorator(Decorator::Transformer)
            .dependencies(dependencies.clone())
            .triggers(triggers.clone())
            .tables(tables.clone())
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name.as_str())?)
                    .build()?,
                create.clone(),
            )
            .in_project(p.id);

        let service = RegisterFunctionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;

        assert_register(&db, &UserId::admin(), &collection, &create, &response).await
    }

    #[td_test::test(db)]
    async fn test_register_table_output(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
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
            .decorator(Decorator::Transformer)
            .dependencies(dependencies.clone())
            .triggers(triggers.clone())
            .tables(tables.clone())
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name.as_str())?)
                    .build()?,
                create.clone(),
            )
            .in_project(p.id);

        let service = RegisterFunctionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;

        assert_register(&db, &UserId::admin(), &collection, &create, &response).await
    }

    #[td_test::test(db)]
    async fn test_register_table_output_duplicate_output_tables_err(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let _collection = db
            .scoped(p.id)
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
            .decorator(Decorator::Transformer)
            .dependencies(dependencies.clone())
            .triggers(triggers.clone())
            .tables(tables.clone())
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name.as_str())?)
                    .build()?,
                create.clone(),
            )
            .in_project(p.id);

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
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
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
            .decorator(Decorator::Transformer)
            .dependencies(dependencies.clone())
            .triggers(triggers.clone())
            .tables(tables.clone())
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name.as_str())?)
                    .build()?,
                create.clone(),
            )
            .in_project(p.id);

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
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
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
            .decorator(Decorator::Transformer)
            .dependencies(dependencies.clone())
            .triggers(triggers.clone())
            .tables(tables.clone())
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name.as_str())?)
                    .build()?,
                create.clone(),
            )
            .in_project(p.id);

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

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name.as_str())?)
                    .build()?,
                create.clone(),
            )
            .in_project(p.id);

        let service = RegisterFunctionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;

        assert_register(&db, &UserId::admin(), &collection, &create, &response).await
    }

    #[td_test::test(db)]
    async fn test_register_tables_dependencies_triggers(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
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
            .decorator(Decorator::Transformer)
            .dependencies(dependencies.clone())
            .triggers(triggers.clone())
            .tables(tables.clone())
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name.as_str())?)
                    .build()?,
                create.clone(),
            )
            .in_project(p.id);

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

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name.as_str())?)
                    .build()?,
                create.clone(),
            )
            .in_project(p.id);

        let service = RegisterFunctionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;

        assert_register(&db, &UserId::admin(), &collection, &create, &response).await
    }

    #[td_test::test(db)]
    async fn test_register_tables_dependencies_different_collections(
        db: TdPool,
    ) -> Result<(), TdError> {
        test_register_tables_dependencies_triggers_different_collections(db, true, false).await
    }

    #[td_test::test(db)]
    async fn test_register_tables_triggers_different_collections(
        db: TdPool,
    ) -> Result<(), TdError> {
        test_register_tables_dependencies_triggers_different_collections(db, false, true).await
    }

    // Within a project there is no cross-collection permission: every
    // collection's public (non-private) tables are usable from any function in
    // the project. Referencing another collection's public tables as
    // dependencies/triggers always succeeds.
    async fn test_register_tables_dependencies_triggers_different_collections(
        db: TdPool,
        deps_diff_collection: bool,
        triggers_diff_collection: bool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name_1 = CollectionName::try_from("collection_1")?;
        let _collection_1 = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name_1, &UserId::admin())
            .await?;
        let collection_name_2 = CollectionName::try_from("collection_2")?;
        let collection_2 = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name_2, &UserId::admin())
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
            .decorator(Decorator::Transformer)
            .dependencies(dependencies.clone())
            .triggers(triggers.clone())
            .tables(tables.clone())
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name_1.as_str())?)
                    .build()?,
                create.clone(),
            )
            .in_project(p.id);

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
            .decorator(Decorator::Transformer)
            .dependencies(dependencies.clone())
            .triggers(triggers.clone())
            .tables(tables.clone())
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name_2.as_str())?)
                    .build()?,
                create.clone(),
            )
            .in_project(p.id);

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
            .decorator(Decorator::Transformer)
            .dependencies(dependencies.clone())
            .triggers(triggers.clone())
            .tables(tables.clone())
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name_2.as_str())?)
                    .build()?,
                create.clone(),
            )
            .in_project(p.id);

        let service = RegisterFunctionService::with_defaults(db.clone()).service();
        let res = service.raw_oneshot(request).await;

        assert_register(&db, &UserId::admin(), &collection_2, &create, &res?).await?;
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_register_same_name(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let _ = db
            .scoped(p.id)
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
            .decorator(Decorator::Transformer)
            .dependencies(dependencies.clone())
            .triggers(triggers.clone())
            .tables(tables.clone())
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name.as_str())?)
                    .build()?,
                create.clone(),
            )
            .in_project(p.id);

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
        let p = db.seed().test_project().await?;
        let collection_name_1 = CollectionName::try_from("collection_1")?;
        let collection_1 = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name_1, &UserId::admin())
            .await?;
        let collection_name_2 = CollectionName::try_from("collection_2")?;
        let collection_2 = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name_2, &UserId::admin())
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
                .decorator(Decorator::Transformer)
                .dependencies(dependencies.clone())
                .triggers(triggers.clone())
                .tables(tables.clone())
                .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
                .reuse_frozen_tables(false)
                .build()?;
            db.scoped(p.id).seed().function(collection, &create).await?;
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
            .decorator(Decorator::Transformer)
            .dependencies(dependencies)
            .triggers(triggers)
            .tables(tables)
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name_2.as_str())?)
                    .build()?,
                create.clone(),
            )
            .in_project(p.id);

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
        let p = db.seed().test_project().await?;
        let collection_name_1 = CollectionName::try_from("collection_1")?;
        db.scoped(p.id)
            .seed()
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
            .decorator(Decorator::Transformer)
            .dependencies(Some(vec![]))
            .triggers(Some(vec![]))
            .tables(tables)
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name_1.as_str())?)
                    .build()?,
                create,
            )
            .in_project(p.id);

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
        let p = db.seed().test_project().await?;
        let collection_name_1 = CollectionName::try_from("collection_1")?;
        db.scoped(p.id)
            .seed()
            .collection(&collection_name_1, &UserId::admin())
            .await?;

        let bundle_id = BundleId::default();
        let create = FunctionRegister::builder()
            .try_name("function")?
            .try_description("description")?
            .bundle_id(bundle_id)
            .try_snippet("snippet")?
            .decorator(Decorator::Transformer)
            .dependencies(Some(vec![TableDependencyDto::try_from(
                "table_0@00000000000000000000000004",
            )?]))
            .triggers(Some(vec![]))
            .tables(Some(vec![TableNameDto::try_from("table_0")?]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name_1.as_str())?)
                    .build()?,
                create,
            )
            .in_project(p.id);

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
            .decorator(Decorator::Transformer)
            .dependencies(Some(vec![TableDependencyDto::try_from(
                "table_0@00000000000000000000000004..00000000000000000000000004",
            )?]))
            .triggers(Some(vec![]))
            .tables(Some(vec![TableNameDto::try_from("table_0")?]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name_1.as_str())?)
                    .build()?,
                create,
            )
            .in_project(p.id);

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
        let p = db.seed().test_project().await?;
        let collection_name_1 = CollectionName::try_from("collection_1")?;
        db.scoped(p.id)
            .seed()
            .collection(&collection_name_1, &UserId::admin())
            .await?;

        let bundle_id = BundleId::default();
        let create = FunctionRegister::builder()
            .try_name("function")?
            .try_description("description")?
            .bundle_id(bundle_id)
            .try_snippet("snippet")?
            .decorator(Decorator::Transformer)
            .dependencies(Some(vec![TableDependencyDto::try_from(
                "table_0@HEAD..HEAD~2",
            )?]))
            .triggers(Some(vec![]))
            .tables(Some(vec![TableNameDto::try_from("table_0")?]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name_1.as_str())?)
                    .build()?,
                create,
            )
            .in_project(p.id);

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
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("collection")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let create = FunctionRegister::builder()
            .try_name("function")?
            .try_description("description")?
            .bundle_id(BundleId::default())
            .try_snippet("snippet")?
            .decorator(Decorator::Transformer)
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
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name.as_str())?)
                    .build()?,
                create,
            )
            .in_project(p.id);
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
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("collection")?;
        db.scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let create = FunctionRegister::builder()
            .try_name("function")?
            .try_description("description")?
            .bundle_id(BundleId::default())
            .try_snippet("snippet")?
            .decorator(Decorator::Transformer)
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

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name.as_str())?)
                    .build()?,
                create,
            )
            .in_project(p.id);

        let service = RegisterFunctionService::with_defaults(db.clone()).service();
        let res = service.raw_oneshot(request).await;
        assert!(res.is_err());
        let err = res.err().unwrap();
        assert!(format!("{err}").contains("Multiple workers are not supported."));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_register_stamps_stage_base_path_from_collection(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("prod_coll")?;
        db.scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        // Move the collection onto a non-default base_path before registering.
        let prod = set_collection_base_path(&db, p.id, &collection_name, "/prod").await?;

        let create = FunctionRegister::builder()
            .try_name("stamped_fn")?
            .try_description("description")?
            .bundle_id(BundleId::default())
            .try_snippet("snippet")?
            .decorator(Decorator::Transformer)
            .dependencies(None)
            .triggers(None)
            .tables(None)
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name.as_str())?)
                    .build()?,
                create,
            )
            .in_project(p.id);
        let service = RegisterFunctionService::with_defaults(db.clone()).service();
        service.raw_oneshot(request).await?;

        let function = active_function(&db, &FunctionName::try_from("stamped_fn")?).await?;
        assert_eq!(
            function.stage_base_path, prod,
            "stage_base_path is stamped from the collection base_path at register"
        );
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_register_flips_bundle_to_registered(db: TdPool) -> Result<(), TdError> {
        use crate::Context;
        use crate::function::services::upload::UploadFunctionService;
        use axum::body::Body;
        use axum::extract::Request;
        use ta_services::factory::ServiceFactory;
        use td_objects::dxo::function_upload::FunctionUpload;

        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;
        let context = Context::with_defaults(db.clone());

        // Upload a bundle so a Pending BundleDB row exists for the function to claim.
        let upload_request = Request::builder()
            .body(Body::from(b"PAYLOAD".to_vec()))
            .unwrap();
        let upload = FunctionUpload::new(upload_request);
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name.as_str())?)
                    .build()?,
                upload,
            )
            .in_project(p.id);
        let bundle = UploadFunctionService::build(&context)
            .service()
            .raw_oneshot(request)
            .await?;
        assert_eq!(
            db.live().fetch_all::<BundleDB>(&bundle.id).await?[0].status,
            BundleStatus::Pending,
            "bundle starts Pending right after upload"
        );

        let create = FunctionRegister::builder()
            .try_name("flip_fn")?
            .try_description("description")?
            .bundle_id(bundle.id)
            .try_snippet("snippet")?
            .decorator(Decorator::Transformer)
            .dependencies(None)
            .triggers(None)
            .tables(None)
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name.as_str())?)
                    .build()?,
                create,
            )
            .in_project(p.id);
        let service = RegisterFunctionService::with_defaults(db.clone()).service();
        service.raw_oneshot(request).await?;

        // The bundle the function claimed is flipped Pending -> Registered.
        let function = active_function(&db, &FunctionName::try_from("flip_fn")?).await?;
        let bundles: Vec<BundleDB> = db.live().fetch_all::<BundleDB>(&function.bundle_id).await?;
        assert_eq!(bundles.len(), 1);
        assert_eq!(bundles[0].status, BundleStatus::Registered);
        assert_eq!(bundles[0].collection_id, collection.id);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_register_base_path_override(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("prod_coll")?;
        db.scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        // Non-default collection base_path so we can tell override from stamping.
        let prod = set_collection_base_path(&db, p.id, &collection_name, "/prod").await?;

        let create = FunctionRegister::builder()
            .try_name("override_fn")?
            .try_description("description")?
            .bundle_id(BundleId::default())
            .try_snippet("snippet")?
            .decorator(Decorator::Transformer)
            .base_path(Some(BasePath::try_from("/fn")?))
            .dependencies(None)
            .triggers(None)
            .tables(None)
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection_name.as_str())?)
                    .build()?,
                create,
            )
            .in_project(p.id);
        let service = RegisterFunctionService::with_defaults(db.clone()).service();
        service.raw_oneshot(request).await?;

        let function = active_function(&db, &FunctionName::try_from("override_fn")?).await?;
        assert_eq!(
            function.base_path,
            Some(BasePath::try_from("/fn")?),
            "base_path override is persisted on the function version"
        );
        assert_eq!(
            function.stage_base_path, prod,
            "stage_base_path still comes from the collection, not the override"
        );
        Ok(())
    }

    // ---- publisher/subscriber connector validation ----

    fn pubsub_register(
        name: &str,
        decorator: Decorator,
        connection: Option<&str>,
    ) -> Result<FunctionRegister, TdError> {
        let mut builder = FunctionRegister::builder();
        builder
            .try_name(name)?
            .try_description("d")?
            .bundle_id(BundleId::default())
            .try_snippet("s")?
            .decorator(decorator)
            .dependencies(None)
            .triggers(None)
            .tables(None)
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false);
        if let Some(connection) = connection {
            builder.connection(Some(ConnectionKind::try_from(connection)?));
        }
        Ok(builder.build()?)
    }

    async fn register_in(
        db: &TdPool,
        project_id: ProjectId,
        collection: &str,
        create: FunctionRegister,
    ) -> Result<Function, TdError> {
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection)?)
                    .build()?,
                create,
            )
            .in_project(project_id);
        RegisterFunctionService::with_defaults(db.clone())
            .service()
            .raw_oneshot(request)
            .await
    }

    /// Like [`register_in`] but runs the service under the Kubernetes
    /// orchestrator (where a publisher/subscriber connection kind is
    /// mandatory) rather than the default local Supervisor.
    async fn register_in_kubernetes(
        db: &TdPool,
        project_id: ProjectId,
        collection: &str,
        create: FunctionRegister,
    ) -> Result<Function, TdError> {
        use std::sync::Arc;
        use ta_services::factory::ServiceFactory;
        use td_bridge::test::TestBridgeProvider;
        use td_bridge::{BridgeProviderHandle, Orchestrator};

        let mut context = crate::Context::with_defaults(db.clone());
        context.bridge = Arc::new(BridgeProviderHandle::new(Arc::new(
            TestBridgeProvider::default().with_orchestrator(Orchestrator::Kubernetes),
        )));
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(collection)?)
                    .build()?,
                create,
            )
            .in_project(project_id);
        RegisterFunctionService::build(&context)
            .service()
            .raw_oneshot(request)
            .await
    }

    #[td_test::test(db)]
    async fn test_register_publisher_matching_connector_ok(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        create_collection(&db, p.id, "src_pub", Some(GroupName::source())).await;
        let create = pubsub_register("pub_fn", Decorator::Publisher, Some("mysqlSrcConn"))?;
        register_in_kubernetes(&db, p.id, "src_pub", create).await?;

        let function = active_function(&db, &FunctionName::try_from("pub_fn")?).await?;
        assert_eq!(function.decorator, Decorator::Publisher);
        assert_eq!(
            function.connection,
            Some(ConnectionKind::try_from("mysqlSrcConn")?)
        );
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_register_subscriber_matching_connector_ok(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        create_collection(&db, p.id, "dst_sub", Some(GroupName::destination())).await;
        let create = pubsub_register("sub_fn", Decorator::Subscriber, Some("mysqlDestConn"))?;
        register_in_kubernetes(&db, p.id, "dst_sub", create).await?;

        let function = active_function(&db, &FunctionName::try_from("sub_fn")?).await?;
        assert_eq!(function.decorator, Decorator::Subscriber);
        assert_eq!(
            function.connection,
            Some(ConnectionKind::try_from("mysqlDestConn")?)
        );
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_register_publisher_connector_mismatch_err(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        create_collection(&db, p.id, "src_mm", Some(GroupName::source())).await;
        let create = pubsub_register("mm_fn", Decorator::Publisher, Some("postgresSrcConn"))?;
        let err = register_in_kubernetes(&db, p.id, "src_mm", create)
            .await
            .unwrap_err();
        let err = err.domain_err::<RegisterFunctionError>();
        assert!(matches!(
            err,
            RegisterFunctionError::ConnectionKindMismatch(..)
        ));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_register_publisher_requires_connector_err(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        // Under Kubernetes a publisher must declare its connection kind.
        create_collection(&db, p.id, "src_req", Some(GroupName::source())).await;
        let create = pubsub_register("req_fn", Decorator::Publisher, None)?;
        let err = register_in_kubernetes(&db, p.id, "src_req", create)
            .await
            .unwrap_err();
        let err = err.domain_err::<RegisterFunctionError>();
        assert!(matches!(
            err,
            RegisterFunctionError::ConnectionKindRequired(_)
        ));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_register_publisher_without_connection_err(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        // A plain (DataFactory) collection carries no connection.
        let collection_name = CollectionName::try_from("fac_pub")?;
        db.scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;
        let create = pubsub_register("facpub_fn", Decorator::Publisher, Some("mysqlSrcConn"))?;
        let err = register_in_kubernetes(&db, p.id, "fac_pub", create)
            .await
            .unwrap_err();
        let err = err.domain_err::<RegisterFunctionError>();
        assert!(matches!(
            err,
            RegisterFunctionError::CollectionHasNoConnection(_)
        ));
        Ok(())
    }

    // TODO FOR 2.0: remove together with the Supervisor exemption in
    // `ensure_connection_kind_matches`. Pins the 1.x behaviour: under the
    // local Supervisor a publisher registers with no connection kind (the
    // legacy `tabsdata` client never sends one); under Kubernetes the same
    // registration is rejected -- see
    // `test_register_publisher_requires_connector_err`.
    #[td_test::test(db)]
    async fn test_register_publisher_legacy_no_connection_ok(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        // A plain (DataFactory) collection carries no connection.
        let collection_name = CollectionName::try_from("fac_legacy_pub")?;
        db.scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;
        let create = pubsub_register("legacy_pub_fn", Decorator::Publisher, None)?;
        register_in(&db, p.id, "fac_legacy_pub", create).await?;

        let function = active_function(&db, &FunctionName::try_from("legacy_pub_fn")?).await?;
        assert_eq!(function.decorator, Decorator::Publisher);
        assert_eq!(function.connection, None);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_register_transformer_with_connector_err(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        create_collection(&db, p.id, "src_tf", Some(GroupName::source())).await;
        let create = pubsub_register("tf_fn", Decorator::Transformer, Some("mysqlSrcConn"))?;
        let err = register_in_kubernetes(&db, p.id, "src_tf", create)
            .await
            .unwrap_err();
        let err = err.domain_err::<RegisterFunctionError>();
        assert!(matches!(
            err,
            RegisterFunctionError::ConnectionKindNotAllowed(_)
        ));
        Ok(())
    }
}
