//
// Copyright 2026 Tabs Data Inc.
//

use crate::function::layers::enable::{update_function_db_builder, update_table_db_builder};
use crate::function::layers::register_workers;
use crate::scheduler::layers::request::notify_scheduler;
use std::collections::HashMap;
use ta_services::factory::service_factory;
use td_authz::tower::{AuthzOn, CollAdmin, CollDev};
use td_authz::{Authz, AuthzContext};
use td_execution::SchedulerNotify;
use td_objects::And;
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::crudl::{RequestContext, UpdateRequest};
use td_objects::dxo::function::{
    Function, FunctionBuilder, FunctionDB, FunctionDBBuilder, FunctionDBWithNames, FunctionEnable,
};
use td_objects::dxo::table::{TableDB, TableDBBuilder};
use td_objects::rest_urls::FunctionParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{
    CollectionId, CollectionIdName, FunctionIdName, FunctionVersionId, WorkerConfig, WorkerTypeDto,
};
use td_queries::By;
use td_queries::{insert, insert_vec};
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = EnableFunctionService,
    request = UpdateRequest<FunctionParam, FunctionEnable>,
    response = Function,
    connection = TransactionProvider,
    scope = "Project",
    context = AuthzContext,
    context = SchedulerNotify,
)]
fn service() {
    layers!(
        from_fn(With::<UpdateRequest<FunctionParam, FunctionEnable>>::extract::<RequestContext>),
        from_fn(
            With::<UpdateRequest<FunctionParam, FunctionEnable>>::extract_name::<FunctionParam>
        ),
        from_fn(
            With::<UpdateRequest<FunctionParam, FunctionEnable>>::extract_data::<FunctionEnable>
        ),
        // Extract collection and current function from request.
        from_fn(With::<FunctionParam>::extract::<CollectionIdName>),
        from_fn(With::<FunctionParam>::extract::<FunctionIdName>),
        // Get collection. Extract collection id and name.
        from_fn(By::<CollectionIdName>::fetch_one::<CollectionDB>),
        from_fn(With::<CollectionDB>::extract::<CollectionId>),
        // check requester is coll_admin or coll_dev for the function's collection
        from_fn(AuthzOn::<CollectionId>::set),
        from_fn(Authz::<CollAdmin, CollDev>::check),
        // Get function. Extract function id and name.
        from_fn(
            By::<And![CollectionIdName, FunctionIdName]>::fetch_one_current::<
                { FunctionDBWithNames::Available },
                FunctionDBWithNames,
            >
        ),
        // This is, before update function id and function version id. Function id does
        // not change, but function version id does.
        from_fn(With::<FunctionDBWithNames>::extract::<FunctionVersionId>),
        // Re-register function with new enabled flag.
        from_fn(By::<FunctionVersionId>::fetch_one::<FunctionDB>),
        from_fn(With::<FunctionDB>::try_convert_to::<FunctionDBBuilder, _>),
        from_fn(update_function_db_builder),
        from_fn(With::<RequestContext>::update::<FunctionDBBuilder, _>),
        from_fn(With::<FunctionEnable>::update::<FunctionDBBuilder, _>),
        from_fn(With::<FunctionDBBuilder>::build::<FunctionDB>),
        from_fn(insert::<FunctionDB>),
        // Re-register tables (dependencies and triggers remain valid)
        from_fn(By::<FunctionVersionId>::fetch_all::<TableDB>),
        from_fn(With::<TableDB>::vec_convert_to::<TableDBBuilder, _>),
        from_fn(update_table_db_builder),
        from_fn(With::<RequestContext>::update::<Vec<TableDBBuilder>, _>),
        from_fn(With::<Vec<TableDBBuilder>>::build::<Vec<TableDB>>),
        from_fn(insert_vec::<TableDB>),
        // Re-register workers
        from_fn(With::<FunctionVersionId>::some),
        from_fn(With::<Option<HashMap<WorkerTypeDto, WorkerConfig>>>::default),
        register_workers(),
        // Response
        // Extract new function version id
        from_fn(With::<FunctionDB>::extract::<FunctionVersionId>),
        from_fn(By::<FunctionVersionId>::fetch_one::<FunctionDBWithNames>),
        from_fn(With::<FunctionDBWithNames>::try_convert_to::<FunctionBuilder, _>),
        from_fn(With::<FunctionBuilder>::build::<Function>),
        // Wake the scheduler so worker state changes (cron Stop/Start)
        // are picked up immediately instead of at the next polling tick.
        from_fn(notify_scheduler),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::str::FromStr;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_database::scoped::InProject;
    use td_error::TdError;
    use td_objects::Buildable;
    use td_objects::dxo::dependency::DependencyDB;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::dxo::trigger::TriggerDB;
    use td_objects::dxo::worker::WorkerDB;
    use td_objects::types::basic::BundleId;
    use td_objects::types::basic::CollectionName;
    use td_objects::types::basic::{
        AccessTokenId, Decorator, Enabled, FunctionRuntimeValues, ProjectId, RoleId, TableNameDto,
        UserId,
    };
    use td_objects::types::composed::{TableDependencyDto, TableTriggerDto};
    use td_queries::test_utils::SeedEntry;
    use td_queries::{CurrentEntry, LiveEntry};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_enable_function(db: TdPool) {
        use crate::function::layers::register::{build_workers, stop_current_workers};
        use td_database::scoped::Scoped;
        use td_objects::dxo::table::TableDBBuilder;
        use td_objects::dxo::worker::{WorkerDB, WorkerDBBuilder};
        use td_queries::insert_vec;
        use td_tower::metadata::type_of_val;

        EnableFunctionService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<Scoped<UpdateRequest<FunctionParam, FunctionEnable>, ProjectId>, Function>(
            &[
                type_of_val(&With::<UpdateRequest<FunctionParam, FunctionEnable>>::extract::<RequestContext>),
                type_of_val(&
                    With::<UpdateRequest<FunctionParam, FunctionEnable>>::extract_name::<FunctionParam>
                ),
                type_of_val(&
                    With::<UpdateRequest<FunctionParam, FunctionEnable>>::extract_data::<FunctionEnable>
                ),
                // Extract collection and current function from request.
                type_of_val(&With::<FunctionParam>::extract::<CollectionIdName>),
                type_of_val(&With::<FunctionParam>::extract::<FunctionIdName>),
                // Get collection. Extract collection id and name.
                type_of_val(&By::<CollectionIdName>::fetch_one::<CollectionDB>),
                type_of_val(&With::<CollectionDB>::extract::<CollectionId>),
                // check requester is coll_admin or coll_dev for the function's collection
                type_of_val(&AuthzOn::<CollectionId>::set),
                type_of_val(&Authz::<CollAdmin, CollDev>::check),
                // Get function. Extract function id and name.
                type_of_val(&
                    By::<And![CollectionIdName, FunctionIdName]>::fetch_one_current::<
                        { FunctionDBWithNames::Available },
                        FunctionDBWithNames,
                    >
                ),
                // This is, before update function id and function version id. Function id does
                // not change, but function version id does.
                type_of_val(&With::<FunctionDBWithNames>::extract::<FunctionVersionId>),
                // Re-register function with new enabled flag.
                type_of_val(&By::<FunctionVersionId>::fetch_one::<FunctionDB>),
                type_of_val(&With::<FunctionDB>::try_convert_to::<FunctionDBBuilder, _>),
                type_of_val(&update_function_db_builder),
                type_of_val(&With::<RequestContext>::update::<FunctionDBBuilder, _>),
                type_of_val(&With::<FunctionEnable>::update::<FunctionDBBuilder, _>),
                type_of_val(&With::<FunctionDBBuilder>::build::<FunctionDB>),
                type_of_val(&insert::<FunctionDB>),
                // Re-register tables (dependencies and triggers remain valid)
                type_of_val(&By::<FunctionVersionId>::fetch_all::<TableDB>),
                type_of_val(&With::<TableDB>::vec_convert_to::<TableDBBuilder, _>),
                type_of_val(&update_table_db_builder),
                type_of_val(&With::<RequestContext>::update::<Vec<TableDBBuilder>, _>),
                type_of_val(&With::<Vec<TableDBBuilder>>::build::<Vec<TableDB>>),
                type_of_val(&insert_vec::<TableDB>),
                // Re-register workers
                type_of_val(&With::<FunctionVersionId>::some),
                type_of_val(&With::<Option<HashMap<WorkerTypeDto, WorkerConfig>>>::default),
                // Set stopped for current workers and insert into workers(sql) new workers.
                type_of_val(&stop_current_workers),
                type_of_val(&With::<FunctionDB>::try_convert_to::<WorkerDBBuilder, _>),
                type_of_val(&With::<ProjectId>::set::<WorkerDBBuilder>),
                type_of_val(&build_workers),
                type_of_val(&insert_vec::<WorkerDB>),
                // Response
                // Extract new function version id
                type_of_val(&With::<FunctionDB>::extract::<FunctionVersionId>),
                type_of_val(&By::<FunctionVersionId>::fetch_one::<FunctionDBWithNames>),
                type_of_val(&With::<FunctionDBWithNames>::try_convert_to::<FunctionBuilder, _>),
                type_of_val(&With::<FunctionBuilder>::build::<Function>),
                type_of_val(&notify_scheduler),
            ],
        );
    }

    #[td_test::test(db)]
    async fn test_enable_enabled_empty_function(db: TdPool) -> Result<(), TdError> {
        test_enable_function(db, None, None, None, None, Enabled::TRUE, Enabled::TRUE).await
    }

    #[td_test::test(db)]
    async fn test_enable_disabled_empty_function(db: TdPool) -> Result<(), TdError> {
        test_enable_function(db, None, None, None, None, Enabled::FALSE, Enabled::TRUE).await
    }

    #[td_test::test(db)]
    async fn test_disable_enabled_empty_function(db: TdPool) -> Result<(), TdError> {
        test_enable_function(db, None, None, None, None, Enabled::TRUE, Enabled::FALSE).await
    }

    #[td_test::test(db)]
    async fn test_disable_disabled_empty_function(db: TdPool) -> Result<(), TdError> {
        test_enable_function(db, None, None, None, None, Enabled::FALSE, Enabled::FALSE).await
    }

    #[td_test::test(db)]
    async fn test_enable_disabled(db: TdPool) -> Result<(), TdError> {
        test_enable_function(
            db,
            Some(vec![
                TableNameDto::try_from("foo_1")?,
                TableNameDto::try_from("foo_2")?,
            ]),
            Some(vec![
                TableDependencyDto::try_from("dummy_1")?,
                TableDependencyDto::try_from("dummy_2")?,
            ]),
            Some(vec![
                TableTriggerDto::try_from("dummy_1")?,
                TableTriggerDto::try_from("dummy_2")?,
            ]),
            Some(HashMap::from([(
                WorkerTypeDto::CronTrigger,
                WorkerConfig::default(),
            )])),
            Enabled::FALSE,
            Enabled::TRUE,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_disable_enabled(db: TdPool) -> Result<(), TdError> {
        test_enable_function(
            db,
            Some(vec![
                TableNameDto::try_from("foo_1")?,
                TableNameDto::try_from("foo_2")?,
            ]),
            None,
            None,
            None,
            Enabled::FALSE,
            Enabled::TRUE,
        )
        .await
    }

    async fn test_enable_function(
        db: TdPool,
        tables: Option<Vec<TableNameDto>>,
        dependencies: Option<Vec<TableDependencyDto>>,
        triggers: Option<Vec<TableTriggerDto>>,
        workers: Option<HashMap<WorkerTypeDto, WorkerConfig>>,
        enabled: Enabled,
        enabled_update: Enabled,
    ) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(ProjectId::sandbox())
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        // dummy to allow for triggers and dependencies to exist
        let dummy = FunctionRegister::builder()
            .try_name("dummy_foo")?
            .try_description("foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("foo snippet")?
            .decorator(Decorator::Publisher)
            .tables(Some(vec![
                TableNameDto::try_from("dummy_1")?,
                TableNameDto::try_from("dummy_2")?,
            ]))
            .dependencies(None)
            .triggers(None)
            .runtime_values(FunctionRuntimeValues::try_from("foo runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let _dummy_function = db
            .scoped(ProjectId::sandbox())
            .seed()
            .function(&collection, &dummy)
            .await?;

        let create = FunctionRegister::builder()
            .try_name("foo")?
            .try_description("foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("foo snippet")?
            .decorator(Decorator::Publisher)
            .tables(tables.clone())
            .dependencies(dependencies.clone())
            .triggers(triggers.clone())
            .workers(workers.clone())
            .runtime_values(FunctionRuntimeValues::try_from("foo runtime values")?)
            .reuse_frozen_tables(false)
            .enabled(enabled)
            .build()?;

        let created_function = db
            .scoped(ProjectId::sandbox())
            .seed()
            .function(&collection, &create)
            .await?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                .update(
                    FunctionParam::builder()
                        .collection(CollectionIdName::from_str(&format!("{}", collection.name))?)
                        .function(FunctionIdName::from_str("foo")?)
                        .build()?,
                    FunctionEnable {
                        enabled: enabled_update,
                    },
                )
                .in_project(ProjectId::sandbox());

        let service = EnableFunctionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;

        // Different versions, same function.
        assert_ne!(response.id, created_function.id);
        assert_ne!(response.defined_on, created_function.defined_on);

        assert_eq!(response.collection_id, created_function.collection_id);
        assert_eq!(response.name, created_function.name);
        assert_eq!(response.description, created_function.description);
        assert_eq!(response.decorator, created_function.decorator);
        assert_eq!(response.connector, created_function.connector);
        assert_eq!(response.runtime_values, created_function.runtime_values);
        assert_eq!(response.function_id, created_function.function_id);
        assert_eq!(response.data_location, created_function.data_location);
        assert_eq!(response.storage_version, created_function.storage_version);
        assert_eq!(response.bundle_id, created_function.bundle_id);
        assert_eq!(response.snippet, created_function.snippet);
        assert_eq!(response.status, created_function.status);

        assert_eq!(created_function.enabled, enabled);
        assert_eq!(response.enabled, enabled_update);

        // And assert that the current versions are related to the new one
        let current_function: Vec<FunctionDB> = db
            .current()
            .fetch_all::<FunctionDB>(&response.function_id)
            .await?;
        assert_eq!(current_function.len(), 1);
        let current_function = current_function.first().unwrap();
        assert_eq!(current_function.id, response.id);
        assert_eq!(current_function.enabled, enabled_update);

        let current_tables: Vec<TableDB> =
            db.live().fetch_all::<TableDB>(&current_function.id).await?;
        assert_eq!(current_tables.len(), tables.unwrap_or_default().len());

        let current_dependencies: Vec<DependencyDB> = db
            .current()
            .fetch_all::<DependencyDB>(&current_function.function_id)
            .await?;
        assert_eq!(
            current_dependencies.len(),
            dependencies.unwrap_or_default().len()
        );

        let current_triggers: Vec<TriggerDB> = db
            .current()
            .fetch_all::<TriggerDB>(&current_function.function_id)
            .await?;
        assert_eq!(current_triggers.len(), triggers.unwrap_or_default().len());

        let current_workers: Vec<WorkerDB> = db
            .live()
            .fetch_all::<WorkerDB>(&current_function.id)
            .await?;
        assert_eq!(current_workers.len(), workers.unwrap_or_default().len());

        Ok(())
    }
}
