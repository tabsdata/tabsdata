//
// Copyright 2025 Tabs Data Inc.
//

use crate::function::layers::read::{map_create_worker, vec_create_table_dependency};
use std::collections::HashMap;
use ta_services::factory::service_factory;
use td_authz::tower::{AuthzOn, CollAdmin, CollDev, CollExec, CollRead};
use td_authz::{Authz, AuthzContext};
use td_objects::And;
use td_objects::dxo::crudl::{ReadRequest, RequestContext};
use td_objects::dxo::dependency::DependencyDBWithNames;
use td_objects::dxo::function::{
    Function, FunctionBuilder, FunctionDBWithNames, FunctionWithTables, FunctionWithTablesBuilder,
};
use td_objects::dxo::table::TableDBWithNames;
use td_objects::dxo::trigger::TriggerDBWithNames;
use td_objects::dxo::worker::WorkerDBWithNames;
use td_objects::rest_urls::FunctionParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{
    AtTime, CollectionId, CollectionIdName, FunctionId, FunctionIdName, FunctionVersionId, TableId,
    TableName, WorkerId, WorkerTypeDto,
};
use td_objects::types::composed::{TableDependency, TableTrigger};
use td_queries::By;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = ReadFunctionService,
    request = ReadRequest<FunctionParam>,
    response = FunctionWithTables,
    connection = ConnectionProvider,
    scope = "Project",
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<ReadRequest<FunctionParam>>::extract::<RequestContext>),
        from_fn(With::<ReadRequest<FunctionParam>>::extract_name::<FunctionParam>),
        // Extract from request.
        from_fn(With::<FunctionParam>::extract::<CollectionIdName>),
        from_fn(With::<FunctionParam>::extract::<FunctionIdName>),
        // Read function version
        from_fn(With::<RequestContext>::extract::<AtTime>),
        from_fn(
            By::<And![CollectionIdName, FunctionIdName]>::historic_fetch_one::<
                { FunctionDBWithNames::Available },
                FunctionDBWithNames,
            >
        ),
        // check requester is coll_admin or coll_dev for the function's collection
        from_fn(With::<FunctionDBWithNames>::extract::<CollectionId>),
        from_fn(AuthzOn::<CollectionId>::set),
        from_fn(Authz::<CollAdmin, CollDev, CollExec, CollRead>::check),
        // Read function with tables, triggers and dependencies
        from_fn(With::<FunctionDBWithNames>::extract::<FunctionId>),
        from_fn(With::<FunctionDBWithNames>::extract::<FunctionVersionId>),
        // Convert to function read
        from_fn(With::<FunctionDBWithNames>::try_convert_to::<FunctionBuilder, _>),
        from_fn(With::<FunctionBuilder>::build::<Function>),
        from_fn(With::<Function>::try_convert_to::<FunctionWithTablesBuilder, _>),
        // Read tables
        from_fn(
            By::<FunctionId>::historic_fetch_all::<{ TableDBWithNames::User }, TableDBWithNames>
        ),
        from_fn(With::<TableDBWithNames>::vec_convert_to::<TableName, _>),
        from_fn(With::<Vec<TableName>>::set::<FunctionWithTablesBuilder>),
        // Read triggers and dependencies
        // Triggers
        from_fn(
            By::<FunctionId>::historic_fetch_all::<
                { TriggerDBWithNames::Active },
                TriggerDBWithNames,
            >
        ),
        from_fn(With::<Vec<TriggerDBWithNames>>::extract::<Vec<TableId>>),
        from_fn(
            By::<Vec<TableId>>::historic_fetch_all_chunked::<
                { TableDBWithNames::Available },
                TableDBWithNames,
            >
        ),
        from_fn(With::<TableDBWithNames>::vec_convert_to::<TableTrigger, _>),
        from_fn(With::<Vec<TableTrigger>>::set::<FunctionWithTablesBuilder>),
        // Dependencies
        from_fn(
            By::<FunctionId>::historic_fetch_all::<
                { DependencyDBWithNames::User },
                DependencyDBWithNames,
            >
        ),
        from_fn(With::<Vec<DependencyDBWithNames>>::extract::<Vec<TableId>>),
        from_fn(
            By::<Vec<TableId>>::historic_fetch_all_chunked::<
                { TableDBWithNames::Available },
                TableDBWithNames,
            >
        ),
        from_fn(vec_create_table_dependency),
        from_fn(With::<Vec<TableDependency>>::set::<FunctionWithTablesBuilder>),
        // Workers
        from_fn(
            By::<FunctionVersionId>::fetch_all_in::<
                { WorkerDBWithNames::EnabledTriggers },
                WorkerDBWithNames,
            >
        ),
        from_fn(map_create_worker),
        from_fn(With::<HashMap<WorkerTypeDto, WorkerId>>::set::<FunctionWithTablesBuilder>),
        // Build
        from_fn(With::<FunctionWithTablesBuilder>::build::<FunctionWithTables>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_database::scoped::InProject;
    use td_error::TdError;
    use td_objects::Buildable;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, FunctionRuntimeValues, ProjectId,
        RoleId, TableNameDto, UserId, UserName, WorkerConfig,
    };
    use td_objects::types::composed::{TableDependency, TableDependencyDto, TableTriggerDto};
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_read_function_version(db: TdPool) {
        use td_database::scoped::Scoped;
        use td_tower::metadata::type_of_val;

        ReadFunctionService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<Scoped<ReadRequest<FunctionParam>, ProjectId>, FunctionWithTables>(
                &[
                    type_of_val(&With::<ReadRequest<FunctionParam>>::extract::<RequestContext>),
                    type_of_val(&With::<ReadRequest<FunctionParam>>::extract_name::<FunctionParam>),
                    // Extract from request.
                    type_of_val(&With::<FunctionParam>::extract::<CollectionIdName>),
                    type_of_val(&With::<FunctionParam>::extract::<FunctionIdName>),
                    // Read function version
                    type_of_val(&With::<RequestContext>::extract::<AtTime>),
                    type_of_val(
                        &By::<And![CollectionIdName, FunctionIdName]>::historic_fetch_one::<
                            { FunctionDBWithNames::Available },
                            FunctionDBWithNames,
                        >,
                    ),
                    // check requester is coll_admin or coll_dev for the function's collection
                    type_of_val(&With::<FunctionDBWithNames>::extract::<CollectionId>),
                    type_of_val(&AuthzOn::<CollectionId>::set),
                    type_of_val(&Authz::<CollAdmin, CollDev, CollExec, CollRead>::check),
                    // Read function with tables, triggers and dependencies
                    type_of_val(&With::<FunctionDBWithNames>::extract::<FunctionId>),
                    type_of_val(&With::<FunctionDBWithNames>::extract::<FunctionVersionId>),
                    // Convert to function read
                    type_of_val(&With::<FunctionDBWithNames>::try_convert_to::<FunctionBuilder, _>),
                    type_of_val(&With::<FunctionBuilder>::build::<Function>),
                    type_of_val(&With::<Function>::try_convert_to::<FunctionWithTablesBuilder, _>),
                    // Read tables
                    type_of_val(
                        &By::<FunctionId>::historic_fetch_all::<
                            { TableDBWithNames::User },
                            TableDBWithNames,
                        >,
                    ),
                    type_of_val(&With::<TableDBWithNames>::vec_convert_to::<TableName, _>),
                    type_of_val(&With::<Vec<TableName>>::set::<FunctionWithTablesBuilder>),
                    // Read triggers and dependencies
                    // Triggers
                    type_of_val(
                        &By::<FunctionId>::historic_fetch_all::<
                            { TriggerDBWithNames::Active },
                            TriggerDBWithNames,
                        >,
                    ),
                    type_of_val(&With::<Vec<TriggerDBWithNames>>::extract::<Vec<TableId>>),
                    type_of_val(
                        &By::<Vec<TableId>>::historic_fetch_all_chunked::<
                            { TableDBWithNames::Available },
                            TableDBWithNames,
                        >,
                    ),
                    type_of_val(&With::<TableDBWithNames>::vec_convert_to::<TableTrigger, _>),
                    type_of_val(&With::<Vec<TableTrigger>>::set::<FunctionWithTablesBuilder>),
                    // Dependencies
                    type_of_val(
                        &By::<FunctionId>::historic_fetch_all::<
                            { DependencyDBWithNames::User },
                            DependencyDBWithNames,
                        >,
                    ),
                    type_of_val(&With::<Vec<DependencyDBWithNames>>::extract::<Vec<TableId>>),
                    type_of_val(
                        &By::<Vec<TableId>>::historic_fetch_all_chunked::<
                            { TableDBWithNames::Available },
                            TableDBWithNames,
                        >,
                    ),
                    type_of_val(&vec_create_table_dependency),
                    type_of_val(&With::<Vec<TableDependency>>::set::<FunctionWithTablesBuilder>),
                    // Workers
                    type_of_val(
                        &By::<FunctionVersionId>::fetch_all_in::<
                            { WorkerDBWithNames::EnabledTriggers },
                            WorkerDBWithNames,
                        >,
                    ),
                    type_of_val(&map_create_worker),
                    type_of_val(
                        &With::<HashMap<WorkerTypeDto, WorkerId>>::set::<FunctionWithTablesBuilder>,
                    ),
                    // Build
                    type_of_val(&With::<FunctionWithTablesBuilder>::build::<FunctionWithTables>),
                ],
            );
    }

    #[td_test::test(db)]
    async fn test_read(db: TdPool) -> Result<(), TdError> {
        let collection = db
            .scoped(ProjectId::sandbox())
            .seed()
            .collection(&CollectionName::try_from("cofnig")?, &UserId::admin())
            .await?;

        let dependencies = Some(vec![TableDependencyDto::try_from("cofnig/table@HEAD~2")?]);
        let triggers = Some(vec![TableTriggerDto::try_from("cofnig/table")?]);
        let tables = Some(vec![TableNameDto::try_from("table")?]);

        let create = FunctionRegister::builder()
            .try_name("joaquin_workout")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(dependencies.clone())
            .triggers(triggers.clone())
            .tables(tables.clone())
            .workers(Some(HashMap::from([(
                WorkerTypeDto::CronTrigger,
                WorkerConfig::default(),
            )])))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let function = db
            .scoped(ProjectId::sandbox())
            .seed()
            .function(&collection, &create)
            .await?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                .read(
                    FunctionParam::builder()
                        .collection(CollectionIdName::from_str(&format!(
                            "~{}",
                            function.collection_id
                        ))?)
                        .function(FunctionIdName::from_str("joaquin_workout")?)
                        .build()?,
                )
                .in_project(ProjectId::sandbox());

        let service = ReadFunctionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;

        assert_eq!(function.id, response.id);
        assert_eq!(function.collection_id, response.collection_id);
        assert_eq!(function.name, response.name);
        assert_eq!(function.description, response.description);
        assert_eq!(function.function_id, response.function_id);
        assert_eq!(function.data_location, response.data_location);
        assert_eq!(function.storage_version, response.storage_version);
        assert_eq!(function.bundle_id, response.bundle_id);
        assert_eq!(function.snippet, response.snippet);
        assert_eq!(function.defined_on, response.defined_on);
        assert_eq!(function.defined_by_id, response.defined_by_id);
        assert_eq!(function.status, response.status);
        assert_eq!(response.collection, CollectionName::try_from("cofnig")?);
        assert_eq!(response.defined_by, UserName::try_from("admin")?);

        assert_eq!(
            response.dependencies,
            dependencies
                .unwrap_or(vec![])
                .into_iter()
                .map(TableDependency::try_from)
                .collect::<Result<Vec<_>, _>>()?
        );
        assert_eq!(
            response.triggers,
            triggers
                .unwrap_or(vec![])
                .into_iter()
                .map(TableTrigger::try_from)
                .collect::<Result<Vec<_>, _>>()?
        );
        assert_eq!(
            response.tables,
            tables
                .unwrap_or(vec![])
                .into_iter()
                .map(TableName::try_from)
                .collect::<Result<Vec<_>, _>>()?
        );
        assert_eq!(
            response.workers.keys().cloned().collect::<Vec<_>>(),
            vec![WorkerTypeDto::CronTrigger]
        );
        Ok(())
    }
}
