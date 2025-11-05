//
// Copyright 2025 Tabs Data Inc.
//

use crate::function::layers::read::vec_create_table_dependency;
use ta_services::factory::service_factory;
use td_authz::{Authz, AuthzContext};
use td_objects::dxo::crudl::{ReadRequest, RequestContext};
use td_objects::dxo::dependency::defs::DependencyDBRead;
use td_objects::dxo::function::defs::{
    Function, FunctionBuilder, FunctionDBWithNames, FunctionWithTables, FunctionWithTablesBuilder,
};
use td_objects::dxo::table::defs::{TableDBRead, TableDBWithNames};
use td_objects::dxo::trigger::defs::TriggerDBWithNames;
use td_objects::rest_urls::FunctionParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{AuthzOn, CollAdmin, CollDev, CollExec, CollRead};
use td_objects::tower_service::from::{
    BuildService, ConvertIntoMapService, ExtractNameService, ExtractService, ExtractVecService,
    SetService, TryIntoService, With, combine,
};
use td_objects::tower_service::sql::{By, SqlFindService, SqlSelectAllService, SqlSelectService};
use td_objects::types::composed::{TableDependency, TableTrigger};
use td_objects::types::id::{CollectionId, FunctionId, TableId};
use td_objects::types::id_name::{CollectionIdName, FunctionIdName};
use td_objects::types::string::TableName;
use td_objects::types::timestamp::AtTime;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = ReadFunctionService,
    request = ReadRequest<FunctionParam>,
    response = FunctionWithTables,
    connection = ConnectionProvider,
    context = DaoQueries,
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<ReadRequest<FunctionParam>>::extract::<RequestContext>),
        from_fn(With::<ReadRequest<FunctionParam>>::extract_name::<FunctionParam>),
        // Extract from request.
        from_fn(With::<FunctionParam>::extract::<CollectionIdName>),
        from_fn(With::<FunctionParam>::extract::<FunctionIdName>),
        from_fn(combine::<CollectionIdName, FunctionIdName>),
        // Read function version
        from_fn(With::<RequestContext>::extract::<AtTime>),
        from_fn(
            By::<(CollectionIdName, FunctionIdName)>::select_version::<
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
        // Convert to function read
        from_fn(With::<FunctionDBWithNames>::convert_to::<FunctionBuilder, _>),
        from_fn(With::<FunctionBuilder>::build::<Function, _>),
        from_fn(With::<Function>::convert_to::<FunctionWithTablesBuilder, _>),
        // Read tables
        from_fn(By::<FunctionId>::select_all_versions::<{ TableDBRead::Active }, TableDBRead>),
        from_fn(With::<TableDBRead>::vec_convert_to::<TableName, _>),
        from_fn(With::<Vec<TableName>>::set::<FunctionWithTablesBuilder>),
        // Read triggers and dependencies
        // Triggers
        from_fn(
            By::<FunctionId>::select_all_versions::<
                { TriggerDBWithNames::Active },
                TriggerDBWithNames,
            >
        ),
        from_fn(With::<TriggerDBWithNames>::extract_vec::<TableId>),
        from_fn(By::<TableId>::find_versions::<{ TableDBWithNames::Available }, TableDBWithNames>),
        from_fn(With::<TableDBWithNames>::vec_convert_to::<TableTrigger, _>),
        from_fn(With::<Vec<TableTrigger>>::set::<FunctionWithTablesBuilder>),
        // Dependencies
        from_fn(
            By::<FunctionId>::select_all_versions::<{ DependencyDBRead::Active }, DependencyDBRead>
        ),
        from_fn(With::<DependencyDBRead>::extract_vec::<TableId>),
        from_fn(By::<TableId>::find_versions::<{ TableDBWithNames::Available }, TableDBWithNames>),
        from_fn(vec_create_table_dependency),
        from_fn(With::<Vec<TableDependency>>::set::<FunctionWithTablesBuilder>),
        // Build
        from_fn(With::<FunctionWithTablesBuilder>::build::<FunctionWithTables, _>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use ta_services::service::TdService;
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_objects::dxo::function::defs::FunctionRegister;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_function::seed_function;
    use td_objects::types::composed::{TableDependency, TableDependencyDto};
    use td_objects::types::id::{AccessTokenId, BundleId, RoleId, UserId};
    use td_objects::types::string::{
        CollectionName, FunctionRuntimeValues, TableNameDto, UserName,
    };
    use td_objects::types::typed_enum::Decorator;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_read_function_version(db: DbPool) {
        use td_tower::metadata::type_of_val;

        ReadFunctionService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<ReadRequest<FunctionParam>, FunctionWithTables>(&[
                type_of_val(&With::<ReadRequest<FunctionParam>>::extract::<RequestContext>),
                type_of_val(&With::<ReadRequest<FunctionParam>>::extract_name::<FunctionParam>),
                // Extract from request.
                type_of_val(&With::<FunctionParam>::extract::<CollectionIdName>),
                type_of_val(&With::<FunctionParam>::extract::<FunctionIdName>),
                type_of_val(&combine::<CollectionIdName, FunctionIdName>),
                // Read function version
                type_of_val(&With::<RequestContext>::extract::<AtTime>),
                type_of_val(
                    &By::<(CollectionIdName, FunctionIdName)>::select_version::<
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
                // Convert to function read
                type_of_val(&With::<FunctionDBWithNames>::convert_to::<FunctionBuilder, _>),
                type_of_val(&With::<FunctionBuilder>::build::<Function, _>),
                type_of_val(&With::<Function>::convert_to::<FunctionWithTablesBuilder, _>),
                // Read tables
                type_of_val(
                    &By::<FunctionId>::select_all_versions::<{ TableDBRead::Active }, TableDBRead>,
                ),
                type_of_val(&With::<TableDBRead>::vec_convert_to::<TableName, _>),
                type_of_val(&With::<Vec<TableName>>::set::<FunctionWithTablesBuilder>),
                // Read triggers and dependencies
                // Triggers
                type_of_val(
                    &By::<FunctionId>::select_all_versions::<
                        { TriggerDBWithNames::Active },
                        TriggerDBWithNames,
                    >,
                ),
                type_of_val(&With::<TriggerDBWithNames>::extract_vec::<TableId>),
                type_of_val(
                    &By::<TableId>::find_versions::<
                        { TableDBWithNames::Available },
                        TableDBWithNames,
                    >,
                ),
                type_of_val(&With::<TableDBWithNames>::vec_convert_to::<TableTrigger, _>),
                type_of_val(&With::<Vec<TableTrigger>>::set::<FunctionWithTablesBuilder>),
                // Dependencies
                type_of_val(
                    &By::<FunctionId>::select_all_versions::<
                        { DependencyDBRead::Active },
                        DependencyDBRead,
                    >,
                ),
                type_of_val(&With::<DependencyDBRead>::extract_vec::<TableId>),
                type_of_val(
                    &By::<TableId>::find_versions::<
                        { TableDBWithNames::Available },
                        TableDBWithNames,
                    >,
                ),
                type_of_val(&vec_create_table_dependency),
                type_of_val(&With::<Vec<TableDependency>>::set::<FunctionWithTablesBuilder>),
                // Build
                type_of_val(&With::<FunctionWithTablesBuilder>::build::<FunctionWithTables, _>),
            ]);
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_read(db: DbPool) -> Result<(), TdError> {
        let collection =
            seed_collection(&db, &CollectionName::try_from("cofnig")?, &UserId::admin()).await;

        let dependencies = Some(vec![TableDependencyDto::try_from("cofnig/table@HEAD~2")?]);
        let triggers = None;
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
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let function = seed_function(&db, &collection, &create).await;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).read(
                FunctionParam::builder()
                    .try_collection(format!("~{}", function.collection_id))?
                    .try_function("joaquin_workout")?
                    .build()?,
            );

        let service = ReadFunctionService::with_defaults(db.clone())
            .service()
            .await;
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
        Ok(())
    }
}
