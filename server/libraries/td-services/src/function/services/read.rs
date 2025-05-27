//
// Copyright 2025 Tabs Data Inc.
//

use std::sync::Arc;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{ReadRequest, RequestContext};
use td_objects::rest_urls::FunctionParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::from::{
    builder, combine, BuildService, ConvertIntoMapService, ExtractNameService, ExtractService,
    SetService, TryIntoService, With,
};
use td_objects::tower_service::sql::{By, SqlSelectAllService, SqlSelectService};
use td_objects::types::basic::{
    AtTime, CollectionIdName, FunctionIdName, FunctionStatus, FunctionVersionId, TableDependency,
    TableName, TableTrigger,
};
use td_objects::types::dependency::DependencyDBWithNames;
use td_objects::types::function::{
    Function, FunctionBuilder, FunctionDBWithNames, FunctionWithTables, FunctionWithTablesBuilder,
};
use td_objects::types::table::TableDBWithNames;
use td_objects::types::trigger::TriggerDBWithNames;
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{ConnectionProvider, SrvCtxProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct ReadFunctionService {
    provider: ServiceProvider<ReadRequest<FunctionParam>, FunctionWithTables, TdError>,
}

impl ReadFunctionService {
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
                from_fn(With::<ReadRequest<FunctionParam>>::extract::<RequestContext>),
                from_fn(With::<ReadRequest<FunctionParam>>::extract_name::<FunctionParam>),

                ConnectionProvider::new(db),

                // Extract from request.
                from_fn(With::<FunctionParam>::extract::<CollectionIdName>),
                from_fn(With::<FunctionParam>::extract::<FunctionIdName>),
                from_fn(combine::<CollectionIdName, FunctionIdName>),

                // Read function version
                from_fn(With::<RequestContext>::extract::<AtTime>),
                from_fn(FunctionStatus::active),
                from_fn(By::<(CollectionIdName, FunctionIdName)>::select_version::<DaoQueries, FunctionDBWithNames>),
                from_fn(With::<FunctionDBWithNames>::extract::<FunctionVersionId>),

                // Read function with tables
                // Builder
                from_fn(builder::<FunctionWithTablesBuilder>),

                // Read function version
                from_fn(By::<FunctionVersionId>::select::<DaoQueries, FunctionDBWithNames>),
                from_fn(With::<FunctionDBWithNames>::convert_to::<FunctionBuilder, _>),
                from_fn(With::<FunctionBuilder>::build::<Function, _>),
                from_fn(With::<Function>::set::<FunctionWithTablesBuilder>),

                // Read tables
                from_fn(By::<FunctionVersionId>::select_all::<DaoQueries, DependencyDBWithNames>),
                from_fn(With::<DependencyDBWithNames>::vec_convert_to::<TableDependency, _>),
                from_fn(With::<Vec<TableDependency>>::set::<FunctionWithTablesBuilder>),

                // Read dependencies
                from_fn(By::<FunctionVersionId>::select_all::<DaoQueries, TriggerDBWithNames>),
                from_fn(With::<TriggerDBWithNames>::vec_convert_to::<TableTrigger, _>),
                from_fn(With::<Vec<TableTrigger>>::set::<FunctionWithTablesBuilder>),

                // Read triggers
                from_fn(By::<FunctionVersionId>::select_all::<DaoQueries, TableDBWithNames>),
                from_fn(With::<TableDBWithNames>::vec_convert_to::<TableName, _>),
                from_fn(With::<Vec<TableName>>::set::<FunctionWithTablesBuilder>),

                // Build
                from_fn(With::<FunctionWithTablesBuilder>::build::<FunctionWithTables, _>),
            ))
        }
    }

    pub async fn service(
        &self,
    ) -> TdBoxService<ReadRequest<FunctionParam>, FunctionWithTables, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_function::seed_function;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, FunctionRuntimeValues, RoleId,
        TableDependencyDto, TableName, TableNameDto, UserId, UserName,
    };
    use td_objects::types::function::FunctionRegister;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_read_function_version(db: DbPool) {
        use td_tower::metadata::{type_of_val, Metadata};

        let queries = Arc::new(DaoQueries::default());
        let provider = ReadFunctionService::provider(db, queries);
        let service = provider.make().await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();

        metadata.assert_service::<ReadRequest<FunctionParam>, FunctionWithTables>(&[
            type_of_val(&With::<ReadRequest<FunctionParam>>::extract::<RequestContext>),
            type_of_val(&With::<ReadRequest<FunctionParam>>::extract_name::<FunctionParam>),
            // Extract from request.
            type_of_val(&With::<FunctionParam>::extract::<CollectionIdName>),
            type_of_val(&With::<FunctionParam>::extract::<FunctionIdName>),
            type_of_val(&combine::<CollectionIdName, FunctionIdName>),
            // Read function version
            type_of_val(&With::<RequestContext>::extract::<AtTime>),
            type_of_val(&FunctionStatus::active),
            type_of_val(
                &By::<(CollectionIdName, FunctionIdName)>::select_version::<
                    DaoQueries,
                    FunctionDBWithNames,
                >,
            ),
            type_of_val(&With::<FunctionDBWithNames>::extract::<FunctionVersionId>),
            // Read function with tables
            // Builder
            type_of_val(&builder::<FunctionWithTablesBuilder>),
            // Read function version
            type_of_val(&By::<FunctionVersionId>::select::<DaoQueries, FunctionDBWithNames>),
            type_of_val(&With::<FunctionDBWithNames>::convert_to::<FunctionBuilder, _>),
            type_of_val(&With::<FunctionBuilder>::build::<Function, _>),
            type_of_val(&With::<Function>::set::<FunctionWithTablesBuilder>),
            // Read tables
            type_of_val(&By::<FunctionVersionId>::select_all::<DaoQueries, DependencyDBWithNames>),
            type_of_val(&With::<DependencyDBWithNames>::vec_convert_to::<TableDependency, _>),
            type_of_val(&With::<Vec<TableDependency>>::set::<FunctionWithTablesBuilder>),
            // Read dependencies
            type_of_val(&By::<FunctionVersionId>::select_all::<DaoQueries, TriggerDBWithNames>),
            type_of_val(&With::<TriggerDBWithNames>::vec_convert_to::<TableTrigger, _>),
            type_of_val(&With::<Vec<TableTrigger>>::set::<FunctionWithTablesBuilder>),
            // Read triggers
            type_of_val(&By::<FunctionVersionId>::select_all::<DaoQueries, TableDBWithNames>),
            type_of_val(&With::<TableDBWithNames>::vec_convert_to::<TableName, _>),
            type_of_val(&With::<Vec<TableName>>::set::<FunctionWithTablesBuilder>),
            // Build
            type_of_val(&With::<FunctionWithTablesBuilder>::build::<FunctionWithTables, _>),
        ]);
    }

    #[td_test::test(sqlx)]
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

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            true,
        )
        .read(
            FunctionParam::builder()
                .try_collection(format!("~{}", function.collection_id()))?
                .try_function("joaquin_workout")?
                .build()?,
        );

        let service = ReadFunctionService::new(db.clone()).service().await;
        let response = service.raw_oneshot(request).await;
        let response = response?;

        assert_eq!(function.id(), response.function_version().id());
        assert_eq!(
            function.collection_id(),
            response.function_version().collection_id()
        );
        assert_eq!(function.name(), response.function_version().name());
        assert_eq!(
            function.description(),
            response.function_version().description()
        );
        assert_eq!(
            function.function_id(),
            response.function_version().function_id()
        );
        assert_eq!(
            function.data_location(),
            response.function_version().data_location()
        );
        assert_eq!(
            function.storage_version(),
            response.function_version().storage_version()
        );
        assert_eq!(
            function.bundle_id(),
            response.function_version().bundle_id()
        );
        assert_eq!(function.snippet(), response.function_version().snippet());
        assert_eq!(
            function.defined_on(),
            response.function_version().defined_on()
        );
        assert_eq!(
            function.defined_by_id(),
            response.function_version().defined_by_id()
        );
        assert_eq!(function.status(), response.function_version().status());
        assert_eq!(
            *response.function_version().collection(),
            CollectionName::try_from("cofnig")?
        );
        assert_eq!(
            *response.function_version().defined_by(),
            UserName::try_from("admin")?
        );

        assert_eq!(
            *response.dependencies(),
            dependencies
                .unwrap_or(vec![])
                .into_iter()
                .map(TableDependency::try_from)
                .collect::<Result<Vec<_>, _>>()?
        );
        assert_eq!(
            *response.triggers(),
            triggers
                .unwrap_or(vec![])
                .into_iter()
                .map(TableTrigger::try_from)
                .collect::<Result<Vec<_>, _>>()?
        );
        assert_eq!(
            *response.tables(),
            tables
                .unwrap_or(vec![])
                .into_iter()
                .map(TableName::try_from)
                .collect::<Result<Vec<_>, _>>()?
        );
        Ok(())
    }
}
