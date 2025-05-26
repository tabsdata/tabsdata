//
// Copyright 2025 Tabs Data Inc.
//

use crate::function::services::read_version::ReadFunctionVersionService;
use std::sync::Arc;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::ReadRequest;
use td_objects::rest_urls::FunctionParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::from::{
    builder, combine, BuildService, ConvertIntoMapService, ExtractNameService, ExtractService,
    SetService, VecBuildService, With,
};
use td_objects::tower_service::sql::{By, SqlSelectAllService, SqlSelectIdOrNameService};
use td_objects::types::basic::{CollectionIdName, FunctionId, FunctionIdName, FunctionVersionId};
use td_objects::types::function::{
    FunctionDBWithNames, FunctionVersion, FunctionVersionBuilder, FunctionVersionDBWithNames,
    FunctionVersionWithAllVersions, FunctionVersionWithAllVersionsBuilder,
    FunctionVersionWithTables,
};
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{ConnectionProvider, SrvCtxProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct ReadFunctionService {
    provider: ServiceProvider<ReadRequest<FunctionParam>, FunctionVersionWithAllVersions, TdError>,
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
                from_fn(With::<ReadRequest<FunctionParam>>::extract_name::<FunctionParam>),

                ConnectionProvider::new(db),

                // Extract from request.
                from_fn(With::<FunctionParam>::extract::<CollectionIdName>),
                from_fn(With::<FunctionParam>::extract::<FunctionIdName>),
                from_fn(combine::<CollectionIdName, FunctionIdName>),

                // Read function
                from_fn(By::<(CollectionIdName, FunctionIdName)>::select::<DaoQueries, FunctionDBWithNames>),
                from_fn(With::<FunctionDBWithNames>::extract::<FunctionVersionId>),

                // Read function version with tables
                ReadFunctionVersionService::function_version_with_tables_by_id(),

                // Read all versions (without table information)
                from_fn(With::<FunctionDBWithNames>::extract::<FunctionId>),
                from_fn(By::<FunctionId>::select_all::<DaoQueries, FunctionVersionDBWithNames>),
                from_fn(With::<FunctionVersionDBWithNames>::vec_convert_to::<FunctionVersionBuilder, _>),
                from_fn(With::<FunctionVersionBuilder>::vec_build::<FunctionVersion, _>),

                // Response
                from_fn(builder::<FunctionVersionWithAllVersionsBuilder>),
                from_fn(With::<FunctionVersionWithTables>::set::<FunctionVersionWithAllVersionsBuilder>),
                from_fn(With::<Vec<FunctionVersion>>::set::<FunctionVersionWithAllVersionsBuilder>),
                from_fn(With::<FunctionVersionWithAllVersionsBuilder>::build::<FunctionVersionWithAllVersions, _>),
            ))
        }
    }

    pub async fn service(
        &self,
    ) -> TdBoxService<ReadRequest<FunctionParam>, FunctionVersionWithAllVersions, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_objects::crudl::RequestContext;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_function::seed_function;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, FunctionRuntimeValues, RoleId,
        TableDependency, TableName, TableNameDto, TableTrigger, UserId, UserName,
    };
    use td_objects::types::function::FunctionRegister;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_read_function(db: DbPool) {
        use td_objects::tower_service::from::TryIntoService;
        use td_objects::tower_service::sql::SqlSelectService;
        use td_objects::types::basic::{
            FunctionVersionId, TableDependency, TableName, TableTrigger,
        };
        use td_objects::types::dependency::DependencyVersionDBWithNames;
        use td_objects::types::function::FunctionVersionWithTablesBuilder;
        use td_objects::types::table::TableVersionDBWithNames;
        use td_objects::types::trigger::TriggerVersionDBWithNames;

        use td_tower::metadata::{type_of_val, Metadata};

        let queries = Arc::new(DaoQueries::default());
        let provider = ReadFunctionService::provider(db, queries);
        let service = provider.make().await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();

        metadata.assert_service::<ReadRequest<FunctionParam>, FunctionVersionWithAllVersions>(&[
            type_of_val(&With::<ReadRequest<FunctionParam>>::extract_name::<FunctionParam>),

            // Extract from request.
            type_of_val(&With::<FunctionParam>::extract::<CollectionIdName>),
            type_of_val(&With::<FunctionParam>::extract::<FunctionIdName>),
            type_of_val(&combine::<CollectionIdName, FunctionIdName>),

            // Read function
            type_of_val(&By::<(CollectionIdName, FunctionIdName)>::select::<DaoQueries, FunctionDBWithNames>),
            type_of_val(&With::<FunctionDBWithNames>::extract::<FunctionVersionId>),

            // Read function version with tables
            // Builder
            type_of_val(&builder::<FunctionVersionWithTablesBuilder>),

            // Read function version
            type_of_val(&By::<FunctionVersionId>::select::<DaoQueries, FunctionVersionDBWithNames>),
            type_of_val(&With::<FunctionVersionDBWithNames>::convert_to::<FunctionVersionBuilder, _>),
            type_of_val(&With::<FunctionVersionBuilder>::build::<FunctionVersion, _>),
            type_of_val(&With::<FunctionVersion>::set::<FunctionVersionWithTablesBuilder>),

            // Read tables
            type_of_val(&By::<FunctionVersionId>::select_all::<DaoQueries, DependencyVersionDBWithNames>),
            type_of_val(&With::<DependencyVersionDBWithNames>::vec_convert_to::<TableDependency, _>),
            type_of_val(&With::<Vec<TableDependency>>::set::<FunctionVersionWithTablesBuilder>),

            // Read dependencies
            type_of_val(&By::<FunctionVersionId>::select_all::<DaoQueries, TriggerVersionDBWithNames>),
            type_of_val(&With::<TriggerVersionDBWithNames>::vec_convert_to::<TableTrigger, _>),
            type_of_val(&With::<Vec<TableTrigger>>::set::<FunctionVersionWithTablesBuilder>),

            // Read triggers
            type_of_val(&By::<FunctionVersionId>::select_all::<DaoQueries, TableVersionDBWithNames>),
            type_of_val(&With::<TableVersionDBWithNames>::vec_convert_to::<TableName, _>),
            type_of_val(&With::<Vec<TableName>>::set::<FunctionVersionWithTablesBuilder>),

            // Build
            type_of_val(&With::<FunctionVersionWithTablesBuilder>::build::<FunctionVersionWithTables, _>),

            // Read all versions (without table information)
            type_of_val(&With::<FunctionDBWithNames>::extract::<FunctionId>),
            type_of_val(&By::<FunctionId>::select_all::<DaoQueries, FunctionVersionDBWithNames>),
            type_of_val(&With::<FunctionVersionDBWithNames>::vec_convert_to::<FunctionVersionBuilder, _>),
            type_of_val(&With::<FunctionVersionBuilder>::vec_build::<FunctionVersion, _>),

            // Response
            type_of_val(&builder::<FunctionVersionWithAllVersionsBuilder>),
            type_of_val(&With::<FunctionVersionWithTables>::set::<FunctionVersionWithAllVersionsBuilder>),
            type_of_val(&With::<Vec<FunctionVersion>>::set::<FunctionVersionWithAllVersionsBuilder>),
            type_of_val(&With::<FunctionVersionWithAllVersionsBuilder>::build::<FunctionVersionWithAllVersions, _>),
        ]);
    }

    #[td_test::test(sqlx)]
    async fn test_read(db: DbPool) -> Result<(), TdError> {
        let collection =
            seed_collection(&db, &CollectionName::try_from("cofnig")?, &UserId::admin()).await;

        let dependencies = None;
        let triggers = None;
        let tables = None;

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

        let (function, function_version) = seed_function(&db, &collection, &create).await;

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

        let current = response.current();
        let version = current.function_version();
        assert_eq!(version.id(), function_version.id());
        assert_eq!(version.collection_id(), function_version.collection_id());
        assert_eq!(version.name(), function_version.name());
        assert_eq!(version.description(), function_version.description());
        assert_eq!(version.function_id(), function_version.function_id());
        assert_eq!(version.data_location(), function_version.data_location());
        assert_eq!(
            version.storage_version(),
            function_version.storage_version()
        );
        assert_eq!(version.bundle_id(), function_version.bundle_id());
        assert_eq!(version.snippet(), function_version.snippet());
        assert_eq!(version.defined_on(), function_version.defined_on());
        assert_eq!(version.defined_by_id(), function_version.defined_by_id());
        assert_eq!(version.status(), function_version.status());
        assert_eq!(*version.collection(), CollectionName::try_from("cofnig")?);
        assert_eq!(*version.defined_by(), UserName::try_from("admin")?);

        assert_eq!(
            *current.dependencies(),
            dependencies
                .unwrap_or(vec![])
                .into_iter()
                .map(TableDependency::try_from)
                .collect::<Result<Vec<_>, _>>()?
        );
        assert_eq!(
            *current.triggers(),
            triggers
                .unwrap_or(vec![])
                .into_iter()
                .map(TableTrigger::try_from)
                .collect::<Result<Vec<_>, _>>()?
        );
        assert_eq!(
            *current.tables(),
            tables
                .unwrap_or(vec![])
                .into_iter()
                .map(TableName::try_from)
                .collect::<Result<Vec<_>, _>>()?
        );

        let all = response.all();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].function_id(), function.id());
        Ok(())
    }

    #[td_test::test(sqlx)]
    async fn test_read_multiple(db: DbPool) -> Result<(), TdError> {
        let collection =
            seed_collection(&db, &CollectionName::try_from("cofnig")?, &UserId::admin()).await;

        let dependencies = None;
        let triggers = None;
        let tables = Some(vec![TableNameDto::try_from("table1")?]);

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
        let (function_1, _) = seed_function(&db, &collection, &create).await;

        let tables = Some(vec![
            TableNameDto::try_from("table1")?,
            TableNameDto::try_from("table2")?,
        ]);

        // Just add an output table.
        let create = create.to_builder().tables(tables.clone()).build()?;
        let (function_2, function_version_2) = seed_function(&db, &collection, &create).await;

        assert_eq!(function_1.id(), function_2.id());

        let function = &function_1;
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

        let current = response.current();
        let version = current.function_version();
        assert_eq!(version.id(), function_version_2.id());
        assert_eq!(version.collection_id(), function_version_2.collection_id());
        assert_eq!(version.name(), function_version_2.name());
        assert_eq!(version.description(), function_version_2.description());
        assert_eq!(version.function_id(), function_version_2.function_id());
        assert_eq!(version.data_location(), function_version_2.data_location());
        assert_eq!(
            version.storage_version(),
            function_version_2.storage_version()
        );
        assert_eq!(version.bundle_id(), function_version_2.bundle_id());
        assert_eq!(version.snippet(), function_version_2.snippet());
        assert_eq!(version.defined_on(), function_version_2.defined_on());
        assert_eq!(version.defined_by_id(), function_version_2.defined_by_id());
        assert_eq!(version.status(), function_version_2.status());
        assert_eq!(*version.collection(), CollectionName::try_from("cofnig")?);
        assert_eq!(*version.defined_by(), UserName::try_from("admin")?);

        assert_eq!(
            *current.dependencies(),
            dependencies
                .unwrap_or(vec![])
                .into_iter()
                .map(TableDependency::try_from)
                .collect::<Result<Vec<_>, _>>()?
        );
        assert_eq!(
            *current.triggers(),
            triggers
                .unwrap_or(vec![])
                .into_iter()
                .map(TableTrigger::try_from)
                .collect::<Result<Vec<_>, _>>()?
        );
        assert_eq!(
            *current.tables(),
            tables
                .unwrap_or(vec![])
                .into_iter()
                .map(TableName::try_from)
                .collect::<Result<Vec<_>, _>>()?
        );

        let all = response.all();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].function_id(), function_1.id());
        assert_eq!(all[1].function_id(), function_2.id());
        Ok(())
    }
}
