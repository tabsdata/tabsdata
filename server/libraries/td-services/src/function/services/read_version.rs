//
// Copyright 2025 Tabs Data Inc.
//

use std::sync::Arc;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::ReadRequest;
use td_objects::rest_urls::FunctionVersionParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::from::{
    builder, combine, BuildService, ConvertIntoMapService, ExtractNameService, ExtractService,
    SetService, TryIntoService, With,
};
use td_objects::tower_service::sql::{
    By, SqlSelectAllService, SqlSelectIdOrNameService, SqlSelectService,
};
use td_objects::types::basic::{
    CollectionIdName, FunctionVersionId, FunctionVersionIdName, TableDependency, TableName,
    TableTrigger,
};
use td_objects::types::dependency::DependencyVersionDBWithNames;
use td_objects::types::function::{
    FunctionVersion, FunctionVersionBuilder, FunctionVersionDBWithNames, FunctionVersionWithTables,
    FunctionVersionWithTablesBuilder,
};
use td_objects::types::table::TableVersionDBWithNames;
use td_objects::types::trigger::TriggerVersionDBWithNames;
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{ConnectionProvider, SrvCtxProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{l, layers, p, service_provider};

pub struct ReadFunctionVersionService {
    provider:
        ServiceProvider<ReadRequest<FunctionVersionParam>, FunctionVersionWithTables, TdError>,
}

impl ReadFunctionVersionService {
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
                from_fn(With::<ReadRequest<FunctionVersionParam>>::extract_name::<FunctionVersionParam>),

                ConnectionProvider::new(db),

                // Extract from request.
                from_fn(With::<FunctionVersionParam>::extract::<CollectionIdName>),
                from_fn(With::<FunctionVersionParam>::extract::<FunctionVersionIdName>),
                from_fn(combine::<CollectionIdName, FunctionVersionIdName>),

                // Read function version
                from_fn(By::<(CollectionIdName, FunctionVersionIdName)>::select::<DaoQueries, FunctionVersionDBWithNames>),
                from_fn(With::<FunctionVersionDBWithNames>::extract::<FunctionVersionId>),

                // Read function with tables
                ReadFunctionVersionService::function_version_with_tables_by_id(),
            ))
        }
    }

    l! {
        function_version_with_tables_by_id() {
            layers!(
                // Builder
                from_fn(builder::<FunctionVersionWithTablesBuilder>),

                // Read function version
                from_fn(By::<FunctionVersionId>::select::<DaoQueries, FunctionVersionDBWithNames>),
                from_fn(With::<FunctionVersionDBWithNames>::convert_to::<FunctionVersionBuilder, _>),
                from_fn(With::<FunctionVersionBuilder>::build::<FunctionVersion, _>),
                from_fn(With::<FunctionVersion>::set::<FunctionVersionWithTablesBuilder>),

                // Read tables
                from_fn(By::<FunctionVersionId>::select_all::<DaoQueries, DependencyVersionDBWithNames>),
                from_fn(With::<DependencyVersionDBWithNames>::vec_convert_to::<TableDependency, _>),
                from_fn(With::<Vec<TableDependency>>::set::<FunctionVersionWithTablesBuilder>),

                // Read dependencies
                from_fn(By::<FunctionVersionId>::select_all::<DaoQueries, TriggerVersionDBWithNames>),
                from_fn(With::<TriggerVersionDBWithNames>::vec_convert_to::<TableTrigger, _>),
                from_fn(With::<Vec<TableTrigger>>::set::<FunctionVersionWithTablesBuilder>),

                // Read triggers
                from_fn(By::<FunctionVersionId>::select_all::<DaoQueries, TableVersionDBWithNames>),
                from_fn(With::<TableVersionDBWithNames>::vec_convert_to::<TableName, _>),
                from_fn(With::<Vec<TableName>>::set::<FunctionVersionWithTablesBuilder>),

                // Build
                from_fn(With::<FunctionVersionWithTablesBuilder>::build::<FunctionVersionWithTables, _>),
            )
        }
    }

    pub async fn service(
        &self,
    ) -> TdBoxService<ReadRequest<FunctionVersionParam>, FunctionVersionWithTables, TdError> {
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
        TableDependencyDto, TableName, TableNameDto, UserId, UserName,
    };
    use td_objects::types::function::FunctionRegister;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_read_function_version(db: DbPool) {
        use td_tower::metadata::{type_of_val, Metadata};

        let queries = Arc::new(DaoQueries::default());
        let provider = ReadFunctionVersionService::provider(db, queries);
        let service = provider.make().await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();

        metadata.assert_service::<ReadRequest<FunctionVersionParam>, FunctionVersionWithTables>(&[
            type_of_val(
                &With::<ReadRequest<FunctionVersionParam>>::extract_name::<FunctionVersionParam>,
            ),
            // Extract from request.
            type_of_val(&With::<FunctionVersionParam>::extract::<CollectionIdName>),
            type_of_val(&With::<FunctionVersionParam>::extract::<FunctionVersionIdName>),
            type_of_val(&combine::<CollectionIdName, FunctionVersionIdName>),
            // Read function version
            type_of_val(
                &By::<(CollectionIdName, FunctionVersionIdName)>::select::<
                    DaoQueries,
                    FunctionVersionDBWithNames,
                >,
            ),
            type_of_val(&With::<FunctionVersionDBWithNames>::extract::<FunctionVersionId>),
            // Read function with tables
            // Builder
            type_of_val(&builder::<FunctionVersionWithTablesBuilder>),
            // Read function version
            type_of_val(&By::<FunctionVersionId>::select::<DaoQueries, FunctionVersionDBWithNames>),
            type_of_val(
                &With::<FunctionVersionDBWithNames>::convert_to::<FunctionVersionBuilder, _>,
            ),
            type_of_val(&With::<FunctionVersionBuilder>::build::<FunctionVersion, _>),
            type_of_val(&With::<FunctionVersion>::set::<FunctionVersionWithTablesBuilder>),
            // Read tables
            type_of_val(
                &By::<FunctionVersionId>::select_all::<DaoQueries, DependencyVersionDBWithNames>,
            ),
            type_of_val(
                &With::<DependencyVersionDBWithNames>::vec_convert_to::<TableDependency, _>,
            ),
            type_of_val(&With::<Vec<TableDependency>>::set::<FunctionVersionWithTablesBuilder>),
            // Read dependencies
            type_of_val(
                &By::<FunctionVersionId>::select_all::<DaoQueries, TriggerVersionDBWithNames>,
            ),
            type_of_val(&With::<TriggerVersionDBWithNames>::vec_convert_to::<TableTrigger, _>),
            type_of_val(&With::<Vec<TableTrigger>>::set::<FunctionVersionWithTablesBuilder>),
            // Read triggers
            type_of_val(
                &By::<FunctionVersionId>::select_all::<DaoQueries, TableVersionDBWithNames>,
            ),
            type_of_val(&With::<TableVersionDBWithNames>::vec_convert_to::<TableName, _>),
            type_of_val(&With::<Vec<TableName>>::set::<FunctionVersionWithTablesBuilder>),
            // Build
            type_of_val(
                &With::<FunctionVersionWithTablesBuilder>::build::<FunctionVersionWithTables, _>,
            ),
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

        let (function, function_version) = seed_function(&db, &collection, &create).await;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            true,
        )
        .read(
            FunctionVersionParam::builder()
                .try_collection(format!("~{}", function.collection_id()))?
                .try_function_version("joaquin_workout")?
                .build()?,
        );

        let service = ReadFunctionVersionService::new(db.clone()).service().await;
        let response = service.raw_oneshot(request).await;
        let response = response?;

        let version = response.function_version();
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
