//
// Copyright 2025 Tabs Data Inc.
//

use crate::table::layers::delete::{
    build_deleted_table_version, build_frozen_function_versions_dependencies,
};
use std::sync::Arc;
use td_authz::{Authz, AuthzContext};
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{DeleteRequest, RequestContext};
use td_objects::rest_urls::TableParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{AuthzOn, CollAdmin, CollDev};
use td_objects::tower_service::from::{
    combine, ExtractNameService, ExtractService, TryIntoService, UpdateService, With,
};
use td_objects::tower_service::sql::{
    insert, insert_vec, By, SqlSelectAllService, SqlSelectService,
};
use td_objects::types::basic::{
    AtTime, CollectionId, CollectionIdName, CollectionName, DependencyStatus, TableId, TableIdName,
    TableStatus, TableVersionId,
};
use td_objects::types::collection::CollectionDB;
use td_objects::types::dependency::DependencyDB;
use td_objects::types::function::FunctionDB;
use td_objects::types::table::{TableDB, TableDBBuilder, TableDBWithNames};
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{SrvCtxProvider, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct TableDeleteService {
    provider: ServiceProvider<DeleteRequest<TableParam>, (), TdError>,
}

impl TableDeleteService {
    pub fn new(db: DbPool, authz_context: Arc<AuthzContext>) -> Self {
        let queries = Arc::new(DaoQueries::default());
        Self {
            provider: Self::provider(db, queries, authz_context),
        }
    }

    p! {
        provider(db: DbPool, queries: Arc<DaoQueries>, authz_context: Arc<AuthzContext>) {
            service_provider!(layers!(
                TransactionProvider::new(db),
                SrvCtxProvider::new(queries),
                SrvCtxProvider::new(authz_context),

                from_fn(With::<DeleteRequest<TableParam>>::extract::<RequestContext>),
                from_fn(With::<DeleteRequest<TableParam>>::extract_name::<TableParam>),

                // Extract collection and table from request.
                from_fn(With::<TableParam>::extract::<CollectionIdName>),
                from_fn(With::<TableParam>::extract::<TableIdName>),

                // Get collection. Extract collection id and name.
                from_fn(By::<CollectionIdName>::select::<DaoQueries, CollectionDB>),
                from_fn(With::<CollectionDB>::extract::<CollectionName>),
                from_fn(With::<CollectionDB>::extract::<CollectionId>),

                // check requester has collection permissions
                from_fn(AuthzOn::<CollectionId>::set),
                from_fn(Authz::<CollAdmin, CollDev>::check),

                // Get table. Extract table id, table version id, function id and function version id.
                from_fn(combine::<CollectionIdName, TableIdName>),
                from_fn(With::<RequestContext>::extract::<AtTime>),
                from_fn(TableStatus::frozen),
                from_fn(By::<(CollectionIdName, TableIdName)>::select_version::<DaoQueries, TableDBWithNames>),
                from_fn(With::<TableDBWithNames>::extract::<TableId>),
                from_fn(With::<TableDBWithNames>::extract::<TableVersionId>),

                // Insert into function_versions(sql) entries with status=Frozen,
                // for all functions with status=Active that have the table as dependency
                // at the current time.
                from_fn(DependencyStatus::active),
                from_fn(By::<TableId>::select_all_versions::<DaoQueries, DependencyDB>),
                from_fn(build_frozen_function_versions_dependencies::<DaoQueries>),
                from_fn(insert_vec::<DaoQueries, FunctionDB>),

                // Insert into table_versions(sql) status=Deleted.
                from_fn(By::<TableVersionId>::select::<DaoQueries, TableDB>),
                from_fn(With::<TableDB>::convert_to::<TableDBBuilder, _>),
                from_fn(With::<RequestContext>::update::<TableDBBuilder, _>),
                from_fn(build_deleted_table_version),
                from_fn(insert::<DaoQueries, TableDB>),
            ))
        }
    }

    pub async fn service(&self) -> TdBoxService<DeleteRequest<TableParam>, (), TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::function::services::update::UpdateFunctionService;
    use crate::table::services::tests::{assert_delete, assert_not_deleted};
    use td_objects::crudl::RequestContext;
    use td_objects::rest_urls::FunctionParam;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_function::seed_function;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, Decorator, FunctionRuntimeValues, RoleId, TableDependencyDto,
        TableName, TableNameDto, UserId,
    };
    use td_objects::types::function::{FunctionRegister, FunctionUpdate};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_delete_table(db: DbPool) {
        use td_tower::metadata::{type_of_val, Metadata};

        let queries = Arc::new(DaoQueries::default());
        let authz_context = Arc::new(AuthzContext::default());

        let provider = TableDeleteService::provider(db, queries, authz_context);
        let service = provider.make().await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();

        metadata.assert_service::<DeleteRequest<TableParam>, ()>(&[
            type_of_val(&With::<DeleteRequest<TableParam>>::extract::<RequestContext>),
            type_of_val(&With::<DeleteRequest<TableParam>>::extract_name::<TableParam>),
            // Extract collection and table from request.
            type_of_val(&With::<TableParam>::extract::<CollectionIdName>),
            type_of_val(&With::<TableParam>::extract::<TableIdName>),
            // Get collection. Extract collection id and name.
            type_of_val(&By::<CollectionIdName>::select::<DaoQueries, CollectionDB>),
            type_of_val(&With::<CollectionDB>::extract::<CollectionName>),
            type_of_val(&With::<CollectionDB>::extract::<CollectionId>),
            // check requester has collection permissions
            type_of_val(&AuthzOn::<CollectionId>::set),
            type_of_val(&Authz::<CollAdmin, CollDev>::check),
            // Get table. Extract table id, table version id, function id and function version id.
            type_of_val(&combine::<CollectionIdName, TableIdName>),
            type_of_val(&With::<RequestContext>::extract::<AtTime>),
            type_of_val(&TableStatus::frozen),
            type_of_val(
                &By::<(CollectionIdName, TableIdName)>::select_version::<
                    DaoQueries,
                    TableDBWithNames,
                >,
            ),
            type_of_val(&With::<TableDBWithNames>::extract::<TableId>),
            type_of_val(&With::<TableDBWithNames>::extract::<TableVersionId>),
            // Insert into function_versions(sql) entries with status=Frozen,
            // for all functions with status=Active that have the table as dependency
            // at the current time.
            type_of_val(&DependencyStatus::active),
            type_of_val(&By::<TableId>::select_all_versions::<DaoQueries, DependencyDB>),
            type_of_val(&build_frozen_function_versions_dependencies::<DaoQueries>),
            type_of_val(&insert_vec::<DaoQueries, FunctionDB>),
            // Insert into table_versions(sql) status=Deleted.
            type_of_val(&By::<TableVersionId>::select::<DaoQueries, TableDB>),
            type_of_val(&With::<TableDB>::convert_to::<TableDBBuilder, _>),
            type_of_val(&With::<RequestContext>::update::<TableDBBuilder, _>),
            type_of_val(&build_deleted_table_version),
            type_of_val(&insert::<DaoQueries, TableDB>),
        ]);
    }

    #[td_test::test(sqlx)]
    async fn test_delete_table(db: DbPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &UserId::admin()).await;

        // Create a function with some tables.
        let create = FunctionRegister::builder()
            .try_name("joaquin_workout")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![
                TableNameDto::try_from("super_table")?,
                TableNameDto::try_from("keep_this_one")?,
            ]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let _ = seed_function(&db, &collection, &create).await;

        // Update the function to remove the tables (set it to frozen).
        let update = FunctionUpdate::builder()
            .try_name("joaquin_workout")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![TableNameDto::try_from("keep_this_one")?]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sys_admin(),
            true,
        )
        .update(
            FunctionParam::builder()
                .try_collection(format!("~{}", collection.id()))?
                .try_function("joaquin_workout")?
                .build()?,
            update.clone(),
        );

        let service = UpdateFunctionService::new(db.clone()).service().await;
        let response = service.raw_oneshot(request).await;
        let _response = response?;

        // Test remove tables.
        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sys_admin(),
            true,
        )
        .delete(
            TableParam::builder()
                .try_collection(format!("~{}", collection.id()))?
                .try_table("super_table")?
                .build()?,
        );

        let authz_context = Arc::new(AuthzContext::default());

        let service = TableDeleteService::new(db.clone(), authz_context)
            .service()
            .await;
        service.raw_oneshot(request).await?;

        assert_delete(
            &db,
            &UserId::admin(),
            &collection,
            &TableName::try_from("super_table")?,
        )
        .await
    }

    #[td_test::test(sqlx)]
    async fn test_delete_function_with_dependency(db: DbPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &UserId::admin()).await;

        // Create a function with some tables.
        let create = FunctionRegister::builder()
            .try_name("joaquin_workout")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![
                TableNameDto::try_from("super_table")?,
                TableNameDto::try_from("keep_this_one")?,
            ]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let _ = seed_function(&db, &collection, &create).await;

        // Create a function depending on it.
        let create = FunctionRegister::builder()
            .try_name("joaquin_dependant_function")?
            .try_description("joaquin_dependant_function description")?
            .bundle_id(BundleId::default())
            .try_snippet("joaquin_dependant_function snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(Some(vec![TableDependencyDto::try_from("super_table")?]))
            .triggers(None)
            .tables(None)
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let _ = seed_function(&db, &collection, &create).await;

        // Update the function to remove the tables (set it to frozen).
        let update = FunctionUpdate::builder()
            .try_name("joaquin_workout")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![TableNameDto::try_from("keep_this_one")?]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sys_admin(),
            true,
        )
        .update(
            FunctionParam::builder()
                .try_collection(format!("~{}", collection.id()))?
                .try_function("joaquin_workout")?
                .build()?,
            update.clone(),
        );

        let service = UpdateFunctionService::new(db.clone()).service().await;
        let response = service.raw_oneshot(request).await;
        let _response = response?;

        // Test remove tables.
        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sys_admin(),
            true,
        )
        .delete(
            TableParam::builder()
                .try_collection(format!("~{}", collection.id()))?
                .try_table("super_table")?
                .build()?,
        );

        let authz_context = Arc::new(AuthzContext::default());

        let service = TableDeleteService::new(db.clone(), authz_context)
            .service()
            .await;
        service.raw_oneshot(request).await?;

        assert_delete(
            &db,
            &UserId::admin(),
            &collection,
            &TableName::try_from("super_table")?,
        )
        .await?;

        assert_not_deleted(
            &db,
            &UserId::admin(),
            &collection,
            &TableName::try_from("keep_this_one")?,
        )
        .await?;

        Ok(())
    }
}
