//
// Copyright 2025 Tabs Data Inc.
//

use crate::table::layers::delete::{
    build_deleted_table_version, build_frozen_function_version_table,
    build_frozen_function_versions_dependencies, update_frozen_functions,
};
use std::sync::Arc;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::DeleteRequest;
use td_objects::rest_urls::TableParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::extractor::extract_req_name;
use td_objects::tower_service::from::{combine, ExtractService, With};
use td_objects::tower_service::sql::{
    insert, insert_vec, By, SqlDeleteService, SqlSelectAllService, SqlSelectIdOrNameService,
    SqlSelectService,
};
use td_objects::types::basic::{
    CollectionIdName, CollectionName, FunctionVersionId, TableId, TableIdName, TableVersionId,
};
use td_objects::types::collection::CollectionDB;
use td_objects::types::dependency::DependencyDB;
use td_objects::types::function::FunctionVersionDB;
use td_objects::types::table::{TableDB, TableDBWithNames, TableVersionDB};
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{SrvCtxProvider, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct DeleteTableService {
    provider: ServiceProvider<DeleteRequest<TableParam>, (), TdError>,
}

impl DeleteTableService {
    pub fn new(db: DbPool) -> Self {
        let queries = Arc::new(DaoQueries::default());
        Self {
            provider: Self::provider(db, queries),
        }
    }

    p! {
        provider(db: DbPool, queries: Arc<DaoQueries>) -> TdError {
            service_provider!(layers!(
                SrvCtxProvider::new(queries),
                from_fn(extract_req_name::<DeleteRequest<TableParam>, _>),

                TransactionProvider::new(db),

                // Extract collection and table from request.
                from_fn(With::<TableParam>::extract::<CollectionIdName>),
                from_fn(With::<TableParam>::extract::<TableIdName>),

                // Get collection. Extract collection id and name.
                from_fn(By::<CollectionIdName>::select::<DaoQueries, CollectionDB>),
                from_fn(With::<CollectionDB>::extract::<CollectionName>),

                // Get table. Extract table id, table version id, function id and function version id.
                from_fn(combine::<CollectionIdName, TableIdName>),
                from_fn(By::<(CollectionIdName, TableIdName)>::select::<DaoQueries, TableDBWithNames>),
                from_fn(With::<TableDBWithNames>::extract::<TableId>),
                from_fn(With::<TableDBWithNames>::extract::<TableVersionId>),
                from_fn(With::<TableDBWithNames>::extract::<FunctionVersionId>),

                // Insert into function_versions(sql) entries with status=Frozen, for the function
                // generating the table.
                from_fn(By::<FunctionVersionId>::select::<DaoQueries, FunctionVersionDB>),
                from_fn(build_frozen_function_version_table),
                from_fn(insert::<DaoQueries, FunctionVersionDB>),

                // Insert into function_versions(sql) entries with status=Frozen,
                // for all functions with status=Active that have the table as dependency.
                from_fn(By::<TableId>::select_all::<DaoQueries, DependencyDB>),
                from_fn(build_frozen_function_versions_dependencies::<DaoQueries>),
                from_fn(insert_vec::<DaoQueries, FunctionVersionDB>),

                // Update functions(sql) with status=Frozen, for the previous function versions.
                // Both table generating function and dependant functions.
                from_fn(update_frozen_functions::<DaoQueries>),

                // Insert into table_versions(sql) status=Deleted.
                from_fn(By::<TableVersionId>::select::<DaoQueries, TableVersionDB>),
                from_fn(build_deleted_table_version),
                from_fn(insert::<DaoQueries, TableVersionDB>),

                // Delete tables(sql) table.
                from_fn(By::<TableId>::delete::<DaoQueries, TableDB>),
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
    use td_common::id::Id;
    use td_objects::crudl::RequestContext;
    use td_objects::rest_urls::FunctionParam;
    use td_objects::test_utils::seed_collection2::seed_collection;
    use td_objects::test_utils::seed_function2::seed_function;
    use td_objects::test_utils::seed_user::admin_user;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, FunctionRuntimeValues, RoleId, TableDependency, TableName, UserId,
    };
    use td_objects::types::function::{FunctionRegister, FunctionUpdate};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_delete_table(db: DbPool) {
        use td_tower::metadata::{type_of_val, Metadata};

        let queries = Arc::new(DaoQueries::default());
        let provider = DeleteTableService::provider(db, queries);
        let service = provider.make().await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();

        metadata.assert_service::<DeleteRequest<TableParam>, ()>(&[
            type_of_val(&extract_req_name::<DeleteRequest<TableParam>, _>),
            // Extract collection and table from request.
            type_of_val(&With::<TableParam>::extract::<CollectionIdName>),
            type_of_val(&With::<TableParam>::extract::<TableIdName>),
            // Get collection. Extract collection id and name.
            type_of_val(&By::<CollectionIdName>::select::<DaoQueries, CollectionDB>),
            type_of_val(&With::<CollectionDB>::extract::<CollectionName>),
            // Get table. Extract table id, table version id, function id and function version id.
            type_of_val(&combine::<CollectionIdName, TableIdName>),
            type_of_val(
                &By::<(CollectionIdName, TableIdName)>::select::<DaoQueries, TableDBWithNames>,
            ),
            type_of_val(&With::<TableDBWithNames>::extract::<TableId>),
            type_of_val(&With::<TableDBWithNames>::extract::<TableVersionId>),
            type_of_val(&With::<TableDBWithNames>::extract::<FunctionVersionId>),
            // Insert into function_versions(sql) entries with status=Frozen, for the function
            // generating the table.
            type_of_val(&By::<FunctionVersionId>::select::<DaoQueries, FunctionVersionDB>),
            type_of_val(&build_frozen_function_version_table),
            type_of_val(&insert::<DaoQueries, FunctionVersionDB>),
            // Insert into function_versions(sql) entries with status=Frozen,
            // for all functions with status=Active that have the table as dependency.
            type_of_val(&By::<TableId>::select_all::<DaoQueries, DependencyDB>),
            type_of_val(&build_frozen_function_versions_dependencies::<DaoQueries>),
            type_of_val(&insert_vec::<DaoQueries, FunctionVersionDB>),
            // Update functions(sql) with status=Frozen, for the previous function versions.
            // Both table generating function and dependant functions.
            type_of_val(&update_frozen_functions::<DaoQueries>),
            // Insert into table_versions(sql) status=Deleted.
            type_of_val(&By::<TableVersionId>::select::<DaoQueries, TableVersionDB>),
            type_of_val(&build_deleted_table_version),
            type_of_val(&insert::<DaoQueries, TableVersionDB>),
            // Delete tables(sql) table.
            type_of_val(&By::<TableId>::delete::<DaoQueries, TableDB>),
        ]);
    }

    #[td_test::test(sqlx)]
    async fn test_delete_table(db: DbPool) -> Result<(), TdError> {
        let admin_id = UserId::from(Id::try_from(&admin_user(&db).await)?);
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &admin_id).await;

        // Create a function with some tables.
        let create = FunctionRegister::builder()
            .try_name("joaquin_workout")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![
                TableName::try_from("super_table")?,
                TableName::try_from("keep_this_one")?,
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
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![TableName::try_from("keep_this_one")?]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
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
            RoleId::user(),
            true,
        )
        .delete(
            TableParam::builder()
                .try_collection(format!("~{}", collection.id()))?
                .try_table("super_table")?
                .build()?,
        );

        let service = DeleteTableService::new(db.clone()).service().await;
        service.raw_oneshot(request).await?;

        assert_delete(
            &db,
            &admin_id,
            &collection,
            &TableName::try_from("super_table")?,
        )
        .await
    }

    #[td_test::test(sqlx)]
    async fn test_delete_function_with_dependency(db: DbPool) -> Result<(), TdError> {
        let admin_id = UserId::from(Id::try_from(&admin_user(&db).await)?);
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &admin_id).await;

        // Create a function with some tables.
        let create = FunctionRegister::builder()
            .try_name("joaquin_workout")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![
                TableName::try_from("super_table")?,
                TableName::try_from("keep_this_one")?,
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
            .dependencies(Some(vec![TableDependency::try_from("super_table")?]))
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
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![TableName::try_from("keep_this_one")?]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
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
            RoleId::user(),
            true,
        )
        .delete(
            TableParam::builder()
                .try_collection(format!("~{}", collection.id()))?
                .try_table("super_table")?
                .build()?,
        );

        let service = DeleteTableService::new(db.clone()).service().await;
        service.raw_oneshot(request).await?;

        assert_delete(
            &db,
            &admin_id,
            &collection,
            &TableName::try_from("super_table")?,
        )
        .await?;

        assert_not_deleted(
            &db,
            &admin_id,
            &collection,
            &TableName::try_from("keep_this_one")?,
        )
        .await?;

        Ok(())
    }
}
