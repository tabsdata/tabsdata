//
// Copyright 2025 Tabs Data Inc.
//

pub mod delete;
mod download;
pub mod list;
mod list_data_versions;
mod sample;
mod schema;

use crate::table::services::download::TableDownloadService;
use crate::table::services::list::TableListService;
use crate::table::services::list_data_versions::TableListDataVersionsService;
use crate::table::services::sample::{BoxedSyncStream, TableSampleService};
use crate::table::services::schema::TableSchemaService;
use std::sync::Arc;
use td_authz::AuthzContext;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{ListRequest, ListResponse, ReadRequest};
use td_objects::types::table::{
    CollectionAtName, Table, TableAtName, TableDataVersion, TableSampleAtName, TableSchema,
};
use td_storage::{SPath, Storage};
use td_tower::service_provider::TdBoxService;

pub struct TableServices {
    list_table: TableListService,
    list_table_data_versions: TableListDataVersionsService,
    table_schema: TableSchemaService,
    table_download: TableDownloadService,
    table_sample: TableSampleService,
}

impl TableServices {
    pub fn new(db: DbPool, authz_context: Arc<AuthzContext>, storage: Arc<Storage>) -> Self {
        Self {
            list_table: TableListService::new(db.clone(), authz_context.clone()),
            list_table_data_versions: TableListDataVersionsService::new(
                db.clone(),
                authz_context.clone(),
            ),
            table_schema: TableSchemaService::new(db.clone(), authz_context.clone()),
            table_download: TableDownloadService::new(db.clone(), authz_context.clone()),
            table_sample: TableSampleService::new(
                db.clone(),
                authz_context.clone(),
                storage.clone(),
            ),
        }
    }

    pub async fn list_table_service(
        &self,
    ) -> TdBoxService<ListRequest<CollectionAtName>, ListResponse<Table>, TdError> {
        self.list_table.service().await
    }

    pub async fn list_table_data_versions_service(
        &self,
    ) -> TdBoxService<ListRequest<TableAtName>, ListResponse<TableDataVersion>, TdError> {
        self.list_table_data_versions.service().await
    }

    pub async fn table_schema_service(
        &self,
    ) -> TdBoxService<ReadRequest<TableAtName>, TableSchema, TdError> {
        self.table_schema.service().await
    }

    pub async fn table_download_service(
        &self,
    ) -> TdBoxService<ReadRequest<TableAtName>, SPath, TdError> {
        self.table_download.service().await
    }

    pub async fn table_sample_service(
        &self,
    ) -> TdBoxService<ReadRequest<TableSampleAtName>, BoxedSyncStream, TdError> {
        self.table_sample.service().await
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_objects::crudl::handle_sql_err;
    use td_objects::sql::{DaoQueries, SelectBy};
    use td_objects::types::basic::{Frozen, FunctionStatus, TableName, TableStatus, UserId};
    use td_objects::types::collection::CollectionDB;
    use td_objects::types::dependency::DependencyVersionDB;
    use td_objects::types::function::{FunctionDB, FunctionVersionDB};
    use td_objects::types::table::{TableDB, TableVersionDB};

    pub async fn assert_delete(
        db: &DbPool,
        user_id: &UserId,
        collection: &CollectionDB,
        table_name: &TableName,
    ) -> Result<(), TdError> {
        let queries = DaoQueries::default();

        // Assert table does not exist
        let table: Option<TableDB> = queries
            .select_by::<TableDB>(&table_name)?
            .build_query_as()
            .fetch_optional(db)
            .await
            .map_err(handle_sql_err)?;
        assert!(table.is_none());

        // Assert previous table versions exists (first active, then frozen, then deleted)
        let table_versions: Vec<TableVersionDB> = queries
            .select_by::<TableVersionDB>(&table_name)?
            .build_query_as()
            .fetch_all(db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(table_versions.len(), 3);
        for table_version in &table_versions {
            assert_eq!(table_version.name(), table_name);
            assert_eq!(table_version.collection_id(), collection.id());
            assert_eq!(table_version.defined_by_id(), user_id);
        }
        assert_eq!(*table_versions[0].status(), TableStatus::Deleted);
        assert_eq!(*table_versions[1].status(), TableStatus::Frozen);
        assert_eq!(*table_versions[2].status(), TableStatus::Active);

        // And all have the same table_id
        assert_eq!(table_versions[0].table_id(), table_versions[1].table_id());
        assert_eq!(table_versions[0].table_id(), table_versions[2].table_id());

        // First one because of default ASC order by.
        let deleted_table_version = &table_versions[0];

        // Assert there is a new function version for the deleted table, in frozen state
        let deleted_table_function_version: FunctionVersionDB = queries
            .select_by::<FunctionVersionDB>(&deleted_table_version.function_version_id())?
            .build_query_as()
            .fetch_one(db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(
            *deleted_table_function_version.status(),
            FunctionStatus::Frozen
        );

        // And that the function is also frozen
        let function: FunctionDB = queries
            .select_by::<FunctionDB>(&deleted_table_function_version.function_id())?
            .build_query_as()
            .fetch_one(db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(*function.frozen(), Frozen::from(true));

        // And assert that all dependant functions and function versions are also frozen
        let dependency_versions: Vec<DependencyVersionDB> = queries
            .select_by::<DependencyVersionDB>(&table_name)?
            .build_query_as()
            .fetch_all(db)
            .await
            .map_err(handle_sql_err)?;

        for dependency_version in &dependency_versions {
            let function: FunctionDB = queries
                .select_by::<FunctionDB>(&dependency_version.function_id())?
                .build_query_as()
                .fetch_one(db)
                .await
                .map_err(handle_sql_err)?;
            assert_eq!(*function.frozen(), Frozen::from(true));

            // Note that the function version in the dependency still points to the active function version
            // thus being active itself.
            let function_version: FunctionVersionDB = queries
                .select_by::<FunctionVersionDB>(&function.function_version_id())?
                .build_query_as()
                .fetch_one(db)
                .await
                .map_err(handle_sql_err)?;
            assert_eq!(*function_version.status(), FunctionStatus::Frozen);
        }

        Ok(())
    }

    // Used to assert a table did not get deleted when it shouldn't have
    pub async fn assert_not_deleted(
        db: &DbPool,
        user_id: &UserId,
        collection: &CollectionDB,
        table_name: &TableName,
    ) -> Result<(), TdError> {
        let queries = DaoQueries::default();

        // Assert table does exist
        let table: Option<TableDB> = queries
            .select_by::<TableDB>(&table_name)?
            .build_query_as()
            .fetch_optional(db)
            .await
            .map_err(handle_sql_err)?;
        assert!(table.is_some());

        // Assert previous table versions exists, always active
        let table_versions: Vec<TableVersionDB> = queries
            .select_by::<TableVersionDB>(&table_name)?
            .build_query_as()
            .fetch_all(db)
            .await
            .map_err(handle_sql_err)?;
        for table_version in &table_versions {
            assert_eq!(table_version.name(), table_name);
            assert_eq!(table_version.collection_id(), collection.id());
            assert_eq!(table_version.defined_by_id(), user_id);
            assert_eq!(*table_version.status(), TableStatus::Active);
        }

        // Function can still be frozen if another table got deleted

        Ok(())
    }
}
