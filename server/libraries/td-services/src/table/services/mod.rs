//
// Copyright 2025 Tabs Data Inc.
//

pub mod delete;
mod download;
mod list;
pub mod list_by_collection;
mod list_data_versions;
mod sample;
mod schema;

use crate::table::services::delete::TableDeleteService;
use crate::table::services::download::TableDownloadService;
use crate::table::services::list::TableListService;
use crate::table::services::list_by_collection::TableListByCollectionService;
use crate::table::services::list_data_versions::TableListDataVersionsService;
use crate::table::services::sample::TableSampleService;
use crate::table::services::schema::TableSchemaService;
use std::sync::Arc;
use td_authz::AuthzContext;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{DeleteRequest, ListRequest, ListResponse, ReadRequest};
use td_objects::rest_urls::{AtTimeParam, TableParam};
use td_objects::sql::DaoQueries;
use td_objects::types::execution::TableDataVersion;
use td_objects::types::stream::BoxedSyncStream;
use td_objects::types::table::{
    CollectionAtName, Table, TableAtIdName, TableSampleAtName, TableSchema,
};
use td_storage::{SPath, Storage};
use td_tower::service_provider::TdBoxService;

pub struct TableServices {
    list_table_by_collection: TableListByCollectionService,
    list_table: TableListService,
    list_table_data_versions: TableListDataVersionsService,
    table_schema: TableSchemaService,
    table_download: TableDownloadService,
    table_sample: TableSampleService,
    table_delete: TableDeleteService,
}

impl TableServices {
    pub fn new(db: DbPool, authz_context: Arc<AuthzContext>, storage: Arc<Storage>) -> Self {
        let queries = Arc::new(DaoQueries::default());
        Self {
            list_table_by_collection: TableListByCollectionService::new(
                db.clone(),
                queries.clone(),
                authz_context.clone(),
            ),
            list_table: TableListService::new(db.clone(), queries.clone(), authz_context.clone()),
            list_table_data_versions: TableListDataVersionsService::new(
                db.clone(),
                queries.clone(),
                authz_context.clone(),
            ),
            table_schema: TableSchemaService::new(
                db.clone(),
                queries.clone(),
                authz_context.clone(),
                storage.clone(),
            ),
            table_download: TableDownloadService::new(
                db.clone(),
                queries.clone(),
                authz_context.clone(),
            ),
            table_sample: TableSampleService::new(
                db.clone(),
                queries.clone(),
                authz_context.clone(),
                storage.clone(),
            ),
            table_delete: TableDeleteService::new(
                db.clone(),
                queries.clone(),
                authz_context.clone(),
            ),
        }
    }

    pub async fn list_table_by_collection_service(
        &self,
    ) -> TdBoxService<ListRequest<CollectionAtName>, ListResponse<Table>, TdError> {
        self.list_table_by_collection.service().await
    }

    pub async fn list_table_service(
        &self,
    ) -> TdBoxService<ListRequest<AtTimeParam>, ListResponse<Table>, TdError> {
        self.list_table.service().await
    }

    pub async fn list_table_data_versions_service(
        &self,
    ) -> TdBoxService<ListRequest<TableAtIdName>, ListResponse<TableDataVersion>, TdError> {
        self.list_table_data_versions.service().await
    }

    pub async fn table_schema_service(
        &self,
    ) -> TdBoxService<ReadRequest<TableAtIdName>, TableSchema, TdError> {
        self.table_schema.service().await
    }

    pub async fn table_download_service(
        &self,
    ) -> TdBoxService<ReadRequest<TableAtIdName>, Option<SPath>, TdError> {
        self.table_download.service().await
    }

    pub async fn table_sample_service(
        &self,
    ) -> TdBoxService<ReadRequest<TableSampleAtName>, BoxedSyncStream, TdError> {
        self.table_sample.service().await
    }

    pub async fn table_delete_service(
        &self,
    ) -> TdBoxService<DeleteRequest<TableParam>, (), TdError> {
        self.table_delete.service().await
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_objects::crudl::handle_sql_err;
    use td_objects::sql::cte::CteQueries;
    use td_objects::sql::{DaoQueries, SelectBy};
    use td_objects::types::basic::{FunctionStatus, TableName, TableStatus, UserId};
    use td_objects::types::collection::CollectionDB;
    use td_objects::types::dependency::DependencyDB;
    use td_objects::types::function::FunctionDB;
    use td_objects::types::table::TableDB;

    pub async fn assert_delete(
        db: &DbPool,
        user_id: &UserId,
        collection: &CollectionDB,
        table_name: &TableName,
    ) -> Result<(), TdError> {
        let queries = DaoQueries::default();

        // Assert current table version is deleted
        let table_versions: Vec<TableDB> = queries
            .select_versions_at::<TableDB>(None, None, &table_name)?
            .build_query_as()
            .fetch_all(db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(table_versions.len(), 1);
        assert_eq!(table_versions[0].name(), table_name);
        assert_eq!(table_versions[0].collection_id(), collection.id());
        assert_eq!(table_versions[0].defined_by_id(), user_id);
        assert_eq!(*table_versions[0].status(), TableStatus::Deleted);

        let deleted_table = &table_versions[0];

        // Assert previous table versions exists (first active, then frozen, then deleted)
        let table_versions: Vec<TableDB> = queries
            .select_by::<TableDB>(&table_name)?
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

        // And that the function version is still active
        let function: FunctionDB = queries
            .select_versions_at::<FunctionDB>(None, None, &deleted_table.function_id())?
            .build_query_as()
            .fetch_one(db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(*function.status(), FunctionStatus::Active);

        // And assert that all dependant function versions are also frozen
        let dependency_versions: Vec<DependencyDB> = queries
            .select_versions_at::<DependencyDB>(None, None, &table_versions[0].table_id())?
            .build_query_as()
            .fetch_all(db)
            .await
            .map_err(handle_sql_err)?;

        for dependency_version in &dependency_versions {
            let function: FunctionDB = queries
                .select_versions_at::<FunctionDB>(None, None, &dependency_version.function_id())?
                .build_query_as()
                .fetch_one(db)
                .await
                .map_err(handle_sql_err)?;
            assert_eq!(*function.status(), FunctionStatus::Frozen);

            // Note that the function version in the dependency still points to the active function version
            // thus being active itself.
            let function_version: FunctionDB = queries
                .select_by::<FunctionDB>(&function.id())?
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
            .select_versions_at::<TableDB>(None, None, &table_name)?
            .build_query_as()
            .fetch_optional(db)
            .await
            .map_err(handle_sql_err)?;
        assert!(table.is_some());

        // Assert previous table versions exists, always active
        let table_versions: Vec<TableDB> = queries
            .select_by::<TableDB>(&table_name)?
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
