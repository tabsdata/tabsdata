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
use ta_services::factory::ServiceFactory;

#[derive(ServiceFactory)]
pub struct TableServices {
    pub list_by_collection: TableListByCollectionService,
    pub list: TableListService,
    pub list_data_versions: TableListDataVersionsService,
    pub schema: TableSchemaService,
    pub download: TableDownloadService,
    pub sample: TableSampleService,
    pub delete: TableDeleteService,
}

#[cfg(test)]
pub(crate) mod tests {
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_objects::dxo::collection::CollectionDB;
    use td_objects::dxo::crudl::handle_sql_err;
    use td_objects::dxo::dependency::DependencyDB;
    use td_objects::dxo::function::FunctionDB;
    use td_objects::dxo::table::TableDB;
    use td_objects::sql::cte::CteQueries;
    use td_objects::sql::{DaoQueries, SelectBy};
    use td_objects::types::basic::{FunctionStatus, TableName, TableStatus, UserId};

    pub async fn assert_delete(
        db: &DbPool,
        user_id: &UserId,
        collection: &CollectionDB,
        table_name: &TableName,
    ) -> Result<(), TdError> {
        let queries = DaoQueries::default();

        // Assert current table version is deleted
        let table_versions: Vec<TableDB> = queries
            .select_versions_at::<{ TableDB::All }, TableDB>(None, &table_name)?
            .build_query_as()
            .fetch_all(db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(table_versions.len(), 1);
        assert_eq!(table_versions[0].name, *table_name);
        assert_eq!(table_versions[0].collection_id, collection.id);
        assert_eq!(table_versions[0].defined_by_id, *user_id);
        assert_eq!(table_versions[0].status, TableStatus::Deleted);

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
            assert_eq!(table_version.name, *table_name);
            assert_eq!(table_version.collection_id, collection.id);
            assert_eq!(table_version.defined_by_id, *user_id);
        }
        assert_eq!(table_versions[0].status, TableStatus::Deleted);
        assert_eq!(table_versions[1].status, TableStatus::Frozen);
        assert_eq!(table_versions[2].status, TableStatus::Active);

        // And all have the same table_id
        assert_eq!(table_versions[0].table_id, table_versions[1].table_id);
        assert_eq!(table_versions[0].table_id, table_versions[2].table_id);

        // And that the function version is still active
        let function: FunctionDB = queries
            .select_versions_at::<{ FunctionDB::All }, FunctionDB>(
                None,
                &deleted_table.function_id,
            )?
            .build_query_as()
            .fetch_one(db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(function.status, FunctionStatus::Active);

        // And assert that all dependant function versions are also frozen
        let dependency_versions: Vec<DependencyDB> = queries
            .select_versions_at::<{ DependencyDB::All }, DependencyDB>(
                None,
                &table_versions[0].table_id,
            )?
            .build_query_as()
            .fetch_all(db)
            .await
            .map_err(handle_sql_err)?;

        for dependency_version in &dependency_versions {
            let function: FunctionDB = queries
                .select_versions_at::<{ FunctionDB::All }, FunctionDB>(
                    None,
                    &dependency_version.function_id,
                )?
                .build_query_as()
                .fetch_one(db)
                .await
                .map_err(handle_sql_err)?;
            assert_eq!(function.status, FunctionStatus::Frozen);

            // Note that the function version in the dependency still points to the active function version
            // thus being active itself.
            let function_version: FunctionDB = queries
                .select_by::<FunctionDB>(&function.id)?
                .build_query_as()
                .fetch_one(db)
                .await
                .map_err(handle_sql_err)?;
            assert_eq!(function_version.status, FunctionStatus::Frozen);
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
            .select_versions_at::<{ TableDB::All }, TableDB>(None, &table_name)?
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
            assert_eq!(table_version.name, *table_name);
            assert_eq!(table_version.collection_id, collection.id);
            assert_eq!(table_version.defined_by_id, *user_id);
            assert_eq!(table_version.status, TableStatus::Active);
        }

        // Function can still be frozen if another table got deleted

        Ok(())
    }
}
