//
// Copyright 2025 Tabsdata Inc.
//

pub mod delete;
mod download;
mod download_state;
mod flow;
mod list;
pub mod list_by_collection;
mod list_data_versions;
mod list_history;
mod list_history_by_collection;
mod sample;
mod schema;

use crate::table::services::delete::TableDeleteService;
use crate::table::services::download::TableDownloadService;
use crate::table::services::download_state::TableStateDownloadService;
use crate::table::services::flow::TableFlowGraphService;
use crate::table::services::list::TableListService;
use crate::table::services::list_by_collection::TableListByCollectionService;
use crate::table::services::list_data_versions::TableListDataVersionsService;
use crate::table::services::list_history::TableListHistoryService;
use crate::table::services::list_history_by_collection::TableListHistoryByCollectionService;
use crate::table::services::sample::TableSampleService;
use crate::table::services::schema::TableSchemaService;
use ta_services::factory::ServiceFactory;

#[derive(ServiceFactory)]
pub struct TableServices {
    pub list_by_collection: TableListByCollectionService,
    pub list: TableListService,
    pub list_history: TableListHistoryService,
    pub list_history_by_collection: TableListHistoryByCollectionService,
    pub list_data_versions: TableListDataVersionsService,
    pub schema: TableSchemaService,
    pub download: TableDownloadService,
    pub download_state: TableStateDownloadService,
    pub sample: TableSampleService,
    pub delete: TableDeleteService,
    pub flow_graph: TableFlowGraphService,
}

#[cfg(test)]
pub(crate) mod tests {
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::dxo::collection::CollectionDB;
    use td_objects::dxo::dependency::DependencyDB;
    use td_objects::dxo::function::FunctionDB;
    use td_objects::dxo::table::TableDB;
    use td_objects::types::basic::{FunctionStatus, TableName, TableStatus, UserId};
    use td_queries::{CurrentEntry, LiveEntry};
    pub async fn assert_delete(
        db: &TdPool,
        user_id: &UserId,
        collection: &CollectionDB,
        table_name: &TableName,
    ) -> Result<(), TdError> {
        // Assert current table version is deleted
        let table_versions: Vec<TableDB> = db.current().fetch_all::<TableDB>(&table_name).await?;
        assert_eq!(table_versions.len(), 1);
        assert_eq!(table_versions[0].name, *table_name);
        assert_eq!(table_versions[0].collection_id, collection.id);
        assert_eq!(table_versions[0].defined_by_id, *user_id);
        assert_eq!(table_versions[0].status, TableStatus::Deleted);

        let deleted_table = &table_versions[0];

        // Assert previous table versions exists (first active, then frozen, then deleted)
        let table_versions: Vec<TableDB> = db.live().fetch_all(&table_name).await?;
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
        let function: FunctionDB = db
            .current()
            .fetch_one::<FunctionDB>(&deleted_table.function_id)
            .await?;
        assert_eq!(function.status, FunctionStatus::Active);

        // And assert that all dependant function versions are also frozen
        let dependency_versions: Vec<DependencyDB> = db
            .current()
            .fetch_all::<DependencyDB>(&table_versions[0].table_id)
            .await?;

        for dependency_version in &dependency_versions {
            let function: FunctionDB = db
                .current()
                .fetch_one::<FunctionDB>(&dependency_version.function_id)
                .await?;
            assert_eq!(function.status, FunctionStatus::Frozen);

            // Note that the function version in the dependency still points to the active function version
            // thus being active itself.
            let function_version: FunctionDB = db.live().fetch_one(&function.id).await?;
            assert_eq!(function_version.status, FunctionStatus::Frozen);
        }

        Ok(())
    }

    // Used to assert a table did not get deleted when it shouldn't have
    pub async fn assert_not_deleted(
        db: &TdPool,
        user_id: &UserId,
        collection: &CollectionDB,
        table_name: &TableName,
    ) -> Result<(), TdError> {
        // Assert table does exist
        let table: Option<TableDB> = db.current().fetch_optional::<TableDB>(&table_name).await?;
        assert!(table.is_some());

        // Assert previous table versions exists, always active
        let table_versions: Vec<TableDB> = db.live().fetch_all(&table_name).await?;
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
