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
mod sample;
mod schema;

#[cfg(test)]
pub(crate) mod tests {
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::dxo::collection::ActiveCollectionDB;
    use td_objects::dxo::dependency::DependencyDB;
    use td_objects::dxo::function::FunctionDB;
    use td_objects::dxo::table::TableDB;
    use td_objects::types::basic::{FunctionStatus, TableName, TableStatus, UserId};
    use td_queries::{CurrentEntry, LiveEntry};
    pub async fn assert_delete(
        db: &TdPool,
        user_id: &UserId,
        collection: &ActiveCollectionDB,
        table_name: &TableName,
    ) -> Result<(), TdError> {
        let table_versions: Vec<TableDB> = db.current().fetch_all::<TableDB>(&table_name).await?;
        assert_eq!(table_versions.len(), 1);
        assert_eq!(table_versions[0].name, *table_name);
        assert_eq!(table_versions[0].collection_id, collection.id);
        assert_eq!(table_versions[0].defined_by_id, *user_id);
        assert_eq!(table_versions[0].status, TableStatus::Deleted);

        let deleted_table = &table_versions[0];

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

        assert_eq!(table_versions[0].table_id, table_versions[1].table_id);
        assert_eq!(table_versions[0].table_id, table_versions[2].table_id);

        let function: FunctionDB = db
            .current()
            .fetch_one::<FunctionDB>(&deleted_table.function_id)
            .await?;
        assert_eq!(function.status, FunctionStatus::Active);

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

    pub async fn assert_not_deleted(
        db: &TdPool,
        user_id: &UserId,
        collection: &ActiveCollectionDB,
        table_name: &TableName,
    ) -> Result<(), TdError> {
        let table: Option<TableDB> = db.current().fetch_optional::<TableDB>(&table_name).await?;
        assert!(table.is_some());

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
