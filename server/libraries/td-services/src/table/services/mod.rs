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
    use td_objects::and;
    use td_objects::dxo::collection::ActiveCollectionDB;
    use td_objects::dxo::dependency::DependencyDB;
    use td_objects::dxo::function::FunctionDB;
    use td_objects::dxo::table::{TableDB, TableDBWithNames};
    use td_objects::types::basic::{FunctionStatus, TableIdName, TableName, TableStatus, UserId};
    use td_queries::Queries;
    pub async fn assert_delete(
        db: &TdPool,
        user_id: &UserId,
        collection: &ActiveCollectionDB,
        table_name: &TableName,
    ) -> Result<(), TdError> {
        let live: Vec<TableDB> = db.latest().fetch_all(&table_name).await?;
        assert!(live.is_empty());

        let table_versions: Vec<TableDB> = db.history().fetch_all(&table_name).await?;
        assert_eq!(table_versions.len(), 2);
        for table_version in &table_versions {
            assert_eq!(table_version.base.name, *table_name);
            assert_eq!(table_version.base.collection_id, collection.id);
            assert_eq!(table_version.base.defined_by_id, *user_id);
            assert!(!table_version.valid_to.is_infinity());
        }
        assert_eq!(table_versions[0].base.status, TableStatus::Frozen);
        assert_eq!(table_versions[1].base.status, TableStatus::Active);
        assert_eq!(
            table_versions[0].base.table_id,
            table_versions[1].base.table_id
        );

        let function: FunctionDB = db
            .latest()
            .fetch_one::<FunctionDB>(&table_versions[0].base.function_id)
            .await?;
        assert_eq!(function.base.status, FunctionStatus::Active);

        let dependency_versions: Vec<DependencyDB> = db
            .latest()
            .fetch_all::<DependencyDB>(&table_versions[0].base.table_id)
            .await?;

        for dependency_version in &dependency_versions {
            let function: FunctionDB = db
                .latest()
                .fetch_one::<FunctionDB>(&dependency_version.base.function_id)
                .await?;
            assert_eq!(function.base.status, FunctionStatus::Frozen);

            // Note that the function version in the dependency still points to the active function version
            // thus being active itself.
            let function_version: FunctionDB = db.fetch_one(&function.base.id).await?;
            assert_eq!(function_version.base.status, FunctionStatus::Frozen);
        }

        Ok(())
    }

    pub async fn assert_not_deleted(
        db: &TdPool,
        user_id: &UserId,
        collection: &ActiveCollectionDB,
        table_name: &TableName,
    ) -> Result<(), TdError> {
        let table: Option<TableDBWithNames> = db
            .latest()
            .fetch_optional(and!(&collection.id, &TableIdName::Name(table_name.clone())))
            .await?;
        assert!(table.is_some());

        let table_versions: Vec<TableDB> = db.history().fetch_all(&table_name).await?;
        for table_version in &table_versions {
            assert_eq!(table_version.base.name, *table_name);
            assert_eq!(table_version.base.collection_id, collection.id);
            assert_eq!(table_version.base.defined_by_id, *user_id);
            assert_eq!(table_version.base.status, TableStatus::Active);
        }

        // Function can still be frozen if another table got deleted

        Ok(())
    }
}
