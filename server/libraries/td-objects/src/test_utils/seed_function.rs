//
// Copyright 2025 Tabs Data Inc.
//

use crate::dxo::collection::CollectionDB;
use crate::dxo::crudl::{ReadRequest, RequestContext};
use crate::dxo::dependency::DependencyDBBuilder;
use crate::dxo::function::{FunctionDB, FunctionDBBuilder, FunctionRegister};
use crate::dxo::table::{TableDBBuilder, TableDBWithNames};
use crate::dxo::trigger::TriggerDBBuilder;
use crate::sql::{DaoQueries, Insert, SelectBy};
use crate::types::basic::{
    AccessTokenId, DataLocation, DependencyPos, DependencyStatus, RoleId, StorageVersion,
    TableFunctionParamPos, TableId, TableName, TableStatus, TriggerStatus, UserId,
};
use td_database::sql::DbPool;

pub async fn seed_function(
    db: &DbPool,
    collection: &CollectionDB,
    function_create: &FunctionRegister,
) -> FunctionDB {
    let request_context: ReadRequest<String> = RequestContext::with(
        AccessTokenId::default(),
        UserId::admin(),
        RoleId::sec_admin(),
    )
    .read("");
    let request_context = request_context.context;

    let queries = DaoQueries::default();

    // Function version builder
    let builder = FunctionDBBuilder::try_from(function_create).unwrap();
    let builder = FunctionDBBuilder::try_from((&request_context, builder)).unwrap();
    let mut builder = FunctionDBBuilder::from((&collection.id, builder));
    let function_db = builder
        .data_location(DataLocation::default())
        .storage_version(StorageVersion::default())
        .build()
        .unwrap();

    // Insert function version
    queries
        .insert(&function_db)
        .unwrap()
        .build()
        .execute(db)
        .await
        .unwrap();

    // Dependencies, tables, triggers
    // Very similar to build_table_versions
    if let Some(tables) = &function_create.tables {
        let builder = TableDBBuilder::try_from(&function_db).unwrap();
        let builder = TableDBBuilder::try_from((&request_context, builder)).unwrap();

        for (pos, table_name) in tables.iter().enumerate() {
            let table = builder
                .clone()
                .table_id(TableId::default())
                .name(TableName::try_from(table_name).unwrap())
                .function_param_pos(Some(TableFunctionParamPos::try_from(pos as i32).unwrap()))
                .status(TableStatus::Active)
                .build()
                .unwrap();

            queries
                .insert(&table)
                .unwrap()
                .build()
                .execute(db)
                .await
                .unwrap();
        }
    }

    // Very similar to build_dependency_versions
    if let Some(dependency_tables) = &function_create.dependencies {
        let builder = DependencyDBBuilder::try_from(&function_db).unwrap();
        let builder = DependencyDBBuilder::try_from((&request_context, builder)).unwrap();

        for (pos, dependency_table) in dependency_tables.iter().enumerate() {
            let (table_collection, table_name) = {
                let collection = dependency_table
                    .collection
                    .clone()
                    .unwrap_or(collection.name.clone());
                (
                    collection,
                    TableName::try_from(dependency_table.table.clone()).unwrap(),
                )
            };

            let table_db: TableDBWithNames = queries
                .select_by::<TableDBWithNames>(&(table_collection, table_name))
                .unwrap()
                .build_query_as()
                .fetch_one(db)
                .await
                .unwrap();

            let dependency = builder
                .clone()
                .table_collection_id(table_db.collection_id)
                .table_function_id(table_db.function_id)
                .table_id(table_db.table_id)
                .table_versions(dependency_table.versions.clone())
                .dep_pos(DependencyPos::try_from(pos as i32).unwrap())
                .status(DependencyStatus::Active)
                .system(false)
                .build()
                .unwrap();

            queries
                .insert(&dependency)
                .unwrap()
                .build()
                .execute(db)
                .await
                .unwrap();
        }
    }

    // Very similar to build_trigger_versions
    if let Some(trigger_tables) = &function_create.triggers {
        let builder = TriggerDBBuilder::try_from(&function_db).unwrap();
        let builder = TriggerDBBuilder::try_from((&request_context, builder)).unwrap();

        for trigger_table in trigger_tables {
            let (table_collection, table_name) = {
                let collection = trigger_table
                    .collection
                    .clone()
                    .unwrap_or(collection.name.clone());
                (
                    collection,
                    TableName::try_from(trigger_table.table.clone()).unwrap(),
                )
            };

            let table_db: TableDBWithNames = queries
                .select_by::<TableDBWithNames>(&(table_collection, table_name))
                .unwrap()
                .build_query_as()
                .fetch_one(db)
                .await
                .unwrap();

            let trigger = builder
                .clone()
                .trigger_by_collection_id(table_db.collection_id)
                .trigger_by_function_id(table_db.function_id)
                .trigger_by_table_id(table_db.table_id)
                .status(TriggerStatus::Active)
                .system(false)
                .build()
                .unwrap();

            queries
                .insert(&trigger)
                .unwrap()
                .build()
                .execute(db)
                .await
                .unwrap();
        }
    }

    function_db
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::SelectBy;
    use crate::test_utils::seed_collection::seed_collection;
    use crate::types::basic::{BundleId, CollectionName, Decorator, UserId};
    use td_security::ENCODED_ID_SYSTEM;

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_seed_function(db: DbPool) {
        let collection = seed_collection(
            &db,
            &CollectionName::try_from("collection").unwrap(),
            &UserId::try_from(ENCODED_ID_SYSTEM).unwrap(),
        )
        .await;

        let dependencies = None;
        let triggers = None;
        let tables = None;

        let create = FunctionRegister::builder()
            .try_name("joaquin")
            .unwrap()
            .try_description("function_foo description")
            .unwrap()
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")
            .unwrap()
            .decorator(Decorator::Publisher)
            .dependencies(dependencies)
            .triggers(triggers)
            .tables(tables)
            .try_runtime_values("mock runtime values")
            .unwrap()
            .reuse_frozen_tables(false)
            .build()
            .unwrap();

        let function = seed_function(&db, &collection, &create).await;

        let found: FunctionDB = DaoQueries::default()
            .select_by::<FunctionDB>(&function.id)
            .unwrap()
            .build_query_as()
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(function.id, found.id);
        assert_eq!(function.collection_id, found.collection_id);
        assert_eq!(function.name, found.name);
        assert_eq!(function.description, found.description);
        assert_eq!(function.runtime_values, found.runtime_values);
        assert_eq!(function.function_id, found.function_id);
        assert_eq!(function.data_location, found.data_location);
        assert_eq!(function.storage_version, found.storage_version);
        assert_eq!(function.bundle_id, found.bundle_id);
        assert_eq!(function.snippet, found.snippet);
        assert_eq!(function.defined_on, found.defined_on);
        assert_eq!(function.defined_by_id, found.defined_by_id);
        assert_eq!(function.status, found.status);
    }
}
