//
// Copyright 2025 Tabs Data Inc.
//

use crate::crudl::{ReadRequest, RequestContext};
use crate::sql::{DaoQueries, Insert, SelectBy, UpdateBy};
use crate::test_utils::seed_user::admin_user;
use crate::types::basic::{DependencyPos, TableFunctionParamPos, TableId};
use crate::types::collection::CollectionDB;
use crate::types::dependency::{DependencyDBBuilder, DependencyVersionDBBuilder};
use crate::types::function::{
    FunctionCreate, FunctionDB, FunctionDBBuilder, FunctionVersionDB, FunctionVersionDBBuilder,
};
use crate::types::table::{TableDB, TableDBBuilder, TableDBWithNames, TableVersionDBBuilder};
use crate::types::trigger::{TriggerDBBuilder, TriggerVersionDBBuilder};
use td_database::sql::DbPool;

pub async fn seed_function(
    db: &DbPool,
    collection: &CollectionDB,
    function_create: &FunctionCreate,
) -> (FunctionDB, FunctionVersionDB) {
    let admin_id = admin_user(db).await;
    let request_context: ReadRequest<String> =
        RequestContext::with(&admin_id, "r", true).await.read("");
    let request_context = request_context.context();

    let queries = DaoQueries::default();

    // Function version builder
    let builder = FunctionVersionDBBuilder::try_from(function_create).unwrap();
    let builder = FunctionVersionDBBuilder::try_from((request_context, builder)).unwrap();
    let builder = FunctionVersionDBBuilder::from((collection.id(), builder));
    let function_db_version = builder.build().unwrap();

    // Create function if non-existent, and update if existent.
    let builder = FunctionDBBuilder::try_from(&function_db_version).unwrap();
    let function_db = builder.build().unwrap();

    let (function_db, function_db_version) = match queries
        .select_by::<FunctionDB>(&(
            function_db_version.collection_id(),
            function_db_version.name(),
        ))
        .unwrap()
        .build_query_as::<FunctionDB>()
        .fetch_optional(db)
        .await
        .unwrap()
    {
        Some(updated) => {
            let function_db = function_db.to_builder().id(updated.id()).build().unwrap();
            queries
                .update_by::<_, FunctionDB>(&function_db, &updated.id())
                .unwrap()
                .build()
                .execute(db)
                .await
                .unwrap();

            let function_db_version = function_db_version
                .to_builder()
                .function_id(function_db.id())
                .build()
                .unwrap();

            (function_db, function_db_version)
        }
        None => {
            queries
                .insert(&function_db)
                .unwrap()
                .build()
                .execute(db)
                .await
                .unwrap();
            (function_db, function_db_version)
        }
    };

    // Insert function version
    queries
        .insert(&function_db_version)
        .unwrap()
        .build()
        .execute(db)
        .await
        .unwrap();

    // Dependencies, tables, triggers
    // Very similar to build_table_versions
    if let Some(tables) = function_create.tables() {
        let builder = TableVersionDBBuilder::try_from(&function_db_version).unwrap();
        let builder = TableVersionDBBuilder::try_from((request_context, builder)).unwrap();

        for (pos, table_name) in tables.iter().enumerate() {
            let table_version = builder
                .clone()
                .table_id(TableId::default())
                .name(table_name)
                .function_param_pos(Some(TableFunctionParamPos::try_from(pos as i16).unwrap()))
                .build()
                .unwrap();

            queries
                .insert(&table_version)
                .unwrap()
                .build()
                .execute(db)
                .await
                .unwrap();

            // Create table if non-existent, and update if existent.
            let builder = TableDBBuilder::try_from(&table_version).unwrap();
            let builder = TableDBBuilder::try_from((&function_db, builder)).unwrap();
            let table = builder.build().unwrap();

            match queries
                .select_by::<TableDB>(&(table_version.collection_id(), table_version.name()))
                .unwrap()
                .build_query_as::<TableDB>()
                .fetch_optional(db)
                .await
                .unwrap()
            {
                Some(updated) => {
                    queries
                        .update_by::<_, TableDB>(&table, &updated.id())
                        .unwrap()
                        .build()
                        .execute(db)
                        .await
                        .unwrap();
                }
                None => {
                    queries
                        .insert(&table)
                        .unwrap()
                        .build()
                        .execute(db)
                        .await
                        .unwrap();
                }
            };
        }
    }

    // Very similar to build_dependency_versions
    if let Some(dependency_tables) = function_create.dependencies() {
        let builder = DependencyVersionDBBuilder::try_from(&function_db_version).unwrap();
        let builder = DependencyVersionDBBuilder::try_from((request_context, builder)).unwrap();

        for (pos, dependency_table) in dependency_tables.iter().enumerate() {
            let (table_collection, table_name) = {
                let collection = dependency_table
                    .collection()
                    .as_ref()
                    .unwrap_or(collection.name());
                (collection, dependency_table.table())
            };

            let table_db: TableDBWithNames = queries
                .select_by::<TableDBWithNames>(&(table_collection, table_name))
                .unwrap()
                .build_query_as()
                .fetch_one(db)
                .await
                .unwrap();

            let dependency_version = builder
                .clone()
                .table_collection_id(table_db.collection_id())
                .table_id(table_db.id())
                .table_name(table_db.name())
                .table_version_id(table_db.table_version_id())
                .table_versions(dependency_table.versions())
                .dep_pos(DependencyPos::try_from(pos as i16).unwrap())
                .build()
                .unwrap();

            queries
                .insert(&dependency_version)
                .unwrap()
                .build()
                .execute(db)
                .await
                .unwrap();

            let builder = DependencyDBBuilder::try_from(&dependency_version).unwrap();
            let dependency = builder.build().unwrap();

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
    if let Some(trigger_tables) = function_create.triggers() {
        let builder = TriggerVersionDBBuilder::try_from(&function_db_version).unwrap();
        let builder = TriggerVersionDBBuilder::try_from((request_context, builder)).unwrap();

        for trigger_table in trigger_tables {
            let (table_collection, table_name) = {
                let collection = trigger_table
                    .collection()
                    .as_ref()
                    .unwrap_or(collection.name());
                (collection, trigger_table.table())
            };

            let table_db: TableDBWithNames = queries
                .select_by::<TableDBWithNames>(&(table_collection, table_name))
                .unwrap()
                .build_query_as()
                .fetch_one(db)
                .await
                .unwrap();

            let dependency_version = builder
                .clone()
                .trigger_by_collection_id(table_db.collection_id())
                .trigger_by_function_id(table_db.function_id())
                .trigger_by_function_version_id(table_db.function_version_id())
                .trigger_by_table_id(table_db.id())
                .build()
                .unwrap();

            queries
                .insert(&dependency_version)
                .unwrap()
                .build()
                .execute(db)
                .await
                .unwrap();

            let builder = TriggerDBBuilder::try_from(&dependency_version).unwrap();
            let trigger = builder.build().unwrap();

            queries
                .insert(&trigger)
                .unwrap()
                .build()
                .execute(db)
                .await
                .unwrap();
        }
    }

    (function_db, function_db_version)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::SelectBy;
    use crate::test_utils::seed_collection2::seed_collection;
    use crate::types::basic::UserId;
    use crate::types::basic::{BundleId, CollectionName};
    use td_security::ENCODED_ID_SYSTEM;

    #[td_test::test(sqlx)]
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

        let create = FunctionCreate::builder()
            .try_name("joaquin")
            .unwrap()
            .try_description("function_foo description")
            .unwrap()
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")
            .unwrap()
            .dependencies(dependencies)
            .triggers(triggers)
            .tables(tables)
            .try_runtime_values("mock runtime values")
            .unwrap()
            .reuse_frozen_tables(false)
            .build()
            .unwrap();

        let (function, function_version) = seed_function(&db, &collection, &create).await;

        let found: FunctionDB = DaoQueries::default()
            .select_by::<FunctionDB>(&(function.id()))
            .unwrap()
            .build_query_as()
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(function.id(), found.id());
        assert_eq!(function.collection_id(), found.collection_id());
        assert_eq!(function.name(), found.name());
        assert_eq!(function.function_version_id(), found.function_version_id());
        assert_eq!(function.frozen(), found.frozen());
        assert_eq!(function.created_on(), found.created_on());
        assert_eq!(function.created_by_id(), found.created_by_id());

        let found: FunctionVersionDB = DaoQueries::default()
            .select_by::<FunctionVersionDB>(&(function_version.id()))
            .unwrap()
            .build_query_as()
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(function_version.id(), found.id());
        assert_eq!(function_version.collection_id(), found.collection_id());
        assert_eq!(function_version.name(), found.name());
        assert_eq!(function_version.description(), found.description());
        assert_eq!(function_version.runtime_values(), found.runtime_values());
        assert_eq!(function_version.function_id(), found.function_id());
        assert_eq!(function_version.data_location(), found.data_location());
        assert_eq!(function_version.storage_version(), found.storage_version());
        assert_eq!(function_version.bundle_id(), found.bundle_id());
        assert_eq!(function_version.snippet(), found.snippet());
        assert_eq!(function_version.defined_on(), found.defined_on());
        assert_eq!(function_version.defined_by_id(), found.defined_by_id());
        assert_eq!(function_version.status(), found.status());
    }
}
