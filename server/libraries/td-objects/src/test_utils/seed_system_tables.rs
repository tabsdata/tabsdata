//
// Copyright 2025 Tabs Data Inc.
//

use crate::dxo::TryUpdater;
use crate::dxo::crudl::{ReadRequest, RequestContext};
use crate::dxo::dependency::DependencyDBBuilder;
use crate::dxo::function::FunctionDB;
use crate::dxo::table::{TableDB, TableDBBuilder, TableDBWithNames};
use crate::sql::{DaoQueries, Insert, SelectBy};
use crate::system::{SYSTEM_INPUT_TABLE_DEPENDENCY_PREFIXES, SYSTEM_OUTPUT_TABLE_NAMES_PREFIXES};
use crate::table_ref::Versions;
use crate::types::basic::{
    AccessTokenId, DependencyPos, DependencyStatus, RoleId, TableFunctionParamPos, TableId,
    TableName, TableStatus, UserId,
};
use crate::types::composed::TableVersions;
use td_database::sql::DbPool;

// Very similar to seed_function or build_table_versions, but to seed system tables
pub async fn seed_system_tables(db: &DbPool, function: &FunctionDB) -> Vec<TableDB> {
    let request_context: ReadRequest<String> = RequestContext::with(
        AccessTokenId::default(),
        UserId::admin(),
        RoleId::sec_admin(),
    )
    .read("");
    let request_context = request_context.context;

    let queries = DaoQueries::default();

    // System tables
    let mut system_tables = vec![];
    for (pos, prefix) in SYSTEM_OUTPUT_TABLE_NAMES_PREFIXES.iter().enumerate() {
        let mut builder = TableDBBuilder::try_from(function).unwrap();
        let builder = builder.try_update_from(&request_context).unwrap();

        let table = builder
            .clone()
            .table_id(TableId::default())
            .name(TableName::try_from(format!("{prefix}__{}", function.function_id)).unwrap())
            .function_param_pos(Some(
                TableFunctionParamPos::try_from(-(pos as i32 + 1)).unwrap(),
            ))
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

        system_tables.push(table);
    }

    // System dependencies
    for (pos, prefix) in SYSTEM_INPUT_TABLE_DEPENDENCY_PREFIXES.iter().enumerate() {
        let mut builder = DependencyDBBuilder::try_from(function).unwrap();
        let builder = builder.try_update_from(&request_context).unwrap();

        let (table_collection, table_name) = (
            function.collection_id,
            TableName::try_from(format!("{prefix}__{}", function.function_id)).unwrap(),
        );

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
            .table_versions(TableVersions::new(Versions::None))
            .dep_pos(DependencyPos::try_from(-(pos as i32 + 1)).unwrap())
            .status(DependencyStatus::Active)
            .system(true)
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

    system_tables
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dxo::function::FunctionRegister;
    use crate::sql::SelectBy;
    use crate::test_utils::seed_collection::seed_collection;
    use crate::test_utils::seed_function::seed_function;
    use crate::types::basic::{BundleId, CollectionName, Decorator, UserId};
    use td_security::ENCODED_ID_SYSTEM;

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_seed_system_tables(db: DbPool) {
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
        let system_tables = seed_system_tables(&db, &function).await;

        assert_eq!(system_tables.len(), 1);
        for table in system_tables {
            let found: TableDB = DaoQueries::default()
                .select_by::<TableDB>(&table.id)
                .unwrap()
                .build_query_as()
                .fetch_one(&db)
                .await
                .unwrap();
            assert_eq!(table.id, found.id);
            assert_eq!(table.collection_id, found.collection_id);
            assert_eq!(table.table_id, found.table_id);
            assert_eq!(table.name, found.name);
            assert_eq!(table.function_id, found.function_id);
            assert_eq!(table.function_version_id, found.function_version_id);
            assert_eq!(table.function_param_pos, found.function_param_pos);
            assert_eq!(table.private, found.private);
            assert_eq!(table.partitioned, found.partitioned);
            assert_eq!(table.defined_on, found.defined_on);
            assert_eq!(table.defined_by_id, found.defined_by_id);
            assert_eq!(table.status, found.status);
        }
    }
}
