//
// Copyright 2025 Tabs Data Inc.
//

use crate::sql::{DaoQueries, Insert};
use crate::types::basic::TableFunctionParamPos;
use crate::types::collection::CollectionDB;
use crate::types::execution::{ExecutionDB, FunctionRunDB, TableDataVersionDB, TransactionDB};
use crate::types::table::TableVersionDB;
use td_database::sql::DbPool;

pub async fn seed_table_data_version(
    db: &DbPool,
    collection: &CollectionDB,
    execution: &ExecutionDB,
    transaction: &TransactionDB,
    function_run: &FunctionRunDB,
    table: &TableVersionDB,
) -> TableDataVersionDB {
    let queries = DaoQueries::default();

    let table_data_version_db = TableDataVersionDB::builder()
        .collection_id(collection.id())
        .table_id(table.table_id())
        .name(table.name())
        .table_version_id(table.id())
        .function_version_id(table.function_version_id())
        .has_data(None)
        .execution_id(execution.id())
        .transaction_id(transaction.id())
        .function_run_id(function_run.id())
        .function_param_pos(TableFunctionParamPos::try_from(0).unwrap())
        .build()
        .unwrap();

    queries
        .insert::<TableDataVersionDB>(&table_data_version_db)
        .unwrap()
        .build()
        .execute(db)
        .await
        .unwrap();

    table_data_version_db
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::SelectBy;
    use crate::test_utils::seed_collection::seed_collection;
    use crate::test_utils::seed_execution::seed_execution;
    use crate::test_utils::seed_function::seed_function;
    use crate::test_utils::seed_function_run::seed_function_run;
    use crate::test_utils::seed_transaction::seed_transaction;
    use crate::types::basic::{
        BundleId, CollectionName, Decorator, TableName, TransactionKey, UserId,
    };
    use crate::types::execution::FunctionRunStatus;
    use crate::types::function::FunctionRegister;
    use td_database::sql::DbPool;
    use td_security::ENCODED_ID_SYSTEM;

    #[td_test::test(sqlx)]
    async fn test_seed_table_data_version(db: DbPool) {
        let collection = seed_collection(
            &db,
            &CollectionName::try_from("collection").unwrap(),
            &UserId::try_from(ENCODED_ID_SYSTEM).unwrap(),
        )
        .await;

        let table_name = TableName::try_from("table_version").unwrap();

        let dependencies = None;
        let triggers = None;
        let tables = vec![table_name.clone()];

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

        let (_, function_version) = seed_function(&db, &collection, &create).await;

        let execution = seed_execution(&db, &collection, &function_version).await;

        let transaction_key = TransactionKey::try_from("ANY").unwrap();
        let transaction = seed_transaction(&db, &execution, &transaction_key).await;

        let function_run = seed_function_run(
            &db,
            &collection,
            &function_version,
            &execution,
            &transaction,
            &FunctionRunStatus::Scheduled,
        )
        .await;

        let table_version = DaoQueries::default()
            .select_by::<TableVersionDB>(&(collection.id(), &table_name))
            .unwrap()
            .build_query_as()
            .fetch_one(&db)
            .await
            .unwrap();

        let table_data_version = seed_table_data_version(
            &db,
            &collection,
            &execution,
            &transaction,
            &function_run,
            &table_version,
        )
        .await;

        assert_eq!(table_data_version.collection_id(), collection.id());
        assert_eq!(table_data_version.table_id(), table_version.table_id());
        assert_eq!(table_data_version.table_version_id(), table_version.id());
        assert_eq!(
            table_data_version.function_version_id(),
            function_version.id()
        );
        assert_eq!(*table_data_version.has_data(), None);
        assert_eq!(table_data_version.execution_id(), execution.id());
        assert_eq!(table_data_version.transaction_id(), transaction.id());
        assert_eq!(table_data_version.function_run_id(), function_run.id());
    }
}
