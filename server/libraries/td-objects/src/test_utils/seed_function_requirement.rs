//
// Copyright 2025 Tabs Data Inc.
//

use crate::sql::{DaoQueries, Insert};
use crate::types::basic::{DependencyPos, VersionPos};
use crate::types::collection::CollectionDB;
use crate::types::execution::{
    ExecutionDB, FunctionRequirementDB, FunctionRunDB, TableDataVersionDB, TransactionDB,
};
use crate::types::table::TableDB;
use td_database::sql::DbPool;

#[allow(clippy::too_many_arguments)]
pub async fn seed_function_requirement(
    db: &DbPool,
    collection: &CollectionDB,
    execution: &ExecutionDB,
    transaction: &TransactionDB,
    function_run: &FunctionRunDB,
    requirement_table: &TableDB,
    requirement_function_run: Option<&FunctionRunDB>,
    requirement_table_data_version: Option<&TableDataVersionDB>,
    requirement_dependency_pos: Option<&DependencyPos>,
    requirement_version_pos: &VersionPos,
) -> FunctionRequirementDB {
    let function_requirement_db = FunctionRequirementDB::builder()
        .collection_id(collection.id())
        .execution_id(execution.id())
        .transaction_id(transaction.id())
        .function_run_id(function_run.id())
        .requirement_table_id(requirement_table.table_id())
        .requirement_function_version_id(requirement_table.function_version_id())
        .requirement_table_version_id(requirement_table.id())
        .requirement_function_run_id(requirement_function_run.map(|f| *f.id()))
        .requirement_table_data_version_id(requirement_table_data_version.map(|f| *f.id()))
        .requirement_dependency_pos(requirement_dependency_pos.cloned())
        .requirement_version_pos(requirement_version_pos)
        .build()
        .unwrap();

    let queries = DaoQueries::default();
    queries
        .insert::<FunctionRequirementDB>(&function_requirement_db)
        .unwrap()
        .build()
        .execute(db)
        .await
        .unwrap();

    function_requirement_db
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::SelectBy;
    use crate::test_utils::seed_collection::seed_collection;
    use crate::test_utils::seed_execution::seed_execution;
    use crate::test_utils::seed_function::seed_function;
    use crate::test_utils::seed_function_run::seed_function_run;
    use crate::test_utils::seed_table_data_version::seed_table_data_version;
    use crate::test_utils::seed_transaction::seed_transaction;
    use crate::types::basic::{
        BundleId, CollectionName, Decorator, FunctionRunStatus, TableNameDto, TransactionKey,
        UserId,
    };
    use crate::types::function::FunctionRegister;
    use td_database::sql::DbPool;
    use td_security::ENCODED_ID_SYSTEM;

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_seed_function_requirement(db: DbPool) {
        let queries = DaoQueries::default();

        let collection = seed_collection(
            &db,
            &CollectionName::try_from("collection").unwrap(),
            &UserId::try_from(ENCODED_ID_SYSTEM).unwrap(),
        )
        .await;

        let create = FunctionRegister::builder()
            .try_name("joaquin")
            .unwrap()
            .try_description("function_foo description")
            .unwrap()
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")
            .unwrap()
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(vec![TableNameDto::try_from("table_1").unwrap()])
            .try_runtime_values("mock runtime values")
            .unwrap()
            .reuse_frozen_tables(false)
            .build()
            .unwrap();

        let function = seed_function(&db, &collection, &create).await;

        let execution = seed_execution(&db, &function).await;

        let transaction_key = TransactionKey::try_from("ANY").unwrap();
        let transaction = seed_transaction(&db, &execution, &transaction_key).await;

        let function_run = seed_function_run(
            &db,
            &collection,
            &function,
            &execution,
            &transaction,
            &FunctionRunStatus::Scheduled,
        )
        .await;

        let requirement_tables = queries
            .select_by::<TableDB>(&(function.id()))
            .unwrap()
            .build_query_as()
            .fetch_all(&db)
            .await
            .unwrap();
        assert_eq!(requirement_tables.len(), 1);
        let requirement_table = &requirement_tables[0];

        let requirement_table_data_version = seed_table_data_version(
            &db,
            &collection,
            &execution,
            &transaction,
            &function_run,
            requirement_table,
        )
        .await;

        let create = FunctionRegister::builder()
            .try_name("joaquin_2")
            .unwrap()
            .try_description("function_foo description")
            .unwrap()
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")
            .unwrap()
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(None)
            .try_runtime_values("mock runtime values")
            .unwrap()
            .reuse_frozen_tables(false)
            .build()
            .unwrap();

        let function = seed_function(&db, &collection, &create).await;

        let execution = seed_execution(&db, &function).await;

        let transaction_key = TransactionKey::try_from("ANY").unwrap();
        let transaction = seed_transaction(&db, &execution, &transaction_key).await;

        let requirement_function_run = seed_function_run(
            &db,
            &collection,
            &function,
            &execution,
            &transaction,
            &FunctionRunStatus::Scheduled,
        )
        .await;

        let requirement = seed_function_requirement(
            &db,
            &collection,
            &execution,
            &transaction,
            &function_run,
            requirement_table,
            Some(&requirement_function_run),
            Some(&requirement_table_data_version),
            Some(&DependencyPos::try_from(0).unwrap()),
            &VersionPos::try_from(0).unwrap(),
        )
        .await;

        assert_eq!(requirement.collection_id(), collection.id());
        assert_eq!(requirement.execution_id(), execution.id());
        assert_eq!(requirement.transaction_id(), transaction.id());
        assert_eq!(requirement.function_run_id(), function_run.id());
        assert_eq!(
            requirement.requirement_table_id(),
            requirement_table.table_id()
        );
        assert_eq!(
            requirement.requirement_function_version_id(),
            requirement_table.function_version_id()
        );
        assert_eq!(
            requirement.requirement_table_version_id(),
            requirement_table.id()
        );
        assert_eq!(
            *requirement.requirement_function_run_id(),
            Some(*requirement_function_run.id())
        );
        assert_eq!(
            *requirement.requirement_table_data_version_id(),
            Some(*requirement_table_data_version.id())
        );
        assert_eq!(
            *requirement.requirement_dependency_pos(),
            Some(DependencyPos::try_from(0).unwrap())
        );
        assert_eq!(
            requirement.requirement_version_pos(),
            &VersionPos::try_from(0).unwrap()
        );
    }
}
