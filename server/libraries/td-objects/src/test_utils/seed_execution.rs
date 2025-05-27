//
// Copyright 2025 Tabs Data Inc.
//

use crate::sql::{DaoQueries, Insert};
use crate::types::basic::{ExecutionName, TriggeredOn, UserId};
use crate::types::execution::ExecutionDB;
use crate::types::function::FunctionDB;
use td_database::sql::DbPool;

pub async fn seed_execution(db: &DbPool, function_version: &FunctionDB) -> ExecutionDB {
    let execution_db = ExecutionDB::builder()
        .name(ExecutionName::try_from("test_execution").unwrap())
        .function_version_id(function_version.id())
        .triggered_on(TriggeredOn::now().await)
        .triggered_by_id(UserId::admin())
        .build()
        .unwrap();

    let queries = DaoQueries::default();
    queries
        .insert::<ExecutionDB>(&execution_db)
        .unwrap()
        .build()
        .execute(db)
        .await
        .unwrap();

    execution_db
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::seed_collection::seed_collection;
    use crate::test_utils::seed_function::seed_function;
    use crate::types::basic::{BundleId, CollectionName, Decorator, UserId};
    use crate::types::function::FunctionRegister;
    use td_database::sql::DbPool;
    use td_security::ENCODED_ID_SYSTEM;

    #[td_test::test(sqlx)]
    async fn test_seed_execution(db: DbPool) {
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

        let function_version = seed_function(&db, &collection, &create).await;

        let execution = seed_execution(&db, &function_version).await;
        assert_eq!(
            *execution.name(),
            Some(ExecutionName::try_from("test_execution").unwrap())
        );
        assert_eq!(execution.collection_id(), collection.id());
        assert_eq!(execution.function_version_id(), function_version.id());
        assert_eq!(
            **execution.triggered_by_id(),
            **function_version.defined_by_id()
        );
        assert!(*execution.triggered_on() < TriggeredOn::now().await);
    }
}
