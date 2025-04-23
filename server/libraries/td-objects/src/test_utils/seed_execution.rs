//
// Copyright 2025 Tabs Data Inc.
//

use crate::sql::{DaoQueries, Insert};
use crate::test_utils::seed_user::admin_user;
use crate::types::basic::{ExecutionName, TriggeredOn, UserId};
use crate::types::collection::CollectionDB;
use crate::types::execution::ExecutionDB;
use crate::types::function::FunctionVersionDB;
use td_database::sql::DbPool;

pub async fn seed_execution(
    db: &DbPool,
    collection: &CollectionDB,
    function_version: &FunctionVersionDB,
) -> ExecutionDB {
    let admin_id = admin_user(db).await;
    let admin_id = UserId::try_from(admin_id).unwrap();

    let execution_db = ExecutionDB::builder()
        .name(ExecutionName::try_from("test_execution").unwrap())
        .collection_id(collection.id())
        .function_version_id(function_version.id())
        .triggered_on(TriggeredOn::now().await)
        .triggered_by_id(admin_id)
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
    use crate::test_utils::seed_collection2::seed_collection;
    use crate::test_utils::seed_function2::seed_function;
    use crate::types::basic::BundleId;
    use crate::types::basic::CollectionName;
    use crate::types::basic::UserId;
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
            .dependencies(dependencies)
            .triggers(triggers)
            .tables(tables)
            .try_runtime_values("mock runtime values")
            .unwrap()
            .reuse_frozen_tables(false)
            .build()
            .unwrap();

        let (_, function_version) = seed_function(&db, &collection, &create).await;

        let execution = seed_execution(
            &db,
            &collection,
            &function_version,
            &ExecutionStatus::Scheduled,
        )
        .await;
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
        assert_eq!(*execution.started_on(), None);
        assert_eq!(*execution.ended_on(), None);
        assert_eq!(*execution.status(), ExecutionStatus::Scheduled);
    }
}
