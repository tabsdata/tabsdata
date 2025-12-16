//
// Copyright 2025 Tabs Data Inc.
//

use crate::tests::run_migration_test;
use sqlx::SqlitePool;

#[tokio::test]
async fn test_functions() {
    let target_version = 3;

    async fn setup(pool: &SqlitePool) {
        // test function_id value
        sqlx::query(
            "INSERT INTO collections values('coll_id', 'coll_name', 'desc', 'c_on', 'c_by', 'm_on', 'm_by', NULL)",
        )
            .execute(pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO functions values(
                             'f_id', 'coll_id', 'name', 'desc', 'dec', 'runtime',
                             'FUNCTION_ID', 'data', 'storage', 'bundle', 'snippet', 'defined_on',
                             'defined_by', 'status', 'connector'
                             )",
        )
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO executions values('ex_id', 'ex_name', 'coll_id', 'f_id', 't_on', 't_by')",
        )
        .execute(pool)
        .await
        .unwrap();
    }

    async fn pre_migration(pool: &SqlitePool) {
        let columns: Vec<(String,)> = sqlx::query_as("PRAGMA table_info(executions)")
            .fetch_all(pool)
            .await
            .unwrap()
            .into_iter()
            .map(|row: (i64, String, String, i64, Option<String>, i64)| (row.1,))
            .collect();

        let column_names: Vec<String> = columns.into_iter().map(|(name,)| name).collect();
        assert!(
            !column_names.contains(&"recursion_options".to_string()),
            "Did not expect 'recursion_options' column in 'executions' table before migration"
        );
        assert!(
            !column_names.contains(&"function_id".to_string()),
            "Did not expect 'function_id' column in 'executions' table before migration"
        );
    }

    async fn post_migration(pool: &SqlitePool) {
        let columns: Vec<(String,)> = sqlx::query_as("PRAGMA table_info(executions)")
            .fetch_all(pool)
            .await
            .unwrap()
            .into_iter()
            .map(|row: (i64, String, String, i64, Option<String>, i64)| (row.1,))
            .collect();

        let column_names: Vec<String> = columns.into_iter().map(|(name,)| name).collect();
        assert!(
            column_names.contains(&"recursion_options".to_string()),
            "Expected 'recursion_options' column in 'executions' table after migration"
        );
        assert!(
            column_names.contains(&"function_id".to_string()),
            "Expected 'function_id' column in 'executions' table after migration"
        );

        // test function_id value
        let function_id: String =
            sqlx::query_scalar("SELECT function_id FROM executions WHERE id = 'ex_id'")
                .fetch_one(pool)
                .await
                .unwrap();
        assert_eq!(function_id, "FUNCTION_ID");
    }

    run_migration_test(target_version, setup, pre_migration, post_migration).await;
}
