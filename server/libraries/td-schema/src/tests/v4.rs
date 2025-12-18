//
// Copyright 2025 Tabs Data Inc.
//

use crate::tests::run_migration_test;
use sqlx::SqlitePool;

#[tokio::test]
async fn test_functions() {
    let target_version = 4;

    async fn setup(_pool: &SqlitePool) {}

    async fn pre_migration(pool: &SqlitePool) {
        let columns: Vec<(String,)> = sqlx::query_as("PRAGMA table_info(functions)")
            .fetch_all(pool)
            .await
            .unwrap()
            .into_iter()
            .map(|row: (i64, String, String, i64, Option<String>, i64)| (row.1,))
            .collect();

        let column_names: Vec<String> = columns.into_iter().map(|(name,)| name).collect();
        assert!(
            !column_names.contains(&"enabled".to_string()),
            "Did not expect 'enabled' column in 'functions' table before migration"
        );

        let columns: Vec<(String, i64)> = sqlx::query_as("PRAGMA table_info(workers)")
            .fetch_all(pool)
            .await
            .unwrap()
            .into_iter()
            .map(|row: (i64, String, String, i64, Option<String>, i64)| (row.1, row.3))
            .collect();

        for (name, notnull) in columns {
            if name == "execution_id" || name == "transaction_id" || name == "function_run_id" {
                assert_eq!(notnull, 1, "Expected column {} to be NOT NULL", name);
            }
        }

        let fks: Vec<String> = sqlx::query_as("PRAGMA foreign_key_list(workers)")
            .fetch_all(pool)
            .await
            .unwrap()
            .into_iter()
            .map(|row: (i64, i64, String, String, String, String, String, String)| row.2)
            .collect();

        assert!(
            fks.contains(&"function_runs".to_string()),
            "Expected foreign key for function_runs"
        );
    }

    async fn post_migration(pool: &SqlitePool) {
        let columns: Vec<(String,)> = sqlx::query_as("PRAGMA table_info(functions)")
            .fetch_all(pool)
            .await
            .unwrap()
            .into_iter()
            .map(|row: (i64, String, String, i64, Option<String>, i64)| (row.1,))
            .collect();

        let column_names: Vec<String> = columns.into_iter().map(|(name,)| name).collect();
        assert!(
            column_names.contains(&"enabled".to_string()),
            "Expected 'enabled' column in 'functions' table after migration"
        );

        let columns: Vec<(String, i64)> = sqlx::query_as("PRAGMA table_info(workers)")
            .fetch_all(pool)
            .await
            .unwrap()
            .into_iter()
            .map(|row: (i64, String, String, i64, Option<String>, i64)| (row.1, row.3))
            .collect();

        for (name, notnull) in columns {
            if name == "execution_id" || name == "transaction_id" || name == "function_run_id" {
                assert_eq!(notnull, 0, "Expected column {} to be NULLABLE", name);
            }
        }

        let fks: Vec<String> = sqlx::query_as("PRAGMA foreign_key_list(workers)")
            .fetch_all(pool)
            .await
            .unwrap()
            .into_iter()
            .map(|row: (i64, i64, String, String, String, String, String, String)| row.2)
            .collect();

        assert!(
            !fks.contains(&"function_runs".to_string()),
            "Did not expect foreign key for function_runs"
        );
    }

    run_migration_test(target_version, setup, pre_migration, post_migration).await;
}
