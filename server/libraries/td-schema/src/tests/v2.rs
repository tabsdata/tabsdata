//
// Copyright 2025 Tabs Data Inc.
//

use crate::tests::run_migration_test;
use sqlx::SqlitePool;

#[tokio::test]
async fn test_functions() {
    let target_version = 2;

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
            !column_names.contains(&"connector".to_string()),
            "Did not expect 'connector' column in 'functions' table before migration"
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
            column_names.contains(&"connector".to_string()),
            "Expected 'connector' column in 'functions' table after migration"
        );
    }

    run_migration_test(target_version, pre_migration, post_migration).await;
}
