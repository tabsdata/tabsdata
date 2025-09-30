//
// Copyright 2025 Tabs Data Inc.
//

use sqlx::SqlitePool;

#[tokio::test]
async fn test_baseline() {
    // Ephemeral test database
    let pool = SqlitePool::connect("sqlite::memory:")
        .await
        .expect("Failed to connect to in-memory SQLite");

    // Load all migrations at runtime
    let migrator = sqlx::migrate!("resources/schemas/tabsdata/live");
    migrator
        .run(&pool)
        .await
        .expect("Failed to apply all migrations");

    // Assert some required tables exist
    let required_tables = [
        "users",
        "permissions",
        "collections",
        "functions",
        "tables",
        "dependencies",
        "triggers",
    ];
    for table in required_tables {
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?")
                .bind(table)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(count, 1, "Expected table '{}' to exist", table);
    }

    // And then, apply them all again down, so we end up with an empty db
    migrator
        .undo(&pool, 0)
        .await
        .expect("Failed to undo all migrations");

    // Assert that there are no user tables or views left
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT name FROM sqlite_master WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%' AND name != '_sqlx_migrations'"
    )
        .fetch_all(&pool)
        .await
        .unwrap();

    let names: Vec<String> = rows.into_iter().map(|(name,)| name).collect();
    assert!(
        names.is_empty(),
        "Expected no tables or views, found: {}",
        names.join(", ")
    );
}
