//
// Copyright 2025 Tabs Data Inc.
//

use crate::{DB_VERSION_NAME, DB_VERSION_VALUE};
use sqlx::SqlitePool;

#[tokio::test]
async fn test_version() {
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

    // Check the database version matches the one computed
    let database_version: String =
        sqlx::QueryBuilder::new("SELECT value FROM tabsdata_system WHERE name = ")
            .push_bind(DB_VERSION_NAME)
            .build_query_scalar()
            .fetch_one(&pool)
            .await
            .unwrap();
    let database_version = database_version.parse::<usize>().unwrap();

    assert_eq!(
        database_version, *DB_VERSION_VALUE,
        "Expected database version to match the latest migration version. Set the 'vX' file name
        according to the version set in the migration (i.e. XXX_v1.up.sql should set
        {DB_VERSION_NAME} to 1)."
    );
}
