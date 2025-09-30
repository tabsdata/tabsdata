//
// Copyright 2025 Tabs Data Inc.
//

mod base;
mod v1;
mod v2;

use sqlx::migrate::Migrator;
use sqlx::sqlite::SqliteRow;
use sqlx::{Row, SqlitePool};

#[derive(Debug)]
pub(crate) struct VersionMigrationTestSetup {
    pool: SqlitePool,
    target_migrator: Migrator,
}

/// Returns a db pool with all migrations up to target version applied, not included.
/// Also returns the up and down migration for the target version.
/// The target version migration is not applied, and is left to the caller to apply and test.
pub(crate) async fn version_migration_setup(target_version: i64) -> VersionMigrationTestSetup {
    // Ephemeral test database
    let pool = SqlitePool::connect("sqlite::memory:")
        .await
        .expect("Failed to connect to in-memory SQLite");

    // Load all migrations at runtime
    let mut migrator = sqlx::migrate!("resources/schemas/tabsdata/live");
    let migrations = migrator.migrations.to_mut();

    // Filter migrations to keep only those up to the target version
    // We iterate in reverse order because migrations are part of the next version.
    let mut target_migrations = Vec::new();
    let mut apply_before = Vec::new();
    let mut current_version = -1;

    for m in migrations.drain(..).rev() {
        if m.description.starts_with('v') {
            // Update current version only on version migrations
            let version_str = m.description.strip_prefix('v').unwrap();
            current_version = version_str.parse::<i64>().unwrap();
        }

        if current_version < target_version {
            apply_before.push(m);
        } else if current_version == target_version {
            target_migrations.push(m);
        } else {
            // Ignore migrations after the target
            continue;
        }
    }
    // Reverse back to original order
    apply_before.reverse();
    target_migrations.reverse();

    // Keep only the migrations before the target in the migrator
    *migrations = apply_before;

    // Apply migrations before the target
    migrator
        .run(&pool)
        .await
        .expect("Failed to apply migrations");

    // And then, modify with the migrations left
    let mut target_migrator = sqlx::migrate!("resources/schemas/tabsdata/live");
    target_migrator.set_ignore_missing(true);
    let migrations = target_migrator.migrations.to_mut();
    *migrations = target_migrations;

    VersionMigrationTestSetup {
        pool,
        target_migrator,
    }
}

/// Helper to run a migration test with pre- and post-lambdas.
/// It automatically checks that the database version is correctly set up.
pub async fn run_migration_test<Pre, Post>(target_version: i64, pre: Pre, post: Post)
where
    Pre: AsyncFn(&SqlitePool),
    Post: AsyncFn(&SqlitePool),
{
    let VersionMigrationTestSetup {
        pool,
        target_migrator,
    } = version_migration_setup(target_version).await;

    // Run pre-migration checks
    assert_eq!(
        db_version(&pool).await,
        target_version - 1,
        "Pre-migration version check failed"
    );
    pre(&pool).await;

    // Apply up migration
    target_migrator
        .run(&pool)
        .await
        .expect("Failed to apply target migration");

    // Run post-migration checks
    assert_eq!(
        db_version(&pool).await,
        target_version,
        "Post-migration version check failed"
    );
    post(&pool).await;

    // Apply down migration
    target_migrator
        .undo(&pool, 0)
        .await
        .expect("Failed to undo target migration");

    // Optionally re-run pre-migration checks
    assert_eq!(
        db_version(&pool).await,
        target_version - 1,
        "Pre-migration version check failed"
    );
    pre(&pool).await;
}

async fn db_version(pool: &SqlitePool) -> i64 {
    let version: SqliteRow =
        sqlx::query("SELECT value FROM tabsdata_system WHERE name = 'db_version'")
            .fetch_one(pool)
            .await
            .unwrap();
    let version: String = version.get(0);
    version.parse().unwrap()
}
