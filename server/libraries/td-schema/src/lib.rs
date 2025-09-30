//
// Copyright 2024 Tabs Data Inc.
//

#[cfg(test)]
mod tests;

use sqlx::migrate::Migrator;
use std::sync::LazyLock;

/// The name of the system table key for database version.
pub const DB_VERSION_NAME: &str = "db_version";
/// Version that the current migration expects to be using the database on.
/// Taken from live migrations' folder.
pub static DB_VERSION_VALUE: LazyLock<usize> = LazyLock::new(|| {
    let migrations = schema();
    let latest = migrations
        .migrations
        .last()
        .expect("No migrations found in the schema");
    let version_str = latest.description.strip_prefix('v').expect(
        r#"
        Latest migration must be a version upgrader starting with description starting with
        'v', with the version number following it. It should only upgrade the version number of the
        database. Other schema changes should be contained in inbetween scripts."#,
    );
    version_str.parse::<usize>().unwrap()
});

/// Returns the schema for the `tabsdata` database.
///
/// The schema is defined in the `src/schemas/tabsdata` directory using Sqlx migration files
/// created with sqlx CLI: `sqlx migrate add --source resources/schemas/tabsdata/live -r <file_name>`.
pub fn schema() -> &'static Migrator {
    static SCHEMA: Migrator = sqlx::migrate!("resources/schemas/tabsdata/live");
    &SCHEMA
}

#[cfg(feature = "test-utils")]
/// Returns the test schema for the `tabsdata` database.
pub fn test_schema() -> &'static Migrator {
    static SCHEMA: Migrator = sqlx::migrate!("resources/schemas/tabsdata/test");
    &SCHEMA
}
