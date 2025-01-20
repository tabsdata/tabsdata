//
// Copyright 2024 Tabs Data Inc.
//

use sqlx::migrate::Migrator;

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
