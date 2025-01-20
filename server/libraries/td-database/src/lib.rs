//
// Copyright 2025 Tabs Data Inc.
//

use crate::sql::{DbPool, DbSchema, SqliteConfig};

pub mod sql;
pub mod test_utils;

/// Creates a connection pool for the `tabsdata` database.
///
/// If the database does not exist, it is created.
/// Also, If the schema is out of date, it is updated.
pub async fn db(config: &SqliteConfig) -> Result<DbPool, sql::DbError> {
    db_with_schema(config, td_schema::schema()).await
}

pub async fn db_with_schema(
    config: &SqliteConfig,
    schema: &'static DbSchema,
) -> Result<DbPool, sql::DbError> {
    DbPool::new(config, schema).await
}

#[cfg(test)]
mod tests {
    use crate::sql::SqliteConfigBuilder;
    use testdir::testdir;

    #[tokio::test]
    async fn test_tabsdata_db_schema_creation() {
        let db_file = testdir!().join("test.db").to_str().map(str::to_string);
        let config = SqliteConfigBuilder::default().url(db_file).build().unwrap();
        assert!(!crate::db(&config).await.unwrap().is_closed());
    }
}
