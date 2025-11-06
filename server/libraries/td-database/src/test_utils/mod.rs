//
// Copyright 2025 Tabs Data Inc.
//

use crate::sql::SqliteConfigBuilder;
use crate::{DbPool, SqliteConfig, db_with_schema, sql};

/// Creates a connection pool for the `tabsdata` database.
pub async fn db() -> Result<DbPool, sql::DbError> {
    let db = db_with_schema(&test_config(), td_schema::schema()).await?;
    db.upgrade().await?;
    db.check().await?;
    Ok(db)
}

#[cfg(feature = "test-utils")]
/// Creates a connection pool for the `test tabsdata` database.
pub async fn test_db() -> Result<DbPool, sql::DbError> {
    db_with_schema(&test_config(), td_schema::test_schema()).await
}

pub fn test_config() -> SqliteConfig {
    let db_file = testdir::testdir!()
        .join(format!("{}.db", td_common::id::id()))
        .to_str()
        .unwrap()
        .to_string();
    SqliteConfigBuilder::default().url(db_file).build().unwrap()
}
