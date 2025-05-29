//
// Copyright 2025 Tabs Data Inc.
//

use crate::sql::SqliteConfigBuilder;
use crate::{db_with_schema, sql, DbPool, SqliteConfig};

/// Creates a connection pool for the `tabsdata` database.
pub async fn db() -> Result<DbPool, sql::DbError> {
    let db = db_with_schema(&test_config(), td_schema::schema()).await?;
    db.update_db_version().await?;
    db.check_db_version().await?;
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

/// Returns the user and role ids for the given username.
pub async fn user_role_ids(conn: &DbPool, user_name: &str) -> (String, String) {
    let user_id = sqlx::query_scalar("SELECT id FROM users WHERE name = ?")
        .bind(user_name)
        .fetch_one(conn)
        .await
        .unwrap();
    (user_id, "role_id".to_string())
}
