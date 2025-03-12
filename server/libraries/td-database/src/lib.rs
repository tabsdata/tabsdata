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
    use td_security::{
        ENCODED_ID_ROLE_SEC_ADMIN, ENCODED_ID_ROLE_SYS_ADMIN, ENCODED_ID_ROLE_USER,
        ENCODED_ID_USER_ADMIN, ENCODED_ID_USER_ROLE_ADMIN_SEC_ADMIN,
        ENCODED_ID_USER_ROLE_ADMIN_SYS_ADMIN, ENCODED_ID_USER_ROLE_ADMIN_USER,
    };
    use testdir::testdir;

    #[tokio::test]
    async fn test_tabsdata_db_schema_creation() {
        let db_file = testdir!().join("test.db").to_str().map(str::to_string);
        let config = SqliteConfigBuilder::default().url(db_file).build().unwrap();
        assert!(!crate::db(&config).await.unwrap().is_closed());
    }

    #[tokio::test]
    async fn test_tabsdata_db_defaults() {
        let db = crate::test_utils::db().await.unwrap();
        let mut conn = db.acquire().await.unwrap();

        #[derive(sqlx::FromRow)]
        struct Value {
            id: String,
        }

        let row: Value = sqlx::query_as("SELECT id FROM users WHERE name = 'admin'")
            .fetch_one(&mut *conn)
            .await
            .unwrap();
        assert_eq!(row.id, ENCODED_ID_USER_ADMIN);

        let row: Value = sqlx::query_as("SELECT id FROM roles WHERE name = 'sys_admin'")
            .fetch_one(&mut *conn)
            .await
            .unwrap();
        assert_eq!(row.id, ENCODED_ID_ROLE_SYS_ADMIN);

        let row: Value = sqlx::query_as("SELECT id FROM roles WHERE name = 'sec_admin'")
            .fetch_one(&mut *conn)
            .await
            .unwrap();
        assert_eq!(row.id, ENCODED_ID_ROLE_SEC_ADMIN);

        let row: Value = sqlx::query_as("SELECT id FROM roles WHERE name = 'user'")
            .fetch_one(&mut *conn)
            .await
            .unwrap();
        assert_eq!(row.id, ENCODED_ID_ROLE_USER);

        let row: Value =
            sqlx::query_as("SELECT id FROM users_roles WHERE user_id = ?1 AND role_id = ?2")
                .bind(ENCODED_ID_USER_ADMIN)
                .bind(ENCODED_ID_ROLE_SYS_ADMIN)
                .fetch_one(&mut *conn)
                .await
                .unwrap();
        assert_eq!(row.id, ENCODED_ID_USER_ROLE_ADMIN_SYS_ADMIN);

        let row: Value =
            sqlx::query_as("SELECT id FROM users_roles WHERE user_id = ?1 AND role_id = ?2")
                .bind(ENCODED_ID_USER_ADMIN)
                .bind(ENCODED_ID_ROLE_SEC_ADMIN)
                .fetch_one(&mut *conn)
                .await
                .unwrap();
        assert_eq!(row.id, ENCODED_ID_USER_ROLE_ADMIN_SEC_ADMIN);

        let row: Value =
            sqlx::query_as("SELECT id FROM users_roles WHERE user_id = ?1 AND role_id = ?2")
                .bind(ENCODED_ID_USER_ADMIN)
                .bind(ENCODED_ID_ROLE_USER)
                .fetch_one(&mut *conn)
                .await
                .unwrap();
        assert_eq!(row.id, ENCODED_ID_USER_ROLE_ADMIN_USER);
    }
}
