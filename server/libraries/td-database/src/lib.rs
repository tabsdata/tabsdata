//
// Copyright 2025 Tabs Data Inc.
//

use crate::sql::{DbPool, DbSchema, SqliteConfig};

pub mod sql;
pub mod test_utils;

/// Creates a connection pool for the `tabsdata` database.
///
/// If the database does not exist, it is created.
pub async fn db(config: &SqliteConfig) -> Result<DbPool, sql::DbError> {
    db_with_schema(config, td_schema::schema()).await
}

pub async fn db_with_schema(
    config: &SqliteConfig,
    schema: &'static DbSchema,
) -> Result<DbPool, sql::DbError> {
    DbPool::connect(config, schema).await
}

#[cfg(test)]
mod tests {
    use crate::sql::SqliteConfigBuilder;
    use td_security::{
        ENCODED_ID_CA_ALL_SEC_ADMIN, ENCODED_ID_CA_ALL_SYS_ADMIN, ENCODED_ID_CD_ALL_SYS_ADMIN,
        ENCODED_ID_CD_ALL_USER, ENCODED_ID_CR_ALL_SYS_ADMIN, ENCODED_ID_CR_ALL_USER,
        ENCODED_ID_CX_ALL_SYS_ADMIN, ENCODED_ID_CX_ALL_USER, ENCODED_ID_ROLE_SEC_ADMIN,
        ENCODED_ID_ROLE_SYS_ADMIN, ENCODED_ID_ROLE_USER, ENCODED_ID_SA_SYS_ADMIN,
        ENCODED_ID_SS_SEC_ADMIN, ENCODED_ID_SS_SYS_ADMIN, ENCODED_ID_USER_ADMIN,
        ENCODED_ID_USER_ROLE_ADMIN_SEC_ADMIN, ENCODED_ID_USER_ROLE_ADMIN_SYS_ADMIN,
        ENCODED_ID_USER_ROLE_ADMIN_USER,
    };
    use testdir::testdir;

    #[tokio::test]
    async fn test_tabsdata_db_schema_creation() {
        let db_file = testdir!().join("test.db").to_str().map(str::to_string);
        let config = SqliteConfigBuilder::default().url(db_file).build().unwrap();
        let db = crate::db(&config).await.unwrap();
        assert!(db.upgrade().await.is_ok());
        assert!(db.check().await.is_ok());
    }

    #[derive(sqlx::FromRow)]
    struct Value {
        id: String,
    }

    #[tokio::test]
    async fn test_tabsdata_db_default_users() {
        let db = crate::test_utils::db().await.unwrap();
        let mut conn = db.acquire().await.unwrap();

        let row: Value = sqlx::query_as("SELECT id FROM users WHERE name = 'admin'")
            .fetch_one(&mut *conn)
            .await
            .unwrap();
        assert_eq!(row.id, ENCODED_ID_USER_ADMIN);
    }

    #[tokio::test]
    async fn test_tabsdata_db_default_roles() {
        let db = crate::test_utils::db().await.unwrap();
        let mut conn = db.acquire().await.unwrap();

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
    }

    #[tokio::test]
    async fn test_tabsdata_db_default_user_roles() {
        let db = crate::test_utils::db().await.unwrap();
        let mut conn = db.acquire().await.unwrap();

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

    #[tokio::test]
    async fn test_tabsdata_db_default_permissions() {
        let db = crate::test_utils::db().await.unwrap();
        let mut conn = db.acquire().await.unwrap();

        let row: Value = sqlx::query_as(
            r#"
            SELECT id FROM permissions
            WHERE
                role_id = ?1 AND
                permission_type = 'sa' AND
                entity_type = 's' AND
                entity_id = '00000000000000000000000204'
            "#,
        )
        .bind(ENCODED_ID_ROLE_SYS_ADMIN)
        .fetch_one(&mut *conn)
        .await
        .unwrap();
        assert_eq!(row.id, ENCODED_ID_SA_SYS_ADMIN);

        let row: Value = sqlx::query_as(
            r#"
            SELECT id FROM permissions
            WHERE
                role_id = ?1 AND
                permission_type = 'ss' AND
                entity_type = 's' AND
                entity_id = '00000000000000000000000204'
            "#,
        )
        .bind(ENCODED_ID_ROLE_SYS_ADMIN)
        .fetch_one(&mut *conn)
        .await
        .unwrap();
        assert_eq!(row.id, ENCODED_ID_SS_SYS_ADMIN);

        let row: Value = sqlx::query_as(
            r#"
            SELECT id FROM permissions
            WHERE
                role_id = ?1 AND
                permission_type = 'ca' AND
                entity_type = 'c' AND
                entity_id = '00000000000000000000000204'
            "#,
        )
        .bind(ENCODED_ID_ROLE_SYS_ADMIN)
        .fetch_one(&mut *conn)
        .await
        .unwrap();
        assert_eq!(row.id, ENCODED_ID_CA_ALL_SYS_ADMIN);

        let row: Value = sqlx::query_as(
            r#"
            SELECT id FROM permissions
            WHERE
                role_id = ?1 AND
                permission_type = 'cd' AND
                entity_type = 'c' AND
                entity_id = '00000000000000000000000204'
            "#,
        )
        .bind(ENCODED_ID_ROLE_SYS_ADMIN)
        .fetch_one(&mut *conn)
        .await
        .unwrap();
        assert_eq!(row.id, ENCODED_ID_CD_ALL_SYS_ADMIN);

        let row: Value = sqlx::query_as(
            r#"
            SELECT id FROM permissions
            WHERE
                role_id = ?1 AND
                permission_type = 'cx' AND
                entity_type = 'c' AND
                entity_id = '00000000000000000000000204'
            "#,
        )
        .bind(ENCODED_ID_ROLE_SYS_ADMIN)
        .fetch_one(&mut *conn)
        .await
        .unwrap();
        assert_eq!(row.id, ENCODED_ID_CX_ALL_SYS_ADMIN);

        let row: Value = sqlx::query_as(
            r#"
            SELECT id FROM permissions
            WHERE
                role_id = ?1 AND
                permission_type = 'cr' AND
                entity_type = 'c' AND
                entity_id = '00000000000000000000000204'
            "#,
        )
        .bind(ENCODED_ID_ROLE_SYS_ADMIN)
        .fetch_one(&mut *conn)
        .await
        .unwrap();
        assert_eq!(row.id, ENCODED_ID_CR_ALL_SYS_ADMIN);

        let row: Value = sqlx::query_as(
            r#"
            SELECT id FROM permissions
            WHERE
                role_id = ?1 AND
                permission_type = 'ss' AND
                entity_type = 's' AND
                entity_id = '00000000000000000000000204'
            "#,
        )
        .bind(ENCODED_ID_ROLE_SEC_ADMIN)
        .fetch_one(&mut *conn)
        .await
        .unwrap();
        assert_eq!(row.id, ENCODED_ID_SS_SEC_ADMIN);

        let row: Value = sqlx::query_as(
            r#"
            SELECT id FROM permissions
            WHERE
                role_id = ?1 AND
                permission_type = 'ca' AND
                entity_type = 'c' AND
                entity_id = '00000000000000000000000204'
            "#,
        )
        .bind(ENCODED_ID_ROLE_SEC_ADMIN)
        .fetch_one(&mut *conn)
        .await
        .unwrap();
        assert_eq!(row.id, ENCODED_ID_CA_ALL_SEC_ADMIN);

        let row: Value = sqlx::query_as(
            r#"
            SELECT id FROM permissions
            WHERE
                role_id = ?1 AND
                permission_type = 'cd' AND
                entity_type = 'c' AND
                entity_id = '00000000000000000000000204'
            "#,
        )
        .bind(ENCODED_ID_ROLE_USER)
        .fetch_one(&mut *conn)
        .await
        .unwrap();
        assert_eq!(row.id, ENCODED_ID_CD_ALL_USER);

        let row: Value = sqlx::query_as(
            r#"
            SELECT id FROM permissions
            WHERE
                role_id = ?1 AND
                permission_type = 'cx' AND
                entity_type = 'c' AND
                entity_id = '00000000000000000000000204'
            "#,
        )
        .bind(ENCODED_ID_ROLE_USER)
        .fetch_one(&mut *conn)
        .await
        .unwrap();
        assert_eq!(row.id, ENCODED_ID_CX_ALL_USER);

        let row: Value = sqlx::query_as(
            r#"
            SELECT id FROM permissions
            WHERE
                role_id = ?1 AND
                permission_type = 'cr' AND
                entity_type = 'c' AND
                entity_id = '00000000000000000000000204'
            "#,
        )
        .bind(ENCODED_ID_ROLE_USER)
        .fetch_one(&mut *conn)
        .await
        .unwrap();
        assert_eq!(row.id, ENCODED_ID_CR_ALL_USER);
    }
}
