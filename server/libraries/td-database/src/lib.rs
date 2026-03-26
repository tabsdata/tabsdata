//
// Copyright 2025 Tabs Data Inc.
//

#[cfg(not(any(feature = "sqlite", feature = "postgres", feature = "mysql")))]
compile_error!("You must enable at least one database feature: `sqlite`, `postgres` or `mysql`.");

use crate::config::DatabaseConfig;
use crate::error::DbError;
use crate::pool::TdPool;
use sqlx::migrate::Migrator;

pub mod config;
pub mod dynamic;
pub mod error;
pub mod pool;
pub mod sea_query;

#[cfg(feature = "test-utils")]
pub mod test_utils;

/// Creates a connection pool for the `tabsdata` database.
///
/// If the database does not exist, it is created.
pub async fn db(config: &DatabaseConfig) -> Result<TdPool, DbError> {
    db_with_schema(config, td_schema::schema()).await
}

pub async fn db_with_schema(
    config: &DatabaseConfig,
    schema: &'static Migrator,
) -> Result<TdPool, DbError> {
    TdPool::connect(config, schema).await
}

#[cfg(test)]
mod tests {
    use crate::config::DatabaseConfig;
    use crate::raw_query;
    use crate::sea_query::builder::SeaQueryExt;
    use td_security::{
        ENCODED_ID_CA_ALL_SEC_ADMIN, ENCODED_ID_CA_ALL_SYS_ADMIN, ENCODED_ID_CD_ALL_SYS_ADMIN,
        ENCODED_ID_CD_ALL_USER, ENCODED_ID_CR_ALL_SYS_ADMIN, ENCODED_ID_CR_ALL_USER,
        ENCODED_ID_CX_ALL_SYS_ADMIN, ENCODED_ID_CX_ALL_USER, ENCODED_ID_ROLE_SEC_ADMIN,
        ENCODED_ID_ROLE_SYS_ADMIN, ENCODED_ID_ROLE_USER, ENCODED_ID_SA_SYS_ADMIN,
        ENCODED_ID_SS_SEC_ADMIN, ENCODED_ID_SS_SYS_ADMIN, ENCODED_ID_USER_ADMIN,
        ENCODED_ID_USER_ROLE_ADMIN_SEC_ADMIN, ENCODED_ID_USER_ROLE_ADMIN_SYS_ADMIN,
        ENCODED_ID_USER_ROLE_ADMIN_USER,
    };
    use url::Url;

    #[tokio::test]
    async fn test_tabsdata_db_schema_creation() {
        let db_file = tdtestdir!().join("test.db");
        let db_url = Url::from_file_path(db_file).unwrap();
        let config = DatabaseConfig::new(db_url);
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
        let row: Value = raw_query!(db, "SELECT id FROM users WHERE name = 'admin'")
            .build_query_as()
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(row.id, ENCODED_ID_USER_ADMIN);
    }

    #[tokio::test]
    async fn test_tabsdata_db_default_roles() {
        let db = crate::test_utils::db().await.unwrap();
        let mut conn = db.acquire().await.unwrap();

        let row: Value = raw_query!(db, "SELECT id FROM roles WHERE name = 'sys_admin'")
            .build_query_as()
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_eq!(row.id, ENCODED_ID_ROLE_SYS_ADMIN);

        let row: Value = raw_query!(db, "SELECT id FROM roles WHERE name = 'sec_admin'")
            .build_query_as()
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_eq!(row.id, ENCODED_ID_ROLE_SEC_ADMIN);

        let row: Value = raw_query!(db, "SELECT id FROM roles WHERE name = 'user'")
            .build_query_as()
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_eq!(row.id, ENCODED_ID_ROLE_USER);
    }

    #[tokio::test]
    async fn test_tabsdata_db_default_user_roles() {
        let db = crate::test_utils::db().await.unwrap();
        let mut conn = db.acquire().await.unwrap();

        let row: Value = raw_query!(
            db,
            r#"
            SELECT id FROM users_roles
                WHERE
                 user_id = {ENCODED_ID_USER_ADMIN}
                 AND role_id = {ENCODED_ID_ROLE_SYS_ADMIN}
            "#
        )
        .build_query_as()
        .fetch_one(&mut conn)
        .await
        .unwrap();
        assert_eq!(row.id, ENCODED_ID_USER_ROLE_ADMIN_SYS_ADMIN);

        let row: Value = raw_query!(
            db,
            r#"
            SELECT id FROM users_roles
                WHERE
                 user_id = {ENCODED_ID_USER_ADMIN}
                 AND role_id = {ENCODED_ID_ROLE_SEC_ADMIN}
            "#
        )
        .build_query_as()
        .fetch_one(&mut conn)
        .await
        .unwrap();
        assert_eq!(row.id, ENCODED_ID_USER_ROLE_ADMIN_SEC_ADMIN);

        let row: Value = raw_query!(
            db,
            r#"
            SELECT id FROM users_roles
                WHERE
                 user_id = {ENCODED_ID_USER_ADMIN}
                 AND role_id = {ENCODED_ID_ROLE_USER}
            "#
        )
        .build_query_as()
        .fetch_one(&mut conn)
        .await
        .unwrap();
        assert_eq!(row.id, ENCODED_ID_USER_ROLE_ADMIN_USER);
    }

    #[tokio::test]
    async fn test_tabsdata_db_default_permissions() {
        let db = crate::test_utils::db().await.unwrap();
        let mut conn = db.acquire().await.unwrap();

        let row: Value = raw_query!(
            db,
            r#"
            SELECT id FROM permissions
            WHERE
                role_id = {ENCODED_ID_ROLE_SYS_ADMIN} AND
                permission_type = 'sa' AND
                entity_type = 's' AND
                entity_id = '00000000000000000000000204'
            "#
        )
        .build_query_as()
        .fetch_one(&mut conn)
        .await
        .unwrap();
        assert_eq!(row.id, ENCODED_ID_SA_SYS_ADMIN);

        let row: Value = raw_query!(
            db,
            r#"
            SELECT id FROM permissions
            WHERE
                role_id = {ENCODED_ID_ROLE_SYS_ADMIN} AND
                permission_type = 'ss' AND
                entity_type = 's' AND
                entity_id = '00000000000000000000000204'
            "#
        )
        .build_query_as()
        .fetch_one(&mut conn)
        .await
        .unwrap();
        assert_eq!(row.id, ENCODED_ID_SS_SYS_ADMIN);

        let row: Value = raw_query!(
            db,
            r#"
            SELECT id FROM permissions
            WHERE
                role_id = {ENCODED_ID_ROLE_SYS_ADMIN} AND
                permission_type = 'ca' AND
                entity_type = 'c' AND
                entity_id = '00000000000000000000000204'
            "#
        )
        .build_query_as()
        .fetch_one(&mut conn)
        .await
        .unwrap();
        assert_eq!(row.id, ENCODED_ID_CA_ALL_SYS_ADMIN);

        let row: Value = raw_query!(
            db,
            r#"
            SELECT id FROM permissions
            WHERE
                role_id = {ENCODED_ID_ROLE_SYS_ADMIN} AND
                permission_type = 'cd' AND
                entity_type = 'c' AND
                entity_id = '00000000000000000000000204'
            "#
        )
        .build_query_as()
        .fetch_one(&mut conn)
        .await
        .unwrap();
        assert_eq!(row.id, ENCODED_ID_CD_ALL_SYS_ADMIN);

        let row: Value = raw_query!(
            db,
            r#"
            SELECT id FROM permissions
            WHERE
                role_id = {ENCODED_ID_ROLE_SYS_ADMIN} AND
                permission_type = 'cx' AND
                entity_type = 'c' AND
                entity_id = '00000000000000000000000204'
            "#
        )
        .build_query_as()
        .fetch_one(&mut conn)
        .await
        .unwrap();
        assert_eq!(row.id, ENCODED_ID_CX_ALL_SYS_ADMIN);

        let row: Value = raw_query!(
            db,
            r#"
            SELECT id FROM permissions
            WHERE
                role_id = {ENCODED_ID_ROLE_SYS_ADMIN} AND
                permission_type = 'cr' AND
                entity_type = 'c' AND
                entity_id = '00000000000000000000000204'
            "#
        )
        .build_query_as()
        .fetch_one(&mut conn)
        .await
        .unwrap();
        assert_eq!(row.id, ENCODED_ID_CR_ALL_SYS_ADMIN);

        let row: Value = raw_query!(
            db,
            r#"
            SELECT id FROM permissions
            WHERE
                role_id = {ENCODED_ID_ROLE_SEC_ADMIN} AND
                permission_type = 'ss' AND
                entity_type = 's' AND
                entity_id = '00000000000000000000000204'
            "#
        )
        .build_query_as()
        .fetch_one(&mut conn)
        .await
        .unwrap();
        assert_eq!(row.id, ENCODED_ID_SS_SEC_ADMIN);

        let row: Value = raw_query!(
            db,
            r#"
            SELECT id FROM permissions
            WHERE
                role_id = {ENCODED_ID_ROLE_SEC_ADMIN} AND
                permission_type = 'ca' AND
                entity_type = 'c' AND
                entity_id = '00000000000000000000000204'
            "#
        )
        .build_query_as()
        .fetch_one(&mut conn)
        .await
        .unwrap();
        assert_eq!(row.id, ENCODED_ID_CA_ALL_SEC_ADMIN);

        let row: Value = raw_query!(
            db,
            r#"
            SELECT id FROM permissions
            WHERE
                role_id = {ENCODED_ID_ROLE_USER} AND
                permission_type = 'cd' AND
                entity_type = 'c' AND
                entity_id = '00000000000000000000000204'
            "#
        )
        .build_query_as()
        .fetch_one(&mut conn)
        .await
        .unwrap();
        assert_eq!(row.id, ENCODED_ID_CD_ALL_USER);

        let row: Value = raw_query!(
            db,
            r#"
            SELECT id FROM permissions
            WHERE
                role_id = {ENCODED_ID_ROLE_USER} AND
                permission_type = 'cx' AND
                entity_type = 'c' AND
                entity_id = '00000000000000000000000204'
            "#,
        )
        .build_query_as()
        .fetch_one(&mut conn)
        .await
        .unwrap();
        assert_eq!(row.id, ENCODED_ID_CX_ALL_USER);

        let row: Value = raw_query!(
            db,
            r#"
            SELECT id FROM permissions
            WHERE
                role_id = {ENCODED_ID_ROLE_USER} AND
                permission_type = 'cr' AND
                entity_type = 'c' AND
                entity_id = '00000000000000000000000204'
            "#,
        )
        .build_query_as()
        .fetch_one(&mut conn)
        .await
        .unwrap();
        assert_eq!(row.id, ENCODED_ID_CR_ALL_USER);
    }
}
