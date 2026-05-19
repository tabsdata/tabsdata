//
// Copyright 2025 Tabs Data Inc.
//

#[cfg(not(any(feature = "sqlite", feature = "postgres", feature = "mysql")))]
compile_error!("You must enable at least one database feature: `sqlite`, `postgres` or `mysql`.");

use crate::config::DatabaseConfig;
use crate::error::DbError;
use crate::pool::TdPool;

pub mod config;
pub mod dynamic;
pub mod error;
pub mod pool;
pub mod sea_query;

#[cfg(feature = "test-utils")]
pub mod test_utils;

/// Creates a connection pool for the `tabsdata` database.
pub async fn db(config: &DatabaseConfig) -> Result<TdPool, DbError> {
    TdPool::connect(config).await
}

#[cfg(test)]
mod tests {
    use crate::config::DatabaseConfig;
    use crate::error::DbError;
    use crate::pool::TdPool;
    use crate::raw_query;
    use crate::sea_query::builder::SeaQueryExt;
    use td_common::tdtestdir;
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

    // -----------------------------------------------------------------------
    // Schema introspection helpers
    // -----------------------------------------------------------------------

    async fn schema_table_names(db: &TdPool) -> Vec<String> {
        let mut names: Vec<String> = raw_query!(
            db,
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
        .build_query_scalar()
        .fetch_all(db)
        .await
        .unwrap();
        names.sort();
        names
    }

    async fn schema_view_names(db: &TdPool) -> Vec<String> {
        let mut names: Vec<String> = raw_query!(
            db,
            "SELECT name FROM sqlite_master WHERE type = 'view' ORDER BY name"
        )
        .build_query_scalar()
        .fetch_all(db)
        .await
        .unwrap();
        names.sort();
        names
    }

    async fn schema_index_names(db: &TdPool) -> Vec<String> {
        let mut names: Vec<String> = raw_query!(
            db,
            "SELECT name FROM sqlite_master WHERE type = 'index' AND name NOT LIKE 'sqlite_autoindex_%' ORDER BY name"
        )
        .build_query_scalar()
        .fetch_all(db)
        .await
        .unwrap();
        names.sort();
        names
    }

    async fn schema_db_version(db: &TdPool) -> String {
        raw_query!(
            db,
            "SELECT value FROM tabsdata_system WHERE name = 'db_version'"
        )
        .build_query_scalar()
        .fetch_one(db)
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn test_tabsdata_db_schema_creation() {
        let db_file = tdtestdir!().join("test.db");
        let db_url = Url::from_file_path(db_file).unwrap();
        let config = DatabaseConfig::new(db_url).unwrap();
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

    // -------------------------------------------------------------------
    // Schema validation tests — use real TdPool / DynamicPool
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn test_schema_tables() {
        let db = crate::test_utils::db().await.unwrap();
        let tables = schema_table_names(&db).await;

        let expected = vec![
            "bundles",
            "collections",
            "dependencies",
            "dependencies_current",
            "executions",
            "function_patches",
            "function_patches_current",
            "functions",
            "functions_current",
            "inter_collection_permissions",
            "logical_function_run_inputs",
            "logical_function_run_pointers",
            "logical_function_runs",
            "logical_table_data_version_pointers",
            "logical_table_data_versions",
            "permissions",
            "physical_function_runs",
            "physical_table_data_versions",
            "roles",
            "sessions",
            "table_partitions",
            "tables",
            "tables_current",
            "tabsdata_system",
            "transactions",
            "triggers",
            "triggers_current",
            "users",
            "users_roles",
            "workers",
        ];
        assert_eq!(tables, expected);
    }

    #[tokio::test]
    async fn test_schema_views() {
        let db = crate::test_utils::db().await.unwrap();
        let views = schema_view_names(&db).await;

        let expected = vec![
            "collections__with_names",
            "collections_active",
            "dependencies__with_names",
            "dependencies_current__with_names",
            "execution_status_summary",
            "executions__with_names",
            "executions__with_status",
            "functions__with_names",
            "functions_current__with_names",
            "global_status_summary",
            "inter_collection_permissions__with_names",
            "logical_function_runs__with_names",
            "permissions__with_names",
            "physical_function_runs__with_names",
            "physical_table_data_versions__with_function",
            "roles__with_names",
            "sessions__with_names",
            "table_data_versions__active",
            "table_data_versions__with_names",
            "tables__with_names",
            "tables_current__with_names",
            "transaction_status_summary",
            "transactions__with_names",
            "transactions__with_status",
            "triggers__read",
            "triggers__with_names",
            "triggers_current__with_names",
            "users__with_names",
            "users_roles__with_names",
            "workers__with_names",
        ];
        assert_eq!(views, expected);
    }

    #[tokio::test]
    async fn test_schema_indices() {
        let db = crate::test_utils::db().await.unwrap();
        let indices = schema_index_names(&db).await;

        let expected = vec![
            "bundles___collection_id___idx",
            "collections___name___idx",
            "dependencies___dependency_id__defined_on__id___idx",
            "dependencies___function_id___idx",
            "dependencies___table_function_id___idx",
            "dependencies_current___function_id___idx",
            "dependencies_current___table_function_id___idx",
            "executions___collection_id___idx",
            "executions___function_version_id___idx",
            "executions___triggered_by_id___idx",
            "executions___triggered_on___idx",
            "function_patches___function_id__defined_on___idx",
            "function_runs___collection_id___idx",
            "function_runs___execution_id__status___idx",
            "function_runs___function_version_id___idx",
            "function_runs___status___idx",
            "function_runs___transaction_id__status___idx",
            "functions___collection_id___idx",
            "functions___function_id__defined_on__id___idx",
            "functions_current___collection_id___idx",
            "functions_current___status___idx",
            "inter_collection_permissions___from_collection_id__to_collection_id__idx",
            "logical_function_run_inputs___execution_id___idx",
            "logical_function_run_inputs___logical_function_run_id___idx",
            "logical_function_run_pointers___function_run_id___idx",
            "logical_function_run_pointers___logical_function_run_id__set_on__id___idx",
            "logical_function_runs___execution_id___idx",
            "logical_function_runs___execution_id__function_id___idx",
            "logical_function_runs___execution_id__status___idx",
            "logical_function_runs___status___idx",
            "logical_function_runs___transaction_id___idx",
            "logical_function_runs___transaction_id__status___idx",
            "logical_table_data_version_pointers___logical_table_data_version_id__set_on__id___idx",
            "logical_table_data_version_pointers___table_data_version_id___idx",
            "logical_table_data_versions___execution_id___idx",
            "logical_table_data_versions___producer_logical_function_run_id___idx",
            "logical_table_data_versions___table_id__triggered_on___idx",
            "logical_table_data_versions___transaction_id___idx",
            "permissions___role_id__permission_type__entity___idx",
            "roles___name___idx",
            "roles___user_id__role_id___idx",
            "sessions___expires_on___idx",
            "sessions___user_id___idx",
            "table_data_versions___collection_id___idx",
            "table_data_versions___execution_id___idx",
            "table_data_versions___function_run_id___idx",
            "table_data_versions___table_id___has_data___idx",
            "table_data_versions___table_id__triggered_on__id___idx",
            "tables___function_id__status___idx",
            "tables___status___idx",
            "tables___table_id__defined_on__id___idx",
            "tables_current___collection_id___idx",
            "tables_current___function_id___idx",
            "tables_current___status___idx",
            "transactions___collection_id___idx",
            "transactions___execution_id___idx",
            "triggers___collection_id___idx",
            "triggers___function_id___idx",
            "triggers___trigger_by_function_id___idx",
            "triggers___trigger_id__defined_on__id___idx",
            "triggers_current___collection_id___idx",
            "triggers_current___function_id___idx",
            "triggers_current___trigger_by_function_id___idx",
            "users___enabled___idx",
            "users___name___idx",
        ];
        assert_eq!(indices, expected);
    }

    #[tokio::test]
    async fn test_schema_version() {
        let db = crate::test_utils::db().await.unwrap();
        let version = schema_db_version(&db).await;
        assert_eq!(
            version,
            td_schema::CURRENT_VERSION.to_string(),
            "db_version mismatch"
        );
    }

    /// A 1.x database lacks the `tabsdata_version` row and must be rejected
    /// by the major-version gate before any db_version comparison runs.
    #[tokio::test]
    async fn test_pre_2_0_database_rejected() {
        let config = crate::test_utils::test_config();
        let db = TdPool::create(&config).await.unwrap();

        raw_query!(
            db,
            "DELETE FROM tabsdata_system WHERE name = 'tabsdata_version'"
        )
        .build_query()
        .execute(&db)
        .await
        .unwrap();

        let res = db.check().await;
        assert!(
            matches!(res, Err(DbError::IncompatibleMajorVersion(1, _))),
            "expected IncompatibleMajorVersion(1, _), got {res:?}"
        );
    }
}
