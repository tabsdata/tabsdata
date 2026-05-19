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
#[cfg(feature = "embedded-postgres")]
pub mod embedded;
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
    use crate::dynamic::DynamicPool;
    #[cfg(sqlite)]
    use crate::error::DbError;
    use crate::pool::TdPool;
    use crate::raw_query;
    use crate::sea_query::builder::SeaQueryExt;
    use crate::sea_query::kind::{DatabaseBackend, DatabaseKind};
    use td_security::{
        ENCODED_ID_CA_ALL_SEC_ADMIN, ENCODED_ID_CA_ALL_SYS_ADMIN, ENCODED_ID_CD_ALL_SYS_ADMIN,
        ENCODED_ID_CD_ALL_USER, ENCODED_ID_CR_ALL_SYS_ADMIN, ENCODED_ID_CR_ALL_USER,
        ENCODED_ID_CX_ALL_SYS_ADMIN, ENCODED_ID_CX_ALL_USER, ENCODED_ID_ROLE_SEC_ADMIN,
        ENCODED_ID_ROLE_SYS_ADMIN, ENCODED_ID_ROLE_USER, ENCODED_ID_SA_SYS_ADMIN,
        ENCODED_ID_SS_SEC_ADMIN, ENCODED_ID_SS_SYS_ADMIN, ENCODED_ID_USER_ADMIN,
        ENCODED_ID_USER_ROLE_ADMIN_SEC_ADMIN, ENCODED_ID_USER_ROLE_ADMIN_SYS_ADMIN,
        ENCODED_ID_USER_ROLE_ADMIN_USER,
    };

    // -----------------------------------------------------------------------
    // Schema introspection helpers
    // -----------------------------------------------------------------------

    /// Helper that lists user-defined tables across backends. Each backend
    /// has its own catalog (`sqlite_master` on sqlite, `information_schema`
    /// on postgres); branching on the pool variant keeps the assertion list
    /// identical regardless of which backend the test ran against.
    async fn schema_table_names(db: &TdPool) -> Vec<String> {
        let mut names: Vec<String> = match &db.pool {
            #[cfg(sqlite)]
            DynamicPool::Sqlite { ro, .. } => sqlx::query_scalar(
                "SELECT name FROM sqlite_master \
                 WHERE type = 'table' AND name NOT LIKE 'sqlite_%' \
                 ORDER BY name",
            )
            .fetch_all(ro)
            .await
            .unwrap(),
            #[cfg(postgres)]
            DynamicPool::Postgres { ro, .. } => sqlx::query_scalar(
                "SELECT table_name FROM information_schema.tables \
                 WHERE table_schema = 'public' AND table_type = 'BASE TABLE' \
                 ORDER BY table_name",
            )
            .fetch_all(ro)
            .await
            .unwrap(),
            #[cfg(mysql)]
            DynamicPool::MySql { ro, .. } => sqlx::query_scalar(
                "SELECT table_name FROM information_schema.tables \
                 WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE' \
                 ORDER BY table_name",
            )
            .fetch_all(ro)
            .await
            .unwrap(),
        };
        names.sort();
        names
    }

    async fn schema_view_names(db: &TdPool) -> Vec<String> {
        let mut names: Vec<String> = match &db.pool {
            #[cfg(sqlite)]
            DynamicPool::Sqlite { ro, .. } => sqlx::query_scalar(
                "SELECT name FROM sqlite_master WHERE type = 'view' ORDER BY name",
            )
            .fetch_all(ro)
            .await
            .unwrap(),
            #[cfg(postgres)]
            DynamicPool::Postgres { ro, .. } => sqlx::query_scalar(
                "SELECT table_name FROM information_schema.views \
                 WHERE table_schema = 'public' ORDER BY table_name",
            )
            .fetch_all(ro)
            .await
            .unwrap(),
            #[cfg(mysql)]
            DynamicPool::MySql { ro, .. } => sqlx::query_scalar(
                "SELECT table_name FROM information_schema.views \
                 WHERE table_schema = DATABASE() ORDER BY table_name",
            )
            .fetch_all(ro)
            .await
            .unwrap(),
        };
        names.sort();
        names
    }

    /// User-defined indexes only. Filters out backend-managed artifacts so
    /// the assertion list is identical across sqlite and postgres: sqlite
    /// auto-creates `sqlite_autoindex_*` for primary keys; postgres
    /// auto-creates `<table>_pkey` for primary keys. Both kinds are
    /// excluded — we only check the indexes our own schema declared.
    async fn schema_index_names(db: &TdPool) -> Vec<String> {
        let mut names: Vec<String> = match &db.pool {
            #[cfg(sqlite)]
            DynamicPool::Sqlite { ro, .. } => sqlx::query_scalar(
                "SELECT name FROM sqlite_master \
                 WHERE type = 'index' AND name NOT LIKE 'sqlite_autoindex_%' \
                 ORDER BY name",
            )
            .fetch_all(ro)
            .await
            .unwrap(),
            #[cfg(postgres)]
            DynamicPool::Postgres { ro, .. } => sqlx::query_scalar(
                // Excludes postgres-auto-generated indexes:
                //  - `_pkey` — automatic primary key index.
                //  - `_key`  — automatic UNIQUE constraint index.
                // We only assert on indexes the schema explicitly declared.
                "SELECT indexname FROM pg_indexes \
                 WHERE schemaname = 'public' \
                   AND indexname NOT LIKE '%_pkey' \
                   AND indexname NOT LIKE '%_key' \
                 ORDER BY indexname",
            )
            .fetch_all(ro)
            .await
            .unwrap(),
            #[cfg(mysql)]
            DynamicPool::MySql { ro, .. } => sqlx::query_scalar(
                "SELECT DISTINCT index_name FROM information_schema.statistics \
                 WHERE table_schema = DATABASE() AND index_name <> 'PRIMARY' \
                 ORDER BY index_name",
            )
            .fetch_all(ro)
            .await
            .unwrap(),
        };
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

    #[cfg(sqlite)]
    #[tokio::test]
    async fn test_tabsdata_db_schema_creation() {
        let db = crate::test_utils::sqlite::pool().await;
        assert!(db.upgrade().await.is_ok());
        assert!(db.check().await.is_ok());
    }

    #[derive(sqlx::FromRow)]
    struct Value {
        id: String,
    }

    #[cfg(sqlite)]
    #[tokio::test]
    async fn test_tabsdata_db_default_users_sqlite() {
        let db = crate::test_utils::sqlite::pool().await;
        db.upgrade().await.unwrap();
        let row: Value = raw_query!(db, "SELECT id FROM users WHERE name = 'admin'")
            .build_query_as()
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(row.id, ENCODED_ID_USER_ADMIN);
    }

    #[cfg(postgres)]
    #[tokio::test]
    async fn test_tabsdata_db_default_users_postgres() {
        let db = crate::test_utils::postgres::pool().await;
        db.upgrade().await.unwrap();
        let row: Value = raw_query!(db, "SELECT id FROM users WHERE name = 'admin'")
            .build_query_as()
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(row.id, ENCODED_ID_USER_ADMIN);
    }

    #[cfg(sqlite)]
    #[tokio::test]
    async fn test_tabsdata_db_default_roles_sqlite() {
        let db = crate::test_utils::sqlite::pool().await;
        db.upgrade().await.unwrap();
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

    #[cfg(postgres)]
    #[tokio::test]
    async fn test_tabsdata_db_default_roles_postgres() {
        let db = crate::test_utils::postgres::pool().await;
        db.upgrade().await.unwrap();
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

    #[cfg(sqlite)]
    #[tokio::test]
    async fn test_tabsdata_db_default_user_roles_sqlite() {
        let db = crate::test_utils::sqlite::pool().await;
        db.upgrade().await.unwrap();
        test_tabsdata_db_default_user_roles_body(db).await;
    }

    #[cfg(postgres)]
    #[tokio::test]
    async fn test_tabsdata_db_default_user_roles_postgres() {
        let db = crate::test_utils::postgres::pool().await;
        db.upgrade().await.unwrap();
        test_tabsdata_db_default_user_roles_body(db).await;
    }

    async fn test_tabsdata_db_default_user_roles_body(db: TdPool) {
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

    #[cfg(sqlite)]
    #[tokio::test]
    async fn test_tabsdata_db_default_permissions_sqlite() {
        let db = crate::test_utils::sqlite::pool().await;
        db.upgrade().await.unwrap();
        test_tabsdata_db_default_permissions_body(db).await;
    }

    #[cfg(postgres)]
    #[tokio::test]
    async fn test_tabsdata_db_default_permissions_postgres() {
        let db = crate::test_utils::postgres::pool().await;
        db.upgrade().await.unwrap();
        test_tabsdata_db_default_permissions_body(db).await;
    }

    async fn test_tabsdata_db_default_permissions_body(db: TdPool) {
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

    #[cfg(sqlite)]
    #[tokio::test]
    async fn test_schema_tables_sqlite() {
        let db = crate::test_utils::sqlite::pool().await;
        db.upgrade().await.unwrap();
        test_schema_tables_body(db).await;
    }

    #[cfg(postgres)]
    #[tokio::test]
    async fn test_schema_tables_postgres() {
        let db = crate::test_utils::postgres::pool().await;
        db.upgrade().await.unwrap();
        test_schema_tables_body(db).await;
    }

    async fn test_schema_tables_body(db: TdPool) {
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

    #[cfg(sqlite)]
    #[tokio::test]
    async fn test_schema_views_sqlite() {
        let db = crate::test_utils::sqlite::pool().await;
        db.upgrade().await.unwrap();
        test_schema_views_body(db).await;
    }

    #[cfg(postgres)]
    #[tokio::test]
    async fn test_schema_views_postgres() {
        let db = crate::test_utils::postgres::pool().await;
        db.upgrade().await.unwrap();
        test_schema_views_body(db).await;
    }

    async fn test_schema_views_body(db: TdPool) {
        let views = schema_view_names(&db).await;

        let expected = vec![
            "collections__with_names",
            "collections_active",
            "dependencies__with_names",
            "dependencies_current__with_names",
            "execution_status_summary",
            "executions__with_names",
            "executions__with_status",
            "function_runs__with_names",
            "functions__with_names",
            "functions_current__with_names",
            "global_status_summary",
            "inter_collection_permissions__with_names",
            "permissions__with_names",
            "physical_function_runs__with_names",
            "physical_table_data_versions__active",
            "physical_table_data_versions__with_function",
            "physical_table_data_versions__with_names",
            "roles__with_names",
            "sessions__with_names",
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

    #[cfg(sqlite)]
    #[tokio::test]
    async fn test_schema_indices_sqlite() {
        let db = crate::test_utils::sqlite::pool().await;
        db.upgrade().await.unwrap();
        test_schema_indices_body(db).await;
    }

    #[cfg(postgres)]
    #[tokio::test]
    async fn test_schema_indices_postgres() {
        let db = crate::test_utils::postgres::pool().await;
        db.upgrade().await.unwrap();
        test_schema_indices_body(db).await;
    }

    async fn test_schema_indices_body(db: TdPool) {
        let indices = schema_index_names(&db).await;

        // Postgres silently truncates identifiers at 63 characters
        // (NAMEDATALEN-1) — several of our index names exceed that. The
        // expected list below is the full sqlite form; we apply the same
        // truncation rule before comparing on postgres so the assertion
        // reflects what postgres actually emits. Long-term, those names
        // should be shortened in the schema; that's a follow-up.
        let expected_full = vec![
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
            "logical_function_runs___function_id__triggered_on__id___idx",
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
            "physical_table_data_versions___collection_id___idx",
            "physical_table_data_versions___execution_id___idx",
            "physical_table_data_versions___physical_function_run_id___idx",
            "physical_table_data_versions___table_id___has_data___idx",
            "physical_table_data_versions___table_id__triggered_on__id___idx",
            "roles___name___idx",
            "roles___user_id__role_id___idx",
            "sessions___expires_on___idx",
            "sessions___user_id___idx",
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

        let backend_truncate = match db.kind() {
            #[cfg(postgres)]
            DatabaseBackend::Postgres => 63,
            #[allow(unreachable_patterns)]
            _ => usize::MAX,
        };
        let mut expected: Vec<String> = expected_full
            .into_iter()
            .map(|n| n.chars().take(backend_truncate).collect::<String>())
            .collect();
        expected.sort();

        assert_eq!(indices, expected);
    }

    #[cfg(sqlite)]
    #[tokio::test]
    async fn test_schema_version_sqlite() {
        let db = crate::test_utils::sqlite::pool().await;
        db.upgrade().await.unwrap();
        let version = schema_db_version(&db).await;
        assert_eq!(
            version,
            td_schema::CURRENT_VERSION.to_string(),
            "db_version mismatch"
        );
    }

    #[cfg(postgres)]
    #[tokio::test]
    async fn test_schema_version_postgres() {
        let db = crate::test_utils::postgres::pool().await;
        db.upgrade().await.unwrap();
        let version = schema_db_version(&db).await;
        assert_eq!(
            version,
            td_schema::CURRENT_VERSION.to_string(),
            "db_version mismatch"
        );
    }

    #[cfg(sqlite)]
    #[tokio::test]
    async fn test_schema_applies_sqlite() {
        let db = crate::test_utils::sqlite::pool().await;
        db.upgrade().await.expect("schema upgrade on sqlite");
        db.check().await.expect("schema check on sqlite");
    }

    #[cfg(postgres)]
    #[tokio::test]
    async fn test_schema_applies_postgres() {
        let db = crate::test_utils::postgres::pool().await;
        db.upgrade().await.expect("schema upgrade on postgres");
        db.check().await.expect("schema check on postgres");
    }

    /// A 1.x database lacks the `tabsdata_version` row and must be rejected
    /// by the major-version gate before any db_version comparison runs.
    #[cfg(sqlite)]
    #[tokio::test]
    async fn test_pre_2_0_database_rejected() {
        let db = crate::test_utils::sqlite::pool().await;
        db.upgrade().await.unwrap();

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
