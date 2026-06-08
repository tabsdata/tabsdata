//
// Copyright 2025 Tabs Data Inc.
//

#[cfg(not(any(feature = "sqlite", feature = "postgres", feature = "mysql")))]
compile_error!("You must enable at least one database feature: `sqlite`, `postgres` or `mysql`.");

use crate::config::DatabaseConfig;
use crate::error::DbError;
use crate::pool::TdPool;

/// Non-superuser, `NOBYPASSRLS` PostgreSQL role the serving pool switches to in
/// `after_connect` (`SET ROLE`), so row-level security is enforced even when the
/// pool authenticated as a superuser (e.g. embedded). On embedded it is
/// auto-provisioned at startup; external deployments pre-create it and grant
/// the connecting role membership.
pub const APP_ROLE: &str = "td_app";

/// PostgreSQL schema the application's objects live in. Single source of truth:
/// every connection's `search_path` is pinned to it, [`APP_ROLE`] is granted
/// access to it, the upgrader's privileges are checked against it, and schema
/// introspection scopes to it. A constant for now (always `public`); promoting
/// it to a `DatabaseConfig` option is a tracked follow-up — at which point this
/// becomes the default rather than a hardcoded value.
pub const APP_SCHEMA: &str = "public";

pub mod config;
pub mod dynamic;
#[cfg(feature = "embedded-postgres")]
pub mod embedded;
pub mod error;
pub mod pool;
pub mod scoped;
pub mod sea_query;

#[cfg(feature = "test-utils")]
pub mod test_utils;

/// Creates a connection pool for the `tabsdata` database.
pub async fn db(config: &DatabaseConfig) -> Result<TdPool, DbError> {
    TdPool::connect(config).await
}

#[cfg(test)]
mod tests {
    #[cfg(postgres)]
    use crate::APP_ROLE;
    #[cfg(postgres)]
    use crate::dynamic::DynamicConnection;
    use crate::dynamic::DynamicPool;
    use crate::error::DbError;
    use crate::pool::TdPool;
    use crate::raw_query;
    use crate::sea_query::builder::SeaQueryExt;
    use crate::sea_query::kind::{DatabaseBackend, DatabaseKind};
    #[cfg(postgres)]
    use sqlx::postgres::PgPoolOptions;
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
            "groups",
            "inter_collection_permissions",
            "logical_function_run_inputs",
            "logical_function_run_pointers",
            "logical_function_runs",
            "logical_table_data_version_pointers",
            "logical_table_data_versions",
            "permissions",
            "physical_function_runs",
            "physical_table_data_versions",
            "projects",
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
            "groups__with_names",
            "inter_collection_permissions__with_names",
            "logical_table_data_versions__active",
            "permissions__with_names",
            "physical_function_runs__with_names",
            "physical_table_data_versions__with_function",
            "physical_table_data_versions__with_names",
            "projects__with_names",
            "projects_active",
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
            "collections___group_id___idx",
            "collections___project_id__name___idx",
            "dependencies___dependency_id__defined_on__id___idx",
            "dependencies___function_id___idx",
            "dependencies___table_function_id___idx",
            "dependencies_current___function_id___idx",
            "dependencies_current___table_function_id___idx",
            "executions___collection_id___idx",
            "executions___function_id__external_uid___idx",
            "executions___function_version_id___idx",
            "executions___triggered_by_id___idx",
            "executions___triggered_on___idx",
            "function_patches___function_id__defined_on___idx",
            "function_runs___collection_id___idx",
            "function_runs___collection_id__ended_on___idx",
            "function_runs___execution_id__status___idx",
            "function_runs___function_version_id___idx",
            "function_runs___function_version_id__ended_on___idx",
            "function_runs___status___idx",
            "function_runs___status__ended_on___idx",
            "function_runs___transaction_id__status___idx",
            "functions___collection_id___idx",
            "functions___function_id__defined_on__id___idx",
            "functions_current___collection_id___idx",
            "functions_current___status___idx",
            "groups___project_id__name___idx",
            "inter_collection_permissions___from_collection_id__to_collection_id__idx",
            "logical_function_run_inputs___execution_id___idx",
            "logical_function_run_inputs___logical_function_run_id___idx",
            "logical_function_run_pointers___function_run_id___idx",
            "logical_function_run_pointers___logical_function_run_id__set_on__id___idx",
            "logical_function_runs___execution_id___idx",
            "logical_function_runs___execution_id__function_id___idx",
            "logical_function_runs___execution_id__status___idx",
            "logical_function_runs___function_id__triggered_on__id___idx",
            "logical_function_runs___group_id__collection_id___idx",
            "logical_function_runs___status___idx",
            "logical_function_runs___transaction_id___idx",
            "logical_function_runs___transaction_id__status___idx",
            "logical_table_data_version_pointers___logical_table_data_version_id__set_on__id___idx",
            "logical_table_data_version_pointers___table_data_version_id___idx",
            "logical_table_data_versions___execution_id___idx",
            "logical_table_data_versions___group_id___idx",
            "logical_table_data_versions___producer_logical_function_run_id___idx",
            "logical_table_data_versions___table_id__triggered_on___idx",
            "logical_table_data_versions___transaction_id___idx",
            "permissions___role_id__permission_type__entity___idx",
            "physical_table_data_versions___collection_id___idx",
            "physical_table_data_versions___execution_id___idx",
            "physical_table_data_versions___physical_function_run_id___idx",
            "physical_table_data_versions___table_id___has_data___idx",
            "physical_table_data_versions___table_id__triggered_on__id___idx",
            "projects___name___idx",
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
            "transactions___group_id__collection_id___idx",
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

    /// Row-level security end to end on real Postgres.
    ///
    /// The test pool connects as the embedded superuser, which *bypasses* RLS,
    /// so we provision a dedicated `NOBYPASSRLS` role through it and connect a
    /// second pool as that role — the same posture the app must use in
    /// production. Then, with `app.project_id` set per transaction, we assert:
    /// reads are scoped to the project, `WITH CHECK` rejects a cross-project
    /// insert, an unset GUC fails closed (zero rows), and `app.bypass_rls`
    /// spans projects.
    #[cfg(postgres)]
    #[tokio::test]
    async fn test_rls_project_isolation() {
        const INSERT: &str = "INSERT INTO collections \
            (id, name, description, created_on, created_by_id, modified_on, modified_by_id, group_id) \
            VALUES ($1, $2, 'd', now(), 'u', now(), 'u', 'g')";
        const SET_PROJECT: &str = "SELECT set_config('app.project_id', $1, true)";
        const COUNT: &str = "SELECT count(*) FROM collections";

        let db = crate::test_utils::postgres::pool().await;
        db.upgrade().await.unwrap();

        // Provision a non-superuser, NOBYPASSRLS app role (as superuser).
        let mut tx = db.begin().await.unwrap();
        tx.as_mut_conn()
            .execute_batch(&[
                "CREATE ROLE app_user LOGIN PASSWORD 'app_pw' NOBYPASSRLS".to_string(),
                "GRANT USAGE ON SCHEMA public TO app_user".to_string(),
                "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO app_user"
                    .to_string(),
            ])
            .await
            .unwrap();
        tx.commit().await.unwrap();

        // Connect a second pool as the app role.
        let mut url = db.url().clone();
        url.set_username("app_user").unwrap();
        url.set_password(Some("app_pw")).unwrap();
        let app = PgPoolOptions::new()
            .max_connections(2)
            .connect(url.as_str())
            .await
            .unwrap();

        // Project A inserts a collection, omitting project_id — the column
        // default reads app.project_id, so the row is tagged 'A'.
        let mut t = app.begin().await.unwrap();
        sqlx::query(SET_PROJECT)
            .bind("A")
            .execute(&mut *t)
            .await
            .unwrap();
        sqlx::query(INSERT)
            .bind("c_a")
            .bind("ca")
            .execute(&mut *t)
            .await
            .unwrap();
        let pid: String = sqlx::query_scalar("SELECT project_id FROM collections WHERE id = 'c_a'")
            .fetch_one(&mut *t)
            .await
            .unwrap();
        assert_eq!(pid, "A", "omitted project_id should default to the GUC");
        t.commit().await.unwrap();

        // Project B cannot see A's row.
        let mut t = app.begin().await.unwrap();
        sqlx::query(SET_PROJECT)
            .bind("B")
            .execute(&mut *t)
            .await
            .unwrap();
        let n: i64 = sqlx::query_scalar(COUNT).fetch_one(&mut *t).await.unwrap();
        assert_eq!(n, 0, "project B must not see project A rows");
        t.rollback().await.unwrap();

        // WITH CHECK rejects an insert tagged for another project.
        let mut t = app.begin().await.unwrap();
        sqlx::query(SET_PROJECT)
            .bind("B")
            .execute(&mut *t)
            .await
            .unwrap();
        let bad = sqlx::query(
            "INSERT INTO collections \
             (id, name, description, created_on, created_by_id, modified_on, modified_by_id, group_id, project_id) \
             VALUES ('x', 'x', 'd', now(), 'u', now(), 'u', 'g', 'A')",
        )
        .execute(&mut *t)
        .await;
        assert!(
            bad.is_err(),
            "WITH CHECK must reject a cross-project insert"
        );
        t.rollback().await.unwrap();

        // Project B inserts its own collection.
        let mut t = app.begin().await.unwrap();
        sqlx::query(SET_PROJECT)
            .bind("B")
            .execute(&mut *t)
            .await
            .unwrap();
        sqlx::query(INSERT)
            .bind("c_b")
            .bind("cb")
            .execute(&mut *t)
            .await
            .unwrap();
        t.commit().await.unwrap();

        // Project A sees only its own row.
        let mut t = app.begin().await.unwrap();
        sqlx::query(SET_PROJECT)
            .bind("A")
            .execute(&mut *t)
            .await
            .unwrap();
        let n: i64 = sqlx::query_scalar(COUNT).fetch_one(&mut *t).await.unwrap();
        assert_eq!(n, 1, "project A should see exactly its own collection");
        t.rollback().await.unwrap();

        // Unset GUC fails closed: zero rows, no error.
        let mut t = app.begin().await.unwrap();
        let n: i64 = sqlx::query_scalar(COUNT).fetch_one(&mut *t).await.unwrap();
        assert_eq!(
            n, 0,
            "an unset app.project_id must see no rows (fail-closed)"
        );
        t.rollback().await.unwrap();

        // Bypass spans projects.
        let mut t = app.begin().await.unwrap();
        sqlx::query("SELECT set_config('app.bypass_rls', 'on', true)")
            .execute(&mut *t)
            .await
            .unwrap();
        let n: i64 = sqlx::query_scalar(COUNT).fetch_one(&mut *t).await.unwrap();
        assert_eq!(n, 2, "bypass should see every project's rows");
        t.rollback().await.unwrap();
    }

    /// RLS enforced from a *superuser* connection via `SET LOCAL ROLE`.
    ///
    /// The pool authenticates as the embedded superuser (which bypasses RLS),
    /// but switching to a non-superuser role per transaction makes the policy
    /// apply — the mechanism that lets the app enforce isolation without
    /// connecting as a dedicated role. Asserts a project sees only its own rows
    /// and that staying superuser (no SET ROLE) bypasses.
    #[cfg(postgres)]
    #[tokio::test]
    #[allow(irrefutable_let_patterns)] // only the Postgres variant exists in a postgres-only build
    async fn test_rls_enforced_via_set_role() {
        const INSERT: &str = "INSERT INTO collections \
            (id, name, description, created_on, created_by_id, modified_on, modified_by_id, group_id) \
            VALUES ($1, $2, 'd', now(), 'u', now(), 'u', 'g')";
        const COUNT: &str = "SELECT count(*) FROM collections";

        let db = crate::test_utils::postgres::pool().await;
        db.upgrade().await.unwrap(); // also auto-provisions the `td_app` role (embedded)

        // As td_app in project A, insert a collection (project_id defaults to the GUC).
        let mut tx = db.begin().await.unwrap();
        {
            let mut c = tx.as_mut_conn();
            c.set_rls_role("td_app").await.unwrap();
            c.set_project_scope("A").await.unwrap();
            let DynamicConnection::Postgres(h) = &mut c else {
                unreachable!()
            };
            sqlx::query(INSERT)
                .bind("c_a")
                .bind("ca")
                .execute(&mut **h)
                .await
                .unwrap();
            let n: i64 = sqlx::query_scalar(COUNT).fetch_one(&mut **h).await.unwrap();
            assert_eq!(n, 1, "project A sees its own row under SET ROLE");
        }
        tx.commit().await.unwrap();

        // As td_app in project B: cannot see A's row.
        let mut tx = db.begin().await.unwrap();
        {
            let mut c = tx.as_mut_conn();
            c.set_rls_role("td_app").await.unwrap();
            c.set_project_scope("B").await.unwrap();
            let DynamicConnection::Postgres(h) = &mut c else {
                unreachable!()
            };
            let n: i64 = sqlx::query_scalar(COUNT).fetch_one(&mut **h).await.unwrap();
            assert_eq!(
                n, 0,
                "SET ROLE makes RLS apply: project B sees no project A rows"
            );
        }
        tx.rollback().await.unwrap();

        // Without SET ROLE the session stays superuser and bypasses RLS.
        let mut tx = db.begin().await.unwrap();
        {
            let mut c = tx.as_mut_conn();
            c.set_project_scope("B").await.unwrap();
            let DynamicConnection::Postgres(h) = &mut c else {
                unreachable!()
            };
            let n: i64 = sqlx::query_scalar(COUNT).fetch_one(&mut **h).await.unwrap();
            assert_eq!(
                n, 1,
                "a superuser session (no SET ROLE) bypasses RLS and sees all rows"
            );
        }
        tx.rollback().await.unwrap();
    }

    /// `check()` refuses to start when the app role can't be switched to — the
    /// startup precondition that catches a misconfigured external deployment
    /// (operator forgot to create `td_app` or grant the connecting role
    /// membership) at boot, instead of letting the first request fail
    /// mid-flight with an opaque error.
    #[cfg(postgres)]
    #[tokio::test]
    async fn test_check_app_role_switchable_fails_when_role_missing() {
        let db = crate::test_utils::postgres::pool().await;
        db.upgrade().await.unwrap(); // creates schema + td_app (embedded)

        // Sanity: with td_app in place, `check` succeeds.
        db.check()
            .await
            .expect("check should pass with td_app present");

        // Simulate the external-deploy misconfig: remove the role and verify
        // `check` rejects the database with a clear error.
        let mut tx = db.begin().await.unwrap();
        tx.as_mut_conn()
            .execute_batch(&[
                "DROP OWNED BY td_app".to_string(),
                "DROP ROLE td_app".to_string(),
            ])
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let res = db.check().await;
        assert!(
            matches!(res, Err(DbError::AppRoleNotSwitchable(ref r, _)) if r == APP_ROLE),
            "expected AppRoleNotSwitchable({APP_ROLE:?}), got {res:?}"
        );
    }

    /// `check()` refuses to start an **external** deployment whose connecting
    /// role bypasses RLS (superuser / BYPASSRLS) — project isolation rests
    /// entirely on RLS, so such a role would silently expose every tenant to
    /// every other. Embedded (a known dev/test superuser) is exempt.
    #[cfg(postgres)]
    #[tokio::test]
    async fn test_check_rejects_rls_bypassing_role_on_external_deploy() {
        use crate::config::{BackendConfig, DatabaseConfig};

        let db = crate::test_utils::postgres::pool().await; // embedded superuser
        db.upgrade().await.unwrap();
        // Embedded is exempt: the posture issue is a debug note, not fatal.
        db.check().await.expect("embedded check should pass");

        // Connect to the SAME server as a non-embedded pool, still as the
        // bootstrap superuser (which bypasses RLS). check() must refuse.
        let cfg = DatabaseConfig {
            conn: Some(BackendConfig::from_url(db.url().clone()).unwrap()),
            pool: Default::default(),
        };
        let external = TdPool::connect(&cfg).await.unwrap();
        let res = external.check().await;
        assert!(
            matches!(res, Err(DbError::ConnectedRoleBypassesRls)),
            "external superuser deploy must be rejected, got {res:?}"
        );
    }

    /// A maintain-current trigger's write into the `_current` table succeeds
    /// under RLS, and FK validation across RLS tables works.
    ///
    /// When the app role inserts into `functions` (project A), the AFTER INSERT
    /// trigger inserts into `functions_current` as the same (invoker) role — its
    /// `WITH CHECK` must pass because the copied `project_id` matches the GUC.
    /// The function also FK-references `collections`, exercising referential
    /// integrity against an RLS-protected parent. Project B then sees neither row.
    #[cfg(postgres)]
    #[tokio::test]
    #[allow(irrefutable_let_patterns)] // only the Postgres variant exists in a postgres-only build
    async fn test_rls_maintain_current_trigger_under_role() {
        let db = crate::test_utils::postgres::pool().await;
        db.upgrade().await.unwrap(); // also auto-provisions the `td_app` role (embedded)

        // Project A: a collection and a function. The function insert fires the
        // maintain-current trigger into functions_current.
        let mut tx = db.begin().await.unwrap();
        {
            let mut c = tx.as_mut_conn();
            c.set_rls_role("td_app").await.unwrap();
            c.set_project_scope("A").await.unwrap();
            let DynamicConnection::Postgres(h) = &mut c else {
                unreachable!()
            };
            sqlx::query(
                "INSERT INTO collections (id, name, description, created_on, created_by_id, modified_on, modified_by_id, group_id) \
                 VALUES ('col', 'c', 'd', now(), 'u', now(), 'u', 'g')",
            )
            .execute(&mut **h)
            .await
            .unwrap();
            sqlx::query(
                "INSERT INTO functions \
                 (id, collection_id, name, description, decorator, runtime_values, function_id, data_location, \
                  storage_version, bundle_id, snippet, defined_on, defined_by_id, status) \
                 VALUES ('fn1', 'col', 'f', 'd', 'dec', '{}', 'fid', 'loc', '1', 'bun', 'snip', now(), 'u', 'active')",
            )
            .execute(&mut **h)
            .await
            .unwrap();
            let cur: i64 = sqlx::query_scalar("SELECT count(*) FROM functions_current")
                .fetch_one(&mut **h)
                .await
                .unwrap();
            assert_eq!(
                cur, 1,
                "maintain-current trigger wrote functions_current under RLS"
            );
            let pid: String = sqlx::query_scalar(
                "SELECT project_id FROM functions_current WHERE function_id = 'fid'",
            )
            .fetch_one(&mut **h)
            .await
            .unwrap();
            assert_eq!(pid, "A", "trigger copied the source project_id");
        }
        tx.commit().await.unwrap();

        // Project B sees neither the function nor its current snapshot.
        let mut tx = db.begin().await.unwrap();
        {
            let mut c = tx.as_mut_conn();
            c.set_rls_role("td_app").await.unwrap();
            c.set_project_scope("B").await.unwrap();
            let DynamicConnection::Postgres(h) = &mut c else {
                unreachable!()
            };
            let f: i64 = sqlx::query_scalar("SELECT count(*) FROM functions")
                .fetch_one(&mut **h)
                .await
                .unwrap();
            let fc: i64 = sqlx::query_scalar("SELECT count(*) FROM functions_current")
                .fetch_one(&mut **h)
                .await
                .unwrap();
            assert_eq!(
                (f, fc),
                (0, 0),
                "project B sees neither functions nor functions_current rows"
            );
        }
        tx.rollback().await.unwrap();
    }

    /// The `DynamicConnection` GUC primitives set the expected PostgreSQL
    /// session variables on their transaction. (Connection-layer wiring will
    /// call these per request; this checks the primitive in isolation.)
    #[cfg(postgres)]
    #[tokio::test]
    #[allow(irrefutable_let_patterns)] // only the Postgres variant exists in a postgres-only build
    async fn test_set_rls_guc_primitive() {
        let db = crate::test_utils::postgres::pool().await;
        db.upgrade().await.unwrap();

        let mut tx = db.begin().await.unwrap();
        let mut conn = tx.as_mut_conn();
        conn.set_project_scope("PROJ_X").await.unwrap();
        conn.bypass_project_scope().await.unwrap();

        let DynamicConnection::Postgres(h) = &mut conn else {
            unreachable!()
        };
        let project: String = sqlx::query_scalar("SELECT current_setting('app.project_id', true)")
            .fetch_one(&mut **h)
            .await
            .unwrap();
        let bypass: String = sqlx::query_scalar("SELECT current_setting('app.bypass_rls', true)")
            .fetch_one(&mut **h)
            .await
            .unwrap();
        assert_eq!(project, "PROJ_X");
        assert_eq!(bypass, "on");
    }
}
