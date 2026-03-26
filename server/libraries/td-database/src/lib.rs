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
    use crate::pool::TdPool;
    use crate::raw_query;
    use crate::sea_query::builder::SeaQueryExt;
    use crate::sea_query::kind::DatabaseKind;
    use std::collections::BTreeMap;
    use td_common::tdtestdir;
    use td_schema::schema::SchemaVersion;
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

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, sqlx::FromRow)]
    struct ColumnInfo {
        name: String,
        #[sqlx(rename = "type")]
        col_type: String,
        notnull: bool,
        pk: bool,
    }

    async fn schema_table_columns(db: &TdPool, table: &str) -> Vec<ColumnInfo> {
        let sql = format!(
            "SELECT \"name\", \"type\", \"notnull\", \"pk\" FROM pragma_table_info('{table}')"
        );
        let query = crate::sea_query::raw::RawSeaQuery(sea_query::raw_sql::seaql::query(&sql));
        let mut cols: Vec<ColumnInfo> = query.build_query_as().fetch_all(db).await.unwrap();
        cols.sort();
        cols
    }

    #[derive(Debug, PartialEq, Eq)]
    struct SchemaFingerprint {
        tables: Vec<String>,
        views: Vec<String>,
        indices: Vec<String>,
        columns: BTreeMap<String, Vec<ColumnInfo>>,
        db_version: String,
    }

    async fn capture_schema_fingerprint(db: &TdPool) -> SchemaFingerprint {
        let tables = schema_table_names(db).await;
        let views = schema_view_names(db).await;
        let indices = schema_index_names(db).await;
        let mut columns = BTreeMap::new();
        for table in &tables {
            columns.insert(table.clone(), schema_table_columns(db, table).await);
        }
        let db_version = schema_db_version(db).await;
        SchemaFingerprint {
            tables,
            views,
            indices,
            columns,
            db_version,
        }
    }

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
            "function_requirements",
            "function_runs",
            "functions",
            "functions_current",
            "inter_collection_permissions",
            "permissions",
            "roles",
            "sessions",
            "table_data_versions",
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
            "function_requirements__with_names",
            "function_requirements__with_status",
            "function_runs__to_commit",
            "function_runs__to_execute",
            "function_runs__with_names",
            "functions__with_names",
            "functions_current__with_names",
            "global_status_summary",
            "inter_collection_permissions__with_names",
            "permissions__with_names",
            "roles__with_names",
            "sessions__with_names",
            "table_data_versions__active",
            "table_data_versions__with_function",
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
            "function_requirements___function_run_id___idx",
            "function_requirements___function_version_id___idx",
            "function_requirements___requirement_function_run_id___idx",
            "function_requirements___requirement_table_data_version_id___idx",
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

    /// Migration invariant: for each archived version N,
    /// archive(N) + deltas(N+1..=CURRENT) must produce a schema identical to
    /// a fresh installation via TdPool::create().
    ///
    /// Both paths go through DynamicConnection::execute_batch().
    #[tokio::test]
    async fn test_migration_invariant() {
        use td_schema::schema::{archive, current, deltas};

        // Fresh-install fingerprint via production path.
        let fresh_db = crate::test_utils::db().await.unwrap();
        let fresh_fp = capture_schema_fingerprint(&fresh_db).await;

        for archive_version in archive::MIN_ARCHIVE..current::Schema::VERSION {
            // Build migrated DB: archive + deltas.
            let config = crate::test_utils::test_config();
            let migrated_db = TdPool::connect(&config).await.unwrap();

            let mut tx = migrated_db.pool.begin().await.unwrap();
            let archive_stmts = archive::build(archive_version, migrated_db.kind());
            tx.as_mut_conn()
                .execute_batch(&archive_stmts)
                .await
                .unwrap();

            for delta_target in (archive_version + 1)..=current::Schema::VERSION {
                let delta_stmts = deltas::get(delta_target, migrated_db.kind());
                tx.as_mut_conn().execute_batch(&delta_stmts).await.unwrap();
            }
            tx.commit().await.unwrap();

            let migrated_fp = capture_schema_fingerprint(&migrated_db).await;

            assert_eq!(
                fresh_fp.tables, migrated_fp.tables,
                "Table set mismatch: archive v{archive_version} + deltas vs fresh"
            );
            assert_eq!(
                fresh_fp.views, migrated_fp.views,
                "View set mismatch: archive v{archive_version} + deltas vs fresh"
            );
            assert_eq!(
                fresh_fp.indices, migrated_fp.indices,
                "Index set mismatch: archive v{archive_version} + deltas vs fresh"
            );
            assert_eq!(
                fresh_fp.columns, migrated_fp.columns,
                "Column definitions mismatch: archive v{archive_version} + deltas vs fresh"
            );
            assert_eq!(
                fresh_fp.db_version, migrated_fp.db_version,
                "db_version mismatch: archive v{archive_version} + deltas vs fresh"
            );
        }
    }

    /// Historic migration invariant: applying the initial v0 schema, then
    /// historic migrations (→v5) with normalization, then programmatic deltas
    /// (→CURRENT) must produce a schema **identical** to a fresh install —
    /// same tables, views, indices, column definitions, and db_version.
    #[tokio::test]
    async fn test_historic_migration_invariant() {
        use td_schema::schema::archive::historic;
        use td_schema::schema::current;
        use td_schema::schema::deltas;

        // Fresh-install fingerprint via production path.
        let fresh_db = crate::test_utils::db().await.unwrap();
        let fresh_fp = capture_schema_fingerprint(&fresh_db).await;

        // Build a v0 database, apply historic migrations + normalize + deltas.
        let config_historic = crate::test_utils::test_config();
        let db_historic = TdPool::connect(&config_historic).await.unwrap();

        let mut tx = db_historic.pool.begin().await.unwrap();

        let v0_stmts = historic::initial_v0_schema();
        tx.as_mut_conn().execute_batch(&v0_stmts).await.unwrap();

        for v in 1..=historic::MAX_VERSION {
            let stmts = historic::get(v);
            tx.as_mut_conn().execute_batch(&stmts).await.unwrap();
        }

        // Normalize schema to match the v5 archive exactly.
        let stmts = historic::normalize();
        tx.as_mut_conn().execute_batch(&stmts).await.unwrap();

        for v in (historic::MAX_VERSION + 1)..=current::Schema::VERSION {
            let stmts = deltas::get(v, db_historic.kind());
            tx.as_mut_conn().execute_batch(&stmts).await.unwrap();
        }
        tx.commit().await.unwrap();

        let historic_fp = capture_schema_fingerprint(&db_historic).await;

        assert_eq!(
            fresh_fp.tables, historic_fp.tables,
            "Table set mismatch: historic v0→CURRENT vs fresh"
        );
        assert_eq!(
            fresh_fp.views, historic_fp.views,
            "View set mismatch: historic v0→CURRENT vs fresh"
        );
        assert_eq!(
            fresh_fp.indices, historic_fp.indices,
            "Index set mismatch: historic v0→CURRENT vs fresh"
        );
        assert_eq!(
            fresh_fp.columns, historic_fp.columns,
            "Column definitions mismatch: historic v0→CURRENT vs fresh"
        );
        assert_eq!(
            fresh_fp.db_version, historic_fp.db_version,
            "db_version mismatch: historic v0→CURRENT vs fresh"
        );
    }

    /// End-to-end test: upgrade a v0 database all the way to CURRENT_VERSION
    /// using the production `upgrade_db_version()` path.
    #[tokio::test]
    async fn test_historic_upgrade_end_to_end() {
        use td_schema::schema::archive::historic;

        // Create a v0 database manually.
        let config = crate::test_utils::test_config();
        let db = TdPool::connect(&config).await.unwrap();
        let mut tx = db.pool.begin().await.unwrap();
        let v0_stmts = historic::initial_v0_schema();
        tx.as_mut_conn().execute_batch(&v0_stmts).await.unwrap();

        // Also create the _sqlx_migrations table to simulate a real old database.
        tx.as_mut_conn()
            .execute_batch(
                &["CREATE TABLE _sqlx_migrations (version TEXT PRIMARY KEY)".to_string()],
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();

        // Run the production upgrade path.
        db.upgrade_db_version().await.unwrap();

        // Verify we reached CURRENT_VERSION.
        let version = schema_db_version(&db).await;
        assert_eq!(
            version,
            td_schema::CURRENT_VERSION.to_string(),
            "db_version should be CURRENT_VERSION after historic upgrade"
        );

        // Verify _sqlx_migrations was cleaned up.
        let res: Vec<crate::dynamic::DynamicRow> = raw_query!(
            db,
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = '_sqlx_migrations'"
        )
        .build_query()
        .fetch_all(&db)
        .await
        .unwrap();
        assert!(
            res.is_empty(),
            "_sqlx_migrations table should have been dropped"
        );
    }

    /// Verify that a failed upgrade rolls back — the database stays at its
    /// original version.
    #[tokio::test]
    async fn test_upgrade_rollback_on_failure() {
        // Create a fresh database at CURRENT_VERSION.
        let config = crate::test_utils::test_config();
        let db = TdPool::connect(&config).await.unwrap();
        db.upgrade().await.unwrap();

        // Manually set version to an intermediate value that would trigger
        // deltas, but corrupt a table the delta depends on so it fails.
        raw_query!(
            db,
            "UPDATE tabsdata_system SET value = '5' WHERE name = 'db_version'"
        )
        .build_query()
        .execute(&db)
        .await
        .unwrap();

        // Drop a table that v6_to_v7 delta needs, causing the upgrade to fail.
        raw_query!(db, "DROP TABLE functions")
            .build_query()
            .execute(&db)
            .await
            .unwrap();

        // The upgrade should fail.
        let result = db.upgrade_db_version().await;
        assert!(result.is_err(), "Upgrade should fail on corrupted schema");

        // db_version should still read '5' because the transaction rolled back.
        let version = schema_db_version(&db).await;
        assert_eq!(
            version, "5",
            "db_version should remain at 5 after failed upgrade (rollback)"
        );
    }
}
