//
// Copyright 2026 Tabs Data Inc.
//

//! Legacy sqlx-based migrations for upgrading SQLite databases from versions 0–4
//! to v5.
//!
//! These raw SQL files are embedded at compile time. The [`get()`] function
//! returns individual SQL statements for upgrading from version `target - 1` to
//! `target`, mirroring the [`super::super::deltas::get()`] API.
//!
//! After reaching v5 ([`MAX_VERSION`]), the programmatic
//! [`SchemaDelta`](super::super::SchemaDelta) system takes over.
//!
//! **SQLite-only**: the embedded SQL uses SQLite syntax. Multi-backend support
//! was introduced together with the programmatic migration system at v5.

/// The version that historic migrations upgrade to.
/// This equals [`super::MIN_ARCHIVE`] — the oldest version with a programmatic
/// archive snapshot.
pub const MAX_VERSION: usize = 5;

/// Returns the SQL statements to upgrade a SQLite database from version
/// `target_version - 1` to `target_version`.
///
/// Each returned string is a single SQL statement ready for execution.
///
/// # Panics
///
/// Panics if `target_version` is not in range `1..=MAX_VERSION`.
pub fn get(target_version: usize) -> Vec<String> {
    let sql_files: &[&str] = match target_version {
        1 => &[
            include_str!("live/20250531100253_v1.up.sql"),
        ],
        2 => &[
            include_str!("live/20250922095747_functions.up.sql"),
            include_str!("live/20250922095756_v2.up.sql"),
        ],
        3 => &[
            include_str!("live/20251106170725_functions.up.sql"),
            include_str!("live/20251106170859_v3.up.sql"),
        ],
        4 => &[
            include_str!("live/20251201104113_functions.up.sql"),
            include_str!("live/20251201104119_v4.up.sql"),
        ],
        5 => &[
            include_str!("live/20260112120013_indices.up.sql"),
            include_str!("live/20260113102005_functions.up.sql"),
            include_str!("live/20260113102019_v5.up.sql"),
        ],
        _ => panic!("No historic migration for target version {target_version}"),
    };
    sql_files
        .iter()
        .flat_map(|content| split_sql(content))
        .collect()
}

/// Build the initial v0 schema — the base tables created by the first sqlx
/// migrations before any version bumps.
///
/// This is useful for testing the full historic upgrade path (v0 → v5).
pub fn initial_v0_schema() -> Vec<String> {
    let sql_files: &[&str] = &[
        include_str!("live/20240726070228_system.up.sql"),
        include_str!("live/20240726070229_users.up.sql"),
        include_str!("live/20240730113739_collections.up.sql"),
        include_str!("live/20250220130016_functions.up.sql"),
        include_str!("live/20250226144542_roles.up.sql"),
        include_str!("live/20250314111309_permissions.up.sql"),
        include_str!("live/20250331150456_sessions.up.sql"),
    ];
    sql_files
        .iter()
        .flat_map(|content| split_sql(content))
        .collect()
}

/// Normalize a SQLite database that was upgraded via historic migrations so that
/// its schema exactly matches the v5 archive built by sea-query.
///
/// After applying historic migrations (v0 → v5), the table types use legacy
/// spellings (`timestamp_text`, `boolean`) and the index set differs from the
/// programmatic archive. This function rebuilds every table, view, and index
/// using the v5 archive's `build()` output while preserving all existing data.
///
/// **Must be called immediately after `get(5)` and before any programmatic
/// deltas.**
#[cfg(feature = "sqlite")]
pub fn normalize() -> Vec<String> {
    use crate::schema::SchemaVersion;
    use crate::schema::backend::Backend;

    // All v5 view names (frozen — will never change).
    // Listed in reverse dependency order for safe dropping.
    const VIEWS: &[&str] = &[
        "workers__with_names",
        "function_runs__to_commit",
        "function_runs__to_execute",
        "transactions__with_status",
        "executions__with_status",
        "transaction_status_summary",
        "execution_status_summary",
        "global_status_summary",
        "function_requirements__with_names",
        "function_requirements__with_status",
        "table_data_versions__with_names",
        "table_data_versions__active",
        "table_data_versions__with_function",
        "function_runs__with_names",
        "transactions__with_names",
        "executions__with_names",
        "triggers__read",
        "triggers__with_names",
        "dependencies__with_names",
        "tables__with_names",
        "functions__with_names",
        "inter_collection_permissions__with_names",
        "permissions__with_names",
        "users_roles__with_names",
        "roles__with_names",
        "sessions__with_names",
        "collections__with_names",
        "collections_active",
        "users__with_names",
    ];

    // All v5 table names (frozen — will never change).
    const TABLES: &[&str] = &[
        "tabsdata_system",
        "users",
        "collections",
        "roles",
        "users_roles",
        "sessions",
        "permissions",
        "inter_collection_permissions",
        "functions",
        "tables",
        "bundles",
        "dependencies",
        "triggers",
        "executions",
        "transactions",
        "function_runs",
        "table_data_versions",
        "table_partitions",
        "function_requirements",
        "workers",
    ];

    // All indices created by the legacy sqlx migrations (frozen).
    // In SQLite, indices keep their name when a table is renamed, so we
    // must drop them explicitly before the archive rebuild creates new
    // indices with the same names.
    const INDICES: &[&str] = &[
        // From 20250226144542_roles.up.sql
        "roles___name___idx",
        "roles___user_id__role_id___idx",
        // From 20250314111309_permissions.up.sql
        "permissions___role_id__permission_type__entity___idx",
        "permissions___role_id___idx",
        "inter_collection_permissions___from_collection_id__to_collection_id__idx",
        // From 20260112120013_indices.up.sql
        "users___name___idx",
        "users___enabled___idx",
        "collections___name___idx",
        "functions___collection_id___idx",
        "tables___function_id__status___idx",
        "tables___status___idx",
        "executions___collection_id___idx",
        "executions___function_version_id___idx",
        "executions___triggered_by_id___idx",
        "executions___triggered_on___idx",
        "function_runs___collection_id___idx",
        "function_runs___execution_id___idx",
        "function_runs___transaction_id___idx",
        "function_runs___function_version_id___idx",
        "function_runs___status___idx",
        "table_data_versions___collection_id___idx",
        "table_data_versions___execution_id___idx",
        "table_data_versions___function_run_id___idx",
        "table_data_versions___table_id___idx",
        "table_data_versions___table_id___has_data___idx",
        "transactions___collection_id___idx",
        "transactions___execution_id___idx",
        "triggers___collection_id___idx",
        "triggers___function_id___idx",
        "bundles___collection_id___idx",
    ];

    let mut stmts = Vec::new();

    // Disable FK enforcement during the rebuild. We use both pragmas to
    // cover execution inside a transaction (defer_foreign_keys) and outside
    // one in autocommit mode (foreign_keys = OFF).
    stmts.push("PRAGMA defer_foreign_keys = ON".to_string());
    stmts.push("PRAGMA foreign_keys = OFF".to_string());

    // 1. Drop all views (they reference tables by name).
    for view in VIEWS {
        stmts.push(format!("DROP VIEW IF EXISTS \"{view}\""));
    }

    // 2. Drop all explicitly-created indices (they keep their name after
    //    table renames and would collide with the archive rebuild).
    for idx in INDICES {
        stmts.push(format!("DROP INDEX IF EXISTS \"{idx}\""));
    }

    // 3. Rename all tables to __old_* prefix.
    for table in TABLES {
        stmts.push(format!(
            "ALTER TABLE \"{table}\" RENAME TO \"__old_{table}\""
        ));
    }

    // 4. Rebuild the v5 schema with correct sea-query types
    //    (creates tables, views, indices, and seed data).
    stmts.extend(super::v5::Schema::build(Backend::Sqlite));

    // 5. Replace seed data with actual data from old tables.
    for table in TABLES {
        stmts.push(format!("DELETE FROM \"{table}\""));
        stmts.push(format!(
            "INSERT INTO \"{table}\" SELECT * FROM \"__old_{table}\""
        ));
    }

    // 6. Drop old tables.
    for table in TABLES {
        stmts.push(format!("DROP TABLE \"__old_{table}\""));
    }

    // Re-enable FK enforcement.
    stmts.push("PRAGMA foreign_keys = ON".to_string());

    stmts
}

/// Split a SQL file into individual statements.
///
/// Statements are separated by `;`. Empty statements and comment-only
/// fragments are filtered out.
fn split_sql(content: &str) -> Vec<String> {
    content
        .split(';')
        .map(|s| s.trim())
        .filter(|s| {
            // Keep only fragments that contain at least one non-empty, non-comment line.
            s.lines()
                .any(|line| {
                    let trimmed = line.trim();
                    !trimmed.is_empty() && !trimmed.starts_with("--")
                })
        })
        .map(String::from)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_historic_migrations_produce_statements() {
        for v in 1..=MAX_VERSION {
            let stmts = get(v);
            assert!(
                !stmts.is_empty(),
                "Historic migration to v{v} produced no statements"
            );
            // The last statement in each version group should be a version bump.
            let last = stmts.last().unwrap();
            assert!(
                last.contains(&format!("'{v}'")),
                "Historic migration to v{v} should end with a version bump. Last stmt: {last}"
            );
        }
    }

    #[test]
    fn test_initial_v0_schema_produces_statements() {
        let stmts = initial_v0_schema();
        assert!(
            !stmts.is_empty(),
            "Initial v0 schema produced no statements"
        );
        // Should create the tabsdata_system table and seed db_version = '0'.
        assert!(
            stmts.iter().any(|s| s.contains("tabsdata_system")),
            "Initial v0 schema should create tabsdata_system"
        );
    }

    #[test]
    fn test_split_sql_basic() {
        let sql = "CREATE TABLE foo (id TEXT);\nINSERT INTO foo VALUES ('a');";
        let stmts = split_sql(sql);
        assert_eq!(stmts.len(), 2);
        assert_eq!(stmts[0], "CREATE TABLE foo (id TEXT)");
        assert_eq!(stmts[1], "INSERT INTO foo VALUES ('a')");
    }

    #[test]
    fn test_split_sql_filters_comment_only_fragments() {
        let sql = "-- comment\nCREATE TABLE foo (id TEXT);\n-- trailing comment\n";
        let stmts = split_sql(sql);
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn test_split_sql_preserves_inline_comments() {
        let sql = "-- header\nCREATE TABLE foo (id TEXT); -- note\nALTER TABLE foo ADD bar TEXT;";
        let stmts = split_sql(sql);
        assert_eq!(stmts.len(), 2);
    }

    #[test]
    fn test_max_version_matches_min_archive() {
        assert_eq!(
            MAX_VERSION,
            super::super::MIN_ARCHIVE,
            "MAX_VERSION should equal archive::MIN_ARCHIVE"
        );
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn test_normalize_produces_statements() {
        let stmts = normalize();
        assert!(
            !stmts.is_empty(),
            "normalize() produced no statements"
        );
        // Should contain DROP VIEW, ALTER TABLE RENAME, CREATE TABLE,
        // DELETE, INSERT INTO ... SELECT, and DROP TABLE statements.
        assert!(
            stmts.iter().any(|s| s.starts_with("DROP VIEW")),
            "normalize() should drop views"
        );
        assert!(
            stmts.iter().any(|s| s.contains("RENAME TO")),
            "normalize() should rename tables"
        );
        assert!(
            stmts.iter().any(|s| s.starts_with("DROP TABLE")),
            "normalize() should drop old tables"
        );
    }
}
