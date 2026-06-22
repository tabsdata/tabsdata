//
// Copyright 2026 Tabs Data Inc.
//

//! Privileged DDL the `system` module runs to provision and grant. These emit raw
//! `CREATE ROLE` / `CREATE SCHEMA` / `GRANT` / `ALTER DEFAULT PRIVILEGES`
//! statements — session/DDL statements sea_query does not model — run on
//! [`super::SystemPool`].

use crate::dynamic::DynamicPool;
use crate::error::DbError;

#[cfg(postgres)]
pub(super) fn create_owner_role_sql(owner: &str, password: &str) -> String {
    // Race-safe idempotent create (see `create_app_role_sql`): create unconditionally
    // and swallow the duplicate, since concurrent provisions can both pass an
    // `IF NOT EXISTS` check and one then hits the `pg_authid` unique index.
    format!(
        "DO $$ BEGIN CREATE ROLE \"{owner}\" LOGIN PASSWORD '{password}'; \
         EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL; END $$"
    )
}

#[cfg(postgres)]
pub(super) fn create_schema_sql(schema: &str, owner: &str) -> String {
    format!("CREATE SCHEMA IF NOT EXISTS \"{schema}\" AUTHORIZATION \"{owner}\"")
}

#[cfg(postgres)]
pub(super) fn grant_app_role_membership_sql(owner: &str, app_role: &str) -> String {
    format!("GRANT \"{app_role}\" TO \"{owner}\"")
}

#[cfg(postgres)]
pub(super) fn grant_app_role_tables_sql(schema: &str, app_role: &str) -> String {
    format!(
        "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA \"{schema}\" TO \"{app_role}\""
    )
}

#[cfg(postgres)]
pub(super) fn grant_app_role_privileges_sql(schema: &str, app_role: &str) -> [String; 3] {
    [
        format!("GRANT USAGE ON SCHEMA \"{schema}\" TO \"{app_role}\""),
        grant_app_role_tables_sql(schema, app_role),
        format!(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA \"{schema}\" \
             GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO \"{app_role}\""
        ),
    ]
}

/// Run `stmts` as a single committed transaction on `pool`, mapping failures to
/// [`DbError::FailedToCreateOrUpgradeDatabaseSchema`].
pub(super) async fn execute_batch_tx(pool: &DynamicPool, stmts: &[String]) -> Result<(), DbError> {
    let mut tx = pool
        .begin()
        .await
        .map_err(DbError::FailedToCreateOrUpgradeDatabaseSchema)?;
    tx.as_mut_conn()
        .execute_batch(stmts)
        .await
        .map_err(DbError::FailedToCreateOrUpgradeDatabaseSchema)?;
    tx.commit()
        .await
        .map_err(DbError::FailedToCreateOrUpgradeDatabaseSchema)?;
    Ok(())
}

/// True if a database error reports a missing `tabsdata_system` table for any
/// backend enabled at compile time. Each arm is gated on its backend feature so
/// the check stays tight to what's actually linked in.
pub(crate) fn is_undefined_table_error(code: Option<&str>, message: &str) -> bool {
    #[cfg(postgres)]
    if matches!(code, Some("42P01")) {
        return true;
    }
    #[cfg(mysql)]
    if matches!(code, Some("42S02")) {
        return true;
    }
    #[cfg(sqlite)]
    if message.contains("no such table") {
        return true;
    }
    let _ = (code, message);
    false
}

#[cfg(test)]
mod tests {
    use super::is_undefined_table_error;

    #[cfg(postgres)]
    #[test]
    fn undefined_table_error_pg_sqlstate() {
        assert!(is_undefined_table_error(Some("42P01"), ""));
        assert!(!is_undefined_table_error(Some("23505"), ""));
    }

    #[cfg(mysql)]
    #[test]
    fn undefined_table_error_mysql_sqlstate() {
        assert!(is_undefined_table_error(Some("42S02"), ""));
        assert!(!is_undefined_table_error(Some("HY000"), ""));
    }

    #[cfg(sqlite)]
    #[test]
    fn undefined_table_error_sqlite_message() {
        assert!(is_undefined_table_error(None, "no such table: foo"));
        assert!(!is_undefined_table_error(None, "syntax error near 'x'"));
    }
}
