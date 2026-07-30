//
// Copyright 2026 Tabsdata Inc.
//

//! Privileged provisioning DDL — session/DDL statements sea_query does not
//! model — run on [`super::AdminPool`].

use crate::dynamic::DynamicPool;
use crate::error::DbError;

#[cfg(postgres)]
pub(super) fn create_owner_role_sql(owner: &str, password: &str) -> String {
    // Race-safe idempotent create: concurrent provisions can both pass an
    // `IF NOT EXISTS` check and one then hits the `pg_authid` unique index.
    format!(
        "DO $$ BEGIN CREATE ROLE \"{owner}\" LOGIN PASSWORD '{password}'; \
         EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL; END $$"
    )
}

/// `CREATE SCHEMA ... AUTHORIZATION` requires the connected role to be able to
/// `SET ROLE` to the owner. A superuser always can, so the embedded server never
/// needed this; a `CREATEROLE` admin does not get it on the roles it creates,
/// because PostgreSQL 16 split `SET` out of `ADMIN OPTION`. Take the membership
/// explicitly, skipping superusers so their provisioning is unchanged. Failure
/// is not fatal here: the admin may already hold `SET` by an external grant
/// without the `ADMIN` needed to re-grant it, and `CREATE SCHEMA` reports the
/// real problem when it genuinely cannot proceed.
#[cfg(postgres)]
pub(super) fn grant_owner_set_role_sql(owner: &str) -> String {
    format!(
        "DO $$ BEGIN \
           IF NOT (SELECT rolsuper FROM pg_roles WHERE rolname = current_user) THEN \
             EXECUTE format('GRANT %I TO CURRENT_USER', '{owner}'); \
           END IF; \
         EXCEPTION WHEN insufficient_privilege THEN NULL; END $$"
    )
}

#[cfg(postgres)]
pub(super) fn create_schema_sql(schema: &str, owner: &str) -> String {
    format!("CREATE SCHEMA IF NOT EXISTS \"{schema}\" AUTHORIZATION \"{owner}\"")
}

/// Skipped when the membership already holds: granting a role requires `ADMIN`
/// on it (PostgreSQL 16+), which a deployment sharing a cluster-global app role
/// it did not create never has. Re-provisioning such a database then works
/// without the grant, instead of failing on a privilege it does not need.
#[cfg(postgres)]
pub(super) fn grant_app_role_membership_sql(owner: &str, app_role: &str) -> String {
    format!(
        "DO $$ BEGIN \
           IF NOT pg_has_role('{owner}', '{app_role}', 'MEMBER') THEN \
             EXECUTE format('GRANT %I TO %I', '{app_role}', '{owner}'); \
           END IF; \
         END $$"
    )
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
/// backend enabled at compile time.
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
