//
// Copyright 2026 Tabsdata Inc.
//

//! Privileged provisioning DDL — session/DDL statements sea_query does not
//! model — run on [`super::AdminPool`].

use crate::dynamic::DynamicPool;
use crate::error::DbError;

/// Transaction-local GUC the owner password is staged in (see
/// [`execute_batch_tx_with_secrets`]): the batch reads it back with
/// `current_setting` and quotes it with `format(%L)`, so the password never
/// appears in any SQL text. Running the batch without staging it fails loudly
/// (`current_setting` on an unset GUC errors).
#[cfg(postgres)]
pub(super) const OWNER_PASSWORD_GUC: &str = "td.owner_password";

/// Create the owner role, or converge the password of an existing one we own.
/// The password comes from [`OWNER_PASSWORD_GUC`], never from the SQL text.
///
/// The self-grant happens in the same transaction that creates the role: it
/// provides the `SET ROLE` that `CREATE SCHEMA ... AUTHORIZATION` needs
/// (PostgreSQL 16 split `SET` out of role `ADMIN OPTION`) and doubles as the
/// "we created this" marker gating convergence — nothing grants it on the
/// duplicate path, so a foreign role never becomes convergeable, no matter
/// how often a misconfigured provision is re-run. Superusers skip both (the
/// marker would be meaningless: `pg_has_role` is always true for them, and
/// pre-16 `CREATEROLE` can alter any non-superuser role) and keep the old
/// leave-existing-roles-untouched behavior — warned, so a drifted credential
/// is visible at create time instead of as a later serving connect failure.
/// A non-superuser hitting a role
/// that is not its own fails the batch here — `CREATE SCHEMA IF NOT EXISTS`
/// would silently no-op on an already-provisioned schema, hiding the
/// collision until serving fails to authenticate. Adopting a pre-created role
/// is an explicit operator step: `GRANT <owner> TO <admin>`.
#[cfg(postgres)]
pub(super) fn create_owner_role_sql(owner: &str) -> String {
    // Race-safe idempotent create: concurrent provisions can both pass an
    // `IF NOT EXISTS` check and one then hits the `pg_authid` unique index.
    format!(
        "DO $$ BEGIN \
           EXECUTE format('CREATE ROLE %I LOGIN PASSWORD %L', '{owner}', current_setting('{OWNER_PASSWORD_GUC}')); \
           IF NOT (SELECT rolsuper FROM pg_roles WHERE rolname = current_user) THEN \
             EXECUTE format('GRANT %I TO CURRENT_USER', '{owner}'); \
           END IF; \
         EXCEPTION WHEN duplicate_object OR unique_violation THEN \
           IF (SELECT rolsuper FROM pg_roles WHERE rolname = current_user) THEN \
             RAISE WARNING 'role % already exists; leaving it untouched (its password is not converged)', '{owner}'; \
           ELSIF pg_has_role(current_user, '{owner}', 'MEMBER') THEN \
             BEGIN \
               EXECUTE format('ALTER ROLE %I WITH PASSWORD %L', '{owner}', current_setting('{OWNER_PASSWORD_GUC}')); \
             EXCEPTION WHEN insufficient_privilege THEN \
               RAISE WARNING 'role % already exists and is not alterable by %: %; continuing with its existing password', '{owner}', current_user, SQLERRM; \
             END; \
           ELSE \
             RAISE EXCEPTION 'role % already exists and was not provisioned by %; to adopt a pre-created role, GRANT it to this admin first', '{owner}', current_user; \
           END IF; \
         END $$"
    )
}

#[cfg(postgres)]
pub(super) fn create_schema_sql(schema: &str, owner: &str) -> String {
    format!("CREATE SCHEMA IF NOT EXISTS \"{schema}\" AUTHORIZATION \"{owner}\"")
}

/// Create the row-level-security app role, `NOBYPASSRLS`, if it does not
/// exist. Provisioning owns this: creating a cluster-global role must be a
/// deliberate `db create` step, not a connect side effect — on a shared
/// cluster the first creator holds `ADMIN` on it and everyone else does not.
#[cfg(postgres)]
pub(super) fn create_app_role_sql(app_role: &str) -> String {
    // Guard the create with an existence check so the common case (role
    // already present) is a lock-free read of `pg_roles`. Only the first,
    // racing creators write `pg_authid`; the inner handler swallows the
    // lost-race duplicate (`unique_violation`) or an already-existing role
    // (`duplicate_object`) — concurrent test provisions race on this.
    format!(
        "DO $$ BEGIN \
           IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '{app_role}') THEN \
             BEGIN CREATE ROLE \"{app_role}\" NOBYPASSRLS; \
             EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL; END; \
           END IF; \
         END $$"
    )
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

/// [`grant_app_role_membership_sql`] for the connected role itself (the
/// upgrade path runs as the serving login; test schemas have no owner role).
/// The guard probes the capability (`SET LOCAL ROLE`), not membership —
/// PostgreSQL 16 records the creator's implicit grant `WITH SET FALSE`, which
/// a membership check would wrongly accept. Superusers always pass the probe.
/// A failed repair is warned, not fatal — the connected role may lack `ADMIN`
/// on an app role a DBA wired externally, and `db upgrade` must not regress
/// on shapes that served fine; the serving probe reports the real problem if
/// `SET` is genuinely required and absent.
#[cfg(postgres)]
pub(super) fn grant_app_role_self_membership_sql(app_role: &str) -> String {
    format!(
        "DO $$ BEGIN \
           BEGIN \
             EXECUTE format('SET LOCAL ROLE %I', '{app_role}'); \
             RESET ROLE; \
           EXCEPTION WHEN insufficient_privilege THEN \
             BEGIN \
               EXECUTE format('GRANT %I TO CURRENT_USER', '{app_role}'); \
             EXCEPTION WHEN insufficient_privilege THEN \
               RAISE WARNING 'cannot acquire SET on %: %; serving connects will fail if the switch is required', '{app_role}', SQLERRM; \
             END; \
           END; \
         END $$"
    )
}

/// Drop the app role at `db drop`, tolerantly: it is instance-scoped under
/// derived naming, so leaving it would wedge a later re-provision of the same
/// database name under a different admin (no `ADMIN` on the leftover role).
/// `DROP OWNED` clears its grants in this database first; a role that cannot
/// be dropped (shared, or still referenced elsewhere) is warned and kept —
/// teardown must not block on it.
#[cfg(postgres)]
pub(super) fn drop_app_role_sql(app_role: &str) -> String {
    format!(
        "DO $$ BEGIN \
           IF EXISTS (SELECT FROM pg_roles WHERE rolname = '{app_role}') THEN \
             BEGIN \
               IF NOT (SELECT rolsuper FROM pg_roles WHERE rolname = current_user) THEN \
                 BEGIN \
                   EXECUTE format('GRANT %I TO CURRENT_USER', '{app_role}'); \
                 EXCEPTION WHEN insufficient_privilege THEN NULL; END; \
               END IF; \
               EXECUTE format('DROP OWNED BY %I', '{app_role}'); \
               EXECUTE format('DROP ROLE %I', '{app_role}'); \
             EXCEPTION WHEN OTHERS THEN \
               RAISE WARNING 'could not drop app role %: %; leaving it', '{app_role}', SQLERRM; \
             END; \
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

/// Let the app role `ANALYZE` the schema's tables. Test-only: the test factories
/// keep the schema superuser-owned, so without `MAINTAIN` the app role's
/// `ANALYZE` is skipped with a warning and statistics never update. `MAINTAIN`
/// is PostgreSQL 16+, and 15 is still a supported server.
#[cfg(all(postgres, any(test, feature = "test-utils")))]
pub(super) fn grant_app_role_maintain_sql(schema: &str, app_role: &str) -> String {
    format!(
        "DO $$ BEGIN \
           IF current_setting('server_version_num')::int >= 160000 THEN \
             EXECUTE format('GRANT MAINTAIN ON ALL TABLES IN SCHEMA %I TO %I', '{schema}', '{app_role}'); \
           END IF; \
         END $$"
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
    execute_batch_tx_with_secrets(pool, &[], stmts).await
}

/// [`execute_batch_tx`], first staging each secret as a transaction-local GUC
/// through a bound parameter — the statements read them back with
/// `current_setting`, keeping the values out of every SQL text.
pub(super) async fn execute_batch_tx_with_secrets(
    pool: &DynamicPool,
    secrets: &[(&str, &str)],
    stmts: &[String],
) -> Result<(), DbError> {
    let mut tx = pool
        .begin()
        .await
        .map_err(DbError::FailedToCreateOrUpgradeDatabaseSchema)?;
    for (name, value) in secrets {
        tx.as_mut_conn()
            .set_local_secret(name, value)
            .await
            .map_err(DbError::FailedToCreateOrUpgradeDatabaseSchema)?;
    }
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
