//
// Copyright 2026 Tabsdata Inc.
//

//! Privileged provisioning DDL — session/DDL statements sea_query does not
//! model — run on [`super::AdminPool`].
use crate::connection::PgConnectionExt;
use crate::error::DbError;
use sqlx::PgPool;
use td_schema::ACROSS_PROJECTS_ROLE;

/// Transaction-local GUC the owner password is staged in (see
/// [`execute_batch_tx_with_secrets`]): the batch reads it back with
/// `current_setting` and quotes it with `format(%L)`, so the password never
/// appears in any SQL text. Running the batch without staging it fails loudly
/// (`current_setting` on an unset GUC errors).
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

pub(super) fn create_schema_sql(schema: &str, owner: &str) -> String {
    format!("CREATE SCHEMA IF NOT EXISTS \"{schema}\" AUTHORIZATION \"{owner}\"")
}

/// The cluster-global marker role the `across_projects` policy is granted to.
/// It holds no privileges of its own, so instances sharing a cluster share it
/// safely; reach comes from the per-deployment role granted into it.
///
/// Never dropped: `DROP OWNED BY` on it strips the `across_projects` policy
/// from every schema that names it, disarming co-tenant deployments.
pub(super) fn create_across_projects_marker_sql() -> String {
    format!(
        "DO $$ BEGIN \
           IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '{ACROSS_PROJECTS_ROLE}') THEN \
             BEGIN CREATE ROLE \"{ACROSS_PROJECTS_ROLE}\" NOBYPASSRLS NOLOGIN; \
             EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL; END; \
           END IF; \
         END $$"
    )
}

/// The membership that makes the `across_projects` policy apply to a role.
/// Skipped when it already holds.
///
/// Warned rather than fatal when the connected role lacks `ADMIN` on the
/// marker: on a shared cluster only its creator holds that, so a second
/// deployment's non-superuser provisioner cannot grant it and a DBA has to
/// (`GRANT td_across_projects TO <role>`). Failing here would block every
/// upgrade; the preflight reports the missing membership instead.
pub(super) fn grant_across_projects_membership_sql(role: &str) -> String {
    format!(
        "DO $$ BEGIN \
           IF NOT pg_has_role('{role}', '{ACROSS_PROJECTS_ROLE}', 'MEMBER') THEN \
             BEGIN \
               EXECUTE format('GRANT %I TO %I', '{ACROSS_PROJECTS_ROLE}', '{role}'); \
             EXCEPTION WHEN insufficient_privilege THEN \
               RAISE WARNING 'cannot grant % to %: %; cross-project work on this deployment will see no rows until a DBA grants it', '{ACROSS_PROJECTS_ROLE}', '{role}', SQLERRM; \
             END; \
           END IF; \
         END $$"
    )
}

/// [`grant_across_projects_membership_sql`] for the connected role — the
/// schema owner an upgrade runs as, which `FORCE ROW LEVEL SECURITY` binds
/// like anyone else and whose data deltas must reach every project. Warned
/// rather than fatal: an externally wired role may not be grantable here, and
/// the ladder reports the real problem if it cannot see its rows.
pub(super) fn grant_across_projects_self_membership_sql() -> String {
    format!(
        "DO $$ BEGIN \
           IF NOT pg_has_role(current_user, '{ACROSS_PROJECTS_ROLE}', 'MEMBER') THEN \
             BEGIN \
               EXECUTE format('GRANT %I TO CURRENT_USER', '{ACROSS_PROJECTS_ROLE}'); \
             EXCEPTION WHEN insufficient_privilege THEN \
               RAISE WARNING 'cannot grant % to %: %; a data delta over a project-scoped table will see no rows', '{ACROSS_PROJECTS_ROLE}', current_user, SQLERRM; \
             END; \
           END IF; \
         END $$"
    )
}

/// Create a row-level-security role, `NOBYPASSRLS`, if it does not
/// exist. Only `db create` and `db upgrade` run this (the latter to self-heal
/// a dropped role), never a serving connect: creating a cluster-global role
/// is a management step — on a shared cluster the first creator holds `ADMIN`
/// on it and everyone else does not.
pub(super) fn create_rls_role_sql(role: &str) -> String {
    // Guard the create with an existence check so the common case (role
    // already present) is a lock-free read of `pg_roles`. Only the first,
    // racing creators write `pg_authid`; the inner handler swallows the
    // lost-race duplicate (`unique_violation`) or an already-existing role
    // (`duplicate_object`) — concurrent test provisions race on this.
    format!(
        "DO $$ BEGIN \
           IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '{role}') THEN \
             BEGIN CREATE ROLE \"{role}\" NOBYPASSRLS; \
             EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL; END; \
           END IF; \
         END $$"
    )
}

/// Skipped when the membership already holds: granting a role requires `ADMIN`
/// on it (PostgreSQL 16+), which a deployment sharing a cluster-global RLS role
/// it did not create never has. Re-provisioning such a database then works
/// without the grant, instead of failing on a privilege it does not need.
///
/// The grant inherits (PostgreSQL's default): the owner runs upgrades, which
/// `FORCE ROW LEVEL SECURITY` binds, so it must carry the cross-project
/// role's privileges unswitched. Serving connections `SET ROLE` at session
/// level and policies key on `current_user`, so the inheritance never reaches
/// a tenant transaction.
pub(super) fn grant_rls_role_membership_sql(owner: &str, role: &str) -> String {
    format!(
        "DO $$ BEGIN \
           IF NOT pg_has_role('{owner}', '{role}', 'MEMBER') THEN \
             EXECUTE format('GRANT %I TO %I', '{role}', '{owner}'); \
           END IF; \
         END $$"
    )
}

/// [`grant_rls_role_membership_sql`] for the connected role itself (the
/// upgrade path runs as the serving login; test schemas have no owner role).
/// The guard probes the capability (`SET LOCAL ROLE`), not membership —
/// PostgreSQL 16 records the creator's implicit grant `WITH SET FALSE`, which
/// a membership check would wrongly accept. Superusers always pass the probe.
/// A failed repair is warned, not fatal — the connected role may lack `ADMIN`
/// on an RLS role a DBA wired externally, and `db upgrade` must not regress
/// on shapes that served fine; the serving probe reports the real problem if
/// `SET` is genuinely required and absent.
pub(super) fn grant_rls_role_self_membership_sql(role: &str) -> String {
    format!(
        "DO $$ BEGIN \
           BEGIN \
             EXECUTE format('SET LOCAL ROLE %I', '{role}'); \
             RESET ROLE; \
           EXCEPTION WHEN insufficient_privilege THEN \
             BEGIN \
               EXECUTE format('GRANT %I TO CURRENT_USER', '{role}'); \
             EXCEPTION WHEN insufficient_privilege THEN \
               RAISE WARNING 'cannot acquire SET on %: %; serving connects will fail if the switch is required', '{role}', SQLERRM; \
             END; \
           END; \
         END $$"
    )
}

/// Drop a per-deployment RLS role at `db drop`, tolerantly: it is instance-scoped under
/// derived naming, so leaving it would wedge a later re-provision of the same
/// database name under a different admin (no `ADMIN` on the leftover role).
/// `DROP OWNED` clears its grants in this database first; a role that cannot
/// be dropped (shared, or still referenced elsewhere) is warned and kept —
/// teardown must not block on it.
pub(super) fn drop_rls_role_sql(role: &str) -> String {
    format!(
        "DO $$ BEGIN \
           IF EXISTS (SELECT FROM pg_roles WHERE rolname = '{role}') THEN \
             BEGIN \
               IF NOT (SELECT rolsuper FROM pg_roles WHERE rolname = current_user) THEN \
                 BEGIN \
                   EXECUTE format('GRANT %I TO CURRENT_USER', '{role}'); \
                 EXCEPTION WHEN insufficient_privilege THEN NULL; END; \
               END IF; \
               EXECUTE format('DROP OWNED BY %I', '{role}'); \
               EXECUTE format('DROP ROLE %I', '{role}'); \
             EXCEPTION WHEN OTHERS THEN \
               RAISE WARNING 'could not drop RLS role %: %; leaving it', '{role}', SQLERRM; \
             END; \
           END IF; \
         END $$"
    )
}

pub(super) fn grant_rls_role_tables_sql(schema: &str, role: &str) -> String {
    format!(
        "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA \"{schema}\" TO \"{role}\""
    )
}

/// Let an RLS role `ANALYZE` the schema's tables. Test-only: the test factories
/// keep the schema superuser-owned, so without `MAINTAIN` the role's
/// `ANALYZE` is skipped with a warning and statistics never update. `MAINTAIN`
/// is PostgreSQL 16+, and 15 is still a supported server.
#[cfg(any(test, feature = "test-utils"))]
pub(super) fn grant_rls_role_maintain_sql(schema: &str, role: &str) -> String {
    format!(
        "DO $$ BEGIN \
           IF current_setting('server_version_num')::int >= 160000 THEN \
             EXECUTE format('GRANT MAINTAIN ON ALL TABLES IN SCHEMA %I TO %I', '{schema}', '{role}'); \
           END IF; \
         END $$"
    )
}

pub(super) fn grant_rls_role_privileges_sql(schema: &str, role: &str) -> [String; 3] {
    [
        format!("GRANT USAGE ON SCHEMA \"{schema}\" TO \"{role}\""),
        grant_rls_role_tables_sql(schema, role),
        format!(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA \"{schema}\" \
             GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO \"{role}\""
        ),
    ]
}

/// Run `stmts` as a single committed transaction on `pool`, mapping failures to
/// [`DbError::FailedToCreateOrUpgradeDatabaseSchema`].
pub(super) async fn execute_batch_tx(pool: &PgPool, stmts: &[String]) -> Result<(), DbError> {
    execute_batch_tx_with_secrets(pool, &[], stmts).await
}

/// [`execute_batch_tx`], first staging each secret as a transaction-local GUC
/// through a bound parameter — the statements read them back with
/// `current_setting`, keeping the values out of every SQL text.
pub(super) async fn execute_batch_tx_with_secrets(
    pool: &PgPool,
    secrets: &[(&str, &str)],
    stmts: &[String],
) -> Result<(), DbError> {
    let mut trx = pool
        .begin()
        .await
        .map_err(DbError::FailedToCreateOrUpgradeDatabaseSchema)?;
    for (name, value) in secrets {
        trx.set_local_secret(name, value)
            .await
            .map_err(DbError::FailedToCreateOrUpgradeDatabaseSchema)?;
    }
    trx.execute_batch(stmts)
        .await
        .map_err(DbError::FailedToCreateOrUpgradeDatabaseSchema)?;
    trx.commit()
        .await
        .map_err(DbError::FailedToCreateOrUpgradeDatabaseSchema)?;
    Ok(())
}

/// True if a database error reports a missing table (`undefined_table`).
pub fn is_undefined_table_error(code: Option<&str>) -> bool {
    matches!(code, Some("42P01"))
}

#[cfg(test)]
mod tests {
    use super::is_undefined_table_error;

    #[test]
    fn undefined_table_error_pg_sqlstate() {
        assert!(is_undefined_table_error(Some("42P01")));
        assert!(!is_undefined_table_error(Some("23505")));
        assert!(!is_undefined_table_error(None));
    }
}
