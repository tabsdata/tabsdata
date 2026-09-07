//
// Copyright 2026 Tabsdata Inc.
//

//! [`AdminPool`] — the privileged admin/upgrader pool used by the `components` module.

use crate::components::ProvisionSpec;
use crate::components::sql::execute_batch_tx;
#[cfg(any(test, feature = "test-utils"))]
use crate::components::sql::grant_rls_role_maintain_sql;
use crate::components::sql::{
    OWNER_PASSWORD_GUC, create_across_projects_marker_sql, create_owner_role_sql,
    create_rls_role_sql, create_schema_sql, drop_rls_role_sql, execute_batch_tx_with_secrets,
    grant_across_projects_membership_sql, grant_across_projects_self_membership_sql,
    grant_rls_role_membership_sql, grant_rls_role_self_membership_sql,
};
use crate::config::{DatabaseConfig, RlsRoles, Schema};
use crate::error::DbError;
use crate::pool::state::db_version;
use crate::sea_query::builder::SeaQueryExt;
use sea_query::{Expr, Query};
use sqlx::PgPool;
use td_schema::ACROSS_PROJECTS_ROLE;
use tracing::{debug, info, instrument};

/// The RLS roles a component's serving connections switch between, the
/// marker the `across_projects` policy names, and the owner's memberships in
/// all three. Every statement is a no-op when its object already holds.
fn rls_role_statements(owner: &str, roles: &RlsRoles) -> Vec<String> {
    let (tenant, across) = (&roles.in_project, &roles.across_projects);
    vec![
        create_across_projects_marker_sql(),
        create_rls_role_sql(tenant),
        create_rls_role_sql(across),
        grant_across_projects_membership_sql(across),
        grant_across_projects_membership_sql(owner),
        grant_rls_role_membership_sql(owner, tenant),
        grant_rls_role_membership_sql(owner, across),
    ]
}

/// The privileged pool used to provision, upgrade and check schemas. On
/// It keeps the DDL rights the serving `TdPool` gives up: the
/// superuser variant runs `CREATE SCHEMA`/`CREATE ROLE`; the per-component
/// variant connects as the schema owner. Short-lived around a management op.
#[derive(Debug, Clone)]
pub struct AdminPool {
    pub(super) pool: PgPool,
    // Stored (not passed) to stay in lock-step with the connection's
    // `search_path`. `None` on the privileged system entry, which has none.
    schema: Option<Schema>,
    // `None` on the privileged system entry, which never serves.
    roles: Option<RlsRoles>,
}

impl AdminPool {
    /// Connect as a component's **admin/upgrader** role: a raw-role pool with
    /// DDL rights on its (already existing) schema.
    #[instrument(name = "db.admin_pool.connect", skip_all, level = "info", err(Debug))]
    pub async fn connect(config: &DatabaseConfig) -> Result<Self, DbError> {
        info!("db.admin_pool.connect: opening admin pool");
        let roles = config.roles.clone();
        let out = config.connect_admin().await?;
        debug!("Connected to admin pool: {:#?}", out.pool);
        Ok(Self::from_parts(out.pool, out.schema, roles))
    }

    /// Build from an already-opened admin connection.
    pub(crate) fn from_parts(
        pool: PgPool,
        schema: Option<Schema>,
        roles: Option<RlsRoles>,
    ) -> Self {
        Self {
            pool,
            schema,
            roles,
        }
    }

    /// The RLS roles this pool provisions and grants. Panics on the
    /// privileged system entry, which has none.
    pub(super) fn roles(&self) -> &RlsRoles {
        self.roles
            .as_ref()
            .expect("No RLS roles configured: the privileged system entry has none")
    }

    /// The schema this pool's `search_path` is pinned to. Panics on the
    /// privileged system entry, which has no home schema.
    pub(crate) fn schema(&self) -> &Schema {
        self.schema
            .as_ref()
            .expect("No schema configured: the privileged system entry has no home schema")
    }

    /// Provision a component: the login-capable owner role, the schema it
    /// owns, the entry's two RLS roles, and the owner's membership in both.
    /// Idempotent. Table grants come later
    /// ([`UpgradeTransaction::grant_rls_role_privileges`]), once tables exist.
    /// The roles come from the spec — the component entry's own — not from
    /// this (privileged, server-entry) pool's config.
    pub(crate) async fn provision_schema(&self, spec: &ProvisionSpec<'_>) -> Result<(), DbError> {
        {
            let (tenant, across) = (&spec.roles.in_project, &spec.roles.across_projects);
            debug!(
                owner = spec.owner,
                schema = spec.schema,
                in_project = tenant,
                across_projects = across,
                "Provisioning schema, owner and RLS roles"
            );
            let mut stmts = vec![
                create_owner_role_sql(spec.owner),
                create_schema_sql(spec.schema, spec.owner),
            ];
            stmts.extend(rls_role_statements(spec.owner, spec.roles));
            execute_batch_tx_with_secrets(
                &self.pool,
                &[(OWNER_PASSWORD_GUC, spec.password)],
                &stmts,
            )
            .await?;
            info!(
                owner = spec.owner,
                schema = spec.schema,
                in_project = tenant,
                across_projects = across,
                "Provisioned schema, owner and RLS roles"
            );
        }
        Ok(())
    }

    /// The cluster-global half of a component's provisioning, on its own: the
    /// RLS roles, the marker, and the owner's memberships, all idempotent.
    /// `db upgrade` runs it on the privileged server entry before the owner
    /// opens its ladder, so a schema version that introduces a role (v4's
    /// `across_projects`) never asks the owner for a `CREATE ROLE` it cannot
    /// run. Cluster-global, so it stays outside the upgrade transaction:
    /// holding a `pg_authid` lock for a whole upgrade would block every
    /// concurrent provision of the same role.
    pub(crate) async fn ensure_rls_roles_for(
        &self,
        owner: &str,
        roles: &RlsRoles,
    ) -> Result<(), DbError> {
        execute_batch_tx(&self.pool, &rls_role_statements(owner, roles)).await?;
        debug!(
            owner,
            in_project = roles.in_project,
            across_projects = roles.across_projects,
            "Ensured RLS roles and owner memberships"
        );
        Ok(())
    }

    /// Create a schema owned by the connected (superuser) role — the test
    /// factories' path, free of any cluster-global `CREATE ROLE` so tests
    /// provision in parallel. Idempotent.
    pub(crate) async fn create_schema(&self, schema: &str) -> Result<(), DbError> {
        execute_batch_tx(
            &self.pool,
            &[format!("CREATE SCHEMA IF NOT EXISTS \"{schema}\"")],
        )
        .await
    }

    /// Drop a schema and everything in it, leaving roles untouched.
    pub(crate) async fn drop_schema_cascade(&self, schema: &str) -> Result<(), DbError> {
        execute_batch_tx(
            &self.pool,
            &[format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE")],
        )
        .await
    }

    /// Drop a component's schema and its owner role. Destructive; the caller
    /// guards.
    pub(crate) async fn drop_schema(&self, schema: &str, owner: &str) -> Result<(), DbError> {
        let stmts = vec![
            format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"),
            format!("DROP ROLE IF EXISTS \"{owner}\""),
        ];
        execute_batch_tx(&self.pool, &stmts).await?;
        debug!(owner, schema, "Dropped schema and owner");
        Ok(())
    }

    /// Drop an instance-scoped RLS role at `db drop` (tolerant — see
    /// [`drop_rls_role_sql`]). Runs after every component's schema is gone.
    /// Never the `across_projects` marker: it is cluster-global and named by
    /// every deployment's policies.
    pub async fn drop_rls_role(&self, role: &str) -> Result<(), DbError> {
        {
            execute_batch_tx(&self.pool, &[drop_rls_role_sql(role)]).await?;
            debug!(role, "Dropped RLS role");
        }
        Ok(())
    }

    pub async fn schema_exists(&self, schema: &str) -> Result<bool, DbError> {
        self.object_exists("pg_namespace", "nspname", schema).await
    }

    pub async fn role_exists(&self, role: &str) -> Result<bool, DbError> {
        self.object_exists("pg_roles", "rolname", role).await
    }

    async fn object_exists(
        &self,
        catalog: &str,
        column: &str,
        name: &str,
    ) -> Result<bool, DbError> {
        let found: Option<bool> = Query::select()
            .expr(Expr::cust_with_values(
                format!("EXISTS (SELECT FROM {catalog} WHERE {column} = $1)"),
                [name],
            ))
            .build_query_scalar()
            .fetch_optional(&self.pool)
            .await
            .map_err(DbError::SqlError)?;
        Ok(found.unwrap_or(false))
    }

    /// Refuse a PostgreSQL server older than 15: the schema emits
    /// `security_invoker` views (15+); without them views would evaluate as
    /// their superuser owner and bypass project isolation.
    pub(crate) async fn check_pg_min_version(&self) -> Result<(), DbError> {
        crate::pool::state::check_pg_min_version(&self.pool).await
    }

    /// Refuse a server whose `max_locks_per_transaction` cannot hold the
    /// per-row locks a bulk write into a versioned table takes.
    pub(crate) async fn check_pg_lock_capacity(&self) -> Result<(), DbError> {
        crate::pool::state::check_pg_lock_capacity(&self.pool).await
    }

    /// Verify the upgrader role has CREATE on the application schema — caught
    /// up front instead of failing mid-upgrade with an opaque error.
    pub(crate) async fn check_upgrader_permissions(&self) -> Result<(), DbError> {
        let schema = self.schema();
        let can_create: Option<bool> = Query::select()
            .expr(Expr::cust(format!(
                "has_schema_privilege(current_user, '{schema}', 'CREATE')"
            )))
            .build_query_scalar()
            .fetch_optional(&self.pool)
            .await
            .map_err(DbError::SqlError)?;
        if !matches!(can_create, Some(true)) {
            return Err(DbError::UpgraderLacksPermissions(format!(
                "CREATE on schema {schema}"
            )));
        }
        debug!("db.pool.check: upgrader permissions OK");
        Ok(())
    }

    /// The roles and `across_projects` memberships the schema's policies rely
    /// on, before the ladder opens. `db upgrade` ensures them on the
    /// privileged entry first ([`Self::ensure_rls_roles_for`]), but a
    /// non-superuser provisioner holds no `ADMIN` on a marker somebody else
    /// created, so that grant is warned rather than fatal — and without it
    /// cross-project reads return nothing and a data delta rewrites no rows.
    /// The error carries the statements a DBA has to run.
    pub(crate) async fn check_across_projects_memberships(&self) -> Result<(), DbError> {
        let (tenant, across) = (&self.roles().in_project, &self.roles().across_projects);
        let exists = |role: &str| format!("EXISTS (SELECT FROM pg_roles WHERE rolname = '{role}')");
        let (marker, has_tenant, has_across) =
            (exists(ACROSS_PROJECTS_ROLE), exists(tenant), exists(across));
        let missing: Option<String> = Query::select()
            .expr(Expr::cust(format!(
                "trim(both ' ' from \
                   CASE WHEN NOT {has_tenant} \
                     THEN 'CREATE ROLE \"{tenant}\" NOBYPASSRLS; ' ELSE '' END \
                || CASE WHEN NOT {has_across} \
                     THEN 'CREATE ROLE \"{across}\" NOBYPASSRLS; ' ELSE '' END \
                || CASE WHEN NOT {marker} \
                     THEN 'CREATE ROLE \"{ACROSS_PROJECTS_ROLE}\" NOBYPASSRLS NOLOGIN; ' \
                     ELSE '' END \
                || CASE WHEN NOT ({marker} AND {has_across}) \
                        OR NOT pg_has_role('{across}', '{ACROSS_PROJECTS_ROLE}', 'USAGE') \
                     THEN 'GRANT \"{ACROSS_PROJECTS_ROLE}\" TO \"{across}\"; ' \
                     ELSE '' END \
                || CASE WHEN NOT {marker} \
                        OR NOT pg_has_role(current_user, '{ACROSS_PROJECTS_ROLE}', 'USAGE') \
                     THEN 'GRANT \"{ACROSS_PROJECTS_ROLE}\" TO CURRENT_USER; ' \
                     ELSE '' END)"
            )))
            .build_query_scalar()
            .fetch_optional(&self.pool)
            .await
            .map_err(DbError::SqlError)?;
        match missing.as_deref() {
            Some("") | None => {
                debug!("db.pool.check: across-projects memberships OK");
                Ok(())
            }
            Some(sql) => Err(DbError::MissingAcrossProjectsMembership(sql.to_owned())),
        }
    }

    /// [`Self::ensure_rls_roles_for`] for the connected role's own entry —
    /// the test factories' and [`Self::upgrade_source`]'s path, where the
    /// connected role is the superuser and there is no owner to grant to;
    /// the memberships go to the connected role itself. Idempotent.
    pub async fn ensure_rls_roles(&self) -> Result<(), DbError> {
        {
            let (tenant, across) = (&self.roles().in_project, &self.roles().across_projects);
            let stmts = vec![
                create_across_projects_marker_sql(),
                create_rls_role_sql(tenant),
                create_rls_role_sql(across),
                grant_across_projects_membership_sql(across),
                grant_across_projects_self_membership_sql(),
                grant_rls_role_self_membership_sql(tenant),
                grant_rls_role_self_membership_sql(across),
            ];
            execute_batch_tx(&self.pool, &stmts).await?;
            debug!(
                in_project = tenant,
                across_projects = across,
                "Ensured RLS roles"
            );
        }
        Ok(())
    }

    /// Grant both RLS roles USAGE on a bare test schema, before any serving
    /// pool connects. A role whose `search_path` names a schema it cannot use
    /// resolves to no schema at all, so the session-level `SET ROLE` has to
    /// find the grant already in place.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn grant_rls_role_schema_usage(&self, schema: &str) -> Result<(), DbError> {
        let (tenant, across) = (&self.roles().in_project, &self.roles().across_projects);
        execute_batch_tx(
            &self.pool,
            &[
                format!("GRANT USAGE ON SCHEMA \"{schema}\" TO \"{tenant}\""),
                format!("GRANT USAGE ON SCHEMA \"{schema}\" TO \"{across}\""),
            ],
        )
        .await
    }

    /// Grant both RLS roles `MAINTAIN` on `schema`'s tables so their `ANALYZE`
    /// takes effect. Test-only — the test factories keep the schema
    /// superuser-owned, and the RLS roles own nothing. Runs after the tables
    /// exist, i.e. after [`Self::upgrade`].
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn grant_rls_role_maintain(&self, schema: &str) -> Result<(), DbError> {
        {
            execute_batch_tx(
                &self.pool,
                &[
                    grant_rls_role_maintain_sql(schema, &self.roles().in_project),
                    grant_rls_role_maintain_sql(schema, &self.roles().across_projects),
                ],
            )
            .await?;
            debug!(schema, "Granted RLS roles MAINTAIN");
        }
        Ok(())
    }

    /// The recorded `db_version`, or `None` when the schema does not exist.
    pub async fn db_version(&self) -> Result<Option<usize>, DbError> {
        db_version(&self.pool).await
    }

    /// Close the pool, waiting for all connections to be returned and closed.
    pub async fn close(&self) {
        self.pool.close().await;
    }
}
